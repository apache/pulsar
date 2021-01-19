/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.metadata.coordination.impl;

import com.fasterxml.jackson.databind.type.TypeFactory;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.cache.impl.JSONMetadataSerdeSimpleType;
import org.apache.pulsar.metadata.cache.impl.MetadataSerde;

@Slf4j
class LeaderElectionImpl<T> implements LeaderElection<T>, Consumer<Notification> {
    private final String path;
    private final MetadataSerde<T> serde;
    private final MetadataStoreExtended store;
    private final MetadataCache<T> cache;
    private final Consumer<LeaderElectionState> stateChangesListener;

    private LeaderElectionState leaderElectionState;
    private Optional<Long> version = Optional.empty();
    private Optional<T> proposedValue;

    private final ScheduledExecutorService executor;

    private static enum InternalState {
        Init, ElectionInProgress, LeaderIsPresent, Closed
    }

    private InternalState internalState;

    private static final int LEADER_ELECTION_RETRY_DELAY_SECONDS = 5;

    LeaderElectionImpl(MetadataStoreExtended store, Class<T> clazz, String path,
            Consumer<LeaderElectionState> stateChangesListener) {
        this.path = path;
        this.serde = new JSONMetadataSerdeSimpleType<>(TypeFactory.defaultInstance().constructSimpleType(clazz, null));
        this.store = store;
        this.cache = store.getMetadataCache(clazz);
        this.leaderElectionState = LeaderElectionState.NoLeader;
        this.internalState = InternalState.Init;
        this.stateChangesListener = stateChangesListener;
        this.executor = Executors.newScheduledThreadPool(0, new DefaultThreadFactory("leader-election-executor"));

        store.registerListener(this);
    }

    @Override
    public synchronized CompletableFuture<LeaderElectionState> elect(T proposedValue) {
        if (leaderElectionState != LeaderElectionState.NoLeader) {
            return CompletableFuture.completedFuture(leaderElectionState);
        }

        this.proposedValue = Optional.of(proposedValue);
        return elect();
    }

    private synchronized CompletableFuture<LeaderElectionState> elect() {
        // First check if there's already a leader elected
        internalState = InternalState.ElectionInProgress;
        return cache.get(path).thenCompose(optLock -> {
            if (optLock.isPresent()) {
                synchronized (LeaderElectionImpl.this) {
                    internalState = InternalState.LeaderIsPresent;
                    if (leaderElectionState != LeaderElectionState.Following) {
                        leaderElectionState = LeaderElectionState.Following;
                        try {
                            stateChangesListener.accept(leaderElectionState);
                        } catch (Throwable t) {
                            log.warn("Exception in state change listener", t);
                        }
                    }
                }
                return CompletableFuture.completedFuture(leaderElectionState);
            } else {
                return tryToBecomeLeader();
            }
        });
    }

    private synchronized CompletableFuture<LeaderElectionState> tryToBecomeLeader() {
        byte[] payload;
        try {
            payload = serde.serialize(proposedValue.get());
        } catch (Throwable t) {
            return FutureUtils.exception(t);
        }

        CompletableFuture<LeaderElectionState> result = new CompletableFuture<>();
        store.put(path, payload, Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral))
                .thenAccept(stat -> {
                    synchronized (LeaderElectionImpl.this) {
                        if (internalState == InternalState.ElectionInProgress) {
                            // Do a get() in order to force a notification later, if the z-node disappears
                            cache.get(path)
                                    .thenRun(() -> {
                                        synchronized (LeaderElectionImpl.this) {
                                            log.info("Acquired resource lock on {}", path);
                                            internalState = InternalState.LeaderIsPresent;
                                            if (leaderElectionState != LeaderElectionState.Leading) {
                                                leaderElectionState = LeaderElectionState.Leading;
                                                try {
                                                    stateChangesListener.accept(leaderElectionState);
                                                } catch (Throwable t) {
                                                    log.warn("Exception in state change listener", t);
                                                }
                                            }
                                        }
                                        result.complete(leaderElectionState);
                                    }).exceptionally(ex -> {
                                        // We fail to do the get(), so clean up the leader election fail the whole
                                        // operation
                                        store.delete(path, Optional.of(stat.getVersion()))
                                                .thenRun(() -> result.completeExceptionally(ex))
                                                .exceptionally(ex2 -> {
                                                    result.completeExceptionally(ex2);
                                                    return null;
                                                });
                                        return null;
                                    });
                        } else {
                            // LeaderElection was closed in between. Release the lock asynchronously
                            store.delete(path, Optional.of(stat.getVersion()))
                                    .thenRun(() -> result.completeExceptionally(
                                            new AlreadyClosedException("The leader election was already closed")))
                                    .exceptionally(ex -> {
                                        result.completeExceptionally(ex);
                                        return null;
                                    });
                        }
                    }
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof BadVersionException) {
                        // There was a conflict between 2 participants trying to become leaders at same time. Retry
                        // to fetch info on new leader.
                        elect()
                            .thenAccept(lse -> result.complete(lse))
                            .exceptionally(ex2 -> {
                                result.completeExceptionally(ex2);
                                return null;
                            });
                    } else {
                        result.completeExceptionally(ex.getCause());
                    }
                    return null;
                });

        return result;
    }

    @Override
    public void close() throws Exception {
        try {
            asyncClose().join();
        } catch (CompletionException e) {
            throw MetadataStoreException.unwrap(e);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> asyncClose() {
        if (internalState == InternalState.Closed) {
            return CompletableFuture.completedFuture(null);
        }

        internalState = InternalState.Closed;

        executor.shutdownNow();

        if (leaderElectionState != LeaderElectionState.Leading) {
            return CompletableFuture.completedFuture(null);
        }

        return store.delete(path, version);
    }

    @Override
    public synchronized LeaderElectionState getState() {
        return leaderElectionState;
    }

    @Override
    public CompletableFuture<Optional<T>> getLeaderValue() {
        return cache.get(path);
    }

    @Override
    public Optional<T> getLeaderValueIfPresent() {
        return cache.getIfCached(path);
    }

    @Override
    public void accept(Notification notification) {
        if (!path.equals(notification.getPath())) {
            // Ignore notifications we don't care about
            return;
        }

        synchronized (this) {
            if (internalState != InternalState.LeaderIsPresent) {
                // Ignoring notification since we're not trying to become leader
                return;
            }

            if (notification.getType() == NotificationType.Deleted) {
                if (leaderElectionState == LeaderElectionState.Leading) {
                    // We've lost the leadership, switch to follower mode
                    log.info("Leader released for {}", path);
                }

                leaderElectionState = LeaderElectionState.NoLeader;

                if (proposedValue.isPresent()) {
                    elect()
                            .exceptionally(ex -> {
                                log.warn("Leader election for path {} has failed", ex);
                                synchronized (LeaderElectionImpl.this) {
                                    try {
                                        stateChangesListener.accept(leaderElectionState);
                                    } catch (Throwable t) {
                                        log.warn("Exception in state change listener", t);
                                    }

                                    executor.schedule(() -> {
                                        log.info("Retrying Leader election for path {}");
                                        elect();
                                    }, LEADER_ELECTION_RETRY_DELAY_SECONDS, TimeUnit.SECONDS);
                                }
                                return null;
                            });
                }
            }
        }
    }
}
