/*
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
import com.google.common.annotations.VisibleForTesting;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.cache.impl.JSONMetadataSerdeSimpleType;

@Slf4j
class LeaderElectionImpl<T> implements LeaderElection<T> {
    private final String path;
    private final MetadataSerde<T> serde;
    private final MetadataStoreExtended store;
    private final MetadataCache<T> cache;
    private final Consumer<LeaderElectionState> stateChangesListener;
    private final ScheduledFuture<?> updateCachedValueFuture;

    private LeaderElectionState leaderElectionState;
    private Optional<Long> version = Optional.empty();
    private Optional<T> proposedValue;

    private final ScheduledExecutorService executor;
    private final FutureUtil.Sequencer<Void> sequencer;

    private enum InternalState {
        Init, ElectionInProgress, LeaderIsPresent, Closed
    }

    private InternalState internalState;

    private static final int LEADER_ELECTION_RETRY_DELAY_SECONDS = 5;

    LeaderElectionImpl(MetadataStoreExtended store, Class<T> clazz, String path,
            Consumer<LeaderElectionState> stateChangesListener,
                       ScheduledExecutorService executor) {
        this.path = path;
        this.serde = new JSONMetadataSerdeSimpleType<>(TypeFactory.defaultInstance().constructSimpleType(clazz, null));
        this.store = store;
        MetadataCacheConfig metadataCacheConfig = MetadataCacheConfig.builder()
                .expireAfterWriteMillis(-1L)
                .build();
        this.cache = store.getMetadataCache(clazz, metadataCacheConfig);
        this.leaderElectionState = LeaderElectionState.NoLeader;
        this.internalState = InternalState.Init;
        this.stateChangesListener = stateChangesListener;
        this.executor = executor;
        this.sequencer = FutureUtil.Sequencer.create();
        store.registerListener(this::handlePathNotification);
        store.registerSessionListener(this::handleSessionNotification);
        updateCachedValueFuture = executor.scheduleWithFixedDelay(this::getLeaderValue,
                metadataCacheConfig.getRefreshAfterWriteMillis() / 2,
                metadataCacheConfig.getRefreshAfterWriteMillis(), TimeUnit.MILLISECONDS);
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
        return store.get(path).thenCompose(optLock -> {
            if (optLock.isPresent()) {
                return handleExistingLeaderValue(optLock.get());
            } else {
                return tryToBecomeLeader();
            }
        }).thenCompose(leaderElectionState -> {
            // make sure that the cache contains the current leader
            // so that getLeaderValueIfPresent works on all brokers
            cache.refresh(path);
            return cache.get(path)
                    .thenApply(__ -> leaderElectionState);
        });
    }

    private synchronized CompletableFuture<LeaderElectionState> handleExistingLeaderValue(GetResult res) {
        T existingValue;
        try {
            existingValue = serde.deserialize(path, res.getValue(), res.getStat());
        } catch (Throwable t) {
            return FutureUtils.exception(t);
        }

        T value = proposedValue.orElse(null);
        if (existingValue.equals(value)) {
            // If the value is the same as our proposed value, it means this instance was the leader at some
            // point before. The existing value can either be for this same session or for a previous one.
            if (res.getStat().isCreatedBySelf()) {
                log.info("Keeping the existing value {} for {} as it's from the same session stat={}", existingValue,
                        path, res.getStat());
                // The value is still valid because it was created in the same session
                changeState(LeaderElectionState.Leading);
                return CompletableFuture.completedFuture(LeaderElectionState.Leading);
            } else {
                log.info("Conditionally deleting existing equals value {} for {} because it's not created in the "
                        + "current session. stat={}", existingValue, path, res.getStat());
                // Since the value was created in a different session, it might be expiring. We need to delete it
                // and try the election again.
                return store.delete(path, Optional.of(res.getStat().getVersion()))
                        .thenCompose(__ -> tryToBecomeLeader());
            }
        } else if (res.getStat().isCreatedBySelf()) {
            log.warn("Conditionally deleting existing value {} for {} because it's different from the proposed value "
                            + "({}). This is unexpected since it was created within the same session. "
                            + "In tests this could happen because of an invalid shared session id when using mocks.",
                    existingValue, path, value);
            // The existing value is different but was created from the same session
            return store.delete(path, Optional.of(res.getStat().getVersion()))
                    .thenCompose(__ -> tryToBecomeLeader());
        }

        // If the existing value is different, it means there's already another leader
        changeState(LeaderElectionState.Following);
        return CompletableFuture.completedFuture(LeaderElectionState.Following);
    }

    private synchronized void changeState(LeaderElectionState les) {
        internalState = InternalState.LeaderIsPresent;
        if (this.leaderElectionState != les) {
            this.leaderElectionState = les;
            try {
                stateChangesListener.accept(leaderElectionState);
            } catch (Throwable t) {
                log.warn("Exception in state change listener", t);
            }
        }
    }

    private synchronized CompletableFuture<LeaderElectionState> tryToBecomeLeader() {
        T value = proposedValue.get();
        byte[] payload;
        try {
            payload = serde.serialize(path, value);
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
                                            log.info("Acquired leadership on {} with {}", path, value);
                                            internalState = InternalState.LeaderIsPresent;
                                            if (leaderElectionState != LeaderElectionState.Leading) {
                                                leaderElectionState = LeaderElectionState.Leading;
                                                try {
                                                    stateChangesListener.accept(leaderElectionState);
                                                } catch (Throwable t) {
                                                    log.warn("Exception in state change listener", t);
                                                }
                                            }
                                            result.complete(leaderElectionState);
                                        }
                                    }).exceptionally(ex -> {
                                        // We fail to do the get(), so clean up the leader election fail the whole
                                        // operation
                                        log.warn("Failed to get the current state after acquiring leadership on {}. "
                                                + " Conditionally deleting current entry.", path, ex);
                                        store.delete(path, Optional.of(stat.getVersion()))
                                                .thenRun(() -> result.completeExceptionally(ex))
                                                .exceptionally(ex2 -> {
                                                    result.completeExceptionally(ex2);
                                                    return null;
                                                });
                                        return null;
                                    });
                        } else {
                            log.info("Leadership on {} with value {} was lost. "
                                            + "Conditionally deleting entry with stat={}.", path, value, stat);
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
                        log.info("There was a conflict between 2 participants trying to become leaders at the same "
                                        + "time on {}. Attempted with value {}. Retrying.",
                                path, value);
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
        updateCachedValueFuture.cancel(true);
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

        if (leaderElectionState != LeaderElectionState.Leading) {
            return CompletableFuture.completedFuture(null);
        }

        return store.delete(path, version)
                .thenAccept(__ -> {
                            synchronized (LeaderElectionImpl.this) {
                                leaderElectionState = LeaderElectionState.NoLeader;
                            }
                        }
                );
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

    private void handleSessionNotification(SessionEvent event) {
        // Ensure we're only processing one session event at a time.
        sequencer.sequential(() -> FutureUtil.composeAsync(() -> {
            if (event == SessionEvent.Reconnected || event == SessionEvent.SessionReestablished) {
                log.info("Revalidating leadership for {}, event:{}", path, event);
                return elect().thenAccept(leaderState -> {
                    log.info("Resynced leadership for {} - State: {}", path, leaderState);
                }).exceptionally(ex -> {
                    log.warn("Failure when processing session event", ex);
                    return null;
                });
            }
            return CompletableFuture.completedFuture(null);
        }, executor));
    }

    private void handlePathNotification(Notification notification) {
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
                    log.warn("Leadership released for {}", path);
                }

                leaderElectionState = LeaderElectionState.NoLeader;

                if (proposedValue.isPresent()) {
                    elect()
                            .exceptionally(ex -> {
                                log.warn("Leader election for path {} has failed", path, ex);
                                synchronized (LeaderElectionImpl.this) {
                                    try {
                                        stateChangesListener.accept(leaderElectionState);
                                    } catch (Throwable t) {
                                        log.warn("Exception in state change listener", t);
                                    }

                                    if (internalState != InternalState.Closed) {
                                        executor.schedule(() -> {
                                            log.info("Retrying Leader election for path {}", path);
                                            elect();
                                        }, LEADER_ELECTION_RETRY_DELAY_SECONDS, TimeUnit.SECONDS);
                                    }
                                }
                                return null;
                            });
                }
            }
        }
    }

    @VisibleForTesting
    protected ScheduledExecutorService getSchedulerExecutor() {
        return executor;
    }
}
