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
import java.time.Duration;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
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

@CustomLog
class LeaderElectionImpl<T> implements LeaderElection<T> {
    private final String path;
    private final MetadataSerde<T> serde;
    private final MetadataStoreExtended store;
    private final Consumer<LeaderElectionState> stateChangesListener;

    private LeaderElectionState leaderElectionState;
    private Optional<Long> version = Optional.empty();
    private Optional<T> proposedValue;

    // The leader value as known by the election cycle (the leader can only change through an
    // election cycle). Pending while no leader is known — election in progress or the leader node
    // deleted — and completed with the leader value once the election settles. Readers of
    // getLeaderValue() wait on it (bounded by leaderElectionCompletionTimeoutSeconds);
    // getLeaderValueIfPresent() takes a non-blocking snapshot of it.
    private CompletableFuture<Optional<T>> currentLeaderFuture = new CompletableFuture<>();

    private final ScheduledExecutorService executor;
    private final FutureUtil.Sequencer<Void> sequencer;

    private enum InternalState {
        Init, ElectionInProgress, LeaderIsPresent, Closed
    }

    private InternalState internalState;

    private static final int LEADER_ELECTION_RETRY_DELAY_SECONDS = 5;

    // Upper bound for getLeaderValue() waiting on an election that never settles, aligned with the
    // default metadata-store operation timeout (the broker's metadataStoreOperationTimeoutSeconds).
    private volatile int leaderElectionCompletionTimeoutSeconds = 30;

    @VisibleForTesting
    void setLeaderElectionCompletionTimeoutSeconds(int leaderElectionCompletionTimeoutSeconds) {
        this.leaderElectionCompletionTimeoutSeconds = leaderElectionCompletionTimeoutSeconds;
    }

    LeaderElectionImpl(MetadataStoreExtended store, Class<T> clazz, String path,
            Consumer<LeaderElectionState> stateChangesListener,
                       ScheduledExecutorService executor) {
        this.path = path;
        this.serde = new JSONMetadataSerdeSimpleType<>(TypeFactory.defaultInstance().constructSimpleType(clazz, null));
        this.store = store;
        this.leaderElectionState = LeaderElectionState.NoLeader;
        this.internalState = InternalState.Init;
        this.stateChangesListener = stateChangesListener;
        this.executor = executor;
        this.sequencer = FutureUtil.Sequencer.create();
        store.registerListener(this::handlePathNotification);
        store.registerSessionListener(this::handleSessionNotification);
    }

    /**
     * Record the leader value determined by the election cycle, waking up any getLeaderValue()
     * callers waiting for the election to settle.
     */
    private synchronized void leaderKnown(Optional<T> leaderValue) {
        if (currentLeaderFuture.isDone()) {
            currentLeaderFuture = CompletableFuture.completedFuture(leaderValue);
        } else {
            currentLeaderFuture.complete(leaderValue);
        }
    }

    /**
     * Mark the leader as unknown (the leader node was deleted) so getLeaderValue() callers wait for
     * the next election cycle to settle instead of observing a stale value.
     */
    private synchronized void leaderUnknown() {
        if (currentLeaderFuture.isDone()) {
            currentLeaderFuture = new CompletableFuture<>();
        }
    }

    @Override
    public synchronized CompletableFuture<LeaderElectionState> elect(T proposedValue) {
        if (internalState == InternalState.Closed) {
            // Reopened after close() (e.g. the broker's LeaderElectionService is close()d and then
            // start()ed again): reset so a fresh election cycle runs and readers wait for it.
            leaderElectionState = LeaderElectionState.NoLeader;
            currentLeaderFuture = new CompletableFuture<>();
        }
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
                log.info().attr("value", existingValue).attr("path", path).attr("stat", res.getStat())
                        .log("Keeping the existing value as it's from the same session");
                // The value is still valid because it was created in the same session
                leaderKnown(Optional.of(existingValue));
                changeState(LeaderElectionState.Leading);
                return CompletableFuture.completedFuture(LeaderElectionState.Leading);
            } else {
                log.info().attr("value", existingValue).attr("path", path)
                        .attr("stat", res.getStat())
                        .log("Conditionally deleting existing equals value"
                                + " because it's not created in the current session");
                // Since the value was created in a different session, it might be expiring. We need to delete it
                // and try the election again.
                return store.delete(path, Optional.of(res.getStat().getVersion()))
                        .thenCompose(__ -> tryToBecomeLeader());
            }
        } else if (res.getStat().isCreatedBySelf()) {
            log.warn().attr("existingValue", existingValue).attr("path", path).attr("proposedValue", value)
                    .log("Conditionally deleting existing value because it's different from the proposed value."
                            + " This is unexpected since it was created within the same session."
                            + " In tests this could happen because of an invalid shared session id when using mocks.");
            // The existing value is different but was created from the same session
            return store.delete(path, Optional.of(res.getStat().getVersion()))
                    .thenCompose(__ -> tryToBecomeLeader());
        }

        // If the existing value is different, it means there's already another leader
        leaderKnown(Optional.of(existingValue));
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
                log.warn().exception(t).log("Exception in state change listener");
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
                            log.info().attr("path", path).attr("value", value)
                                    .log("Acquired leadership");
                            internalState = InternalState.LeaderIsPresent;
                            leaderKnown(Optional.of(value));
                            if (leaderElectionState != LeaderElectionState.Leading) {
                                leaderElectionState = LeaderElectionState.Leading;
                                try {
                                    stateChangesListener.accept(leaderElectionState);
                                } catch (Throwable t) {
                                    log.warn().exception(t).log("Exception in state change listener");
                                }
                            }
                            result.complete(leaderElectionState);
                        } else {
                            log.info().attr("path", path).attr("value", value).attr("stat", stat)
                                    .log("Leadership was lost. Conditionally deleting entry.");
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
                        log.info().attr("path", path).attr("value", value)
                                .log("There was a conflict between 2 participants"
                                        + " trying to become leaders at the same time."
                                        + " Retrying.");
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
        // A closed election reports "no leader" rather than waiting or failing: callers like the
        // extensible load manager's handleNoChannelOwnerError() key off the resulting
        // "no channel owner" condition to restart the election.
        if (!currentLeaderFuture.isDone()) {
            currentLeaderFuture.complete(Optional.empty());
        }

        if (leaderElectionState != LeaderElectionState.Leading) {
            return CompletableFuture.completedFuture(null);
        }

        return store.delete(path, version)
                .thenAccept(__ -> {
                            synchronized (LeaderElectionImpl.this) {
                                leaderElectionState = LeaderElectionState.NoLeader;
                                // The deleted leader node was ours and a closed instance no longer
                                // observes elections; don't keep reporting ourselves as leader.
                                currentLeaderFuture = CompletableFuture.completedFuture(Optional.empty());
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
        CompletableFuture<Optional<T>> future;
        synchronized (this) {
            if (internalState == InternalState.Init) {
                // This instance never participated in the election (a pure observer, e.g.
                // BookKeeper's MetadataDrivers helpers querying the current auditor): there is no
                // local election cycle to wait for, so the store content is the authoritative
                // answer.
                return readLeaderValueFromStore();
            }
            future = currentLeaderFuture;
        }
        if (future.isDone()) {
            // Hand out a derived future so callers cannot complete the internal one.
            return future.thenApply(value -> value);
        }
        int timeoutSeconds = leaderElectionCompletionTimeoutSeconds;
        return FutureUtil.addTimeoutHandling(whenLeaderKnown(future),
                Duration.ofSeconds(timeoutSeconds), executor,
                () -> FutureUtil.createTimeoutException(
                        "Leader election on path " + path + " did not complete within "
                                + timeoutSeconds + " seconds",
                        LeaderElectionImpl.class, "getLeaderValue()"));
    }

    private CompletableFuture<Optional<T>> readLeaderValueFromStore() {
        return store.get(path).thenApply(optRes -> optRes.map(res -> {
            try {
                return serde.deserialize(path, res.getValue(), res.getStat());
            } catch (Throwable t) {
                throw new CompletionException(t);
            }
        }));
    }

    // Track the internal future without exposing it: completing/cancelling the returned future
    // (e.g. by the timeout handling) must not complete the election's own future.
    private CompletableFuture<Optional<T>> whenLeaderKnown(CompletableFuture<Optional<T>> future) {
        CompletableFuture<Optional<T>> result = new CompletableFuture<>();
        future.whenComplete((value, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else {
                result.complete(value);
            }
        });
        return result;
    }

    @Override
    public Optional<T> getLeaderValueIfPresent() {
        CompletableFuture<Optional<T>> future;
        synchronized (this) {
            future = currentLeaderFuture;
        }
        return future.isDone() && !future.isCompletedExceptionally() ? future.join() : Optional.empty();
    }

    private void handleSessionNotification(SessionEvent event) {
        // Ensure we're only processing one session event at a time.
        sequencer.sequential(() -> FutureUtil.composeAsync(() -> {
            if (event == SessionEvent.Reconnected || event == SessionEvent.SessionReestablished) {
                log.info().attr("path", path).attr("event", event).log("Revalidating leadership");
                return elect().thenAccept(leaderState -> {
                    log.info().attr("path", path).attr("state", leaderState).log("Resynced leadership");
                }).exceptionally(ex -> {
                    log.warn().exception(ex).log("Failure when processing session event");
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
                    log.warn().attr("path", path).log("Leadership released");
                }

                leaderElectionState = LeaderElectionState.NoLeader;
                // The leader is unknown until the re-election below settles; getLeaderValue()
                // callers wait for it instead of observing the stale value.
                leaderUnknown();

                if (proposedValue.isPresent()) {
                    elect()
                            .exceptionally(ex -> {
                                log.warn().attr("path", path).exception(ex).log("Leader election has failed");
                                synchronized (LeaderElectionImpl.this) {
                                    try {
                                        stateChangesListener.accept(leaderElectionState);
                                    } catch (Throwable t) {
                                        log.warn().exception(t).log("Exception in state change listener");
                                    }

                                    if (internalState != InternalState.Closed) {
                                        executor.schedule(() -> {
                                            log.info().attr("path", path).log("Retrying Leader election");
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
