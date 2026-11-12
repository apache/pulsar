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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class LeaderElectionImplTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl", timeOut = 20000)
    public void validateDeadLock(String provider, Supplier<String> urlSupplier)
            throws Exception {
        if (provider.equals("Memory") || provider.equals("RocksDB")) {
            // There are no multiple sessions for the local memory provider
            return;
        }

        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String path = newKey();

        @Cleanup
        CoordinationService cs = new CoordinationServiceImpl(store);

        @Cleanup
        LeaderElectionImpl<String> le = (LeaderElectionImpl<String>) cs.getLeaderElection(String.class,
                path, __ -> {
                });
        final CompletableFuture<Void> blockFuture = new CompletableFuture<>();
        // simulate handleSessionNotification method logic
        le.getSchedulerExecutor().execute(() -> {
            try {
                le.elect("test-2").join();
                blockFuture.complete(null);
            } catch (Throwable ex) {
                blockFuture.completeExceptionally(ex);
            }
        });
        blockFuture.join();
    }

    @Test(timeOut = 20000)
    public void getLeaderValueTimesOutWhenElectionNeverCompletes() throws Exception {
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        // The store never answers, so the election never settles.
        when(store.get(anyString())).thenReturn(new CompletableFuture<>());

        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        LeaderElectionImpl<String> le = new LeaderElectionImpl<>(cs, store, String.class,
                "/getLeaderValueTimesOutWhenElectionNeverCompletes", __ -> {
        }, executor);
        le.setLeaderElectionCompletionTimeoutSeconds(1);

        le.elect("test-1");

        assertThatThrownBy(() -> le.getLeaderValue().join())
                .hasCauseInstanceOf(TimeoutException.class)
                .cause()
                .hasMessageContaining("did not complete within");
    }

    @Test(timeOut = 20000)
    public void closedElectionDoesNotRetryTheElection() throws Exception {
        String path = "/closedElectionDoesNotRetryTheElection";
        @Cleanup
        FaultInjectionMetadataStore store = new FaultInjectionMetadataStore(
                new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build()));
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);
        // Lets the test run the retry the election schedules
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        LeaderElectionImpl<String> le = new LeaderElectionImpl<>(cs, store, String.class, path, __ -> {
        }, executor);
        assertThat(le.elect("test-1").join()).isEqualTo(LeaderElectionState.Leading);

        // The lock is deleted and the election fails to take it again: it schedules a retry
        store.delete(path, Optional.empty()).join();
        store.failConditional(new MetadataStoreException("injected"),
                (op, p) -> op == OperationType.GET && p.equals(path));
        le.handlePathNotification(new Notification(NotificationType.Deleted, path));
        ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).schedule(retry.capture(), anyLong(), any(TimeUnit.class));

        le.close();
        retry.getValue().run();

        assertThat(store.get(path).join()).as("lock of the closed election").isEmpty();
        assertThat(le.getState()).isEqualTo(LeaderElectionState.NoLeader);
    }

    @Test(timeOut = 20000)
    public void sessionRevalidationRetriesUntilSuccessAndCancelsOnClose() throws Exception {
        String path = "/sessionRevalidationRetry";
        @Cleanup
        var store = spy(new FaultInjectionMetadataStore(
                new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build())));
        @Cleanup
        var cs = new CoordinationServiceImpl(store);
        // Run immediate work inline and advance each five-second timer explicitly.
        var executor = mock(ScheduledExecutorService.class);
        var timers = new ArrayDeque<Runnable>();
        var scheduled = mock(ScheduledFuture.class);
        doAnswer(inv -> {
            inv.<Runnable>getArgument(0).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        doAnswer(inv -> {
            timers.add(inv.getArgument(0));
            return scheduled;
        }).when(executor).schedule(any(Runnable.class), eq(5L), eq(TimeUnit.SECONDS));
        var states = new ArrayDeque<LeaderElectionState>();
        @Cleanup
        var election = new LeaderElectionImpl<>(cs, store, String.class, path, states::add, executor);
        assertThat(election.elect("broker").join()).isEqualTo(LeaderElectionState.Leading);

        store.setAlwaysFail(new MetadataStoreException("store unavailable"));
        try {
            election.handleSessionNotification(SessionEvent.Reconnected);
            for (int i = 0; i < 3; i++) {
                assertThat(timers).hasSize(1);
                clearInvocations(store);
                timers.remove().run();
                verify(store).get(path);
            }
        } finally {
            store.unsetAlwaysFail();
        }
        assertThat(timers).hasSize(1);
        clearInvocations(store);
        timers.remove().run();
        verify(store).get(path);
        assertThat(timers).isEmpty();
        assertThat(states).containsExactly(LeaderElectionState.Leading);

        store.failConditional(new MetadataStoreException("store unavailable"),
                (op, p) -> op == OperationType.GET && path.equals(p));
        election.handleSessionNotification(SessionEvent.SessionReestablished);
        assertThat(timers).hasSize(1);
        Runnable lateRetry = timers.remove();
        election.close();
        verify(scheduled).cancel(false);
        // A timer racing with cancellation must not operate on a reopened election.
        assertThat(election.elect("broker").join()).isEqualTo(LeaderElectionState.Leading);
        clearInvocations(store);
        lateRetry.run();
        verify(store, never()).get(path);
        assertThat(timers).isEmpty();

        // A read already in flight when the election closes must not restart the retry loop either.
        var pendingRead = new CompletableFuture<Optional<GetResult>>();
        doReturn(pendingRead).doCallRealMethod().when(store).get(path);
        election.handleSessionNotification(SessionEvent.Reconnected);
        verify(store).get(path);
        election.close();
        pendingRead.completeExceptionally(new MetadataStoreException("late failure"));
        assertThat(timers).isEmpty();
    }

    @Test(timeOut = 20000)
    public void closedElectionDoesNotRetryAfterAConflict() throws Exception {
        String path = "/closedElectionDoesNotRetryAfterAConflict";
        AtomicBoolean conflictOnNextPut = new AtomicBoolean();
        CompletableFuture<Void> conflictReported = new CompletableFuture<>();
        @Cleanup
        FaultInjectionMetadataStore store = new FaultInjectionMetadataStore(
                new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build())) {
            @Override
            public CompletableFuture<Stat> put(String p, byte[] value, Optional<Long> expectedVersion,
                                               Set<Option> opts) {
                if (p.equals(path) && conflictOnNextPut.compareAndSet(true, false)) {
                    // Another participant took the lock first: the conflict is reported once the test
                    // releases it
                    return conflictReported.thenCompose(
                            __ -> FutureUtil.failedFuture(new BadVersionException("injected")));
                }
                return super.put(p, value, expectedVersion, opts);
            }
        };
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        BlockingQueue<LeaderElectionState> stateChanges = new LinkedBlockingQueue<>();
        LeaderElectionImpl<String> le = new LeaderElectionImpl<>(cs, store, String.class, path, stateChanges::add,
                executor);

        conflictOnNextPut.set(true);
        CompletableFuture<LeaderElectionState> election = le.elect("test-1");
        // Closed while taking the lock, before the conflict is reported
        le.close();
        conflictReported.complete(null);

        // The conflict doesn't run the closed election again
        assertThatThrownBy(() -> election.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(AlreadyClosedException.class);
        assertThat(stateChanges).as("state changes").isEmpty();
        assertThat(le.getState()).isEqualTo(LeaderElectionState.NoLeader);
        assertThat(store.get(path).join()).as("lock of the closed election").isEmpty();
    }

    @Test(timeOut = 20000)
    public void electionClosedWhileReadingTheLockStaysClosed() throws Exception {
        String path = "/electionClosedWhileReadingTheLockStaysClosed";
        AtomicBoolean holdNextRead = new AtomicBoolean();
        CompletableFuture<Void> heldReadReleased = new CompletableFuture<>();
        @Cleanup
        FaultInjectionMetadataStore store = new FaultInjectionMetadataStore(
                new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build())) {
            @Override
            public CompletableFuture<Optional<GetResult>> get(String p, Set<Option> opts) {
                CompletableFuture<Optional<GetResult>> result = super.get(p, opts);
                // The read completes once the test releases it
                return p.equals(path) && holdNextRead.compareAndSet(true, false)
                        ? result.thenCombine(heldReadReleased, (r, __) -> r)
                        : result;
            }
        };
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        BlockingQueue<LeaderElectionState> stateChanges = new LinkedBlockingQueue<>();
        LeaderElectionImpl<String> le = new LeaderElectionImpl<>(cs, store, String.class, path, stateChanges::add,
                executor);
        assertThat(le.elect("test-1").join()).isEqualTo(LeaderElectionState.Leading);
        assertThat(stateChanges.poll()).isEqualTo(LeaderElectionState.Leading);

        // A session event revalidates the leadership, and the election is closed while the revalidation
        // reads the lock
        holdNextRead.set(true);
        le.handleSessionNotification(SessionEvent.SessionReestablished);
        // Once this runs, the revalidation is waiting for the read
        executor.submit(() -> {
        }).get();
        le.close();
        // The read returns the lock as it was before close() deleted it
        heldReadReleased.complete(null);

        // That doesn't bring the closed election back
        assertThat(stateChanges).as("state changes after close()").isEmpty();
        assertThat(le.getState()).isEqualTo(LeaderElectionState.NoLeader);
        assertThat(le.getLeaderValue().join()).isEmpty();
    }

    @Test(dataProvider = "distributedImpl", timeOut = 30000)
    public void candidateDoesNotFollowALeaderGoneWhileReadingTheLock(String provider, Supplier<String> urlSupplier)
            throws Exception {
        String path = newKey();
        @Cleanup
        MetadataStoreExtended leaderStore = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        @Cleanup
        CoordinationService leaderCs = new CoordinationServiceImpl(leaderStore);
        LeaderElection<String> leader = leaderCs.getLeaderElection(String.class, path, __ -> {
        });
        assertThat(leader.elect("leader").join()).isEqualTo(LeaderElectionState.Leading);

        AtomicBoolean holdNextRead = new AtomicBoolean();
        CompletableFuture<Optional<GetResult>> heldRead = new CompletableFuture<>();
        CompletableFuture<Void> heldReadReleased = new CompletableFuture<>();
        @Cleanup
        FaultInjectionMetadataStore candidateStore = new FaultInjectionMetadataStore(
                MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build())) {
            @Override
            public CompletableFuture<Optional<GetResult>> get(String p, Set<Option> opts) {
                CompletableFuture<Optional<GetResult>> result = super.get(p, opts);
                if (p.equals(path) && holdNextRead.compareAndSet(true, false)) {
                    // The store answers, and the election gets the answer once the test releases it
                    FutureUtil.completeAfter(heldRead, result);
                    return result.thenCombine(heldReadReleased, (r, __) -> r);
                }
                return result;
            }
        };
        @Cleanup
        CoordinationService candidateCs = new CoordinationServiceImpl(candidateStore);
        BlockingQueue<LeaderElectionState> stateChanges = new LinkedBlockingQueue<>();
        LeaderElection<String> candidate = candidateCs.getLeaderElection(String.class, path, stateChanges::add);
        CompletableFuture<Void> deletionDispatched = deletionDispatched(candidateStore, path);

        holdNextRead.set(true);
        CompletableFuture<LeaderElectionState> election = candidate.elect("candidate");
        assertThat(heldRead.get(10, TimeUnit.SECONDS)).as("lock of the leader").isPresent();
        // The leader goes away, and the candidate's election is notified of it before it gets the lock it read
        leader.close();
        deletionDispatched.get(10, TimeUnit.SECONDS);
        heldReadReleased.complete(null);

        // The candidate doesn't follow the leader that went away: it takes the leadership
        assertThat(election.get(10, TimeUnit.SECONDS)).isEqualTo(LeaderElectionState.Leading);
        assertThat(stateChanges).containsExactly(LeaderElectionState.Leading);
        assertThat(candidate.getLeaderValue().join()).contains("candidate");
    }

    @Test(timeOut = 20000)
    public void electionDoesNotLeadWithALockDeletedWhileTakingIt() throws Exception {
        String path = "/electionDoesNotLeadWithALockDeletedWhileTakingIt";
        AtomicBoolean holdNextWrite = new AtomicBoolean();
        CompletableFuture<Stat> heldWrite = new CompletableFuture<>();
        CompletableFuture<Void> heldWriteReleased = new CompletableFuture<>();
        @Cleanup
        FaultInjectionMetadataStore store = new FaultInjectionMetadataStore(
                new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build())) {
            @Override
            public CompletableFuture<Stat> put(String p, byte[] value, Optional<Long> expectedVersion,
                                               Set<Option> opts) {
                CompletableFuture<Stat> result = super.put(p, value, expectedVersion, opts);
                if (p.equals(path) && holdNextWrite.compareAndSet(true, false)) {
                    // The store writes the lock, and the election learns it once the test releases it
                    FutureUtil.completeAfter(heldWrite, result);
                    return result.thenCombine(heldWriteReleased, (r, __) -> r);
                }
                return result;
            }
        };
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);
        BlockingQueue<LeaderElectionState> stateChanges = new LinkedBlockingQueue<>();
        LeaderElection<String> le = cs.getLeaderElection(String.class, path, stateChanges::add);
        CompletableFuture<Void> deletionDispatched = deletionDispatched(store, path);

        holdNextWrite.set(true);
        CompletableFuture<LeaderElectionState> election = le.elect("test-1");
        heldWrite.get(5, TimeUnit.SECONDS);
        // The lock is deleted, and the election is notified of it before it learns that it wrote the lock
        store.delete(path, Optional.empty()).join();
        deletionDispatched.get(5, TimeUnit.SECONDS);
        heldWriteReleased.complete(null);

        // The election doesn't lead without the lock: it takes it again
        assertThat(election.get(5, TimeUnit.SECONDS)).isEqualTo(LeaderElectionState.Leading);
        assertThat(stateChanges).containsExactly(LeaderElectionState.Leading);
        assertThat(store.get(path).join()).as("lock of the leader").isPresent();
    }

    @Test(timeOut = 20000)
    public void electionClosedWhileReadingTheLockItTookReleasesIt() throws Exception {
        String path = "/electionClosedWhileReadingTheLockItTookReleasesIt";
        AtomicBoolean holdNextWrite = new AtomicBoolean();
        CompletableFuture<Stat> heldWrite = new CompletableFuture<>();
        CompletableFuture<Void> heldWriteReleased = new CompletableFuture<>();
        AtomicBoolean holdNextRead = new AtomicBoolean();
        CompletableFuture<Optional<GetResult>> heldRead = new CompletableFuture<>();
        CompletableFuture<Void> heldReadReleased = new CompletableFuture<>();
        @Cleanup
        FaultInjectionMetadataStore store = new FaultInjectionMetadataStore(
                new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build())) {
            @Override
            public CompletableFuture<Stat> put(String p, byte[] value, Optional<Long> expectedVersion,
                                               Set<Option> opts) {
                CompletableFuture<Stat> result = super.put(p, value, expectedVersion, opts);
                if (p.equals(path) && holdNextWrite.compareAndSet(true, false)) {
                    // The store writes the lock, and the election learns it once the test releases it
                    FutureUtil.completeAfter(heldWrite, result);
                    return result.thenCombine(heldWriteReleased, (r, __) -> r);
                }
                return result;
            }

            @Override
            public CompletableFuture<Optional<GetResult>> get(String p, Set<Option> opts) {
                CompletableFuture<Optional<GetResult>> result = super.get(p, opts);
                if (p.equals(path) && holdNextRead.compareAndSet(true, false)) {
                    // The store answers, and the election gets the answer once the test releases it
                    FutureUtil.completeAfter(heldRead, result);
                    return result.thenCombine(heldReadReleased, (r, __) -> r);
                }
                return result;
            }
        };
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        BlockingQueue<LeaderElectionState> stateChanges = new LinkedBlockingQueue<>();
        LeaderElectionImpl<String> le = new LeaderElectionImpl<>(cs, store, String.class, path, stateChanges::add,
                executor);

        holdNextWrite.set(true);
        CompletableFuture<LeaderElectionState> election = le.elect("test-1");
        heldWrite.get(5, TimeUnit.SECONDS);
        // The election is notified of an older deletion of the lock before it learns that it wrote the lock, so it
        // reads the lock again, and it's closed before it gets the answer
        le.handlePathNotification(new Notification(NotificationType.Deleted, path));
        holdNextRead.set(true);
        heldWriteReleased.complete(null);
        assertThat(heldRead.get(5, TimeUnit.SECONDS)).as("lock read again").isPresent();
        le.close();

        // close() released the lock the election took, although the election didn't lead
        assertThat(store.get(path).join()).as("lock of the closed election").isEmpty();
        heldReadReleased.complete(null);
        assertThatThrownBy(() -> election.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(AlreadyClosedException.class);
        assertThat(stateChanges).as("state changes").isEmpty();
        assertThat(le.getState()).isEqualTo(LeaderElectionState.NoLeader);
        assertThat(store.get(path).join()).as("lock of the closed election").isEmpty();
    }

    /**
     * Completes once the store has called the listeners registered before this one, such as the one of the leader
     * elections, with the deletion of the path.
     */
    private static CompletableFuture<Void> deletionDispatched(MetadataStoreExtended store, String path) {
        CompletableFuture<Void> dispatched = new CompletableFuture<>();
        store.registerCancellableListener(n -> {
            if (n.getType() == NotificationType.Deleted && n.getPath().equals(path)) {
                dispatched.complete(null);
            }
        });
        return dispatched;
    }
}
