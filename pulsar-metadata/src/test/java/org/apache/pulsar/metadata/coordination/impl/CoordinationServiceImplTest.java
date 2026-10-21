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
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * The leader elections of {@link CoordinationServiceImpl}: how they are tracked, and how a closed one stays out of
 * the election.
 */
public class CoordinationServiceImplTest {

    private static final String PATH = "/leader";

    @Test(timeOut = 30000)
    public void closedElectionIsDropped() throws Exception {
        @Cleanup
        TestStore store = new TestStore();
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);

        // Like the controllers of scalable topics that are created and deleted: one election per topic
        for (int i = 0; i < 3; i++) {
            String path = PATH + "-" + i;
            LeaderElection<String> election = cs.getLeaderElection(String.class, path, __ -> {
            });
            assertThat(election.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);
            assertThat(cs.getLeaderElections().get(path)).isSameAs(election);

            election.close();
            assertThat(cs.getLeaderElections()).as("elections after closing %s", path).doesNotContainKey(path);
        }

        // One listener of each kind dispatches to the open elections, so the closed ones are no longer reachable
        // from the store
        assertThat(store.listenerCount).as("store listeners").hasValue(1);
        assertThat(store.sessionListenerCount).as("session listeners").hasValue(1);
    }

    @Test(timeOut = 30000)
    public void closeUnregistersListeners() throws Exception {
        @Cleanup
        TestStore store = new TestStore();
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);
        cs.getLeaderElection(String.class, PATH, __ -> {
        });
        cs.getLockManager(String.class);
        // The listeners of the elections, and the ones of the lock manager
        assertThat(store.listenerCount).as("store listeners").hasValue(2);
        assertThat(store.sessionListenerCount).as("session listeners").hasValue(2);

        cs.close();

        // The store can outlive the service, for example when it's shared by the brokers of a test
        assertThat(store.listenerCount).as("store listeners").hasValue(0);
        assertThat(store.sessionListenerCount).as("session listeners").hasValue(0);
    }

    @Test(timeOut = 30000)
    public void electionCreatedAfterACloseReportsToItsOwnListener() throws Exception {
        @Cleanup
        TestStore store = new TestStore();
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);

        List<LeaderElectionState> firstStates = new CopyOnWriteArrayList<>();
        LeaderElection<String> first = cs.getLeaderElection(String.class, PATH, firstStates::add);
        assertThat(first.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);
        first.close();

        // Like the controller of a scalable topic created again after the previous one was closed
        List<LeaderElectionState> secondStates = new CopyOnWriteArrayList<>();
        LeaderElection<String> second = cs.getLeaderElection(String.class, PATH, secondStates::add);
        assertThat(second).isNotSameAs(first);
        assertThat(second.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);
        // The deletion of the first lock can reach the second election after it took the lock, and make it
        // revalidate its leadership: it may report Leading more than once
        assertThat(secondStates).as("states reported to the second listener").containsOnly(
                LeaderElectionState.Leading);
        assertThat(firstStates).as("states reported to the first listener").containsExactly(
                LeaderElectionState.Leading);
    }

    @Test(timeOut = 30000)
    public void reopenedElectionKeepsReceivingNotifications() throws Exception {
        @Cleanup
        TestStore store = new TestStore();
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);

        LeaderElection<String> election = cs.getLeaderElection(String.class, PATH, __ -> {
        });
        assertThat(election.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);
        election.close();

        // Like the broker's LeaderElectionService, closed and started again
        assertThat(election.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);
        assertThat(cs.getLeaderElections().get(PATH)).isSameAs(election);

        // It still sees its lock deleted, and takes it again
        store.delete(PATH, Optional.empty()).join();
        Awaitility.await().untilAsserted(() ->
                assertThat(store.get(PATH).join()).as("lock taken again").isPresent());

        // And closing the coordination service closes it, which releases the lock
        cs.close();
        assertThat(election.getState()).isEqualTo(LeaderElectionState.NoLeader);
        assertThat(store.get(PATH).join()).as("lock after closing the coordination service").isEmpty();
    }

    @Test(timeOut = 30000)
    public void closedElectionIsNotReopenedOverANewerOne() throws Exception {
        @Cleanup
        TestStore store = new TestStore();
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);

        LeaderElection<String> first = cs.getLeaderElection(String.class, PATH, __ -> {
        });
        assertThat(first.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);
        first.close();
        LeaderElection<String> second = cs.getLeaderElection(String.class, PATH, __ -> {
        });
        assertThat(second.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);

        // Both would compete for the same lock: the newer election keeps the path
        assertThatThrownBy(() -> first.elect("broker-1").join()).hasCauseInstanceOf(AlreadyClosedException.class);
        assertThat(first.getState()).isEqualTo(LeaderElectionState.NoLeader);
        assertThat(cs.getLeaderElections().get(PATH)).isSameAs(second);
        assertThat(second.getState()).isEqualTo(LeaderElectionState.Leading);
    }

    @Test(timeOut = 30000)
    public void closedElectionIgnoresSessionEvents() throws Exception {
        @Cleanup
        TestStore store = new TestStore();
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);

        List<LeaderElectionState> states = new CopyOnWriteArrayList<>();
        LeaderElection<String> election = cs.getLeaderElection(String.class, PATH, states::add);
        assertThat(election.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);
        election.close();

        store.triggerSessionEvent(SessionEvent.SessionReestablished);
        awaitQueuedTasks(election);

        assertThat(store.get(PATH).join()).as("lock of the closed election").isEmpty();
        assertThat(election.getState()).isEqualTo(LeaderElectionState.NoLeader);
        assertThat(states).as("reported states").containsExactly(LeaderElectionState.Leading);
    }

    @Test(timeOut = 30000)
    public void electionClosedWithASessionEventPendingIgnoresIt() throws Exception {
        @Cleanup
        TestStore store = new TestStore();
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);

        List<LeaderElectionState> states = new CopyOnWriteArrayList<>();
        LeaderElection<String> election = cs.getLeaderElection(String.class, PATH, states::add);
        assertThat(election.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);

        // Hold the executor, so that the session event reaches the election while it's open, and is processed
        // once it's closed
        CountDownLatch release = new CountDownLatch(1);
        ((LeaderElectionImpl<?>) election).getSchedulerExecutor().execute(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            store.triggerSessionEvent(SessionEvent.SessionReestablished);
            election.close();
        } finally {
            release.countDown();
        }
        awaitQueuedTasks(election);

        assertThat(store.get(PATH).join()).as("lock of the closed election").isEmpty();
        assertThat(election.getState()).isEqualTo(LeaderElectionState.NoLeader);
        assertThat(states).as("reported states").containsExactly(LeaderElectionState.Leading);
    }

    @Test(timeOut = 30000)
    public void neverElectedElectionIgnoresSessionEvents() throws Exception {
        @Cleanup
        TestStore store = new TestStore();
        @Cleanup
        CoordinationServiceImpl leaderCs = new CoordinationServiceImpl(store);
        @Cleanup
        CoordinationServiceImpl observerCs = new CoordinationServiceImpl(store);

        LeaderElection<String> leader = leaderCs.getLeaderElection(String.class, PATH, __ -> {
        });
        assertThat(leader.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);

        // Only reads the leader, like BookKeeper's tools reading the current auditor
        LeaderElectionImpl<String> observer = (LeaderElectionImpl<String>) observerCs.getLeaderElection(
                String.class, PATH, __ -> {
                });
        observer.setLeaderElectionCompletionTimeoutSeconds(1);

        store.triggerSessionEvent(SessionEvent.SessionReestablished);
        awaitQueuedTasks(observer);

        // It doesn't take part in the election, so it keeps reading the leader from the store
        assertThat(observer.getLeaderValue()).succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(Optional.of("broker-1"));
        assertThat(observer.getState()).isEqualTo(LeaderElectionState.NoLeader);
    }

    @Test(timeOut = 30000)
    public void electionClosedWhileReelectingDoesNotTakeTheLock() throws Exception {
        @Cleanup
        TestStore store = new TestStore();
        @Cleanup
        CoordinationServiceImpl cs = new CoordinationServiceImpl(store);

        List<LeaderElectionState> states = new CopyOnWriteArrayList<>();
        LeaderElection<String> election = cs.getLeaderElection(String.class, PATH, states::add);
        assertThat(election.elect("broker-1").join()).isEqualTo(LeaderElectionState.Leading);

        // The lock is deleted, and the election is closed while it reads the lock to elect again
        CompletableFuture<Void> readGate = new CompletableFuture<>();
        store.readGate = readGate;
        store.delete(PATH, Optional.empty()).join();
        Awaitility.await().untilAsserted(() -> assertThat(store.heldReads).hasValue(1));
        election.close();
        store.writes.clear();
        store.readGate = null;
        readGate.complete(null);

        assertThat(store.writes).as("paths written after the close").doesNotContain(PATH);
        assertThat(store.get(PATH).join()).as("lock of the closed election").isEmpty();
        assertThat(election.getState()).isEqualTo(LeaderElectionState.NoLeader);
        assertThat(states).as("reported states").containsExactly(LeaderElectionState.Leading);
    }

    // The elections do their work, e.g. processing a session event, on the coordination service's executor
    private static void awaitQueuedTasks(LeaderElection<?> election) throws Exception {
        ((LeaderElectionImpl<?>) election).getSchedulerExecutor().submit(() -> {
        }).get();
    }

    /**
     * An in-memory store that counts the listeners registered on it and not unregistered, can trigger session
     * events, records the paths written, and can hold the reads until the test lets them go.
     */
    private static class TestStore extends FaultInjectionMetadataStore {
        private final AtomicInteger listenerCount = new AtomicInteger();
        private final AtomicInteger sessionListenerCount = new AtomicInteger();
        private final List<String> writes = new CopyOnWriteArrayList<>();
        private final AtomicInteger heldReads = new AtomicInteger();
        // While set, the reads wait for it to complete
        private volatile CompletableFuture<Void> readGate;

        TestStore() throws Exception {
            super(new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build()));
        }

        @Override
        public CompletableFuture<Optional<GetResult>> get(String path, Set<Option> opts) {
            CompletableFuture<Void> gate = readGate;
            if (gate == null) {
                return super.get(path, opts);
            }
            heldReads.incrementAndGet();
            return gate.thenCompose(__ -> super.get(path, opts));
        }

        @Override
        public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion,
                                           Set<Option> opts) {
            writes.add(path);
            return super.put(path, value, expectedVersion, opts);
        }

        @Override
        public Runnable registerCancellableListener(Consumer<Notification> listener) {
            return countRegistration(listenerCount, super.registerCancellableListener(listener));
        }

        @Override
        public Runnable registerCancellableSessionListener(Consumer<SessionEvent> listener) {
            return countRegistration(sessionListenerCount, super.registerCancellableSessionListener(listener));
        }

        private static Runnable countRegistration(AtomicInteger count, Runnable cancelRegistration) {
            count.incrementAndGet();
            AtomicBoolean cancelled = new AtomicBoolean();
            return () -> {
                if (cancelled.compareAndSet(false, true)) {
                    count.decrementAndGet();
                }
                cancelRegistration.run();
            };
        }
    }
}
