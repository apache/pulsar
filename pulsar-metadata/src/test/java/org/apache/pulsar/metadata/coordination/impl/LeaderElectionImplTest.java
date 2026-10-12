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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
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
}
