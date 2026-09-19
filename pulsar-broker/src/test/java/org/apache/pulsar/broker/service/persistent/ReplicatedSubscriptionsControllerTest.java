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
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import java.time.Clock;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.qos.MonotonicClock;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.stats.OpenTelemetryReplicatedSubscriptionStats;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class ReplicatedSubscriptionsControllerTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testFinalSnapshotMarkerPublishFailureKeepsSnapshotPending() {
        PulsarService pulsar = mock(PulsarService.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("rawtypes")
        ScheduledFuture timer = mock(ScheduledFuture.class);
        ServiceConfiguration config = new ServiceConfiguration();
        config.setReplicatedSubscriptionsSnapshotFrequencyMillis(60_000);
        config.setReplicatedSubscriptionsSnapshotTimeoutSeconds(3);
        OpenTelemetryReplicatedSubscriptionStats stats = mock(OpenTelemetryReplicatedSubscriptionStats.class);
        BrokerService brokerService = mock(BrokerService.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        Replicator replicator = mock(Replicator.class);
        List<Topic.PublishContext> publishContexts = new ArrayList<>();
        AtomicReference<Runnable> scheduledSnapshotTask = new AtomicReference<>();

        when(topic.getName()).thenReturn("persistent://public/default/t1");
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(topic.getLastMaxReadPositionMovedForwardTimestamp()).thenReturn(1L);
        when(topic.getReplicators()).thenReturn(Map.of("remote", replicator));
        when(replicator.isConnected()).thenReturn(true);
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(pulsar.getExecutor()).thenReturn(executor);
        when(pulsar.getConfiguration()).thenReturn(config);
        when(pulsar.getOpenTelemetryReplicatedSubscriptionStats()).thenReturn(stats);
        when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    scheduledSnapshotTask.set(invocation.getArgument(0, Runnable.class));
                    return timer;
                });
        doAnswer(invocation -> {
            publishContexts.add(invocation.getArgument(1, Topic.PublishContext.class));
            return null;
        }).when(topic).publishMessage(any(ByteBuf.class), any(Topic.PublishContext.class));

        ReplicatedSubscriptionsController controller = new ReplicatedSubscriptionsController(topic, "local");
        String snapshotId = null;
        try {
            Assert.assertNotNull(scheduledSnapshotTask.get());
            scheduledSnapshotTask.get().run();
            Assert.assertEquals(controller.pendingSnapshots().size(), 1);
            snapshotId = controller.pendingSnapshots().keySet().iterator().next();
            Assert.assertEquals(publishContexts.size(), 1);

            ByteBuf responseMarker = Markers.newReplicatedSubscriptionsSnapshotResponse(snapshotId, "local",
                    "remote", 11, 11);
            try {
                Commands.skipMessageMetadata(responseMarker);
                controller.receivedReplicatedSubscriptionMarker(PositionFactory.create(1, 1),
                        MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_RESPONSE_VALUE, responseMarker);
            } finally {
                responseMarker.release();
            }
            Assert.assertEquals(publishContexts.size(), 2);

            publishContexts.get(1).completed(new RuntimeException("final marker publish failed"), -1, -1);

            Assert.assertTrue(controller.pendingSnapshots().containsKey(snapshotId),
                    "Snapshot should remain pending after the final snapshot marker publish fails");
            Assert.assertFalse(controller.getLastCompletedSnapshotId().isPresent(),
                    "Failed final snapshot marker publish should not update the last completed snapshot id");
        } finally {
            if (snapshotId != null && controller.pendingSnapshots().containsKey(snapshotId)) {
                controller.snapshotCompleted(snapshotId);
            }
            controller.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTimeoutCleanupDoesNotRecordTimeoutWhenSnapshotCompletedConcurrently() {
        PulsarService pulsar = mock(PulsarService.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("rawtypes")
        ScheduledFuture timer = mock(ScheduledFuture.class);
        ServiceConfiguration config = new ServiceConfiguration();
        config.setReplicatedSubscriptionsSnapshotFrequencyMillis(60_000);
        config.setReplicatedSubscriptionsSnapshotTimeoutSeconds(3);
        OpenTelemetryReplicatedSubscriptionStats stats = mock(OpenTelemetryReplicatedSubscriptionStats.class);
        BrokerService brokerService = mock(BrokerService.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        Replicator replicator = mock(Replicator.class);
        AtomicReference<Runnable> scheduledSnapshotTask = new AtomicReference<>();

        when(topic.getName()).thenReturn("persistent://public/default/t1");
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(topic.getLastMaxReadPositionMovedForwardTimestamp()).thenReturn(1L, 1L, 0L, 0L);
        when(topic.getReplicators()).thenReturn(Map.of("remote", replicator));
        when(replicator.isConnected()).thenReturn(true);
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(pulsar.getExecutor()).thenReturn(executor);
        when(pulsar.getConfiguration()).thenReturn(config);
        when(pulsar.getOpenTelemetryReplicatedSubscriptionStats()).thenReturn(stats);
        when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    scheduledSnapshotTask.set(invocation.getArgument(0, Runnable.class));
                    return timer;
                });

        ReplicatedSubscriptionsController controller = new ReplicatedSubscriptionsController(topic, "local");
        try {
            Assert.assertNotNull(scheduledSnapshotTask.get());
            scheduledSnapshotTask.get().run();
            Assert.assertEquals(controller.pendingSnapshots().size(), 1);
            String snapshotId = controller.pendingSnapshots().keySet().iterator().next();
            ReplicatedSubscriptionsSnapshotBuilder originalBuilder = controller.pendingSnapshots().get(snapshotId);

            AtomicBoolean completedDuringTimeoutCheck = new AtomicBoolean();
            ReplicatedSubscriptionsSnapshotBuilder completedBuilder =
                    new ReplicatedSubscriptionsSnapshotBuilder(controller, Set.of("remote"), config,
                            Clock.systemUTC()) {
                        @Override
                        boolean isTimedOut() {
                            if (completedDuringTimeoutCheck.compareAndSet(false, true)) {
                                controller.snapshotCompleted(snapshotId);
                            }
                            return true;
                        }
                    };
            Assert.assertTrue(controller.pendingSnapshots().replace(snapshotId, originalBuilder, completedBuilder));

            scheduledSnapshotTask.get().run();

            Assert.assertTrue(completedDuringTimeoutCheck.get());
            Assert.assertFalse(controller.pendingSnapshots().containsKey(snapshotId));
            verify(stats).recordSnapshotCompleted(anyLong());
            verify(stats, never()).recordSnapshotTimedOut(anyLong());
        } finally {
            controller.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshotRequestWhenReplicatorRemovedConcurrentlyDoesNotThrow() throws Exception {
        // Use a real PersistentTopic instance so that the replicator removal happens via the production code path
        // (PersistentTopic#removeReplicator), instead of directly doing "replicators.remove(..)" in the test.
        PulsarService pulsar = mock(PulsarService.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("rawtypes")
        ScheduledFuture timer = mock(ScheduledFuture.class);
        ServiceConfiguration config = new ServiceConfiguration();
        config.setReplicatorPrefix("pulsar.repl");
        config.setEnableReplicatedSubscriptions(true);
        OpenTelemetryReplicatedSubscriptionStats stats = mock(OpenTelemetryReplicatedSubscriptionStats.class);
        MonotonicClock monotonicClock = System::nanoTime;
        BacklogQuotaManager backlogQuotaManager = mock(BacklogQuotaManager.class);
        when(backlogQuotaManager.getDefaultQuota()).thenReturn(BacklogQuotaImpl.builder()
                .limitSize(0)
                .limitTime(0)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                .build());
        BrokerService brokerService = mock(BrokerService.class);
        ManagedLedger ledger = mock(ManagedLedger.class);
        MessageDeduplication messageDeduplication = mock(MessageDeduplication.class);

        when(brokerService.getClock()).thenReturn(Clock.systemUTC());
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.getBacklogQuotaManager()).thenReturn(backlogQuotaManager);

        when(pulsar.getExecutor()).thenReturn(executor);
        when(pulsar.getConfiguration()).thenReturn(config);
        when(pulsar.getOpenTelemetryReplicatedSubscriptionStats()).thenReturn(stats);
        when(pulsar.getMonotonicClock()).thenReturn(monotonicClock);
        when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(timer);

        doAnswer(invocation -> {
            DeleteCursorCallback callback = invocation.getArgument(1);
            callback.deleteCursorComplete(null);
            return null;
        }).when(ledger).asyncDeleteCursor(anyString(), any(DeleteCursorCallback.class), any());

        PersistentTopic topic =
                new PersistentTopic("persistent://public/default/t1", brokerService, ledger, messageDeduplication);

        Replicator replicator = mock(Replicator.class);
        when(replicator.terminate()).thenReturn(CompletableFuture.completedFuture(null));
        topic.getReplicators().put("remote", replicator);

        // Create a spy topic that blocks at the moment of "replicators.get(remote)" so we can deterministically
        // reproduce the race between:
        // 1) Replicator removal (policy update -> PersistentTopic#removeReplicator)
        // 2) Handling an in-flight snapshot request marker from that remote cluster
        PersistentTopic topicSpy = Mockito.spy(topic);
        Map<String, Replicator> delegateReplicators = topic.getReplicators();
        CountDownLatch enteredGet = new CountDownLatch(1);
        CountDownLatch allowGet = new CountDownLatch(1);
        Map<String, Replicator> blockingReplicators = new AbstractMap<>() {
            @Override
            public Replicator get(Object key) {
                enteredGet.countDown();
                try {
                    allowGet.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return delegateReplicators.get(key);
            }

            @Override
            public Set<Entry<String, Replicator>> entrySet() {
                return delegateReplicators.entrySet();
            }
        };
        when(topicSpy.getReplicators()).thenReturn(blockingReplicators);

        ReplicatedSubscriptionsController controller = new ReplicatedSubscriptionsController(topicSpy, "local");

        ByteBuf marker = Markers.newReplicatedSubscriptionsSnapshotRequest("snapshot-1", "remote");
        try {
            // The controller expects the "payload" buffer whose readerIndex already points to the marker payload.
            Commands.skipMessageMetadata(marker);

            @Cleanup("shutdownNow")
            var pool = Executors.newSingleThreadExecutor();
            var handlingFuture = pool.submit(() -> controller.receivedReplicatedSubscriptionMarker(
                    PositionFactory.create(1, 1),
                    MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST_VALUE,
                    marker));

            // Wait until the marker handling code is about to read the replicators map.
            Assert.assertTrue(enteredGet.await(10, TimeUnit.SECONDS));

            // Remove the replicator using the production code path.
            topic.removeReplicator("remote").join();
            verify(replicator).terminate();
            verify(ledger).asyncDeleteCursor(anyString(), any(DeleteCursorCallback.class), eq(null));

            // Let the marker handling continue and observe a missing replicator.
            allowGet.countDown();
            handlingFuture.get(10, TimeUnit.SECONDS);
        } finally {
            marker.release();
            controller.close();
        }
    }
}
