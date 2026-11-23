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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.stats.OpenTelemetryReplicatedSubscriptionStats;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.annotations.Test;

/**
 * Regression for a transient subscription update append failure without further data or ACKs.
 *
 * This isolates the controller's responsibility after one transient UPDATE append failure.
 * It does not emulate BookKeeper fencing, ownership changes, or topic reload recovery.
 * The only timer is captured and ticked directly; no real sleeps or private-state reflection.
 */
@Test(groups = "broker-replication")
public class ReplicatedSubscriptionUpdateRetryTest {

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testUpdateAppendFailureRetriesWithoutFurtherDataOrAcknowledgements() throws Exception {
        PulsarService pulsar = mock(PulsarService.class);
        BrokerService brokerService = mock(BrokerService.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        Replicator replicator = mock(Replicator.class);
        PersistentSubscription subscription = mock(PersistentSubscription.class);
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(subscription.isReplicated()).thenReturn(true);
        when(subscription.getCursor()).thenReturn(cursor);
        when(cursor.getMarkDeletedPosition()).thenReturn(PositionFactory.create(1, 3));
        when(topic.getSubscription("sub")).thenReturn(subscription);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        ScheduledFuture timer = mock(ScheduledFuture.class);
        ServiceConfiguration config = new ServiceConfiguration();
        config.setReplicatedSubscriptionsSnapshotFrequencyMillis(1_000);
        config.setReplicatedSubscriptionsSnapshotTimeoutSeconds(30);
        AtomicReference<Runnable> periodicTask = new AtomicReference<>();
        AtomicReference<ReplicatedSubscriptionsSnapshot> completedMarker = new AtomicReference<>();
        AtomicBoolean failFirstUpdate = new AtomicBoolean(true);
        AtomicInteger appendOrdinal = new AtomicInteger();
        AtomicInteger updateAppendAttempts = new AtomicInteger();
        AtomicInteger successfulUpdates = new AtomicInteger();
        List<Integer> markerTypes = new ArrayList<>();

        when(topic.getName()).thenReturn("persistent://public/default/update-retry-repro");
        when(topic.getBrokerService()).thenReturn(brokerService);
        // One historical activity timestamp. It never moves during this test.
        when(topic.getLastMaxReadPositionMovedForwardTimestamp()).thenReturn(1L);
        when(topic.getReplicators()).thenReturn(Map.of("remote", replicator));
        when(replicator.isConnected()).thenReturn(true);
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(pulsar.getExecutor()).thenReturn(executor);
        when(pulsar.getConfiguration()).thenReturn(config);
        when(pulsar.getOpenTelemetryReplicatedSubscriptionStats())
                .thenReturn(mock(OpenTelemetryReplicatedSubscriptionStats.class));
        when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    periodicTask.set(invocation.getArgument(0, Runnable.class));
                    return timer;
                });
        doAnswer(invocation -> {
            ByteBuf marker = invocation.getArgument(0, ByteBuf.class);
            Topic.PublishContext callback = invocation.getArgument(1, Topic.PublishContext.class);
            int markerType = Commands.peekMessageMetadata(marker, "update-retry-repro", -1).getMarkerType();
            markerTypes.add(markerType);
            if (markerType == MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_VALUE) {
                int readerIndex = marker.readerIndex();
                try {
                    Commands.skipMessageMetadata(marker);
                    completedMarker.set(new ReplicatedSubscriptionsSnapshot()
                            .copyFrom(Markers.parseReplicatedSubscriptionsSnapshot(marker)));
                } finally {
                    marker.readerIndex(readerIndex);
                }
            }
            if (markerType == MarkerType.REPLICATED_SUBSCRIPTION_UPDATE_VALUE) {
                updateAppendAttempts.incrementAndGet();
                if (failFirstUpdate.compareAndSet(true, false)) {
                    callback.completed(new ManagedLedgerException("Injected UPDATE append failure"), -1, -1);
                    return null;
                }
                successfulUpdates.incrementAndGet();
            }
            callback.completed(null, 1, appendOrdinal.incrementAndGet());
            return null;
        }).when(topic).publishMessage(any(ByteBuf.class), any(Topic.PublishContext.class));

        ReplicatedSubscriptionsController controller = new ReplicatedSubscriptionsController(topic, "local");
        try {
            assertThat(periodicTask.get()).isNotNull();
            periodicTask.get().run();
            assertThat(controller.pendingSnapshots()).hasSize(1);
            String snapshotId = controller.pendingSnapshots().keySet().iterator().next();

            // The response is a real protocol marker, with local position 1:2 after request 1:1.
            appendOrdinal.set(2);
            ByteBuf response = Markers.newReplicatedSubscriptionsSnapshotResponse(
                    snapshotId, "local", "remote", 11, 11);
            try {
                Commands.skipMessageMetadata(response);
                controller.receivedReplicatedSubscriptionMarker(PositionFactory.create(1, 2),
                        MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_RESPONSE_VALUE, response);
            } finally {
                response.release();
            }
            assertThat(controller.pendingSnapshots()).isEmpty();
            assertThat(controller.getLastCompletedSnapshotId()).contains(snapshotId);
            assertThat(completedMarker.get()).isNotNull();

            ReplicatedSubscriptionSnapshotCache cache = new ReplicatedSubscriptionSnapshotCache(
                    "sub", 3, ignored -> 1L);
            cache.addNewSnapshot(completedMarker.get());
            ReplicatedSubscriptionSnapshotCache.SnapshotResult selected =
                    cache.advancedMarkDeletePosition(PositionFactory.create(1, 3));
            assertThat(selected).isNotNull();
            assertThat(cache.size()).isZero();
            assertThat(cache.advancedMarkDeletePosition(PositionFactory.create(1, 3))).isNull();

            // Exactly one subscription update call: its first append fails, then the fault is gone.
            controller.localSubscriptionUpdated("sub", selected);
            assertThat(updateAppendAttempts.get()).isEqualTo(1);
            assertThat(successfulUpdates.get()).isZero();
            assertThat(failFirstUpdate.get()).isFalse();

            // Tick only the controller's actual periodic work. No new data, ACK, snapshot response,
            // cache insertion or localSubscriptionUpdated call is introduced to mask the lost update.
            for (int i = 0; i < 5; i++) {
                periodicTask.get().run();
            }
            assertThat(controller.pendingSnapshots()).isEmpty();
            assertThat(markerTypes.stream()
                    .filter(type -> type == MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST_VALUE).count())
                    .as("Quiescent timer ticks must not create another snapshot as a recovery shortcut")
                    .isEqualTo(1);
            assertThat(updateAppendAttempts.get())
                    .as("A failed UPDATE must retry after recovery without another local ACK or user write")
                    .isGreaterThanOrEqualTo(2);
            assertThat(successfulUpdates.get()).isGreaterThanOrEqualTo(1);
        } finally {
            controller.close();
        }
    }
}
