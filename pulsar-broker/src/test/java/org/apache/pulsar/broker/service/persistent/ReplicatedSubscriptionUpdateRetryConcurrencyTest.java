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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.stats.OpenTelemetryReplicatedSubscriptionStats;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsUpdate;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class ReplicatedSubscriptionUpdateRetryConcurrencyTest {
    @DataProvider
    public Object[][] outcomes() {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider
    public Object[][] invalidSubscriptions() {
        return new Object[][] {{"deleted"}, {"disabled"}, {"replaced"}, {"rewound"}};
    }

    @Test
    public void testPendingAppendIsNotDuplicatedAndSuccessRetiresIt() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.controller.localSubscriptionUpdated("sub", snapshot(10, 30));
            fixture.tick(5);
            assertThat(fixture.updates).hasSize(1);
            fixture.updates.get(0).complete(true);
            fixture.tick(5);
            assertThat(fixture.updates).hasSize(1);
            assertThat(fixture.updates.get(0).buffer.refCnt()).isZero();
        }
    }

    @Test
    public void testSynchronousAndAsynchronousFailuresKeepRetryingThenStop() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.throwNext = true;
            fixture.controller.localSubscriptionUpdated("sub", snapshot(10, 30));
            assertThat(fixture.updates).hasSize(1);
            fixture.tick(1);
            assertThat(fixture.updates).hasSize(2);
            fixture.updates.get(1).complete(false);
            fixture.tick(1);
            assertThat(fixture.updates).hasSize(3);
            fixture.updates.get(2).complete(true);
            fixture.tick(5);
            assertThat(fixture.updates).hasSize(3);
            assertThat(fixture.updates).allSatisfy(update -> assertThat(update.buffer.refCnt()).isZero());
        }
    }

    @Test(dataProvider = "outcomes")
    public void testOldCompletionCannotDiscardNewerCoherentSnapshot(boolean oldSucceeded) throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.controller.localSubscriptionUpdated("sub", snapshot(10, 30));
            UpdateAttempt old = fixture.updates.get(0);
            // Later local cuts need not be component-wise maxima of concurrent snapshot responses.
            fixture.controller.localSubscriptionUpdated("sub", snapshot(20, 7));
            old.complete(oldSucceeded);
            fixture.tick(1);
            assertThat(fixture.updates).hasSizeGreaterThanOrEqualTo(2);
            UpdateAttempt newer = fixture.updates.get(1);
            assertThat(newer.remoteEntry()).isEqualTo(7);
            newer.complete(false);
            fixture.tick(1);
            assertThat(fixture.updates).hasSize(3);
            assertThat(fixture.updates.get(2).remoteEntry()).isEqualTo(7);
            fixture.updates.get(2).complete(true);
            fixture.tick(5);
            assertThat(fixture.updates).hasSize(3);
        }
    }

    @Test
    public void testLateOlderCallerCannotReplaceNewerPendingCut() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.controller.localSubscriptionUpdated("sub", snapshot(20, 7));
            fixture.controller.localSubscriptionUpdated("sub", snapshot(10, 30));
            fixture.updates.get(0).complete(false);
            fixture.tick(1);
            assertThat(fixture.updates).hasSize(2);
            assertThat(fixture.updates.get(1).remoteEntry()).isEqualTo(7);
            fixture.updates.get(1).complete(true);
        }
    }

    @Test(dataProvider = "outcomes")
    public void testCloseRejectsReadmissionAndLateCompletionCannotRetry(boolean succeeded) throws Exception {
        Fixture fixture = new Fixture();
        try {
            fixture.controller.localSubscriptionUpdated("sub", snapshot(10, 30));
            fixture.controller.close();
            fixture.controller.localSubscriptionUpdated("sub", snapshot(20, 7));
            fixture.updates.get(0).complete(succeeded);
            fixture.tick(5);
            assertThat(fixture.updates).hasSize(1);
        } finally {
            fixture.close();
        }
    }

    @Test(dataProvider = "invalidSubscriptions")
    public void testInvalidatedSubscriptionDoesNotRetryOldState(String invalidation) throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.controller.localSubscriptionUpdated("sub", snapshot(10, 30));
            fixture.updates.get(0).complete(false);
            switch (invalidation) {
                case "deleted" -> fixture.current.set(null);
                case "disabled" -> when(fixture.subscription.isReplicated()).thenReturn(false);
                case "replaced" -> fixture.current.set(mock(PersistentSubscription.class));
                case "rewound" -> fixture.markDelete.set(PositionFactory.create(1, 0));
                default -> throw new AssertionError(invalidation);
            }
            fixture.tick(5);
            assertThat(fixture.updates).hasSize(1);
        }
    }

    @Test
    public void testValidLowerSnapshotAfterCursorResetReplacesInvalidHigherPendingCut() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.controller.localSubscriptionUpdated("sub", snapshot(20, 30));
            fixture.updates.get(0).complete(false);
            fixture.markDelete.set(PositionFactory.create(1, 6));
            fixture.controller.localSubscriptionUpdated("sub", snapshot(5, 2));
            fixture.tick(1);
            assertThat(fixture.updates).hasSize(2);
            assertThat(fixture.updates.get(1).remoteEntry()).isEqualTo(2);
            fixture.updates.get(1).complete(true);
            fixture.tick(5);
            assertThat(fixture.updates).hasSize(2);
        }
    }

    @Test
    public void testInlineSuccessfulPublishRetiresPendingEntryWithoutReentrantMutation() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.completeInline = true;
            fixture.controller.localSubscriptionUpdated("sub", snapshot(10, 30));
            fixture.tick(5);
            assertThat(fixture.updates).hasSize(1);
            assertThat(fixture.updates.get(0).buffer.refCnt()).isZero();
        }
    }

    private static ReplicatedSubscriptionSnapshotCache.SnapshotResult snapshot(long local, long remote) {
        Position position = PositionFactory.create(1, local);
        return new ReplicatedSubscriptionSnapshotCache.SnapshotResult(position, List.of(
                new ReplicatedSubscriptionSnapshotCache.ClusterEntry("local", position),
                new ReplicatedSubscriptionSnapshotCache.ClusterEntry("remote", PositionFactory.create(11, remote))));
    }

    private static final class UpdateAttempt {
        final Topic.PublishContext callback;
        final ReplicatedSubscriptionsUpdate update;
        final ByteBuf buffer;

        UpdateAttempt(Topic.PublishContext callback, ReplicatedSubscriptionsUpdate update, ByteBuf buffer) {
            this.callback = callback;
            this.update = update;
            this.buffer = buffer;
        }

        long remoteEntry() {
            return update.getClustersList().stream().filter(cluster -> cluster.getCluster().equals("remote"))
                    .findFirst().orElseThrow().getMessageId().getEntryId();
        }

        void complete(boolean succeeded) {
            callback.completed(succeeded ? null : new ManagedLedgerException("Injected append failure"),
                    succeeded ? 1 : -1, succeeded ? 100 : -1);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final class Fixture implements AutoCloseable {
        final PersistentSubscription subscription = mock(PersistentSubscription.class);
        final AtomicReference<PersistentSubscription> current = new AtomicReference<>(subscription);
        final AtomicReference<Position> markDelete = new AtomicReference<>(PositionFactory.create(1, 1_000));
        final List<UpdateAttempt> updates = new ArrayList<>();
        final AtomicReference<Runnable> timer = new AtomicReference<>();
        final ReplicatedSubscriptionsController controller;
        boolean throwNext;
        boolean completeInline;

        Fixture() throws Exception {
            PersistentTopic topic = mock(PersistentTopic.class);
            BrokerService broker = mock(BrokerService.class);
            PulsarService pulsar = mock(PulsarService.class);
            ManagedCursor cursor = mock(ManagedCursor.class);
            ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
            when(topic.getName()).thenReturn("persistent://public/default/update-retry-concurrency");
            when(topic.getBrokerService()).thenReturn(broker);
            when(topic.getSubscription("sub")).thenAnswer(ignored -> current.get());
            when(topic.getLastMaxReadPositionMovedForwardTimestamp()).thenReturn(0L);
            when(topic.getReplicators()).thenReturn(Map.of());
            when(subscription.isReplicated()).thenReturn(true);
            when(subscription.getCursor()).thenReturn(cursor);
            when(cursor.getMarkDeletedPosition()).thenAnswer(ignored -> markDelete.get());
            when(broker.pulsar()).thenReturn(pulsar);
            when(pulsar.getExecutor()).thenReturn(executor);
            when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());
            when(pulsar.getOpenTelemetryReplicatedSubscriptionStats())
                    .thenReturn(mock(OpenTelemetryReplicatedSubscriptionStats.class));
            when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                    .thenAnswer(invocation -> {
                        timer.set(invocation.getArgument(0, Runnable.class));
                        return mock(ScheduledFuture.class);
                    });
            doAnswer(invocation -> {
                ByteBuf marker = invocation.getArgument(0, ByteBuf.class);
                assertThat(Commands.peekMessageMetadata(marker, "update-retry", -1).getMarkerType())
                        .isEqualTo(MarkerType.REPLICATED_SUBSCRIPTION_UPDATE_VALUE);
                int index = marker.readerIndex();
                ReplicatedSubscriptionsUpdate update;
                try {
                    Commands.skipMessageMetadata(marker);
                    update = new ReplicatedSubscriptionsUpdate()
                            .copyFrom(Markers.parseReplicatedSubscriptionsUpdate(marker));
                } finally {
                    marker.readerIndex(index);
                }
                updates.add(new UpdateAttempt(invocation.getArgument(1, Topic.PublishContext.class), update, marker));
                if (throwNext) {
                    throwNext = false;
                    throw new IllegalStateException("Injected synchronous append rejection");
                }
                if (completeInline) {
                    updates.get(updates.size() - 1).complete(true);
                }
                return null;
            }).when(topic).publishMessage(any(ByteBuf.class), any(Topic.PublishContext.class));
            controller = new ReplicatedSubscriptionsController(topic, "local");
        }

        void tick(int count) {
            for (int i = 0; i < count; i++) {
                timer.get().run();
            }
        }

        @Override
        public void close() {
            controller.close();
        }
    }
}
