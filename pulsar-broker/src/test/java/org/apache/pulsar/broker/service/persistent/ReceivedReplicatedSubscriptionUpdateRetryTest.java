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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.stats.OpenTelemetryReplicatedSubscriptionStats;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Regressions for received subscription updates after transient local persistence failures.
 * Isolates one received UPDATE with no second delivery, local user write or local ACK.
 * Does not claim to emulate real BookKeeper cursor failure or replicator cursor persistence.
 */
@Test(groups = "broker-replication")
public class ReceivedReplicatedSubscriptionUpdateRetryTest {

    @DataProvider
    public Object[][] snapshotGates() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "snapshotGates")
    public void testReceivedUpdateRetriesFailedCursorAcknowledgementWithoutNewTraffic(boolean idle) throws Exception {
        try (Fixture fixture = new Fixture(true, true, false, idle)) {
            fixture.receiveOneUpdate();
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.markDelete.get()).isEqualTo(PositionFactory.create(1, 0));
            assertThat(fixture.failAck.get()).isFalse();

            fixture.tickFiveTimes();

            assertThat(fixture.ackAttempts.get())
                    .as("One received UPDATE must retry its failed target-cursor ACK after recovery")
                    .isGreaterThanOrEqualTo(2);
            assertThat(fixture.markDelete.get()).isEqualTo(PositionFactory.create(1, 5));
            assertThat(fixture.createAttempts.get()).isZero();
            verify(fixture.topic, never()).publishMessage(any(ByteBuf.class), any(Topic.PublishContext.class));
        }
    }

    @Test(dataProvider = "snapshotGates")
    public void testReceivedUpdateRetriesFailedSubscriptionCreationWithoutNewTraffic(boolean idle) throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, idle)) {
            fixture.receiveOneUpdate();
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.ackAttempts.get()).isZero();
            assertThat(fixture.currentSubscription.get()).isNull();
            assertThat(fixture.failCreate.get()).isFalse();

            fixture.tickFiveTimes();

            assertThat(fixture.createAttempts.get())
                    .as("One received UPDATE must retry its failed target-subscription creation after recovery")
                    .isGreaterThanOrEqualTo(2);
            assertThat(fixture.currentSubscription.get()).isSameAs(fixture.subscription);
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.markDelete.get()).isEqualTo(PositionFactory.create(1, 5));
            verify(fixture.topic, never()).publishMessage(any(ByteBuf.class), any(Topic.PublishContext.class));
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final PersistentTopic topic = mock(PersistentTopic.class);
        private final PersistentSubscription subscription = mock(PersistentSubscription.class);
        private final AtomicReference<PersistentSubscription> currentSubscription = new AtomicReference<>();
        private final AtomicReference<Position> markDelete = new AtomicReference<>(PositionFactory.create(1, 0));
        private final AtomicReference<Runnable> periodicTask = new AtomicReference<>();
        private final AtomicInteger ackAttempts = new AtomicInteger();
        private final AtomicInteger createAttempts = new AtomicInteger();
        private final AtomicBoolean failAck;
        private final AtomicBoolean failCreate;
        private final ReplicatedSubscriptionsController controller;

        @SuppressWarnings({"rawtypes", "unchecked"})
        private Fixture(boolean exists, boolean failFirstAck, boolean failFirstCreate, boolean idle) {
            failAck = new AtomicBoolean(failFirstAck);
            failCreate = new AtomicBoolean(failFirstCreate);
            currentSubscription.set(exists ? subscription : null);
            PulsarService pulsar = mock(PulsarService.class);
            BrokerService brokerService = mock(BrokerService.class);
            ManagedCursor cursor = mock(ManagedCursor.class);
            ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
            ScheduledFuture timer = mock(ScheduledFuture.class);
            Replicator replicator = mock(Replicator.class);
            ServiceConfiguration config = new ServiceConfiguration();
            config.setEnableReplicatedSubscriptions(true);
            config.setReplicatedSubscriptionsSnapshotFrequencyMillis(1_000);

            when(topic.getName()).thenReturn("persistent://public/default/received-update-retry-repro");
            when(topic.getBrokerService()).thenReturn(brokerService);
            when(topic.getLastMaxReadPositionMovedForwardTimestamp()).thenReturn(idle ? 0L : 1L);
            when(topic.getReplicators()).thenReturn(Map.of("remote", replicator));
            when(replicator.isConnected()).thenReturn(idle);
            when(brokerService.pulsar()).thenReturn(pulsar);
            when(brokerService.getPulsar()).thenReturn(pulsar);
            when(pulsar.getExecutor()).thenReturn(executor);
            when(pulsar.getConfiguration()).thenReturn(config);
            when(pulsar.getOpenTelemetryReplicatedSubscriptionStats())
                    .thenReturn(mock(OpenTelemetryReplicatedSubscriptionStats.class));
            when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                    .thenAnswer(invocation -> {
                        periodicTask.set(invocation.getArgument(0, Runnable.class));
                        return timer;
                    });
            when(subscription.getCursor()).thenReturn(cursor);
            when(subscription.isReplicated()).thenReturn(true);
            when(cursor.getMarkDeletedPosition()).thenAnswer(invocation -> markDelete.get());
            when(topic.getSubscription("sub")).thenAnswer(invocation -> currentSubscription.get());
            when(subscription.acknowledgeMessageAsync(any(), eq(AckType.Cumulative), any()))
                    .thenAnswer(invocation -> {
                        List<Position> positions = invocation.getArgument(0);
                        assertThat(positions).containsExactly(PositionFactory.create(1, 5));
                        ackAttempts.incrementAndGet();
                        if (failAck.compareAndSet(true, false)) {
                            return CompletableFuture.<Void>failedFuture(
                                    new ManagedLedgerException("Injected received UPDATE cursor ACK failure"));
                        }
                        markDelete.set(positions.get(0));
                        return CompletableFuture.<Void>completedFuture(null);
                    });
            when(topic.createSubscription(eq("sub"), eq(InitialPosition.Earliest), eq(true), any()))
                    .thenAnswer(invocation -> {
                        createAttempts.incrementAndGet();
                        if (failCreate.compareAndSet(true, false)) {
                            return CompletableFuture.<Subscription>failedFuture(
                                    new ManagedLedgerException(
                                            "Injected received UPDATE subscription creation failure"));
                        }
                        currentSubscription.set(subscription);
                        return CompletableFuture.<Subscription>completedFuture(subscription);
                    });
            controller = new ReplicatedSubscriptionsController(topic, "local");
            assertThat(periodicTask.get()).isNotNull();
        }

        private void receiveOneUpdate() {
            ByteBuf update = Markers.newReplicatedSubscriptionsUpdate("sub",
                    Map.of("local", new MarkersMessageIdData().setLedgerId(1).setEntryId(5)));
            try {
                Commands.skipMessageMetadata(update);
                controller.receivedReplicatedSubscriptionMarker(PositionFactory.create(2, 9),
                        MarkerType.REPLICATED_SUBSCRIPTION_UPDATE_VALUE, update);
            } finally {
                update.release();
            }
        }

        private void tickFiveTimes() {
            for (int i = 0; i < 5; i++) {
                periodicTask.get().run();
            }
            assertThat(controller.pendingSnapshots()).isEmpty();
        }

        @Override
        public void close() {
            controller.close();
        }
    }
}
