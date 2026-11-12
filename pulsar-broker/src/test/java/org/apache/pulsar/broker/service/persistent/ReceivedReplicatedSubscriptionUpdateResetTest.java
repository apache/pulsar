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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.stats.OpenTelemetryReplicatedSubscriptionStats;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Exercises real subscription and managed cursor resets with controllable BookKeeper completion. */
@Test(groups = "broker-replication", timeOut = 30000)
public class ReceivedReplicatedSubscriptionUpdateResetTest {

    @Test
    public void testUpdateReceivedDuringResetIsDiscardedAfterResetCompletes() throws Exception {
        try (Fixture fixture = new Fixture()) {
            long cursorLedgerEntry = fixture.cursor.getCursorLedgerLastEntry();
            CompletableFuture<Void> resetGate = fixture.bookkeeper.addEntryResponseGate();
            try {
                CompletableFuture<Void> reset = fixture.subscription.resetCursor(fixture.positions.get(1));
                Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(fixture.cursor.getCursorLedgerLastEntry()).isGreaterThan(cursorLedgerEntry));
                assertThat(reset).isNotDone();

                fixture.receive(5);
                assertThat(fixture.ackAttempts.get()).isZero();
                assertThat(fixture.lastAcknowledgement.get()).isNull();

                resetGate.complete(null);
                assertThat(reset).succeedsWithin(5, TimeUnit.SECONDS);
                assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(0));
                fixture.tick();
                assertThat(fixture.ackAttempts.get()).isZero();
                assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(0));

                fixture.receive(5);
                assertThat(fixture.lastAcknowledgement.get()).succeedsWithin(5, TimeUnit.SECONDS);
                assertThat(fixture.ackAttempts.get()).isEqualTo(1);
                assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(5));
            } finally {
                resetGate.complete(null);
            }
        }
    }

    @DataProvider
    public Object[][] resetScenarios() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "resetScenarios")
    public void testResetRetiresPendingTargetWhenMarkDeleteReturnsToAdmissionPosition(boolean rewindAndCatchUp)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.failNextAcknowledgement.set(true);
            fixture.receive(5);
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(2));

            // Both cases end at the admission position. Position comparison alone cannot observe the seek.
            int resetReadPosition = rewindAndCatchUp ? 1 : 3;
            assertThat(fixture.subscription.resetCursor(fixture.positions.get(resetReadPosition)))
                    .succeedsWithin(5, TimeUnit.SECONDS);
            if (rewindAndCatchUp) {
                fixture.cursor.markDelete(fixture.positions.get(2));
            }
            assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(2));
            fixture.tick();
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(2));
        }
    }

    @Test(dataProvider = "resetScenarios")
    public void testResetBetweenAdmissionAndFirstAcknowledgementRetiresTarget(boolean resetBackwards)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            int resetReadPosition = resetBackwards ? 1 : 3;
            fixture.beforeSubscriptionLookup = lookup -> {
                if (lookup == 2) {
                    // The first lookup admitted the target and captured its cursor state. Finish a
                    // real reset before validation returns and the controller invokes its first ACK.
                    assertThat(fixture.subscription.resetCursor(fixture.positions.get(resetReadPosition)))
                            .succeedsWithin(5, TimeUnit.SECONDS);
                }
            };
            fixture.receive(5);
            fixture.tick();
            assertThat(fixture.ackAttempts.get()).isZero();
            assertThat(fixture.cursor.getMarkDeletedPosition())
                    .isEqualTo(fixture.positions.get(resetReadPosition - 1));

            fixture.receive(5);
            assertThat(fixture.lastAcknowledgement.get()).succeedsWithin(5, TimeUnit.SECONDS);
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
        }
    }

    @Test(dataProvider = "resetScenarios")
    public void testResetAfterControllerChecksBeforeAcknowledgementAdmissionRetiresTarget(boolean resetBackwards)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            int resetReadPosition = resetBackwards ? 1 : 3;
            AtomicBoolean interleaved = new AtomicBoolean();
            doAnswer(invocation -> {
                if (interleaved.compareAndSet(false, true)) {
                    // All controller-side checks have returned. Complete the local seek at the
                    // final handoff, before the subscription atomically admits the physical ACK.
                    assertThat(fixture.subscription.resetCursor(fixture.positions.get(resetReadPosition)))
                            .succeedsWithin(5, TimeUnit.SECONDS);
                }
                return invocation.callRealMethod();
            }).when(fixture.subscription).acknowledgeReplicatedSubscriptionMessage(any(), any(), any(), anyBoolean());

            fixture.receive(5);
            fixture.tick();
            assertThat(interleaved).isTrue();
            assertThat(fixture.ackAttempts.get()).isZero();
            assertThat(fixture.cursor.getMarkDeletedPosition())
                    .isEqualTo(fixture.positions.get(resetReadPosition - 1));

            fixture.receive(5);
            assertThat(fixture.lastAcknowledgement.get()).succeedsWithin(5, TimeUnit.SECONDS);
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
        }
    }

    @Test
    public void testPendingAcknowledgementFailureDuringResetDoesNotRetryAfterCompletion() throws Exception {
        try (Fixture fixture = new Fixture()) {
            CompletableFuture<Void> pendingAcknowledgement = new CompletableFuture<>();
            fixture.nextAcknowledgement.set(pendingAcknowledgement);
            fixture.receive(5);
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.lastAcknowledgement.get()).isSameAs(pendingAcknowledgement).isNotDone();

            long cursorLedgerEntry = fixture.cursor.getCursorLedgerLastEntry();
            CompletableFuture<Void> resetGate = fixture.bookkeeper.addEntryResponseGate();
            try {
                CompletableFuture<Void> reset = fixture.subscription.resetCursor(fixture.positions.get(1));
                Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(fixture.cursor.getCursorLedgerLastEntry()).isGreaterThan(cursorLedgerEntry));
                assertThat(reset).isNotDone();
                pendingAcknowledgement.completeExceptionally(
                        new ManagedLedgerException.MetaStoreException("Injected delayed ACK failure during reset"));
                resetGate.complete(null);
                assertThat(reset).succeedsWithin(5, TimeUnit.SECONDS);
                fixture.tick();
                assertThat(fixture.ackAttempts.get()).isEqualTo(1);
                assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(0));
            } finally {
                resetGate.complete(null);
            }
        }
    }

    @Test
    public void testAdmittedResetFailureRetiresEarlierPendingTarget() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.failNextAcknowledgement.set(true);
            fixture.receive(5);
            fixture.bookkeeper.addEntryFailAfter(0, BKException.Code.WriteException);
            // The managed cursor falls back to metadata persistence after a BookKeeper error.
            fixture.metadataStore.failConditional(new MetadataStoreException("Injected cursor reset failure"),
                    (operation, path) -> operation == FaultInjectionMetadataStore.OperationType.PUT
                            && path.endsWith("/sub"));
            assertThat(fixture.subscription.resetCursor(fixture.positions.get(1)))
                    .failsWithin(5, TimeUnit.SECONDS);
            fixture.tick();
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(2));
        }
    }

    @Test(dataProvider = "resetScenarios")
    public void testTimerLateBindingRetiresTargetReceivedBeforeLocalReset(boolean resetBackwards) throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            fixture.createBehavior = attempt -> CompletableFuture.failedFuture(
                    new ManagedLedgerException.MetaStoreException("Injected transient subscription creation failure"));
            fixture.receive(5);
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.ackAttempts.get()).isZero();

            fixture.currentSubscription.set(fixture.subscription);
            int resetReadPosition = resetBackwards ? 1 : 3;
            assertThat(fixture.subscription.resetCursor(fixture.positions.get(resetReadPosition)))
                    .succeedsWithin(5, TimeUnit.SECONDS);
            fixture.tick();
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.ackAttempts.get()).isZero();
            assertThat(fixture.cursor.getMarkDeletedPosition())
                    .isEqualTo(fixture.positions.get(resetReadPosition - 1));

            fixture.receive(4);
            assertThat(fixture.lastAcknowledgement.get()).succeedsWithin(5, TimeUnit.SECONDS);
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(4));
        }
    }

    @Test(dataProvider = "resetScenarios")
    public void testCreationCompletionRetiresTargetReceivedBeforeLocalReset(boolean resetBackwards) throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            CompletableFuture<Subscription> creation = new CompletableFuture<>();
            fixture.createBehavior = attempt -> creation;
            fixture.receive(5);
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.ackAttempts.get()).isZero();

            fixture.currentSubscription.set(fixture.subscription);
            int resetReadPosition = resetBackwards ? 1 : 3;
            assertThat(fixture.subscription.resetCursor(fixture.positions.get(resetReadPosition)))
                    .succeedsWithin(5, TimeUnit.SECONDS);
            creation.complete(fixture.subscription);
            fixture.tick();
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.ackAttempts.get()).isZero();
            assertThat(fixture.cursor.getMarkDeletedPosition())
                    .isEqualTo(fixture.positions.get(resetReadPosition - 1));

            fixture.receive(4);
            assertThat(fixture.lastAcknowledgement.get()).succeedsWithin(5, TimeUnit.SECONDS);
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(4));
        }
    }

    @Test(dataProvider = "resetScenarios")
    public void testFreshUpdateAfterLocalResetDoesNotRetainHigherUnboundTarget(boolean resetBackwards)
            throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            fixture.createBehavior = attempt -> CompletableFuture.failedFuture(
                    new ManagedLedgerException.MetaStoreException("Injected transient subscription creation failure"));
            fixture.receive(5);
            assertThat(fixture.createAttempts.get()).isEqualTo(1);

            fixture.currentSubscription.set(fixture.subscription);
            int resetReadPosition = resetBackwards ? 1 : 3;
            assertThat(fixture.subscription.resetCursor(fixture.positions.get(resetReadPosition)))
                    .succeedsWithin(5, TimeUnit.SECONDS);
            // The fresh marker reaches admission before the timer has discovered the local subscription.
            fixture.receive(4);
            assertThat(fixture.lastAcknowledgement.get()).succeedsWithin(5, TimeUnit.SECONDS);
            fixture.tick();
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(4));
        }
    }

    @Test(dataProvider = "resetScenarios")
    public void testLateBindingWithoutLocalResetStillAppliesPendingTarget(boolean creationCompletes) throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            CompletableFuture<Subscription> creation = new CompletableFuture<>();
            fixture.createBehavior = attempt -> creation;
            fixture.receive(5);
            assertThat(fixture.createAttempts.get()).isEqualTo(1);

            fixture.currentSubscription.set(fixture.subscription);
            if (creationCompletes) {
                creation.complete(fixture.subscription);
            } else {
                creation.completeExceptionally(new ManagedLedgerException.MetaStoreException(
                        "Injected transient subscription creation failure"));
                fixture.tick();
            }
            assertThat(fixture.lastAcknowledgement.get()).succeedsWithin(5, TimeUnit.SECONDS);
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(5));
        }
    }

    @Test
    public void testFreshLowerUpdateWithoutLocalResetPreservesHigherUnboundTarget() throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            fixture.createBehavior = attempt -> CompletableFuture.failedFuture(
                    new ManagedLedgerException.MetaStoreException("Injected transient subscription creation failure"));
            fixture.receive(5);
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.ackAttempts.get()).isZero();

            fixture.currentSubscription.set(fixture.subscription);
            fixture.receive(4);
            assertThat(fixture.lastAcknowledgement.get()).succeedsWithin(5, TimeUnit.SECONDS);
            fixture.tick();
            assertThat(fixture.ackAttempts.get()).isEqualTo(1);
            assertThat(fixture.cursor.getMarkDeletedPosition()).isEqualTo(fixture.positions.get(5));
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final OrderedExecutor bookkeeperExecutor = OrderedExecutor.newBuilder()
                .name("received-update-reset-bk").numThreads(1).build();
        private final PulsarMockBookKeeper bookkeeper;
        private final FaultInjectionMetadataStore metadataStore;
        private final ManagedLedgerFactoryImpl factory;
        private final ManagedLedger ledger;
        private final ManagedCursorImpl cursor;
        private final List<Position> positions = new ArrayList<>();
        private final PersistentSubscription subscription;
        private final AtomicReference<PersistentSubscription> currentSubscription = new AtomicReference<>();
        private final AtomicInteger createAttempts = new AtomicInteger();
        private IntFunction<CompletableFuture<Subscription>> createBehavior;
        private final ReplicatedSubscriptionsController controller;
        private final AtomicInteger ackAttempts = new AtomicInteger();
        private final AtomicInteger subscriptionLookups = new AtomicInteger();
        private IntConsumer beforeSubscriptionLookup = ignored -> { };
        private final AtomicBoolean failNextAcknowledgement = new AtomicBoolean();
        private final AtomicReference<CompletableFuture<Void>> lastAcknowledgement = new AtomicReference<>();
        private final AtomicReference<CompletableFuture<Void>> nextAcknowledgement = new AtomicReference<>();
        private final AtomicReference<Runnable> timerTask = new AtomicReference<>();

        private Fixture() throws Exception {
            this(true);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private Fixture(boolean exists) throws Exception {
            metadataStore = new FaultInjectionMetadataStore(MetadataStoreExtended.create(
                    "memory:received-update-reset-" + UUID.randomUUID(), MetadataStoreConfig.builder().build()));
            bookkeeper = new PulsarMockBookKeeper(bookkeeperExecutor);
            factory = new ManagedLedgerFactoryImpl(metadataStore, bookkeeper);
            ManagedLedgerConfig ledgerConfig = new ManagedLedgerConfig().setThrottleMarkDelete(0)
                    .setRetentionSizeInMB(10).setRetentionTime(1, TimeUnit.HOURS);
            ledger = factory.open("received-update-reset", ledgerConfig);
            cursor = (ManagedCursorImpl) ledger.openCursor("sub", InitialPosition.Earliest);
            for (int i = 0; i < 6; i++) {
                positions.add(ledger.addEntry(new byte[] {(byte) i}));
            }
            cursor.markDelete(positions.get(2));

            PulsarService pulsar = mock(PulsarService.class);
            BrokerService brokerService = mock(BrokerService.class);
            PersistentTopic topic = mock(PersistentTopic.class);
            ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
            ServiceConfiguration config = new ServiceConfiguration();
            config.setReplicatedSubscriptionsSnapshotFrequencyMillis(1000);
            when(topic.getName()).thenReturn("persistent://public/default/received-update-reset");
            when(topic.getBrokerService()).thenReturn(brokerService);
            when(topic.getManagedLedger()).thenReturn(ledger);
            when(brokerService.pulsar()).thenReturn(pulsar);
            when(brokerService.getPulsar()).thenReturn(pulsar);
            when(pulsar.getConfig()).thenReturn(config);
            when(pulsar.getConfiguration()).thenReturn(config);
            when(pulsar.getOpenTelemetryReplicatedSubscriptionStats())
                    .thenReturn(mock(OpenTelemetryReplicatedSubscriptionStats.class));
            when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                    .thenAnswer(invocation -> {
                        timerTask.set(invocation.getArgument(0));
                        return mock(ScheduledFuture.class);
                    });
            subscription = spy(new PersistentSubscription(topic, "sub", cursor, null));
            currentSubscription.set(exists ? subscription : null);
            createBehavior = attempt -> CompletableFuture.completedFuture(subscription);
            when(topic.getSubscription("sub")).thenAnswer(ignored -> {
                beforeSubscriptionLookup.accept(subscriptionLookups.incrementAndGet());
                return currentSubscription.get();
            });
            when(topic.createSubscription(eq("sub"), eq(InitialPosition.Earliest), eq(true), any()))
                    .thenAnswer(ignored -> createBehavior.apply(createAttempts.incrementAndGet()));
            doAnswer(invocation -> {
                ackAttempts.incrementAndGet();
                CompletableFuture<Void> result = nextAcknowledgement.getAndSet(null);
                if (result == null) {
                    result = failNextAcknowledgement.compareAndSet(true, false)
                            ? CompletableFuture.failedFuture(new ManagedLedgerException.MetaStoreException(
                                    "Injected transient metadata failure before cursor acknowledgement"))
                            : (CompletableFuture<Void>) invocation.callRealMethod();
                }
                lastAcknowledgement.set(result);
                return result;
            }).when(subscription).acknowledgeMessageAsync(any(), eq(AckType.Cumulative), any());
            controller = new ReplicatedSubscriptionsController(topic, "local", executor);
        }

        private void receive(int entry) {
            Position target = positions.get(entry);
            ByteBuf marker = Markers.newReplicatedSubscriptionsUpdate("sub", Map.of("local",
                    new MarkersMessageIdData().setLedgerId(target.getLedgerId()).setEntryId(target.getEntryId())));
            try {
                Commands.skipMessageMetadata(marker);
                controller.receivedReplicatedSubscriptionMarker(target, MarkerType.REPLICATED_SUBSCRIPTION_UPDATE_VALUE,
                        marker);
            } finally {
                marker.release();
            }
        }

        private void tick() {
            for (int i = 0; i < 5; i++) {
                timerTask.get().run();
            }
        }

        @Override
        public void close() throws Exception {
            controller.close();
            try {
                factory.shutdown();
            } finally {
                bookkeeper.shutdown();
                bookkeeperExecutor.shutdownNow();
                metadataStore.close();
            }
        }
    }
}
