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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.stats.OpenTelemetryReplicatedSubscriptionStats;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Tests received UPDATE retry ownership through controlled public-API completion interleavings. */
@Test(groups = "broker-replication")
public class ReceivedReplicatedSubscriptionUpdateRetryConcurrencyTest {
    private static final String SUBSCRIPTION = "sub";

    @DataProvider
    public Object[][] completionOutcomes() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "completionOutcomes")
    public void testHigherTargetSurvivesOlderCompletion(boolean olderSucceeds) throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            CompletableFuture<Void> older = new CompletableFuture<>();
            AtomicBoolean recoverHigher = new AtomicBoolean();
            fixture.original.ackBehavior = (target, attempt) -> target.getEntryId() == 5 ? older
                    : recoverHigher.get() ? CompletableFuture.completedFuture(null) : failed("higher ACK failed");
            fixture.receive(5);
            fixture.receive(9);
            fixture.tick(2);
            complete(older, olderSucceeds);
            recoverHigher.set(true);
            fixture.tick(5);

            assertThat(fixture.original.markDelete.get()).isEqualTo(position(9));
            assertThat(fixture.original.ackTargets).contains(position(9));
            assertThat(fixture.original.ackTargets.stream().filter(position(5)::equals).count()).isEqualTo(1);
            assertThat(fixture.createAttempts.get()).isZero();
        }
    }

    @Test
    public void testOlderMarkerDoesNotDowngradePendingTarget() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            CompletableFuture<Void> first = new CompletableFuture<>();
            fixture.original.ackBehavior = (target, attempt) -> attempt == 1 ? first
                    : CompletableFuture.completedFuture(null);
            fixture.receive(9);
            fixture.receive(5);
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(9));
            first.completeExceptionally(new IllegalStateException("first ACK failed"));
            fixture.tick(5);

            assertThat(fixture.original.markDelete.get()).isEqualTo(position(9));
            assertThat(fixture.original.ackTargets).containsExactly(position(9), position(9));
        }
    }

    @Test
    public void testPendingAcknowledgementIsNotDuplicatedByTimerTicks() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            CompletableFuture<Void> pending = new CompletableFuture<>();
            fixture.original.ackBehavior = (target, attempt) -> pending;
            fixture.receive(5);
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(5));
            pending.complete(null);
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(5));
            assertThat(fixture.original.markDelete.get()).isEqualTo(position(5));
        }
    }

    @Test
    public void testPendingCreationIsNotDuplicatedByTimerTicks() throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            CompletableFuture<Subscription> pending = new CompletableFuture<>();
            fixture.createBehavior = attempt -> pending;
            fixture.receive(5);
            fixture.tick(5);
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.original.ackTargets).isEmpty();
            pending.complete(fixture.original.subscription);
            fixture.tick(5);
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.original.ackTargets).containsExactly(position(5));
        }
    }

    @Test
    public void testCreationRetryThenAcknowledgementRetryDoesNotCreateAgain() throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            fixture.createBehavior = attempt -> attempt == 1 ? failed("creation failed")
                    : CompletableFuture.completedFuture(fixture.original.subscription);
            fixture.original.ackBehavior = (target, attempt) -> attempt == 1 ? failed("ACK after creation failed")
                    : CompletableFuture.completedFuture(null);
            fixture.receive(5);
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.original.ackTargets).isEmpty();
            fixture.tick(1);
            assertThat(fixture.createAttempts.get()).isEqualTo(2);
            assertThat(fixture.original.ackTargets).containsExactly(position(5));
            fixture.tick(5);

            assertThat(fixture.createAttempts.get()).isEqualTo(2);
            assertThat(fixture.original.ackTargets).containsExactly(position(5), position(5));
            assertThat(fixture.original.markDelete.get()).isEqualTo(position(5));
        }
    }

    @Test
    public void testSynchronousAcknowledgementFailureRetainsRetry() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.original.ackBehavior = (target, attempt) -> {
                if (attempt == 1) {
                    throw new IllegalStateException("ACK method threw synchronously");
                }
                return CompletableFuture.completedFuture(null);
            };
            fixture.receive(5);
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(5), position(5));
            assertThat(fixture.original.markDelete.get()).isEqualTo(position(5));
        }
    }

    @Test
    public void testSynchronousCreationFailureRetainsRetry() throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            fixture.createBehavior = attempt -> {
                if (attempt == 1) {
                    throw new IllegalStateException("creation method threw synchronously");
                }
                return CompletableFuture.completedFuture(fixture.original.subscription);
            };
            fixture.receive(5);
            fixture.tick(5);
            assertThat(fixture.createAttempts.get()).isEqualTo(2);
            assertThat(fixture.original.ackTargets).containsExactly(position(5));
            assertThat(fixture.original.markDelete.get()).isEqualTo(position(5));
        }
    }

    @Test
    public void testInitialUpdateStillAppliesToExistingNonReplicatedSubscription() throws Exception {
        try (Fixture fixture = new Fixture(true, false)) {
            fixture.receive(5);
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(5));
            assertThat(fixture.original.markDelete.get()).isEqualTo(position(5));
            assertThat(fixture.createAttempts.get()).isZero();
        }
    }

    @Test
    public void testFailedAcknowledgementAlreadyCoveredByCursorIsRetired() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.original.ackBehavior = (target, attempt) -> {
                fixture.original.markDelete.set(target);
                return failed("receipt failed after cursor advanced");
            };
            fixture.receive(5);
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(5));
            assertThat(fixture.original.markDelete.get()).isEqualTo(position(5));
        }
    }

    @Test
    public void testDeletedBoundSubscriptionIsNotResurrected() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            CompletableFuture<Void> first = new CompletableFuture<>();
            fixture.original.ackBehavior = (target, attempt) -> first;
            fixture.receive(5);
            fixture.currentSubscription.set(null);
            first.completeExceptionally(new IllegalStateException("old subscription ACK failed"));
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(5));
            assertThat(fixture.createAttempts.get()).isZero();
            assertThat(fixture.currentSubscription.get()).isNull();
        }
    }

    @Test
    public void testReplacedBoundSubscriptionIgnoresOldUpdateButAcceptsFreshMarker() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            CompletableFuture<Void> first = new CompletableFuture<>();
            fixture.original.ackBehavior = (target, attempt) -> first;
            fixture.receive(5);
            SubscriptionState replacement = new SubscriptionState(true);
            fixture.currentSubscription.set(replacement.subscription);
            first.completeExceptionally(new IllegalStateException("old subscription ACK failed"));
            fixture.tick(5);
            assertThat(replacement.ackTargets).isEmpty();
            assertThat(fixture.createAttempts.get()).isZero();

            fixture.receive(9);
            fixture.tick(5);
            assertThat(replacement.ackTargets).containsExactly(position(9));
            assertThat(replacement.markDelete.get()).isEqualTo(position(9));
        }
    }

    @Test
    public void testCloseBlocksReadmissionAndLateCreationCannotIssueAcknowledgement() throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            CompletableFuture<Subscription> pending = new CompletableFuture<>();
            fixture.createBehavior = attempt -> pending;
            fixture.receive(5);
            fixture.controller.close();
            pending.complete(fixture.original.subscription);
            fixture.receive(9);
            fixture.tick(5);
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.original.ackTargets).isEmpty();
        }
    }

    @Test(dataProvider = "completionOutcomes")
    public void testClosePreventsRetryAfterLateAcknowledgementCompletion(boolean succeeds) throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            CompletableFuture<Void> pending = new CompletableFuture<>();
            fixture.original.ackBehavior = (target, attempt) -> pending;
            fixture.receive(5);
            fixture.controller.close();
            complete(pending, succeeds);
            fixture.receive(9);
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(5));
            assertThat(fixture.createAttempts.get()).isZero();
        }
    }

    @Test
    public void testReusedMarkerProtocolObjectCannotChangePendingNameOrPosition() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            CompletableFuture<Void> first = new CompletableFuture<>();
            fixture.original.ackBehavior = (target, attempt) -> attempt == 1 ? first
                    : CompletableFuture.completedFuture(null);
            fixture.receive(5);
            fixture.receiveIgnoredOtherSubscription();
            first.completeExceptionally(new IllegalStateException("retry after parser object reuse"));
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(5), position(5));
            assertThat(fixture.original.markDelete.get()).isEqualTo(position(5));
            assertThat(fixture.createAttempts.get()).isZero();
        }
    }

    @Test
    public void testLocalCursorAdvancingAfterFailureRetiresRetryWithoutAnotherAcknowledgement() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.original.ackBehavior = (target, attempt) -> failed("initial ACK failed");
            fixture.receive(5);
            fixture.original.markDelete.set(position(9));
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(5));
        }
    }

    @Test
    public void testDelayedCreationRetainsNameAndTargetAfterParserReuse() throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            CompletableFuture<Subscription> pending = new CompletableFuture<>();
            fixture.createBehavior = attempt -> pending;
            fixture.receive(5);
            fixture.receiveIgnoredOtherSubscription();
            fixture.original.ackBehavior = (target, attempt) -> attempt == 1 ? failed("ACK after parser reuse failed")
                    : CompletableFuture.completedFuture(null);
            pending.complete(fixture.original.subscription);
            fixture.tick(5);

            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            assertThat(fixture.original.ackTargets).containsExactly(position(5), position(5));
            assertThat(fixture.original.markDelete.get()).isEqualTo(position(5));
        }
    }

    @Test
    public void testStaleCreationCompletionDoesNotAcknowledgeReplacement() throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            CompletableFuture<Subscription> pending = new CompletableFuture<>();
            fixture.createBehavior = attempt -> pending;
            fixture.receive(5);
            SubscriptionState replacement = new SubscriptionState(true);
            fixture.currentSubscription.set(replacement.subscription);
            fixture.installCreatedSubscription = false;
            pending.complete(fixture.original.subscription);
            fixture.tick(5);

            assertThat(fixture.original.ackTargets).isEmpty();
            assertThat(replacement.ackTargets).isEmpty();
            assertThat(fixture.createAttempts.get()).isEqualTo(1);
            fixture.receive(9);
            assertThat(replacement.ackTargets).containsExactly(position(9));
        }
    }

    @Test
    public void testAcknowledgementCanAdmitNewerMarkerFromAnotherThread() throws Exception {
        try (Fixture fixture = new Fixture(true);
                ExecutorService executor = Executors.newSingleThreadExecutor(
                        new DefaultThreadFactory("received-update-admission"))) {
            AtomicBoolean admittedDuringAcknowledgement = new AtomicBoolean();
            fixture.original.ackBehavior = (target, attempt) -> {
                if (target.equals(position(5))) {
                    assertThat(CompletableFuture.runAsync(() -> fixture.receive(9), executor))
                            .as("The ACK call must run outside the controller admission lock")
                            .succeedsWithin(5, TimeUnit.SECONDS);
                    admittedDuringAcknowledgement.set(true);
                    return failed("older ACK failed after a newer marker was admitted");
                }
                return CompletableFuture.completedFuture(null);
            };
            fixture.receive(5);
            assertThat(admittedDuringAcknowledgement).isTrue();
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).containsExactly(position(5), position(9));
            assertThat(fixture.original.markDelete.get()).isEqualTo(position(9));
        }
    }

    @Test
    public void testCloseCancelsPeriodicRetryOnlyOnce() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.controller.close();
            fixture.controller.close();
            verify(fixture.timer).cancel(true);
            fixture.tick(5);
            assertThat(fixture.original.ackTargets).isEmpty();
        }
    }

    private static Position position(long entry) {
        return PositionFactory.create(1, entry);
    }

    private static <T> CompletableFuture<T> failed(String message) {
        return CompletableFuture.failedFuture(new IllegalStateException(message));
    }

    private static void complete(CompletableFuture<Void> future, boolean succeeds) {
        if (succeeds) {
            future.complete(null);
        } else {
            future.completeExceptionally(new IllegalStateException("older ACK failed"));
        }
    }

    private static final class SubscriptionState {
        private final PersistentSubscription subscription = mock(PersistentSubscription.class);
        private final AtomicReference<Position> markDelete = new AtomicReference<>(position(0));
        private final List<Position> ackTargets = new CopyOnWriteArrayList<>();
        private BiFunction<Position, Integer, CompletableFuture<Void>> ackBehavior =
                (target, attempt) -> CompletableFuture.completedFuture(null);

        private SubscriptionState(boolean replicated) {
            ManagedCursor cursor = mock(ManagedCursor.class);
            when(subscription.getCursor()).thenReturn(cursor);
            when(subscription.isReplicated()).thenReturn(replicated);
            when(cursor.getMarkDeletedPosition()).thenAnswer(invocation -> markDelete.get());
            when(subscription.acknowledgeMessageAsync(any(), eq(AckType.Cumulative), any()))
                    .thenAnswer(invocation -> {
                        List<Position> positions = invocation.getArgument(0);
                        assertThat(positions).hasSize(1);
                        Position target = positions.get(0);
                        ackTargets.add(target);
                        CompletableFuture<Void> completion = ackBehavior.apply(target, ackTargets.size());
                        return completion.thenRun(() -> markDelete.updateAndGet(current ->
                                current.compareTo(target) >= 0 ? current : target));
                    });
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final PersistentTopic topic = mock(PersistentTopic.class);
        private final SubscriptionState original;
        private final AtomicReference<PersistentSubscription> currentSubscription = new AtomicReference<>();
        private final AtomicInteger createAttempts = new AtomicInteger();
        private final AtomicReference<Runnable> timerTask = new AtomicReference<>();
        private final ScheduledFuture<?> timer = mock(ScheduledFuture.class);
        private IntFunction<CompletableFuture<Subscription>> createBehavior;
        private boolean installCreatedSubscription = true;
        private final ReplicatedSubscriptionsController controller;

        private Fixture(boolean exists) {
            this(exists, true);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private Fixture(boolean exists, boolean replicated) {
            original = new SubscriptionState(replicated);
            currentSubscription.set(exists ? original.subscription : null);
            createBehavior = attempt -> CompletableFuture.completedFuture(original.subscription);
            PulsarService pulsar = mock(PulsarService.class);
            BrokerService brokerService = mock(BrokerService.class);
            ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
            Replicator replicator = mock(Replicator.class);
            ServiceConfiguration config = new ServiceConfiguration();
            config.setEnableReplicatedSubscriptions(true);
            config.setReplicatedSubscriptionsSnapshotFrequencyMillis(1_000);

            when(topic.getName()).thenReturn("persistent://public/default/received-update-concurrency");
            when(topic.getBrokerService()).thenReturn(brokerService);
            when(topic.getLastMaxReadPositionMovedForwardTimestamp()).thenReturn(0L);
            when(topic.getReplicators()).thenReturn(Map.of("remote", replicator));
            when(topic.getSubscription(SUBSCRIPTION)).thenAnswer(invocation -> currentSubscription.get());
            when(replicator.isConnected()).thenReturn(true);
            when(brokerService.pulsar()).thenReturn(pulsar);
            when(brokerService.getPulsar()).thenReturn(pulsar);
            when(pulsar.getExecutor()).thenReturn(executor);
            when(pulsar.getConfiguration()).thenReturn(config);
            when(pulsar.getOpenTelemetryReplicatedSubscriptionStats())
                    .thenReturn(mock(OpenTelemetryReplicatedSubscriptionStats.class));
            when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                    .thenAnswer(invocation -> {
                        timerTask.set(invocation.getArgument(0, Runnable.class));
                        return timer;
                    });
            when(topic.createSubscription(eq(SUBSCRIPTION), eq(InitialPosition.Earliest), eq(true), any()))
                    .thenAnswer(invocation -> createBehavior.apply(createAttempts.incrementAndGet())
                            .thenApply(created -> {
                                assertThat(created).isInstanceOf(PersistentSubscription.class);
                                if (installCreatedSubscription) {
                                    currentSubscription.set((PersistentSubscription) created);
                                }
                                return created;
                            }));
            controller = new ReplicatedSubscriptionsController(topic, "local");
            assertThat(timerTask.get()).isNotNull();
        }

        private void receive(long entry) {
            receive(SUBSCRIPTION, Map.of("local", new MarkersMessageIdData().setLedgerId(1).setEntryId(entry)));
        }

        private void receiveIgnoredOtherSubscription() {
            receive("ignored-subscription", Map.of("other-cluster",
                    new MarkersMessageIdData().setLedgerId(9).setEntryId(999)));
        }

        private void receive(String subscriptionName, Map<String, MarkersMessageIdData> positions) {
            ByteBuf marker = Markers.newReplicatedSubscriptionsUpdate(subscriptionName, positions);
            try {
                Commands.skipMessageMetadata(marker);
                controller.receivedReplicatedSubscriptionMarker(PositionFactory.create(2, 9),
                        MarkerType.REPLICATED_SUBSCRIPTION_UPDATE_VALUE, marker);
            } finally {
                marker.release();
            }
        }

        private void tick(int count) {
            for (int i = 0; i < count; i++) {
                timerTask.get().run();
            }
        }

        @Override
        public void close() {
            controller.close();
        }
    }
}
