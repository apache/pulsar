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
package org.apache.pulsar.broker.service.scalable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator.ConsumerStats;
import org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator.RebalanceReason;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SubscriptionCoordinatorMetricsTest {
    private ScalableTopicResources resources;
    private ScheduledExecutorService scheduler;
    private List<Runnable> scheduled;
    private List<RebalanceReason> rebalances;
    private List<SubscriptionCoordinator> coordinators;

    @BeforeMethod
    public void setup() {
        resources = mock(ScalableTopicResources.class);
        when(resources.registerConsumerAsync(any(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(resources.unregisterConsumerAsync(any(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        scheduler = mock(ScheduledExecutorService.class);
        scheduled = new ArrayList<>();
        rebalances = new ArrayList<>();
        coordinators = new ArrayList<>();
        when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    scheduled.add(invocation.getArgument(0));
                    return mock(ScheduledFuture.class);
                });
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        coordinators.forEach(SubscriptionCoordinator::close);
    }

    @Test
    public void testSharedSegmentConsumersAndDisconnectedIdleMembers() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(4), true);
        for (int i = 1; i <= 5; i++) {
            register(coordinator, "c" + i, i);
        }
        assertStats(coordinator, 4, 1, 0, 0);
        assertThat(coordinator.currentAssignment().values())
                .as("all four owners of the shared segment count as active")
                .filteredOn(assignment -> !assignment.assignedSegments().isEmpty())
                .hasSize(4)
                .allSatisfy(assignment -> {
                    assertThat(assignment.assignedSegments()).hasSize(1);
                    assertThat(assignment.assignedSegments().get(0).segmentId()).isZero();
                    assertThat(assignment.assignedSegments().get(0).bucketRanges()).hasSize(4);
                });
        assertThat(rebalances).containsOnly(RebalanceReason.CONSUMER_JOIN).hasSize(5);

        Map<ConsumerSession, ConsumerAssignment> beforeDisconnect = coordinator.currentAssignment();
        coordinator.onConsumerDisconnect("c1");
        Runnable firstExpiry = scheduled.get(scheduled.size() - 1);
        coordinator.onConsumerDisconnect("c5");
        assertStats(coordinator, 3, 0, 2, 0);
        coordinator.onConsumerDisconnect("c5");
        assertStats(coordinator, 3, 0, 2, 0);

        register(coordinator, "c1", 11);
        register(coordinator, "c5", 15);
        firstExpiry.run();
        assertStats(coordinator, 4, 1, 0, 0);
        assertThat(coordinator.currentAssignment()).as("reconnect preserves assignments")
                .isEqualTo(beforeDisconnect);
        assertThat(rebalances).as("disconnects, reconnects, stale grace tasks and snapshots do not rebalance")
                .containsOnly(RebalanceReason.CONSUMER_JOIN).hasSize(5);
    }

    @Test
    public void testCheckpointSurplusConsumersAreIdle() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(4), false);
        coordinator.markCheckpointGroup();
        for (int i = 1; i <= 5; i++) {
            register(coordinator, "c" + i, i);
        }
        assertStats(coordinator, 1, 4, 0, 0);
        coordinator.onLayoutChange(initialLayout(4).splitSegment(0, 0L)).join();
        assertStats(coordinator, 3, 2, 0, 0);
    }

    @Test
    public void testLayoutDrainAndLastLeaveRebalanceReasons() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), true);
        register(coordinator, "c1", 1);
        register(coordinator, "c2", 2);
        coordinator.onLayoutChange(initialLayout(1).splitSegment(0, 0L)).join();
        assertStats(coordinator, 1, 1, 0, 2);

        coordinator.markSegmentsDrained(Set.of(0L));
        assertStats(coordinator, 2, 0, 0, 0);
        coordinator.markSegmentsDrained(Set.of(0L));
        coordinator.unregisterConsumer("unknown", 99).join();
        coordinator.unregisterConsumer("c1", 99).join();
        coordinator.unregisterConsumer("c2", 2).join();
        coordinator.unregisterConsumer("c1", 1).join();
        assertStats(coordinator, 0, 0, 0, 0);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN,
                RebalanceReason.CONSUMER_JOIN, RebalanceReason.LAYOUT_CHANGE,
                RebalanceReason.SEGMENT_DRAINED, RebalanceReason.CONSUMER_LEAVE,
                RebalanceReason.CONSUMER_LEAVE);
    }

    @Test
    public void testBlockedSegmentsExcludeDrainedSealedSegmentsButCheckEveryAncestor() {
        SegmentLayout layout = initialLayout(1).splitSegment(0, 0L).splitSegment(2, 0L);
        SubscriptionCoordinator coordinator = coordinator(layout, true);
        register(coordinator, "c1", 1);
        assertStats(coordinator, 1, 0, 0, 4);

        // Segment 2 is empty and drained, but its children still wait for grandparent 0.
        coordinator.markSegmentsDrained(Set.of(2L));
        assertStats(coordinator, 1, 0, 0, 3);
        coordinator.markSegmentsDrained(Set.of(0L));
        assertStats(coordinator, 1, 0, 0, 0);
        assertThat(coordinator.currentAssignment().values().iterator().next().assignedSegments())
                .as("drained parents retire rather than counting as blocked")
                .extracting(ConsumerAssignment.AssignedSegment::segmentId)
                .containsExactly(1L, 3L, 4L);
    }

    @Test
    public void testRestoreAndDrainCheckerInstallationDoNotCountAsRebalances() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1).splitSegment(0, 0L), false);
        coordinator.restoreConsumers(List.of("c1", "c2"));
        assertStats(coordinator, 0, 0, 2, 0);
        coordinator.installDrainChecker((segment, subscription) -> CompletableFuture.completedFuture(false));
        assertStats(coordinator, 0, 0, 2, 2);
        register(coordinator, "c1", 1);
        assertStats(coordinator, 1, 0, 1, 2);
        register(coordinator, "c2", 2);
        assertStats(coordinator, 1, 1, 0, 2);
        assertThat(rebalances).as("restore, checker recovery and reconnects are not new membership").isEmpty();
    }

    @Test
    public void testGraceEvictionCountsOnceIncludingLastConsumer() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), false);
        register(coordinator, "c1", 1);
        register(coordinator, "c2", 2);
        coordinator.onConsumerDisconnect("c1");
        Runnable firstExpiry = scheduled.get(scheduled.size() - 1);
        firstExpiry.run();
        firstExpiry.run();
        assertStats(coordinator, 1, 0, 0, 0);
        coordinator.onConsumerDisconnect("c2");
        scheduled.get(scheduled.size() - 1).run();
        assertStats(coordinator, 0, 0, 0, 0);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN,
                RebalanceReason.CONSUMER_JOIN, RebalanceReason.CONSUMER_EVICT,
                RebalanceReason.CONSUMER_EVICT);
    }

    @Test
    public void testMembershipRebalancesWaitForSuccessfulPersistence() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), false);
        CompletableFuture<Void> registration = new CompletableFuture<>();
        when(resources.registerConsumerAsync(any(), anyString(), anyString())).thenReturn(registration);
        CompletableFuture<Map<ConsumerSession, ConsumerAssignment>> registering =
                coordinator.registerConsumer("c1", 1, mock(TransportCnx.class));
        assertThat(registering).isNotDone();
        assertStats(coordinator, 0, 0, 0, 0);
        assertThat(rebalances).isEmpty();

        registration.complete(null);
        assertThat(registering).isCompleted();
        assertStats(coordinator, 1, 0, 0, 0);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN);

        CompletableFuture<Void> deletion = new CompletableFuture<>();
        when(resources.unregisterConsumerAsync(any(), anyString(), anyString())).thenReturn(deletion);
        CompletableFuture<Map<ConsumerSession, ConsumerAssignment>> leaving =
                coordinator.unregisterConsumer("c1", 1);
        assertThat(leaving).isNotDone();
        assertStats(coordinator, 1, 0, 0, 0);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN);

        deletion.complete(null);
        assertThat(leaving).isCompletedWithValue(Map.of());
        assertStats(coordinator, 0, 0, 0, 0);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN, RebalanceReason.CONSUMER_LEAVE);
    }

    @Test
    public void testLeaveCompletingAfterCloseDoesNotCount() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), false);
        register(coordinator, "c1", 1);
        CompletableFuture<Void> deletion = new CompletableFuture<>();
        when(resources.unregisterConsumerAsync(any(), anyString(), anyString())).thenReturn(deletion);
        CompletableFuture<Map<ConsumerSession, ConsumerAssignment>> leaving =
                coordinator.unregisterConsumer("c1", 1);

        coordinator.close();
        deletion.complete(null);

        assertThat(leaving).isCompletedWithValue(Map.of());
        assertStats(coordinator, 0, 0, 0, 0);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN);
    }

    @Test
    public void testEvictionSnapshotFollowsPublishedAssignments() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), false);
        register(coordinator, "c1", 1);
        TransportCnx remaining = mock(TransportCnx.class);
        coordinator.registerConsumer("c2", 2, remaining).join();
        clearInvocations(remaining);
        CompletableFuture<Void> deletion = new CompletableFuture<>();
        when(resources.unregisterConsumerAsync(any(), anyString(), anyString())).thenReturn(deletion);
        coordinator.onConsumerDisconnect("c1");
        scheduled.get(scheduled.size() - 1).run();
        // Eviction has removed c1, but c2 is still idle until persistence completes and
        // the original rebalance path actually publishes c2's replacement assignment.
        assertStats(coordinator, 0, 1, 0, 0);
        verify(remaining, times(0)).sendScalableConsumerAssignment(anyLong(), any());
        deletion.complete(null);
        assertStats(coordinator, 1, 0, 0, 0);
        verify(remaining).sendScalableConsumerAssignment(anyLong(), any());
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN,
                RebalanceReason.CONSUMER_JOIN, RebalanceReason.CONSUMER_EVICT);
    }

    @DataProvider
    public Object[][] reconnectDuringEvictionScenarios() {
        return new Object[][]{{true, false}, {true, true}, {false, false}, {false, true}};
    }

    @Test(dataProvider = "reconnectDuringEvictionScenarios")
    public void testReconnectDuringEvictionOnlyUpdatesNotifiedConsumer(
            boolean checkpoint, boolean reconnectAssignee) {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), !checkpoint);
        if (checkpoint) {
            coordinator.markCheckpointGroup();
        }
        register(coordinator, "c1", 1);
        TransportCnx c2 = mock(TransportCnx.class);
        TransportCnx c3 = mock(TransportCnx.class);
        coordinator.registerConsumer("c2", 2, c2).join();
        coordinator.registerConsumer("c3", 3, c3).join();
        clearInvocations(c2, c3);
        CompletableFuture<Void> deletion = new CompletableFuture<>();
        when(resources.unregisterConsumerAsync(any(), anyString(), anyString())).thenReturn(deletion);
        coordinator.onConsumerDisconnect("c1");
        Runnable expiry = scheduled.get(scheduled.size() - 1);
        String reconnectName = reconnectAssignee ? "c2" : "c3";
        TransportCnx reconnected = reconnectAssignee ? c2 : c3;
        TransportCnx peer = reconnectAssignee ? c3 : c2;
        coordinator.onConsumerDisconnect(reconnectName);
        expiry.run();
        assertStats(coordinator, 0, 1, 1, 0);

        coordinator.registerConsumer(reconnectName, 33, reconnected).join();

        // c2's newly computed assignment must not count as active unless c2 itself was notified.
        assertStats(coordinator, reconnectAssignee ? 1 : 0, reconnectAssignee ? 1 : 2, 0, 0);
        verify(peer, never()).sendScalableConsumerAssignment(anyLong(), any());
        verify(reconnected).sendScalableConsumerAssignment(eq(33L),
                argThat(update -> update.getSegmentsCount() == (reconnectAssignee ? 1 : 0)));
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN,
                RebalanceReason.CONSUMER_JOIN, RebalanceReason.CONSUMER_JOIN);
        deletion.complete(null);
        assertStats(coordinator, 1, 1, 0, 0);
        verify(peer).sendScalableConsumerAssignment(anyLong(),
                argThat(update -> update.getSegmentsCount() == (reconnectAssignee ? 0 : 1)));
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN,
                RebalanceReason.CONSUMER_JOIN, RebalanceReason.CONSUMER_JOIN, RebalanceReason.CONSUMER_EVICT);
    }

    @Test
    public void testEvictionCompletingAfterCloseDoesNotCount() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), false);
        register(coordinator, "c1", 1);
        CompletableFuture<Void> deletion = new CompletableFuture<>();
        when(resources.unregisterConsumerAsync(any(), anyString(), anyString())).thenReturn(deletion);
        coordinator.onConsumerDisconnect("c1");
        scheduled.get(scheduled.size() - 1).run();
        assertThat(rebalances).as("eviction waits for its persistence completion")
                .containsExactly(RebalanceReason.CONSUMER_JOIN);

        coordinator.close();
        deletion.complete(null);

        assertStats(coordinator, 0, 0, 0, 0);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN);
    }

    @Test
    public void testFailedRegistrationAndLeaveDoNotCount() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), false);
        when(resources.registerConsumerAsync(any(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("register failed")));
        assertThat(coordinator.registerConsumer("c1", 1, mock(TransportCnx.class)))
                .isCompletedExceptionally();
        assertStats(coordinator, 0, 0, 0, 0);
        assertThat(rebalances).isEmpty();

        when(resources.registerConsumerAsync(any(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        register(coordinator, "c1", 1);
        when(resources.unregisterConsumerAsync(any(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("unregister failed")));
        assertThat(coordinator.unregisterConsumer("c1", 1)).isCompletedExceptionally();
        assertStats(coordinator, 1, 0, 0, 0);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN);
    }

    @Test
    public void testRegistrationCompletingAfterCloseDoesNotCount() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), false);
        register(coordinator, "c1", 1);
        CompletableFuture<Void> persistence = new CompletableFuture<>();
        when(resources.registerConsumerAsync(any(), anyString(), anyString())).thenReturn(persistence);
        CompletableFuture<Map<ConsumerSession, ConsumerAssignment>> registering =
                coordinator.registerConsumer("c2", 2, mock(TransportCnx.class));
        assertStats(coordinator, 1, 0, 0, 0);
        coordinator.close();
        persistence.complete(null);
        assertThat(registering).isCompletedExceptionally();
        assertStats(coordinator, 0, 0, 0, 0);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN);
    }

    @Test
    public void testDrainPollCompletingAfterCloseDoesNotCountOrRearm() {
        CompletableFuture<Boolean> drained = new CompletableFuture<>();
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1).splitSegment(0, 0L), false);
        coordinator.installDrainChecker((segment, subscription) -> drained);
        register(coordinator, "c1", 1);
        scheduled.get(scheduled.size() - 1).run();
        coordinator.close();
        int scheduledBeforeCompletion = scheduled.size();
        drained.complete(true);
        assertStats(coordinator, 0, 0, 0, 0);
        assertThat(scheduled).as("closed drain poll must not rearm").hasSize(scheduledBeforeCompletion);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN);
    }

    @Test
    public void testMetricCollectionDoesNotAcquireCoordinatorMonitor() throws Exception {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), true);
        register(coordinator, "c1", 1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            synchronized (coordinator) {
                assertThat(CompletableFuture.supplyAsync(coordinator::consumerStats, executor)
                        .get(5, TimeUnit.SECONDS)).isEqualTo(new ConsumerStats(1, 0, 0, 0));
            }
        }
    }

    @Test
    public void testConcurrentDuplicateLeavePreservesAssignmentNotifications() {
        SubscriptionCoordinator coordinator = coordinator(initialLayout(1), false);
        register(coordinator, "c1", 1);
        TransportCnx remaining = mock(TransportCnx.class);
        coordinator.registerConsumer("c2", 2, remaining).join();
        clearInvocations(remaining);
        CompletableFuture<Void> firstDelete = new CompletableFuture<>();
        CompletableFuture<Void> secondDelete = new CompletableFuture<>();
        when(resources.unregisterConsumerAsync(any(), anyString(), anyString()))
                .thenReturn(firstDelete, secondDelete);
        var firstLeave = coordinator.unregisterConsumer("c1", 1);
        var secondLeave = coordinator.unregisterConsumer("c1", 1);
        firstDelete.complete(null);
        secondDelete.complete(null);
        assertThat(firstLeave).isCompleted();
        assertThat(secondLeave).isCompleted();
        verify(remaining, times(2)).sendScalableConsumerAssignment(anyLong(), any());
        assertStats(coordinator, 1, 0, 0, 0);
        assertThat(rebalances).containsExactly(RebalanceReason.CONSUMER_JOIN,
                RebalanceReason.CONSUMER_JOIN, RebalanceReason.CONSUMER_LEAVE);
    }

    private SubscriptionCoordinator coordinator(SegmentLayout layout, boolean ordered) {
        SegmentDrainChecker checker = ordered
                ? (segment, subscription) -> CompletableFuture.completedFuture(false) : null;
        SubscriptionCoordinator coordinator = new SubscriptionCoordinator("sub",
                TopicName.get("topic://tenant/ns/metrics"), layout, resources, scheduler,
                Duration.ofMinutes(1), checker, Duration.ofHours(1), Duration.ofHours(2));
        coordinator.setRebalanceListener(rebalances::add);
        coordinators.add(coordinator);
        return coordinator;
    }

    private static SegmentLayout initialLayout(int bucketCount) {
        return SegmentLayout.fromMetadata(ScalableTopicController.createInitialMetadata(1, bucketCount, Map.of()));
    }

    private static void register(SubscriptionCoordinator coordinator, String consumerName, long consumerId) {
        coordinator.registerConsumer(consumerName, consumerId, mock(TransportCnx.class)).join();
    }

    private static void assertStats(SubscriptionCoordinator coordinator, long active, long idle,
                                    long disconnected, long blocked) {
        assertThat(coordinator.consumerStats()).as("coordinator consumer and ancestor-blocking snapshot")
                .isEqualTo(new ConsumerStats(active, idle, disconnected, blocked));
    }
}
