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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubscriptionCoordinatorTest {

    private TopicName topicName;
    private SegmentLayout initialLayout;
    private ScalableTopicResources resources;
    private ScheduledExecutorService scheduler;
    private SubscriptionCoordinator coordinator;

    @BeforeMethod
    public void setup() {
        topicName = TopicName.get("topic://tenant/ns/my-topic");
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(4, Map.of());
        initialLayout = SegmentLayout.fromMetadata(metadata);
        resources = mock(ScalableTopicResources.class);
        // All persistence ops succeed
        when(resources.registerConsumerAsync(any(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(resources.unregisterConsumerAsync(any(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        scheduler = Executors.newSingleThreadScheduledExecutor();
        coordinator = new SubscriptionCoordinator("test-sub", topicName, initialLayout,
                resources, scheduler, Duration.ofMillis(200));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Test
    public void testSingleConsumerGetsAllSegments() throws Exception {
        Map<ConsumerSession, ConsumerAssignment> result =
                coordinator.registerConsumer("consumer-1", 1L, mock(TransportCnx.class)).get();

        assertEquals(result.size(), 1);
        ConsumerAssignment assignment = findByName(result, "consumer-1");
        assertNotNull(assignment);
        assertEquals(assignment.assignedSegments().size(), 4);
        assertEquals(assignment.layoutEpoch(), 0);
    }

    @Test
    public void testTwoConsumersBalanced() throws Exception {
        coordinator.registerConsumer("consumer-1", 1L, mock(TransportCnx.class)).get();
        Map<ConsumerSession, ConsumerAssignment> result =
                coordinator.registerConsumer("consumer-2", 2L, mock(TransportCnx.class)).get();

        assertEquals(result.size(), 2);
        assertEquals(findByName(result, "consumer-1").assignedSegments().size(), 2);
        assertEquals(findByName(result, "consumer-2").assignedSegments().size(), 2);
    }

    @Test
    public void testThreeConsumersWithFourSegments() throws Exception {
        coordinator.registerConsumer("consumer-1", 1L, mock(TransportCnx.class)).get();
        coordinator.registerConsumer("consumer-2", 2L, mock(TransportCnx.class)).get();
        Map<ConsumerSession, ConsumerAssignment> result =
                coordinator.registerConsumer("consumer-3", 3L, mock(TransportCnx.class)).get();

        assertEquals(result.size(), 3);
        int total = result.values().stream()
                .mapToInt(a -> a.assignedSegments().size())
                .sum();
        assertEquals(total, 4);

        Set<Long> assignedIds = new HashSet<>();
        for (ConsumerAssignment assignment : result.values()) {
            for (ConsumerAssignment.AssignedSegment seg : assignment.assignedSegments()) {
                assertTrue(assignedIds.add(seg.segmentId()),
                        "Segment " + seg.segmentId() + " assigned to multiple consumers");
            }
        }
        assertEquals(assignedIds.size(), 4);
    }

    @Test
    public void testUnregisterConsumerRebalances() throws Exception {
        coordinator.registerConsumer("consumer-1", 1L, mock(TransportCnx.class)).get();
        coordinator.registerConsumer("consumer-2", 2L, mock(TransportCnx.class)).get();

        Map<ConsumerSession, ConsumerAssignment> result =
                coordinator.unregisterConsumer("consumer-2").get();

        assertEquals(result.size(), 1);
        assertEquals(findByName(result, "consumer-1").assignedSegments().size(), 4);
    }

    @Test
    public void testLayoutChangeRebalances() throws Exception {
        coordinator.registerConsumer("consumer-1", 1L, mock(TransportCnx.class)).get();

        SegmentLayout newLayout = initialLayout.splitSegment(0);
        Map<ConsumerSession, ConsumerAssignment> result =
                coordinator.onLayoutChange(newLayout).get();

        assertEquals(result.size(), 1);
        assertEquals(findByName(result, "consumer-1").assignedSegments().size(), 5);
    }

    @Test
    public void testEmptyAfterAllConsumersRemoved() throws Exception {
        coordinator.registerConsumer("consumer-1", 1L, mock(TransportCnx.class)).get();
        Map<ConsumerSession, ConsumerAssignment> result =
                coordinator.unregisterConsumer("consumer-1").get();

        assertTrue(result.isEmpty());
        assertTrue(coordinator.getConsumers().isEmpty());
    }

    @Test
    public void testAssignmentDeterminism() throws Exception {
        coordinator.registerConsumer("consumer-a", 1L, mock(TransportCnx.class)).get();
        Map<ConsumerSession, ConsumerAssignment> result1 =
                coordinator.registerConsumer("consumer-b", 2L, mock(TransportCnx.class)).get();

        SubscriptionCoordinator coordinator2 = new SubscriptionCoordinator("test-sub", topicName,
                initialLayout, resources, scheduler, Duration.ofMillis(200));
        coordinator2.registerConsumer("consumer-a", 1L, mock(TransportCnx.class)).get();
        Map<ConsumerSession, ConsumerAssignment> result2 =
                coordinator2.registerConsumer("consumer-b", 2L, mock(TransportCnx.class)).get();

        assertEquals(segmentIds(findByName(result1, "consumer-a")),
                segmentIds(findByName(result2, "consumer-a")));
        assertEquals(segmentIds(findByName(result1, "consumer-b")),
                segmentIds(findByName(result2, "consumer-b")));
    }

    // --- Session lifecycle tests ---

    @Test
    public void testReconnectWithinGracePeriodPreservesAssignment() throws Exception {
        coordinator.registerConsumer("consumer-1", 1L, mock(TransportCnx.class)).get();
        coordinator.registerConsumer("consumer-2", 2L, mock(TransportCnx.class)).get();

        // consumer-1 drops
        coordinator.onConsumerDisconnect("consumer-1");
        assertEquals(coordinator.getConsumers().size(), 2,
                "session should still be tracked during grace period");

        // Reconnect within the grace period with a new connection
        Map<ConsumerSession, ConsumerAssignment> result =
                coordinator.registerConsumer("consumer-1", 99L, mock(TransportCnx.class)).get();

        assertEquals(result.size(), 2);
        assertEquals(findByName(result, "consumer-1").assignedSegments().size(), 2);
        assertEquals(findByName(result, "consumer-2").assignedSegments().size(), 2);

        // The session was not recreated — still 2 consumers, and consumer-1 is reconnected
        ConsumerSession reconnected = coordinator.getConsumers().stream()
                .filter(s -> s.getConsumerName().equals("consumer-1"))
                .findFirst().orElseThrow();
        assertTrue(reconnected.isConnected());
        assertEquals(reconnected.getConsumerId(), 99L);
    }

    @Test
    public void testExpiredSessionIsEvictedAfterGracePeriod() {
        coordinator.registerConsumer("consumer-1", 1L, mock(TransportCnx.class)).join();
        coordinator.registerConsumer("consumer-2", 2L, mock(TransportCnx.class)).join();
        assertEquals(coordinator.getConsumers().size(), 2);

        coordinator.onConsumerDisconnect("consumer-1");

        // Grace period is 200ms in tests — wait for eviction
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Set<ConsumerSession> active = coordinator.getConsumers();
            assertEquals(active.size(), 1);
            assertEquals(active.iterator().next().getConsumerName(), "consumer-2");
        });
    }

    @Test
    public void testRestoreConsumersInstallsDisconnectedSessions() {
        Map<ConsumerSession, ConsumerAssignment> assignments =
                coordinator.restoreConsumers(List.of("consumer-1", "consumer-2"));

        assertEquals(assignments.size(), 2);
        assertEquals(coordinator.getConsumers().size(), 2);
        // Restored sessions start in the disconnected state
        for (ConsumerSession session : coordinator.getConsumers()) {
            assertFalse(session.isConnected());
            assertNotNull(session.getGraceTimer());
        }
    }

    @Test
    public void testRestoredConsumerResumesAssignmentOnReconnect() throws Exception {
        coordinator.restoreConsumers(List.of("consumer-1", "consumer-2"));

        // Reconnect consumer-1 — it should find its existing session and reuse the assignment
        Map<ConsumerSession, ConsumerAssignment> result =
                coordinator.registerConsumer("consumer-1", 42L, mock(TransportCnx.class)).get();

        assertEquals(result.size(), 2);
        assertEquals(findByName(result, "consumer-1").assignedSegments().size(), 2);

        ConsumerSession reconnected = coordinator.getConsumers().stream()
                .filter(s -> s.getConsumerName().equals("consumer-1"))
                .findFirst().orElseThrow();
        assertTrue(reconnected.isConnected());
    }

    // --- Helpers ---

    private static ConsumerAssignment findByName(Map<ConsumerSession, ConsumerAssignment> m, String name) {
        return m.entrySet().stream()
                .filter(e -> name.equals(e.getKey().getConsumerName()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    private static List<Long> segmentIds(ConsumerAssignment assignment) {
        return assignment.assignedSegments().stream()
                .map(ConsumerAssignment.AssignedSegment::segmentId)
                .toList();
    }
}
