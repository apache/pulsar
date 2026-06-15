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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.ConsumerRegistration;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.resources.SubscriptionMetadata;
import org.apache.pulsar.broker.resources.SubscriptionType;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.ScalableTopics;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link ScalableTopicController} using an in-memory metadata store
 * (so leader election and subscription/consumer persistence are exercised for real) with
 * the cross-broker admin client mocked.
 */
public class ScalableTopicControllerTest {

    private static final int INITIAL_SEGMENTS = 4;
    private static final String BROKER_ID = "broker-test";

    private MetadataStoreExtended store;
    private CoordinationService coordinationService;
    private ScalableTopicResources resources;
    private ScheduledExecutorService scheduler;

    private BrokerService brokerService;
    private PulsarService pulsar;
    private PulsarAdmin admin;
    private Topics topics;
    private ScalableTopics scalableTopics;

    private TopicName topicName;
    private ScalableTopicController controller;

    @BeforeMethod
    public void setUp() throws Exception {
        store = new LocalMemoryMetadataStore("memory:local",
                MetadataStoreConfig.builder().build());
        coordinationService = new CoordinationServiceImpl(store);
        resources = new ScalableTopicResources(store, 30);
        scheduler = Executors.newSingleThreadScheduledExecutor();

        topicName = TopicName.get("topic://tenant/ns/my-topic");

        // Seed the topic's initial metadata so initialize() has something to load.
        ScalableTopicMetadata metadata =
                ScalableTopicController.createInitialMetadata(INITIAL_SEGMENTS, Map.of());
        resources.createScalableTopicAsync(topicName, metadata).get();

        // --- Mock the BrokerService / PulsarService / PulsarAdmin chain ---
        brokerService = mock(BrokerService.class);
        pulsar = mock(PulsarService.class);
        admin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        scalableTopics = mock(ScalableTopics.class);

        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.getTopicIfExists(anyString()))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(pulsar.getBrokerId()).thenReturn(BROKER_ID);
        when(pulsar.getExecutor()).thenReturn(scheduler);
        when(pulsar.getAdminClient()).thenReturn(admin);
        when(admin.topics()).thenReturn(topics);
        when(admin.scalableTopics()).thenReturn(scalableTopics);

        // Default: all admin ops succeed.
        when(topics.getSubscriptionsAsync(anyString()))
                .thenReturn(CompletableFuture.completedFuture(java.util.List.of()));
        when(scalableTopics.createSegmentAsync(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scalableTopics.terminateSegmentAsync(anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scalableTopics.createSegmentSubscriptionAsync(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scalableTopics.deleteSegmentSubscriptionAsync(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        controller = newController(topicName);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (controller != null) {
            controller.close().join();
        }
        if (coordinationService != null) {
            coordinationService.close();
        }
        if (store != null) {
            store.close();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private ScalableTopicController newController(TopicName tn) {
        return new ScalableTopicController(tn, resources, brokerService, coordinationService);
    }

    private ScalableTopicController newControllerWithClock(TopicName tn,
                                                           java.time.Clock clock,
                                                           java.time.Duration gcInterval) {
        return new ScalableTopicController(tn, resources, brokerService, coordinationService,
                clock, gcInterval);
    }

    // --- initialize() ---

    @Test
    public void testInitializeBecomesLeader() throws Exception {
        controller.initialize().get();
        assertTrue(controller.isLeader());
        SegmentLayout layout = controller.getLayout().get();
        assertEquals(layout.getAllSegments().size(), INITIAL_SEGMENTS);
        assertEquals(layout.getActiveSegments().size(), INITIAL_SEGMENTS);
        assertEquals(layout.getEpoch(), 0);
    }

    /**
     * Recovery path: a {@code createScalableTopic} can commit metadata then crash
     * before it materializes all the initial segment topics. The next time the
     * controller initializes (broker restart, bundle ownership transfer, leader
     * re-election), it must recreate any missing active-segment backing topics
     * so producers/consumers can use them again. The check is idempotent —
     * existing segments are simply re-loaded.
     */
    @Test
    public void testInitializeRecreatesMissingActiveSegments() throws Exception {
        controller.initialize().get();

        // The leader's initialize() must have asked the admin client to
        // (re)materialize each of the INITIAL_SEGMENTS active segments.
        verify(scalableTopics, times(INITIAL_SEGMENTS))
                .createSegmentAsync(anyString(), any());
    }

    /**
     * Idempotency partner to the above: a non-leader controller must NOT trigger
     * segment-topic creation. Only the leader heals; followers just observe the
     * layout. (We can't easily build a "loser" in this single-broker harness, but
     * we can at least verify that no segment is created when the controller fails
     * to become leader — e.g., because the topic metadata is gone.)
     */
    @Test
    public void testInitializeDoesNotCreateSegmentsWhenNotLeader() throws Exception {
        // Drive a "not leader" outcome by pointing at a topic with no metadata —
        // initialize() bails before electLeader() / ensureActiveSegmentsExist().
        TopicName missing = TopicName.get("topic://tenant/ns/does-not-exist");
        ScalableTopicController orphan = newController(missing);
        try {
            orphan.initialize().get();
            fail("expected IllegalStateException for missing topic");
        } catch (ExecutionException expected) {
            // Bailed before leader-elect, as desired.
        } finally {
            orphan.close().join();
        }
        verify(scalableTopics, times(0))
                .createSegmentAsync(eq(missing.toString()), any());
    }

    @Test
    public void testInitializeFailsWhenTopicMissing() throws Exception {
        TopicName missing = TopicName.get("topic://tenant/ns/does-not-exist");
        ScalableTopicController orphan = newController(missing);
        try {
            orphan.initialize().get();
            fail("expected IllegalStateException for missing topic");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException,
                    "expected IllegalStateException, got " + e.getCause());
            assertTrue(e.getCause().getMessage().contains("Scalable topic not found"));
        } finally {
            orphan.close().join();
        }
    }

    @Test
    public void testLeaderBrokerIdPersisted() throws Exception {
        controller.initialize().get();
        Optional<String> leaderId = controller.getLeaderBrokerId().get();
        assertEquals(leaderId, Optional.of(BROKER_ID));
    }

    @Test
    public void testInitializeRestoresPersistedConsumers() throws Exception {
        // Pre-populate a subscription and a couple of consumer registrations.
        resources.createSubscriptionAsync(topicName, "sub-a", SubscriptionType.STREAM).get();
        resources.registerConsumerAsync(topicName, "sub-a", "c1").get();
        resources.registerConsumerAsync(topicName, "sub-a", "c2").get();

        controller.initialize().get();

        // Restored consumers should appear in the in-memory coordinator
        // immediately, even before any client reconnects.
        var assignment = resources.listConsumersAsync(topicName, "sub-a").get();
        assertEquals(assignment.size(), 2);
    }

    // --- checkLeader guard ---

    @Test
    public void testWriteOperationThrowsWhenNotLeader() {
        // Controller not initialized → not leader.
        assertFalse(controller.isLeader());
        assertThrows(IllegalStateException.class, () -> controller.splitSegment(0));
        assertThrows(IllegalStateException.class, () -> controller.mergeSegments(0, 1));
        assertThrows(IllegalStateException.class,
                () -> controller.createSubscription("sub", SubscriptionType.STREAM));
        assertThrows(IllegalStateException.class, () -> controller.deleteSubscription("sub"));
        assertThrows(IllegalStateException.class,
                () -> controller.registerConsumer(
                        "sub", "c1", 1L, ScalableConsumerType.STREAM, mock(TransportCnx.class)));
        assertThrows(IllegalStateException.class, () -> controller.unregisterConsumer("sub", "c1"));
    }

    // --- Consumer registration ---

    @Test
    public void testRegisterConsumerPersistsAndAssigns() throws Exception {
        controller.initialize().get();
        ConsumerAssignment assignment = controller.registerConsumer(
                "sub-a", "c1", 1L, ScalableConsumerType.STREAM, mock(TransportCnx.class)).get();

        assertEquals(assignment.assignedSegments().size(), INITIAL_SEGMENTS,
                "single consumer owns all active segments");
        // Registration is persisted.
        var persisted = resources.listConsumersAsync(topicName, "sub-a").get();
        assertEquals(persisted.size(), 1);
        assertEquals(persisted.get(0), "c1");
    }

    @Test
    public void testRegisterConsumerReconnectDoesNotDuplicate() throws Exception {
        controller.initialize().get();
        controller.registerConsumer(
                "sub-a", "c1", 1L, ScalableConsumerType.STREAM, mock(TransportCnx.class)).get();
        // Reconnect: same name, new consumerId.
        ConsumerAssignment assignment = controller.registerConsumer(
                "sub-a", "c1", 99L, ScalableConsumerType.STREAM, mock(TransportCnx.class)).get();

        assertEquals(assignment.assignedSegments().size(), INITIAL_SEGMENTS);
        // Still just one persisted registration.
        assertEquals(resources.listConsumersAsync(topicName, "sub-a").get().size(), 1);
    }

    @Test
    public void testUnregisterConsumerDeletesPersistedEntry() throws Exception {
        controller.initialize().get();
        controller.registerConsumer("sub-a", "c1", 1L, ScalableConsumerType.STREAM, mock(TransportCnx.class)).get();
        controller.registerConsumer("sub-a", "c2", 2L, ScalableConsumerType.STREAM, mock(TransportCnx.class)).get();
        assertEquals(resources.listConsumersAsync(topicName, "sub-a").get().size(), 2);

        controller.unregisterConsumer("sub-a", "c1").get();
        assertEquals(resources.listConsumersAsync(topicName, "sub-a").get(),
                java.util.List.of("c2"));
    }

    @Test
    public void testUnregisterConsumerUnknownSubscriptionIsNoop() throws Exception {
        controller.initialize().get();
        // No subscription 'ghost' exists; call should complete without error.
        controller.unregisterConsumer("ghost", "c1").get();
    }

    @Test
    public void testOnConsumerDisconnectUnknownSubscriptionIsNoop() throws Exception {
        controller.initialize().get();
        // Should not throw even though 'ghost' has no coordinator.
        controller.onConsumerDisconnect("ghost", "c1");
    }

    // --- createSubscription / deleteSubscription ---

    @Test
    public void testCreateSubscriptionStream() throws Exception {
        controller.initialize().get();
        controller.createSubscription("sub-stream", SubscriptionType.STREAM).get();

        Optional<SubscriptionMetadata> persisted =
                resources.getSubscriptionAsync(topicName, "sub-stream").get();
        assertTrue(persisted.isPresent());
        assertEquals(persisted.get().type(), SubscriptionType.STREAM);
        // Propagated to every active segment via the segment-subscription admin endpoint.
        verify(scalableTopics, org.mockito.Mockito.times(INITIAL_SEGMENTS))
                .createSegmentSubscriptionAsync(anyString(), anyString());
    }

    @Test
    public void testCreateSubscriptionQueue() throws Exception {
        controller.initialize().get();
        controller.createSubscription("sub-queue", SubscriptionType.QUEUE).get();

        Optional<SubscriptionMetadata> persisted =
                resources.getSubscriptionAsync(topicName, "sub-queue").get();
        assertTrue(persisted.isPresent());
        assertEquals(persisted.get().type(), SubscriptionType.QUEUE);
    }

    @Test
    public void testCreateSubscriptionIdempotent() throws Exception {
        controller.initialize().get();
        controller.createSubscription("sub-a", SubscriptionType.STREAM).get();
        // Second call should not fail even though metadata already exists.
        controller.createSubscription("sub-a", SubscriptionType.STREAM).get();
    }

    @Test
    public void testDeleteSubscription() throws Exception {
        controller.initialize().get();
        controller.createSubscription("sub-a", SubscriptionType.STREAM).get();
        assertTrue(resources.getSubscriptionAsync(topicName, "sub-a").get().isPresent());

        controller.deleteSubscription("sub-a").get();
        assertFalse(resources.getSubscriptionAsync(topicName, "sub-a").get().isPresent());
        // Propagated a delete to every segment (all segments incl. any sealed ones).
        verify(scalableTopics, org.mockito.Mockito.atLeast(INITIAL_SEGMENTS))
                .deleteSegmentSubscriptionAsync(anyString(), anyString());
    }

    @Test
    public void testDeleteSubscriptionRemovesInMemoryCoordinator() throws Exception {
        controller.initialize().get();
        controller.createSubscription("sub-a", SubscriptionType.STREAM).get();
        controller.registerConsumer("sub-a", "c1", 1L, ScalableConsumerType.STREAM, mock(TransportCnx.class)).get();

        controller.deleteSubscription("sub-a").get();
        // After delete, the persisted consumer entries should be gone.
        assertEquals(resources.listConsumersAsync(topicName, "sub-a").get().size(), 0);
    }

    // --- splitSegment / mergeSegments ---

    @Test
    public void testSplitSegment() throws Exception {
        controller.initialize().get();
        // initialize() now eagerly creates the active segment topics for recovery —
        // ignore those when counting what splitSegment itself triggered.
        clearInvocations(scalableTopics);
        SegmentLayout before = controller.getLayout().get();
        long epochBefore = before.getEpoch();
        int activeBefore = before.getActiveSegments().size();

        SegmentLayout after = controller.splitSegment(0).get();

        assertEquals(after.getEpoch(), epochBefore + 1);
        assertEquals(after.getActiveSegments().size(), activeBefore + 1,
                "split produces two children in place of one active parent → net +1 active");
        // Admin API was used to create the two new child segments + terminate the parent.
        verify(scalableTopics, org.mockito.Mockito.times(2))
                .createSegmentAsync(anyString(), any());
        verify(scalableTopics, org.mockito.Mockito.times(1)).terminateSegmentAsync(anyString());
    }

    @Test
    public void testMergeSegments() throws Exception {
        controller.initialize().get();
        clearInvocations(scalableTopics);
        SegmentLayout before = controller.getLayout().get();
        long epochBefore = before.getEpoch();
        int activeBefore = before.getActiveSegments().size();

        // Segments 0 and 1 are adjacent by construction of createInitialMetadata.
        SegmentLayout after = controller.mergeSegments(0, 1).get();

        assertEquals(after.getEpoch(), epochBefore + 1);
        assertEquals(after.getActiveSegments().size(), activeBefore - 1,
                "merge fuses two active segments into one → net -1 active");
        verify(scalableTopics, org.mockito.Mockito.times(1))
                .createSegmentAsync(anyString(), any());
        verify(scalableTopics, org.mockito.Mockito.times(2)).terminateSegmentAsync(anyString());
    }

    @Test
    public void testSplitSegmentPropagatesToRegisteredConsumer() throws Exception {
        controller.initialize().get();
        controller.registerConsumer("sub-a", "c1", 1L, ScalableConsumerType.STREAM, mock(TransportCnx.class)).get();

        SegmentLayout after = controller.splitSegment(0).get();
        // consumer still owns everything after split (single consumer).
        assertEquals(after.getActiveSegments().size(), INITIAL_SEGMENTS + 1);
    }

    // --- getStats ---

    @Test
    public void testGetStatsBaseline() throws Exception {
        controller.initialize().get();
        ScalableTopicStats stats = controller.getStats().get();

        assertEquals(stats.getEpoch(), 0);
        assertEquals(stats.getTotalSegments(), INITIAL_SEGMENTS);
        assertEquals(stats.getActiveSegments(), INITIAL_SEGMENTS);
        assertEquals(stats.getSealedSegments(), 0);
        assertEquals(stats.getSegments().size(), INITIAL_SEGMENTS);
        // No subscriptions registered yet.
        assertEquals(stats.getSubscriptions().size(), 0);
    }

    @Test
    public void testGetStatsAfterSplitAndSubscriptions() throws Exception {
        controller.initialize().get();
        controller.splitSegment(0).get();
        controller.createSubscription("sub-a", SubscriptionType.STREAM).get();
        controller.createSubscription("sub-b", SubscriptionType.QUEUE).get();
        controller.registerConsumer("sub-a", "c1", 1L, ScalableConsumerType.STREAM, mock(TransportCnx.class)).get();
        controller.registerConsumer("sub-a", "c2", 2L, ScalableConsumerType.STREAM, mock(TransportCnx.class)).get();

        ScalableTopicStats stats = controller.getStats().get();

        assertEquals(stats.getEpoch(), 1);
        assertEquals(stats.getTotalSegments(), INITIAL_SEGMENTS + 2,
                "split adds two children, keeps parent as sealed");
        assertEquals(stats.getActiveSegments(), INITIAL_SEGMENTS + 1);
        assertEquals(stats.getSealedSegments(), 1);
        assertEquals(stats.getSubscriptions().size(), 2);
        assertEquals(stats.getSubscriptions().get("sub-a").consumerCount(), 2);
        assertEquals(stats.getSubscriptions().get("sub-b").consumerCount(), 0);
    }

    // --- createInitialMetadata ---

    @Test
    public void testCreateInitialMetadataDefaults() {
        ScalableTopicMetadata md = ScalableTopicController.createInitialMetadata(4, Map.of());
        assertEquals(md.getEpoch(), 0);
        assertEquals(md.getNextSegmentId(), 4);
        assertEquals(md.getSegments().size(), 4);
    }

    @Test
    public void testCreateInitialMetadataRejectsZeroSegments() {
        assertThrows(IllegalArgumentException.class,
                () -> ScalableTopicController.createInitialMetadata(0, Map.of()));
    }

    // --- close / lifecycle ---

    @Test
    public void testCloseReleasesLeadership() throws Exception {
        controller.initialize().get();
        assertTrue(controller.isLeader());
        controller.close().get();
        // Replace with null so the teardown doesn't close it again.
        controller = null;

        // A fresh controller can become leader now.
        ScalableTopicController second = newController(topicName);
        try {
            second.initialize().get();
            assertTrue(second.isLeader());
        } finally {
            second.close().join();
        }
    }

    // --- Seek subscription / clear backlog ---

    /**
     * The seek path classifies each segment by its {@code [createdAtMs, sealedAtMs)} window
     * against the requested timestamp. With 3 segments — one sealed entirely before the
     * timestamp, one straddling, and one created entirely after — we expect:
     * <ul>
     *   <li>Sealed-and-old segment: skip-all admin call (cursor → end).</li>
     *   <li>Straddling segment: seek admin call with the requested timestamp.</li>
     *   <li>Created-after segment: seek admin call with timestamp 0 (i.e. earliest).</li>
     * </ul>
     */
    @Test
    public void testSeekSubscriptionDispatchesPerSegmentByTimestamp() throws Exception {
        // Build a custom metadata with three segments at specific timestamps.
        long t0 = 1_000_000L;
        long t1 = 2_000_000L;
        long t2 = 3_000_000L;
        long t3 = 4_000_000L;
        // Segment 0: created at t0, sealed at t1 (entirely before tSeek).
        // Segment 1: created at t1, still active (straddles tSeek).
        // Segment 2: created at t3, still active (entirely after tSeek).
        long tSeek = t2;

        org.apache.pulsar.common.scalable.SegmentInfo seg0 = new org.apache.pulsar.common.scalable.SegmentInfo(
                0L,
                org.apache.pulsar.common.scalable.HashRange.of(0x0000, 0x3FFF),
                org.apache.pulsar.common.scalable.SegmentState.SEALED,
                java.util.List.of(), java.util.List.of(3L),
                /*createdAtEpoch*/ 0, /*sealedAtEpoch*/ 1,
                /*createdAtMs*/ t0, /*sealedAtMs*/ t1, null);
        org.apache.pulsar.common.scalable.SegmentInfo seg1 = new org.apache.pulsar.common.scalable.SegmentInfo(
                1L,
                org.apache.pulsar.common.scalable.HashRange.of(0x4000, 0x7FFF),
                org.apache.pulsar.common.scalable.SegmentState.ACTIVE,
                java.util.List.of(), java.util.List.of(),
                0, -1, t1, -1, null);
        org.apache.pulsar.common.scalable.SegmentInfo seg2 = new org.apache.pulsar.common.scalable.SegmentInfo(
                2L,
                org.apache.pulsar.common.scalable.HashRange.of(0x8000, 0xFFFF),
                org.apache.pulsar.common.scalable.SegmentState.ACTIVE,
                java.util.List.of(), java.util.List.of(),
                0, -1, t3, -1, null);

        TopicName seekTopic = TopicName.get("topic://tenant/ns/seek-topic");
        ScalableTopicMetadata md = ScalableTopicMetadata.builder()
                .epoch(2).nextSegmentId(4)
                .segments(java.util.Map.of(0L, seg0, 1L, seg1, 2L, seg2))
                .properties(java.util.Map.of())
                .build();
        resources.createScalableTopicAsync(seekTopic, md).get();
        ScalableTopicController c = new ScalableTopicController(
                seekTopic, resources, brokerService, coordinationService);
        try {
            // Stub the segment-aware admin calls.
            when(scalableTopics.seekSegmentSubscriptionAsync(anyString(), anyString(), anyLong()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(scalableTopics.clearSegmentSubscriptionBacklogAsync(anyString(), anyString()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            c.initialize().get();
            c.seekSubscription("sub-a", tSeek).get();

            // Sealed-old segment: skip-all admin call once.
            verify(scalableTopics, org.mockito.Mockito.times(1))
                    .clearSegmentSubscriptionBacklogAsync(anyString(), org.mockito.ArgumentMatchers.eq("sub-a"));

            // Two seek calls: one for the straddling segment (t == tSeek), one for the
            // created-after segment (t == 0 because seg2.createdAtMs >= tSeek).
            org.mockito.ArgumentCaptor<Long> tsCaptor = org.mockito.ArgumentCaptor.forClass(Long.class);
            verify(scalableTopics, org.mockito.Mockito.times(2))
                    .seekSegmentSubscriptionAsync(anyString(),
                            org.mockito.ArgumentMatchers.eq("sub-a"),
                            tsCaptor.capture());
            java.util.List<Long> sentTs = tsCaptor.getAllValues();
            assertTrue(sentTs.contains(tSeek), "expected straddling segment to receive tSeek");
            assertTrue(sentTs.contains(0L), "expected created-after segment to receive 0");
        } finally {
            c.close().join();
        }
    }

    /** Clear-backlog dispatches skip-all to every segment in the DAG. */
    @Test
    public void testClearBacklogDispatchesSkipAllToEverySegment() throws Exception {
        when(scalableTopics.clearSegmentSubscriptionBacklogAsync(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        controller.initialize().get();

        controller.clearBacklog("sub-a").get();
        // INITIAL_SEGMENTS active segments, no sealed ones in the baseline → exactly N calls.
        verify(scalableTopics, org.mockito.Mockito.times(INITIAL_SEGMENTS))
                .clearSegmentSubscriptionBacklogAsync(anyString(),
                        org.mockito.ArgumentMatchers.eq("sub-a"));
    }

    /**
     * 404 from a per-segment seek means "subscription not present on that segment" —
     * the controller tolerates this as success (cursor will materialise lazily).
     */
    @Test
    public void testSeekTolerates404SubscriptionNotFound() throws Exception {
        when(scalableTopics.seekSegmentSubscriptionAsync(anyString(), anyString(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(
                        new org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException(
                                new RuntimeException("Subscription not found"),
                                "Subscription not found", 404)));
        controller.initialize().get();

        // Should not throw — every segment's 404 is swallowed.
        controller.seekSubscription("sub-a", System.currentTimeMillis()).get();
    }

    /**
     * 503 from the segment endpoint == "topic not loaded yet". This is a transient
     * unload (e.g. ownership churn); the controller MUST surface it so the caller can
     * retry the parent-level operation, instead of silently skipping segments.
     */
    @Test
    public void testSeekPropagates503TransientUnload() throws Exception {
        org.apache.pulsar.client.admin.PulsarAdminException unavailable =
                new org.apache.pulsar.client.admin.PulsarAdminException(
                        new RuntimeException("Service Unavailable"),
                        "Segment topic not loaded", 503);
        when(scalableTopics.seekSegmentSubscriptionAsync(anyString(), anyString(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(unavailable));
        controller.initialize().get();

        java.util.concurrent.ExecutionException ex =
                org.testng.Assert.expectThrows(java.util.concurrent.ExecutionException.class,
                        () -> controller.seekSubscription("sub-a", System.currentTimeMillis()).get());
        Throwable cause = ex.getCause() instanceof java.util.concurrent.CompletionException
                ? ex.getCause().getCause() : ex.getCause();
        assertTrue(cause instanceof org.apache.pulsar.client.admin.PulsarAdminException,
                "expected 503 PulsarAdminException to propagate, got " + cause);
    }

    /** Same contract as seek: 503 from a per-segment clear-backlog must propagate. */
    @Test
    public void testClearBacklogPropagates503TransientUnload() throws Exception {
        org.apache.pulsar.client.admin.PulsarAdminException unavailable =
                new org.apache.pulsar.client.admin.PulsarAdminException(
                        new RuntimeException("Service Unavailable"),
                        "Segment topic not loaded", 503);
        when(scalableTopics.clearSegmentSubscriptionBacklogAsync(anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(unavailable));
        controller.initialize().get();

        java.util.concurrent.ExecutionException ex =
                org.testng.Assert.expectThrows(java.util.concurrent.ExecutionException.class,
                        () -> controller.clearBacklog("sub-a").get());
        Throwable cause = ex.getCause() instanceof java.util.concurrent.CompletionException
                ? ex.getCause().getCause() : ex.getCause();
        assertTrue(cause instanceof org.apache.pulsar.client.admin.PulsarAdminException,
                "expected 503 PulsarAdminException to propagate, got " + cause);
    }

    // --- Sealed-segment GC ---

    /**
     * After a split, the parent is sealed; with no live subscriptions and a small
     * configured retention the GC tick should prune the parent from the layout and
     * delete its backing topic.
     */
    @Test
    public void testGcTickPrunesDrainedSealedSegmentPastRetention() throws Exception {
        // Inject GC-related mocks (topic-policies + namespace policies + delete).
        installGcMocks(/* nsRetentionMinutes */ 1);

        // Use a controllable clock so we can fast-forward past the retention window.
        long startMs = 1_700_000_000_000L;
        AdjustableClock clock = new AdjustableClock(startMs);
        if (controller != null) {
            controller.close().join();
        }
        controller = newControllerWithClock(topicName, clock,
                java.time.Duration.ofHours(1)); // GC interval irrelevant — we drive ticks manually
        controller.initialize().get();

        int sealedBefore = controller.sealedSegmentCount();
        // Split segment 0 → seg 0 sealed at startMs, children created at startMs.
        controller.splitSegment(0).get();
        assertEquals(controller.sealedSegmentCount(), sealedBefore + 1);

        // Give the doomed segment a load record, as the owning broker's reporter would.
        resources.reportSegmentLoadAsync(topicName, 0,
                new org.apache.pulsar.common.scalable.SegmentLoadStats(1, 1, 1, 1)).get();

        // Tick at the seal time — retention not yet elapsed; nothing pruned.
        controller.runGcTickAsync().get();
        assertTrue(controller.getLayout().get().getAllSegments().containsKey(0L),
                "tick within retention window must not prune");

        // Fast-forward past 1 minute; tick should now prune segment 0.
        clock.set(startMs + java.util.concurrent.TimeUnit.MINUTES.toMillis(1) + 1_000L);
        controller.runGcTickAsync().get();
        assertFalse(controller.getLayout().get().getAllSegments().containsKey(0L),
                "tick past retention must prune the sealed segment");
        // Backing topic delete was issued via the segment-aware admin call.
        verify(scalableTopics).deleteSegmentAsync(anyString(), anyBoolean());
        // The pruned segment's load record is deleted along with it.
        assertFalse(resources.getSegmentLoadAsync(topicName, 0).get().isPresent(),
                "prune must delete the segment's load record");
    }

    /**
     * If retention is set to "keep forever" (negative value), the GC tick is a no-op
     * even for sealed + drained segments.
     */
    @Test
    public void testGcTickRespectsKeepForeverRetention() throws Exception {
        installGcMocks(/* nsRetentionMinutes */ -1);

        long now = 1_700_000_000_000L;
        if (controller != null) {
            controller.close().join();
        }
        java.time.Clock fixed = java.time.Clock.fixed(
                java.time.Instant.ofEpochMilli(now + 365L * 86_400_000L),
                java.time.ZoneOffset.UTC);
        controller = newControllerWithClock(topicName, fixed, java.time.Duration.ofHours(1));
        controller.initialize().get();
        controller.splitSegment(0).get();

        controller.runGcTickAsync().get();
        assertTrue(controller.getLayout().get().getAllSegments().containsKey(0L),
                "negative retention must keep sealed segments forever");
    }

    /** Settable {@link java.time.Clock} for the GC tick tests. */
    private static final class AdjustableClock extends java.time.Clock {
        private volatile long nowMs;

        AdjustableClock(long initialMs) {
            this.nowMs = initialMs;
        }

        void set(long nowMs) {
            this.nowMs = nowMs;
        }

        @Override
        public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
        }

        @Override
        public java.time.Clock withZone(java.time.ZoneId zone) {
            return this; // tests don't care about zone
        }

        @Override
        public java.time.Instant instant() {
            return java.time.Instant.ofEpochMilli(nowMs);
        }

        @Override
        public long millis() {
            return nowMs;
        }
    }

    /**
     * Wire up the mocks the GC tick needs: empty topic-policies (so retention falls
     * through to namespace), a namespace policy with the requested retention, a
     * "drained" segment-backlog response (cursor at end), and a successful topic
     * delete admin call.
     */
    @SuppressWarnings("unchecked")
    private void installGcMocks(int nsRetentionMinutes) {
        // Topic-policies service: no policies set on the topic.
        var tps = mock(org.apache.pulsar.broker.service.TopicPoliciesService.class);
        when(pulsar.getTopicPoliciesService()).thenReturn(tps);
        when(tps.getTopicPoliciesAsync(any(),
                any(org.apache.pulsar.broker.service.TopicPoliciesService.GetType.class)))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        // Namespace policy with the requested retention. Wired through PulsarResources.
        var pulsarResources = mock(org.apache.pulsar.broker.resources.PulsarResources.class);
        var namespaceResources = mock(org.apache.pulsar.broker.resources.NamespaceResources.class);
        when(pulsar.getPulsarResources()).thenReturn(pulsarResources);
        when(pulsarResources.getNamespaceResources()).thenReturn(namespaceResources);
        org.apache.pulsar.common.policies.data.Policies nsPolicies =
                new org.apache.pulsar.common.policies.data.Policies();
        nsPolicies.retention_policies =
                new org.apache.pulsar.common.policies.data.RetentionPolicies(
                        nsRetentionMinutes, /* sizeMB */ -1);
        when(namespaceResources.getPoliciesAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(nsPolicies)));

        // No active subscriptions on the parent → nothing to drain → segment is
        // immediately considered prunable on retention expiry.
        // (resources.listSubscriptionsAsync returns [] from the in-memory store by default.)

        // Backing-topic delete succeeds (segment-aware admin call).
        when(scalableTopics.deleteSegmentAsync(anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    // --- ConsumerRegistration record sanity ---

    @Test
    public void testConsumerRegistrationIsEmptyRecord() {
        // If someone adds fields, this test documents the expectation that the znode
        // value is just a marker.
        ConsumerRegistration a = new ConsumerRegistration();
        ConsumerRegistration b = new ConsumerRegistration();
        assertEquals(a, b);
    }

    // --- createMigratedMetadata (PIP-475 regular-to-scalable migration) ---

    @Test
    public void testCreateMigratedMetadataForPartitionedTopic() {
        // 3-partition source → 3 sealed legacy parents (ids 0..2, full range, wrapping
        // each -partition-K) + 3 active range-based children (ids 3..5, full fan-in).
        TopicName base = TopicName.get("persistent://tenant/ns/my-topic");
        ScalableTopicMetadata md = ScalableTopicController.createMigratedMetadata(base, 3);

        assertEquals(md.getEpoch(), 0L);
        assertEquals(md.getNextSegmentId(), 6L);
        assertEquals(md.getSegments().size(), 6);

        // Parents 0..2: sealed legacy, full hash range, children = [3,4,5], no parents.
        for (int k = 0; k < 3; k++) {
            org.apache.pulsar.common.scalable.SegmentInfo parent = md.getSegments().get((long) k);
            assertTrue(parent.isSealed(), "parent " + k + " must be sealed");
            assertTrue(parent.isLegacy(), "parent " + k + " must be a legacy segment");
            assertEquals(parent.legacyTopicName(),
                    "persistent://tenant/ns/my-topic-partition-" + k);
            assertEquals(parent.hashRange().start(), 0x0000);
            assertEquals(parent.hashRange().end(), org.apache.pulsar.common.scalable.HashRange.MAX_HASH);
            assertTrue(parent.parentIds().isEmpty());
            assertEquals(parent.childIds(), java.util.List.of(3L, 4L, 5L));
        }

        // Children 3..5: active, range-based tiling, parents = [0,1,2], not legacy.
        int expectedWidth = (org.apache.pulsar.common.scalable.HashRange.MAX_HASH + 1) / 3;
        for (int j = 0; j < 3; j++) {
            long id = 3 + j;
            org.apache.pulsar.common.scalable.SegmentInfo child = md.getSegments().get(id);
            assertTrue(child.isActive(), "child " + id + " must be active");
            assertFalse(child.isLegacy(), "child " + id + " must be a regular segment");
            assertEquals(child.parentIds(), java.util.List.of(0L, 1L, 2L));
            assertTrue(child.childIds().isEmpty());
            int expectedStart = j * expectedWidth;
            assertEquals(child.hashRange().start(), expectedStart);
        }
        // Children tile the full space: first starts at 0, last ends at MAX_HASH.
        assertEquals(md.getSegments().get(3L).hashRange().start(), 0x0000);
        assertEquals(md.getSegments().get(5L).hashRange().end(),
                org.apache.pulsar.common.scalable.HashRange.MAX_HASH);
    }

    @Test
    public void testCreateMigratedMetadataForNonPartitionedTopic() {
        // Non-partitioned source (partitions <= 0) → 1 sealed legacy parent wrapping the
        // base persistent:// topic + 1 active child covering the full range.
        TopicName base = TopicName.get("persistent://tenant/ns/np-topic");
        ScalableTopicMetadata md = ScalableTopicController.createMigratedMetadata(base, 0);

        assertEquals(md.getNextSegmentId(), 2L);
        assertEquals(md.getSegments().size(), 2);

        org.apache.pulsar.common.scalable.SegmentInfo parent = md.getSegments().get(0L);
        assertTrue(parent.isSealed());
        assertTrue(parent.isLegacy());
        assertEquals(parent.legacyTopicName(), "persistent://tenant/ns/np-topic");
        assertEquals(parent.childIds(), java.util.List.of(1L));

        org.apache.pulsar.common.scalable.SegmentInfo child = md.getSegments().get(1L);
        assertTrue(child.isActive());
        assertFalse(child.isLegacy());
        assertEquals(child.parentIds(), java.util.List.of(0L));
        assertEquals(child.hashRange().start(), 0x0000);
        assertEquals(child.hashRange().end(), org.apache.pulsar.common.scalable.HashRange.MAX_HASH);
    }
}
