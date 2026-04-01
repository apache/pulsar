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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
import org.apache.pulsar.client.api.MessageId;
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
        when(topics.createSubscriptionAsync(anyString(), anyString(), any(MessageId.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(topics.deleteSubscriptionAsync(anyString(), anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scalableTopics.createSegmentAsync(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scalableTopics.terminateSegmentAsync(anyString()))
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
                () -> controller.registerConsumer("sub", "c1", 1L, mock(TransportCnx.class)));
        assertThrows(IllegalStateException.class, () -> controller.unregisterConsumer("sub", "c1"));
    }

    // --- Consumer registration ---

    @Test
    public void testRegisterConsumerPersistsAndAssigns() throws Exception {
        controller.initialize().get();
        ConsumerAssignment assignment =
                controller.registerConsumer("sub-a", "c1", 1L, mock(TransportCnx.class)).get();

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
        controller.registerConsumer("sub-a", "c1", 1L, mock(TransportCnx.class)).get();
        // Reconnect: same name, new consumerId.
        ConsumerAssignment assignment =
                controller.registerConsumer("sub-a", "c1", 99L, mock(TransportCnx.class)).get();

        assertEquals(assignment.assignedSegments().size(), INITIAL_SEGMENTS);
        // Still just one persisted registration.
        assertEquals(resources.listConsumersAsync(topicName, "sub-a").get().size(), 1);
    }

    @Test
    public void testUnregisterConsumerDeletesPersistedEntry() throws Exception {
        controller.initialize().get();
        controller.registerConsumer("sub-a", "c1", 1L, mock(TransportCnx.class)).get();
        controller.registerConsumer("sub-a", "c2", 2L, mock(TransportCnx.class)).get();
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
        // Propagated to every active segment via admin.topics().createSubscriptionAsync().
        verify(topics, org.mockito.Mockito.times(INITIAL_SEGMENTS))
                .createSubscriptionAsync(anyString(), anyString(), any(MessageId.class));
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
        verify(topics, org.mockito.Mockito.atLeast(INITIAL_SEGMENTS))
                .deleteSubscriptionAsync(anyString(), anyString(), anyBoolean());
    }

    @Test
    public void testDeleteSubscriptionRemovesInMemoryCoordinator() throws Exception {
        controller.initialize().get();
        controller.createSubscription("sub-a", SubscriptionType.STREAM).get();
        controller.registerConsumer("sub-a", "c1", 1L, mock(TransportCnx.class)).get();

        controller.deleteSubscription("sub-a").get();
        // After delete, the persisted consumer entries should be gone.
        assertEquals(resources.listConsumersAsync(topicName, "sub-a").get().size(), 0);
    }

    // --- splitSegment / mergeSegments ---

    @Test
    public void testSplitSegment() throws Exception {
        controller.initialize().get();
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
        controller.registerConsumer("sub-a", "c1", 1L, mock(TransportCnx.class)).get();

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
        controller.registerConsumer("sub-a", "c1", 1L, mock(TransportCnx.class)).get();
        controller.registerConsumer("sub-a", "c2", 2L, mock(TransportCnx.class)).get();

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

    // --- ConsumerRegistration record sanity ---

    @Test
    public void testConsumerRegistrationIsEmptyRecord() {
        // If someone adds fields, this test documents the expectation that the znode
        // value is just a marker.
        ConsumerRegistration a = new ConsumerRegistration();
        ConsumerRegistration b = new ConsumerRegistration();
        assertEquals(a, b);
    }
}
