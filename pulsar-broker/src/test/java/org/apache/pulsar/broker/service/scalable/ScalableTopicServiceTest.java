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
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
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
 * Unit tests for {@link ScalableTopicService} using an in-memory metadata store for
 * real persistence + leader election, with the cross-broker admin client mocked.
 */
public class ScalableTopicServiceTest {

    private static final String BROKER_ID = "broker-service-test";

    private MetadataStoreExtended store;
    private CoordinationService coordinationService;
    private ScalableTopicResources resources;
    private ScheduledExecutorService scheduler;

    private BrokerService brokerService;
    private PulsarService pulsar;
    private PulsarAdmin admin;
    private Topics topics;
    private ScalableTopics scalableTopicsAdmin;

    private ScalableTopicService service;

    @BeforeMethod
    public void setUp() throws Exception {
        store = new LocalMemoryMetadataStore("memory:local",
                MetadataStoreConfig.builder().build());
        coordinationService = new CoordinationServiceImpl(store);
        resources = new ScalableTopicResources(store, 30);
        scheduler = Executors.newSingleThreadScheduledExecutor();

        brokerService = mock(BrokerService.class);
        pulsar = mock(PulsarService.class);
        admin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        scalableTopicsAdmin = mock(ScalableTopics.class);

        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.getTopicIfExists(anyString()))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(pulsar.getBrokerId()).thenReturn(BROKER_ID);
        when(pulsar.getExecutor()).thenReturn(scheduler);
        when(pulsar.getAdminClient()).thenReturn(admin);
        when(admin.topics()).thenReturn(topics);
        when(admin.scalableTopics()).thenReturn(scalableTopicsAdmin);

        when(topics.getSubscriptionsAsync(anyString()))
                .thenReturn(CompletableFuture.completedFuture(java.util.List.of()));
        when(topics.createSubscriptionAsync(anyString(), anyString(), any(MessageId.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(topics.deleteSubscriptionAsync(anyString(), anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scalableTopicsAdmin.createSegmentAsync(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scalableTopicsAdmin.deleteSegmentAsync(anyString(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scalableTopicsAdmin.terminateSegmentAsync(anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        service = new ScalableTopicService(brokerService, resources, coordinationService);
        service.start();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (service != null) {
            service.close();
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

    private TopicName scalableTopic(String name) {
        return TopicName.get("topic://tenant/ns/" + name);
    }

    // --- createScalableTopic ---

    @Test
    public void testCreateScalableTopicStoresMetadataAndCreatesSegments() throws Exception {
        TopicName tn = scalableTopic("t1");

        service.createScalableTopic(tn, 3).get();

        Optional<ScalableTopicMetadata> md = resources.getScalableTopicMetadataAsync(tn).get();
        assertTrue(md.isPresent());
        assertEquals(md.get().getSegments().size(), 3);
        // Created one segment per initial segment via the segments admin API.
        verify(scalableTopicsAdmin, org.mockito.Mockito.times(3))
                .createSegmentAsync(anyString(), any());
    }

    @Test
    public void testCreateScalableTopicWithProperties() throws Exception {
        TopicName tn = scalableTopic("t-props");
        service.createScalableTopic(tn, 2, Map.of("k", "v")).get();

        ScalableTopicMetadata md = resources.getScalableTopicMetadataAsync(tn).get()
                .orElseThrow();
        assertEquals(md.getProperties().get("k"), "v");
    }

    @Test
    public void testCreateScalableTopicRejectsZeroSegments() {
        TopicName tn = scalableTopic("t-bad");
        try {
            service.createScalableTopic(tn, 0).get();
            fail("expected IllegalArgumentException");
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            assertTrue(cause instanceof IllegalArgumentException,
                    "expected IllegalArgumentException, got " + cause);
        }
    }

    @Test
    public void testCreateScalableTopicRejectsWrongDomain() {
        TopicName persistent = TopicName.get("persistent://tenant/ns/p");
        try {
            service.createScalableTopic(persistent, 1).get();
            fail("expected IllegalArgumentException");
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            assertTrue(cause instanceof IllegalArgumentException,
                    "expected IllegalArgumentException, got " + cause);
        }
    }

    // --- getOrCreateController ---

    @Test
    public void testGetOrCreateControllerCachesInstance() throws Exception {
        TopicName tn = scalableTopic("t-cache");
        service.createScalableTopic(tn, 1).get();

        ScalableTopicController first = service.getOrCreateController(tn).get();
        ScalableTopicController second = service.getOrCreateController(tn).get();
        assertSame(first, second, "same controller returned on re-entry");
    }

    @Test
    public void testGetOrCreateControllerFailsIfTopicMissing() throws Exception {
        TopicName tn = scalableTopic("t-ghost");
        try {
            service.getOrCreateController(tn).get();
            fail("expected failure");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            assertTrue(cause instanceof RuntimeException);
            assertTrue(cause.getMessage().contains("Failed to initialize controller"),
                    "got: " + cause.getMessage());
        }
    }

    @Test
    public void testReleaseControllerDrops() throws Exception {
        TopicName tn = scalableTopic("t-release");
        service.createScalableTopic(tn, 1).get();
        ScalableTopicController first = service.getOrCreateController(tn).get();

        service.releaseController(tn).get();
        ScalableTopicController second = service.getOrCreateController(tn).get();
        assertNotSame(first, second, "a fresh controller should be created after release");
    }

    // --- split / merge delegation ---

    @Test
    public void testSplitSegmentDelegates() throws Exception {
        TopicName tn = scalableTopic("t-split");
        service.createScalableTopic(tn, 2).get();

        service.splitSegment(tn, 0).get();

        ScalableTopicMetadata md = resources.getScalableTopicMetadataAsync(tn).get()
                .orElseThrow();
        assertEquals(md.getEpoch(), 1);
        assertEquals(md.getSegments().size(), 4,
                "split adds two children, keeps parent as sealed");
    }

    @Test
    public void testMergeSegmentsDelegates() throws Exception {
        TopicName tn = scalableTopic("t-merge");
        service.createScalableTopic(tn, 2).get();

        service.mergeSegments(tn, 0, 1).get();

        ScalableTopicMetadata md = resources.getScalableTopicMetadataAsync(tn).get()
                .orElseThrow();
        assertEquals(md.getEpoch(), 1);
        // Two parents sealed + one merged child = 3 segments total.
        assertEquals(md.getSegments().size(), 3);
    }

    // --- subscription admin delegation ---

    @Test
    public void testCreateSubscriptionStream() throws Exception {
        TopicName tn = scalableTopic("t-cs-stream");
        service.createScalableTopic(tn, 2).get();

        service.createSubscription(tn, "sub-x", SubscriptionType.STREAM).get();

        var persisted = resources.getSubscriptionAsync(tn, "sub-x").get();
        assertTrue(persisted.isPresent());
        assertEquals(persisted.get().type(), SubscriptionType.STREAM);
    }

    @Test
    public void testDeleteSubscription() throws Exception {
        TopicName tn = scalableTopic("t-ds");
        service.createScalableTopic(tn, 2).get();
        service.createSubscription(tn, "sub-y", SubscriptionType.STREAM).get();

        service.deleteSubscription(tn, "sub-y").get();

        assertFalse(resources.getSubscriptionAsync(tn, "sub-y").get().isPresent());
    }

    // --- stats ---

    @Test
    public void testGetStats() throws Exception {
        TopicName tn = scalableTopic("t-stats");
        service.createScalableTopic(tn, 3).get();
        service.createSubscription(tn, "sub-a", SubscriptionType.STREAM).get();

        ScalableTopicStats stats = service.getStats(tn).get();
        assertEquals(stats.getActiveSegments(), 3);
        assertEquals(stats.getTotalSegments(), 3);
        assertEquals(stats.getSubscriptions().keySet(), java.util.Set.of("sub-a"));
    }

    // --- consumer registration delegation ---

    @Test
    public void testRegisterConsumerDelegates() throws Exception {
        TopicName tn = scalableTopic("t-reg");
        service.createScalableTopic(tn, 2).get();

        ConsumerAssignment assignment = service.registerConsumer(tn, "sub-z", "c1", 1L,
                mock(TransportCnx.class)).get();
        assertEquals(assignment.assignedSegments().size(), 2);
        assertEquals(resources.listConsumersAsync(tn, "sub-z").get().size(), 1);
    }

    @Test
    public void testOnConsumerDisconnectIsNoopIfNoLocalController() {
        TopicName tn = scalableTopic("t-no-controller");
        // No controller has been created for this topic → call should be a silent no-op.
        service.onConsumerDisconnect(tn, "sub", "c1");
    }

    // --- deleteScalableTopic ---

    @Test
    public void testDeleteScalableTopicCleansUp() throws Exception {
        TopicName tn = scalableTopic("t-del");
        service.createScalableTopic(tn, 2).get();
        assertTrue(resources.scalableTopicExistsAsync(tn).get());

        service.deleteScalableTopic(tn).get();

        assertFalse(resources.scalableTopicExistsAsync(tn).get());
        verify(scalableTopicsAdmin, org.mockito.Mockito.atLeast(2))
                .deleteSegmentAsync(anyString(), anyBoolean());
    }

    // --- concurrent topic isolation ---

    @Test
    public void testControllersForDifferentTopicsAreIndependent() throws Exception {
        TopicName tn1 = scalableTopic("t-indep-1");
        TopicName tn2 = scalableTopic("t-indep-2");
        service.createScalableTopic(tn1, 2).get();
        service.createScalableTopic(tn2, 2).get();

        ScalableTopicController c1 = service.getOrCreateController(tn1).get();
        ScalableTopicController c2 = service.getOrCreateController(tn2).get();

        assertNotSame(c1, c2);
        assertEquals(c1.getTopicName(), tn1);
        assertEquals(c2.getTopicName(), tn2);
    }
}
