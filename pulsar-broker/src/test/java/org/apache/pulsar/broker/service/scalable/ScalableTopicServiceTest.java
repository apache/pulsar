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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.client.CollectorRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.resources.SubscriptionType;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.stats.PulsarBrokerOpenTelemetry;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.ScalableTopics;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;
import org.apache.pulsar.common.policies.data.SegmentTopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link ScalableTopicService} using an in-memory metadata store for
 * real persistence + leader election, with the cross-broker admin client mocked.
 */
public class ScalableTopicServiceTest {

    private static final String BROKER_ID = "broker-service-test";

    private HoldingMetadataStore store;
    private CoordinationService coordinationService;
    private ScalableTopicResources resources;
    /** The broker executor; cancelled tasks leave its queue, so the queue holds the live periodic tasks. */
    private ScheduledThreadPoolExecutor scheduler;

    private BrokerService brokerService;
    private PulsarService pulsar;
    private PulsarAdmin admin;
    private Topics topics;
    private ScalableTopics scalableTopicsAdmin;

    private ScalableTopicService service;
    private String clusterName;

    @BeforeMethod
    public void setUp() throws Exception {
        store = new HoldingMetadataStore(new LocalMemoryMetadataStore("memory:local",
                MetadataStoreConfig.builder().build()));
        coordinationService = new CoordinationServiceImpl(store);
        resources = new ScalableTopicResources(store, 30);
        scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setRemoveOnCancelPolicy(true);

        brokerService = mock(BrokerService.class);
        pulsar = mock(PulsarService.class);
        admin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        scalableTopicsAdmin = mock(ScalableTopics.class);

        when(brokerService.getPulsar()).thenReturn(pulsar);
        ServiceConfiguration config = new ServiceConfiguration();
        // Other tests can keep the shared collector alive, retaining this cluster's counters
        // after teardown. Use a distinct label per test instead of assuming a fresh registry.
        clusterName = "scalable-topic-service-test-" + UUID.randomUUID();
        config.setClusterName(clusterName);
        when(pulsar.getConfiguration()).thenReturn(config);
        PulsarBrokerOpenTelemetry telemetry = mock(PulsarBrokerOpenTelemetry.class);
        when(telemetry.getMeter()).thenReturn(OpenTelemetry.noop().getMeter("test"));
        when(pulsar.getOpenTelemetry()).thenReturn(telemetry);
        when(brokerService.getTopicIfExists(anyString()))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(brokerService.deleteSchema(any(TopicName.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
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

        // Stats: no segment is owned locally; the segment-stats admin endpoint is mocked.
        NamespaceService namespaceService = mock(NamespaceService.class);
        when(pulsar.getNamespaceService()).thenReturn(namespaceService);
        when(namespaceService.isServiceUnitOwnedAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(false));
        when(scalableTopicsAdmin.getSegmentStatsAsync(anyString(), anyLong()))
                .thenAnswer(inv -> CompletableFuture.completedFuture(new SegmentTopicStats()));

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

    @Test
    public void testBrokerMetricsAggregateAndReleaseControllers() throws Exception {
        TopicName first = scalableTopic("metrics-first");
        TopicName second = scalableTopic("metrics-second");
        service.createScalableTopic(first, 2).get();
        service.createScalableTopic(second, 3).get();
        ScalableTopicController leader = service.getOrCreateController(first).get();
        service.getOrCreateController(second).get();
        assertThat(prometheusValue("controller_count")).isEqualTo(2.0);
        assertThat(prometheusSegments("active")).isEqualTo(5.0);
        assertThat(prometheusSegments("sealed")).isZero();

        leader.splitSegment(0).get();
        assertThat(prometheusSegments("active")).isEqualTo(6.0);
        assertThat(prometheusSegments("sealed")).isEqualTo(1.0);
        service.releaseController(first).get();
        assertThat(prometheusValue("controller_count")).isEqualTo(1.0);
        assertThat(prometheusSegments("active")).isEqualTo(3.0);
        assertThat(prometheusSegments("sealed")).isZero();
        service.close();
        assertThat(prometheusValue("controller_count")).isNull();
    }

    @Test
    public void testMetricsCleanupDoesNotChangeControllerLifecycle() throws Exception {
        service.close();
        ScalableTopicResources observedResources = spy(resources);
        service = new ScalableTopicService(brokerService, observedResources, coordinationService);
        TopicName topic = scalableTopic("metrics-cleanup");
        service.createScalableTopic(topic, 1).get();
        ScalableTopicController controller = service.getOrCreateController(topic).get();
        Object[] tasks = scheduler.getQueue().toArray();
        clearInvocations(observedResources, scalableTopicsAdmin);

        service.closeMetrics();

        verifyNoInteractions(observedResources, scalableTopicsAdmin);
        assertThat(prometheusValue("controller_count")).isNull();
        assertThat(controller.isClosed()).isFalse();
        assertThat(controller.isLeader()).isTrue();
        assertThat(store.exists(resources.controllerLockPath(topic)).get()).isTrue();
        assertThat(scheduler.getQueue().toArray()).containsExactlyInAnyOrder(tasks);
    }

    @Test(timeOut = 10_000)
    public void testMetricsCleanupDoesNotWaitForControllerInitialization() throws Exception {
        TopicName topic = scalableTopic("metrics-cleanup-pending-init");
        service.createScalableTopic(topic, 1).get();
        CompletableFuture<Void> create = new CompletableFuture<>();
        when(scalableTopicsAdmin.createSegmentAsync(anyString(), any())).thenReturn(create);
        CompletableFuture<ScalableTopicController> initializing = service.getOrCreateController(topic);
        try {
            assertThat(initializing).isNotDone();
            service.closeMetrics();
            assertThat(prometheusValue("controller_count")).isNull();
            assertThat(initializing).isNotDone();
        } finally {
            create.complete(null);
            initializing.get(5, TimeUnit.SECONDS);
        }
    }

    private Double prometheusValue(String suffix) {
        return CollectorRegistry.defaultRegistry.getSampleValue("pulsar_scalable_topic_" + suffix,
                new String[]{"cluster"}, new String[]{clusterName});
    }

    private Double prometheusSegments(String state) {
        return CollectorRegistry.defaultRegistry.getSampleValue("pulsar_scalable_topic_segment_count",
                new String[]{"cluster", "segment_state"}, new String[]{clusterName, state});
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

    /**
     * A controller whose initialize() fails after winning the election is closed, or its election
     * would keep the controller lock with no controller left to release it, and the consumer
     * sessions it restored would keep their grace timers.
     */
    @Test
    public void testFailedInitializationLeavesNothingRunning() throws Exception {
        // With a broker configuration, a serving leader schedules its auto split/merge tick next to
        // the GC tick. Neither fires during the test (60s).
        when(pulsar.getConfig()).thenReturn(new ServiceConfiguration());
        TopicName tn = scalableTopic("t-init-fails-partial-restore");
        service.createScalableTopic(tn, 1).get();
        String lockPath = resources.controllerLockPath(tn);
        resources.createSubscriptionAsync(tn, "sub-a", SubscriptionType.STREAM).get();
        resources.registerConsumerAsync(tn, "sub-a", "c1").get();
        resources.createSubscriptionAsync(tn, "sub-b", SubscriptionType.STREAM).get();
        resources.registerConsumerAsync(tn, "sub-b", "c2").get();
        // Restoring the consumer sessions runs once the controller won the election. Fail it on
        // sub-b, while sub-a's consumer is restored with a grace timer. The failure is injected
        // once, so the next attempt succeeds.
        MetadataStoreException injected = new MetadataStoreException("injected");
        String consumersPath = resources.subscriptionPath(tn, "sub-b") + "/consumers";
        store.failConditional(injected, (op, path) ->
                op == FaultInjectionMetadataStore.OperationType.GET_CHILDREN && path.equals(consumersPath));

        try {
            service.getOrCreateController(tn).get();
            fail("expected the controller initialization to fail");
        } catch (ExecutionException e) {
            assertSame(FutureUtil.unwrapCompletionException(e.getCause().getCause()), injected,
                    "the attempt must fail restoring the sessions, got: " + e.getCause());
        }
        // The failed attempt never served, so it scheduled no GC or auto split/merge tick. It left
        // c1's grace timer, which the close cancels, and a takeover retry, which the close doesn't
        // cancel but makes a no-op: the queue empties once that retry ran.
        Awaitility.await().untilAsserted(() -> {
            assertFalse(store.exists(lockPath).get(), "the failed controller must give the lock back");
            assertTrue(scheduler.getQueue().isEmpty(), "the failed controller must leave no task scheduled");
        });
        // The close leaves the persisted registrations to the next controller.
        assertEquals(resources.listConsumersAsync(tn, "sub-a").get(), List.of("c1"));
        assertEquals(resources.listConsumersAsync(tn, "sub-b").get(), List.of("c2"));

        // The retry runs the only controller of the topic. Serving, it schedules the GC and auto
        // split/merge ticks, and restores c1 and c2 with grace timers: releasing it cancels them.
        assertTrue(service.getOrCreateController(tn).get().isLeader(), "the retry must lead the topic");
        assertFalse(scheduler.getQueue().isEmpty(), "the serving controller must have scheduled its tasks");
        service.releaseController(tn).get();
        assertFalse(store.exists(lockPath).get());
        assertTrue(scheduler.getQueue().isEmpty(), "no task may outlive the released controller");
    }

    /**
     * A controller that fails to initialize is closed without waiting for its election to delete the
     * lock, so the next attempt can start while that delete is in flight: its election finds the lock
     * of this broker's session and keeps it, and takes it again once the delete went through.
     */
    @Test
    public void testRetryWhileTheFailedControllerDeletesTheLock() throws Exception {
        TopicName tn = scalableTopic("t-init-fails-retry");
        service.createScalableTopic(tn, 1).get();
        String lockPath = resources.controllerLockPath(tn);
        // Fail the session restore once, before any subscription is restored.
        String subscriptionsPath = resources.topicPath(tn) + "/subscriptions";
        store.failConditional(new MetadataStoreException("injected"), (op, path) ->
                op == FaultInjectionMetadataStore.OperationType.GET_CHILDREN && path.equals(subscriptionsPath));
        store.holdNextDelete(lockPath);

        try {
            service.getOrCreateController(tn).get();
            fail("expected the controller initialization to fail");
        } catch (ExecutionException expected) {
            // initialize() failed after the election
        }
        assertTrue(store.isHoldingDelete(), "the failed controller must be deleting the lock");

        ScalableTopicController controller = service.getOrCreateController(tn).get();
        assertTrue(controller.isLeader(), "the retry must lead with the lock it finds");

        store.releaseHeldDelete();
        Awaitility.await().untilAsserted(() ->
                assertTrue(store.exists(lockPath).get(), "the retry's election must take the lock again"));
        assertTrue(controller.isLeader(), "the retry must keep leading");
        assertSame(service.getOrCreateController(tn).get(), controller, "the retry's controller must be the only one");

        service.releaseController(tn).get();
        assertFalse(store.exists(lockPath).get());
        // Once the failed attempt's takeover retry ran, as a no-op.
        Awaitility.await().untilAsserted(() ->
                assertTrue(scheduler.getQueue().isEmpty(), "no task may outlive the released controller"));
    }

    @Test
    public void testSubscribeToMissingTopicsLeavesNoLeaderElectionOpen() throws Exception {
        CoordinationService observedCoordinationService = spy(coordinationService);
        Map<String, LeaderElection<?>> elections = new ConcurrentHashMap<>();
        doAnswer(inv -> {
            LeaderElection<?> election = (LeaderElection<?>) inv.callRealMethod();
            elections.put(inv.getArgument(1), election);
            return election;
        }).when(observedCoordinationService).getLeaderElection(any(), anyString(), any());
        ScalableTopicService observedService =
                new ScalableTopicService(brokerService, resources, observedCoordinationService);
        List<String> lockPaths = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                TopicName missing = scalableTopic("t-missing-" + i);
                lockPaths.add(resources.controllerLockPath(missing));
                try {
                    observedService.registerConsumer(missing, "sub", "c1", i, ScalableConsumerType.STREAM,
                            mock(TransportCnx.class)).get();
                    fail("expected the subscribe to a missing topic to fail");
                } catch (ExecutionException e) {
                    assertTrue(e.getCause().getMessage().contains("Failed to initialize controller"),
                            "got: " + e.getCause().getMessage());
                }
            }
            // The coordination service hands out the same election for a path until it is closed: an
            // election left open would stay with the broker until it restarts. A controller that
            // created none passes too. Snapshot first: the probes' own elections are recorded.
            Map<String, LeaderElection<?>> handedOut = Map.copyOf(elections);
            for (String path : lockPaths) {
                assertNotSame(observedCoordinationService.getLeaderElection(String.class, path, state -> { }),
                        handedOut.get(path),
                        "the election of a controller that failed to initialize must be closed: " + path);
            }
        } finally {
            observedService.close();
        }
    }

    @Test
    public void testControllerCreatedAfterFailedInitializationLeads() throws Exception {
        TopicName tn = scalableTopic("t-late");
        try {
            service.getOrCreateController(tn).get();
            fail("expected failure: the topic doesn't exist yet");
        } catch (ExecutionException expected) {
            // The failed controller must not leave its leader election to the next one.
        }
        service.createScalableTopic(tn, 1).get();

        ScalableTopicController controller = service.getOrCreateController(tn).get();
        assertTrue(controller.isLeader(), "the only controller of the topic must lead it");
    }

    @Test
    public void testClosedControllerIsReplaced() throws Exception {
        TopicName tn = scalableTopic("t-closed");
        service.createScalableTopic(tn, 1).get();
        // A controller closes itself when it finds its topic deleted while taking over.
        ScalableTopicController closed = service.getOrCreateController(tn).get();
        closed.close().get();

        ScalableTopicController controller = service.getOrCreateController(tn).get();
        assertNotSame(controller, closed);
        assertTrue(controller.isLeader(), "the controller replacing the closed one must lead");
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
        assertEquals(stats.getLayout().getSegments().size(), 3);
        assertTrue(stats.getLayout().getSegments().values().stream()
                .allMatch(ScalableTopicStats.LayoutSegment::isActive));
        assertEquals(stats.getSubscriptions().keySet(), java.util.Set.of("sub-a"));
        assertEquals(stats.getSubscriptions().get("sub-a").getType(), ScalableSubscriptionType.STREAM,
                "the type recorded by createSubscription is reported");
    }

    // --- consumer registration delegation ---

    @Test
    public void testRegisterConsumerDelegates() throws Exception {
        TopicName tn = scalableTopic("t-reg");
        service.createScalableTopic(tn, 2).get();

        ConsumerAssignment assignment = service.registerConsumer(tn, "sub-z", "c1", 1L, ScalableConsumerType.STREAM,
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

        // Seed child records under the topic — a subscription and a per-segment load
        // record — to verify the delete is recursive and takes everything with it.
        resources.createSubscriptionAsync(tn, "sub-a",
                org.apache.pulsar.broker.resources.SubscriptionType.STREAM).get();
        resources.reportSegmentLoadAsync(tn, 0,
                new org.apache.pulsar.common.scalable.SegmentLoadStats(1, 1, 1, 1)).get();

        service.deleteScalableTopic(tn).get();

        assertFalse(resources.scalableTopicExistsAsync(tn).get());
        assertFalse(resources.subscriptionExistsAsync(tn, "sub-a").get(),
                "topic delete must remove subscription records");
        assertFalse(resources.getSegmentLoadAsync(tn, 0).get().isPresent(),
                "topic delete must remove segment load records");
        verify(scalableTopicsAdmin, org.mockito.Mockito.atLeast(2))
                .deleteSegmentAsync(anyString(), anyBoolean());
        // Segment deletes leave the shared schema in place; the topic delete removes it.
        verify(brokerService).deleteSchema(tn);
    }

    @Test
    public void testControllerReleasedWhenTopicRecordDeleted() throws Exception {
        TopicName tn = scalableTopic("t-deleted-elsewhere");
        service.createScalableTopic(tn, 1).get();
        service.getOrCreateController(tn).get();
        String lockPath = resources.controllerLockPath(tn);
        assertTrue(store.exists(lockPath).get());

        // Delete the metadata the way an admin delete served by another broker does, without
        // releasing the controller held here first.
        resources.deleteScalableTopicAsync(tn).get();

        Awaitility.await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertFalse(store.exists(lockPath).get(),
                        "the controller lock must not be re-acquired under the deleted topic"));
        try {
            service.getOrCreateController(tn).get();
            fail("the deleted topic must not be served by its old controller");
        } catch (ExecutionException e) {
            assertTrue(e.getCause().getMessage().contains("Failed to initialize controller"),
                    "got: " + e.getCause().getMessage());
        }
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

    @Test
    public void testMetricsInitializationFailureDoesNotPreventConsumerCoordination() throws Exception {
        when(pulsar.getOpenTelemetry().getMeter())
                .thenThrow(new IllegalArgumentException("Injected metrics initialization failure"));
        ScalableTopicService withoutMetrics = new ScalableTopicService(brokerService, resources, coordinationService);
        try {
            withoutMetrics.start();
            TopicName topic = scalableTopic("metrics-unavailable");
            withoutMetrics.createScalableTopic(topic, 1).get();
            ConsumerAssignment assignment = withoutMetrics.registerConsumer(topic, "checkpoint", "one", 1,
                    ScalableConsumerType.CHECKPOINT, mock(TransportCnx.class)).get();
            assertEquals(assignment.assignedSegments().size(), 1);
            assertTrue(withoutMetrics.getOrCreateController(topic).get().isLeader());
            assertEquals(withoutMetrics.consumerStats(), new SubscriptionCoordinator.ConsumerStats(1, 0, 0, 0));
            // The healthy service's existing registration and counters must remain untouched.
            assertEquals(CollectorRegistry.defaultRegistry.getSampleValue("pulsar_scalable_topic_rebalance_total",
                    new String[]{"cluster", "rebalance_reason"}, new String[]{clusterName, "consumer_join"}), 0.0);
        } finally {
            withoutMetrics.close();
        }
    }

    @Test
    public void testConsumerMetricsAggregateLeadersAndRetainCountersAfterRelease() throws Exception {
        TopicName first = scalableTopic("metrics-first");
        TopicName second = scalableTopic("metrics-second");
        service.createScalableTopic(first, 1).get();
        service.createScalableTopic(second, 1).get();
        service.registerConsumer(first, "stream", "one", 1, ScalableConsumerType.STREAM,
                mock(TransportCnx.class)).get();
        service.registerConsumer(second, "checkpoint", "two", 2, ScalableConsumerType.CHECKPOINT,
                mock(TransportCnx.class)).get();
        assertEquals(service.consumerStats(), new SubscriptionCoordinator.ConsumerStats(2, 0, 0, 0));
        service.onConsumerDisconnect(first, "stream", "one");
        assertEquals(service.consumerStats(), new SubscriptionCoordinator.ConsumerStats(1, 0, 1, 0));
        service.releaseController(first).get();
        assertEquals(service.consumerStats(), new SubscriptionCoordinator.ConsumerStats(1, 0, 0, 0));
        service.releaseController(second).get();
        assertEquals(service.consumerStats(), SubscriptionCoordinator.ConsumerStats.EMPTY);
        assertEquals(CollectorRegistry.defaultRegistry.getSampleValue("pulsar_scalable_topic_rebalance_total",
                new String[]{"cluster", "rebalance_reason"}, new String[]{clusterName, "consumer_join"}), 2.0);
        service.start(); // Idempotent: must not reset counters or register another collector.
        assertEquals(CollectorRegistry.defaultRegistry.getSampleValue("pulsar_scalable_topic_rebalance_total",
                new String[]{"cluster", "rebalance_reason"}, new String[]{clusterName, "consumer_join"}), 2.0);
    }

    @Test
    public void testClosingMetricsDoesNotReleaseControllersOrConsumerSessions() throws Exception {
        TopicName topic = scalableTopic("metrics-only-cleanup");
        service.createScalableTopic(topic, 1).get();
        ScalableTopicController controller = service.getOrCreateController(topic).get();
        service.registerConsumer(topic, "checkpoint", "one", 1, ScalableConsumerType.CHECKPOINT,
                mock(TransportCnx.class)).get();
        service.closeMetrics();
        assertTrue(controller.isLeader());
        assertEquals(resources.listConsumersAsync(topic, "checkpoint").get(), List.of("one"));
        assertEquals(service.consumerStats(), new SubscriptionCoordinator.ConsumerStats(1, 0, 0, 0));
        assertEquals(CollectorRegistry.defaultRegistry.getSampleValue("pulsar_scalable_topic_consumer_count",
                new String[]{"cluster", "consumer_state"}, new String[]{clusterName, "active"}), null);
        service.registerConsumer(topic, "checkpoint", "two", 2, ScalableConsumerType.CHECKPOINT,
                mock(TransportCnx.class)).get();
        assertEquals(service.consumerStats(), new SubscriptionCoordinator.ConsumerStats(1, 1, 0, 0));
    }

    @Test
    public void testMetricCollectionDoesNotAcquireControllerMonitor() throws Exception {
        TopicName topic = scalableTopic("metrics-no-controller-lock");
        service.createScalableTopic(topic, 1).get();
        ScalableTopicController controller = service.getOrCreateController(topic).get();
        service.registerConsumer(topic, "checkpoint", "one", 1, ScalableConsumerType.CHECKPOINT,
                mock(TransportCnx.class)).get();
        synchronized (controller) {
            assertEquals(CompletableFuture.supplyAsync(service::consumerStats, scheduler)
                    .get(5, TimeUnit.SECONDS), new SubscriptionCoordinator.ConsumerStats(1, 0, 0, 0));
        }
    }

    /**
     * Holds back the next delete of a path until it is released: {@link FaultInjectionMetadataStore}
     * can fail an operation, but not delay it.
     */
    private static class HoldingMetadataStore extends FaultInjectionMetadataStore {
        private final AtomicReference<String> pathToHold = new AtomicReference<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private final CompletableFuture<Void> heldDelete = new CompletableFuture<>();
        private volatile boolean holding;

        HoldingMetadataStore(MetadataStoreExtended store) {
            super(store);
        }

        /** Hold back the next delete of {@code path}, until {@link #releaseHeldDelete()}. */
        void holdNextDelete(String path) {
            pathToHold.set(path);
        }

        boolean isHoldingDelete() {
            return holding && !release.isDone();
        }

        /** Let the delete held back go through, and wait for it. */
        void releaseHeldDelete() throws Exception {
            release.complete(null);
            heldDelete.get(10, TimeUnit.SECONDS);
        }

        @Override
        public CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion, Set<Option> opts) {
            String held = pathToHold.get();
            // compareAndSet() compares references: pass it the instance it holds.
            if (!path.equals(held) || !pathToHold.compareAndSet(held, null)) {
                return super.delete(path, expectedVersion, opts);
            }
            holding = true;
            FutureUtil.completeAfter(heldDelete, release.thenCompose(__ -> super.delete(path, expectedVersion, opts)));
            return heldDelete;
        }
    }
}
