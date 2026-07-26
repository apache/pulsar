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
import static org.testng.Assert.assertThrows;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.ScalableTopics;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.scalable.SegmentLoadStats;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for the controller's auto split/merge wiring (PIP-483): the periodic /
 * event-driven evaluation reads load records + consumer counts, runs the evaluator, and
 * dispatches to the real splitSegment / mergeSegments paths (against an in-memory metadata
 * store and a mocked cross-broker admin client). The decision logic itself is unit-tested in
 * {@link AutoScalePolicyEvaluatorTest}; here we verify the plumbing actually fires.
 */
public class ScalableTopicControllerAutoScaleTest {

    private static final String BROKER_ID = "broker-test";

    private MetadataStoreExtended store;
    private CoordinationService coordinationService;
    private ScalableTopicResources resources;
    private ScheduledExecutorService scheduler;
    private BrokerService brokerService;
    private PulsarService pulsar;
    private ServiceConfiguration config;
    private ScalableTopics scalableTopics;
    private PulsarResources pulsarResources;
    private NamespaceResources namespaceResources;
    /** Namespace policies served by the mocked resources; null = namespace has no policies. */
    private Policies namespacePolicies;
    private ScalableTopicController controller;
    private TopicName topicName;

    @BeforeMethod
    public void setUp() throws Exception {
        store = new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build());
        coordinationService = new CoordinationServiceImpl(store);
        resources = new ScalableTopicResources(store, 30);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        topicName = TopicName.get("topic://tenant/ns/my-topic");

        // Auto-scale tuned for deterministic single-shot evaluation: no cooldowns/windows so a
        // single evaluateAutoScaleForTest() call acts immediately, low-ish thresholds.
        config = new ServiceConfiguration();
        config.setScalableTopicAutoScaleEnabled(true);
        config.setScalableTopicMaxSegments(64);
        config.setScalableTopicMinSegments(1);
        config.setScalableTopicSplitCooldownSeconds(0);
        config.setScalableTopicMergeCooldownSeconds(0);
        config.setScalableTopicMergeWindowSeconds(0);
        config.setScalableTopicSplitMsgRateInThreshold(10_000);

        brokerService = mock(BrokerService.class);
        pulsar = mock(PulsarService.class);
        PulsarAdmin admin = mock(PulsarAdmin.class);
        Topics topics = mock(Topics.class);
        scalableTopics = mock(ScalableTopics.class);

        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.getTopicIfExists(anyString()))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(pulsar.getBrokerId()).thenReturn(BROKER_ID);
        when(pulsar.getExecutor()).thenReturn(scheduler);
        when(pulsar.getConfig()).thenReturn(config);

        // Namespace policies feed the per-namespace auto-scale override resolution.
        // Default: no policies → broker config applies. Tests install overrides via
        // namespacePolicies. Reset explicitly — TestNG reuses the test instance.
        namespacePolicies = null;
        pulsarResources = mock(PulsarResources.class);
        namespaceResources = mock(NamespaceResources.class);
        when(pulsar.getPulsarResources()).thenReturn(pulsarResources);
        when(pulsarResources.getNamespaceResources()).thenReturn(namespaceResources);
        when(namespaceResources.getPoliciesAsync(any(NamespaceName.class)))
                .thenAnswer(__ -> CompletableFuture.completedFuture(
                        Optional.ofNullable(namespacePolicies)));

        when(pulsar.getAdminClient()).thenReturn(admin);
        when(admin.topics()).thenReturn(topics);
        when(admin.scalableTopics()).thenReturn(scalableTopics);
        when(scalableTopics.createSegmentAsync(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scalableTopics.terminateSegmentAsync(anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
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

    private void startController(int initialSegments) throws Exception {
        resources.createScalableTopicAsync(topicName,
                ScalableTopicController.createInitialMetadata(initialSegments, 4, Map.of())).get();
        controller = new ScalableTopicController(topicName, resources, brokerService,
                coordinationService);
        controller.initialize().get();
    }

    private int activeSegmentCount() throws Exception {
        return controller.getLayout().get().getActiveSegments().size();
    }

    @Test
    public void testLoadDrivenSplit() throws Exception {
        startController(2);
        assertEquals(activeSegmentCount(), 2);

        // Segment 0 is hot on msgRateIn → expect a split.
        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();

        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 3, "hot segment should have been split");
    }

    @Test
    public void testNoSplitWhenUnderThreshold() throws Exception {
        startController(2);
        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(100, 0, 0, 0)).get();

        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 2, "no segment over threshold → no change");
    }

    @Test
    public void testDisabledConfigIsNoOp() throws Exception {
        config.setScalableTopicAutoScaleEnabled(false);
        startController(2);
        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(1_000_000, 0, 0, 0)).get();

        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 2, "disabled → no action even when hot");
    }

    @Test
    public void testColdSegmentsMerge() throws Exception {
        startController(4);
        // All segments cold (no load records written → treated as zero; but merge requires a
        // record present, so write explicit cold records). mergeWindow=0 so they're eligible.
        for (long id = 0; id < 4; id++) {
            resources.reportSegmentLoadAsync(topicName, id, new SegmentLoadStats(1, 0, 0, 0)).get();
        }

        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 3, "a cold adjacent pair should have been merged");
    }

    @Test
    public void testConsumerDrivenSplit() throws Exception {
        startController(1);
        assertEquals(activeSegmentCount(), 1);

        // Two consumers on one segment → need a second segment. registerConsumer fires an
        // event-driven evaluation (fire-and-forget); await its effect.
        controller.registerConsumer("sub", "c1", 1L, ScalableConsumerType.STREAM,
                mock(TransportCnx.class)).get();
        controller.registerConsumer("sub", "c2", 2L, ScalableConsumerType.STREAM,
                mock(TransportCnx.class)).get();

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> assertEquals(activeSegmentCount(), 2,
                        "2 consumers on 1 segment should drive a split"));
    }

    @Test
    public void testNamespaceOverrideDisablesAutoScale() throws Exception {
        namespacePolicies = new Policies();
        namespacePolicies.scalableTopicAutoScalePolicy =
                AutoScalePolicyOverride.builder().enabled(false).build();

        startController(2);
        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();
        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 2,
                "namespace override enabled=false must suppress the split");
    }

    @Test
    public void testTopicOverrideWinsOverNamespace() throws Exception {
        // Namespace disables auto-scale; the topic explicitly re-enables it.
        namespacePolicies = new Policies();
        namespacePolicies.scalableTopicAutoScalePolicy =
                AutoScalePolicyOverride.builder().enabled(false).build();

        startController(2);
        resources.updateScalableTopicAsync(topicName, md -> {
            md.setAutoScalePolicy(AutoScalePolicyOverride.builder().enabled(true).build());
            return md;
        }).get();

        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();
        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 3,
                "topic override enabled=true must win over the namespace disable");
    }

    @Test
    public void testTopicOverrideMaxSegmentsCapsSplit() throws Exception {
        startController(2);
        resources.updateScalableTopicAsync(topicName, md -> {
            md.setAutoScalePolicy(AutoScalePolicyOverride.builder().maxSegments(2).build());
            return md;
        }).get();

        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();
        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 2,
                "per-topic maxSegments=2 must cap the split despite the hot segment");
    }

    @Test
    public void testTopicOverrideSurvivesSplit() throws Exception {
        // The override must survive a layout mutation (toMetadata must carry it over).
        startController(2);
        AutoScalePolicyOverride override =
                AutoScalePolicyOverride.builder().maxSegments(3).build();
        resources.updateScalableTopicAsync(topicName, md -> {
            md.setAutoScalePolicy(override);
            return md;
        }).get();

        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();
        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 3, "first split allowed up to maxSegments=3");
        assertEquals(resources.getScalableTopicMetadataAsync(topicName).get()
                        .orElseThrow().getAutoScalePolicy(), override,
                "the per-topic override must survive the split's metadata rewrite");

        // At the cap now — further hot reports must not split.
        resources.reportSegmentLoadAsync(topicName, 1,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();
        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 3, "capped at the per-topic maxSegments");
    }

    @Test
    public void testInvalidOverrideCombinationFallsBackToDisabled() throws Exception {
        // Namespace and topic overrides that are each valid against the broker defaults but
        // invalid combined: the namespace raises the merge threshold, the topic lowers the
        // matching split threshold below it (hysteresis inversion). The controller must not
        // fail the evaluation chain — it treats auto split/merge as disabled until fixed.
        namespacePolicies = new Policies();
        namespacePolicies.scalableTopicAutoScalePolicy = AutoScalePolicyOverride.builder()
                .mergeMsgRateInThreshold(5_000.0)
                .build();

        startController(2);
        resources.updateScalableTopicAsync(topicName, md -> {
            md.setAutoScalePolicy(AutoScalePolicyOverride.builder()
                    .splitMsgRateInThreshold(2_000.0)
                    .build());
            return md;
        }).get();

        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();
        // Must complete normally (no IllegalArgumentException) and take no action.
        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 2,
                "invalid override combination must disable auto split/merge, not split");
    }

    @Test
    public void testConsumerBurstConvergesWithoutTicks() throws Exception {
        // A group of consumers joining in quick succession must converge to one segment
        // each purely from the event-driven evaluations + post-split follow-up chain — no
        // periodic tick and no manual evaluation calls.
        startController(1);
        for (int i = 1; i <= 4; i++) {
            controller.registerConsumer("sub", "c" + i, i, ScalableConsumerType.STREAM,
                    mock(TransportCnx.class)).get();
        }
        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(
                () -> assertEquals(activeSegmentCount(), 4,
                        "4 consumers must drive convergence to 4 segments"));
    }

    @Test
    public void testSplitCooldownBlocksSecondSplit() throws Exception {
        config.setScalableTopicSplitCooldownSeconds(3600); // 1h — blocks a second split
        startController(2);
        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();

        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 3, "first split happens");

        // Still hot, but within cooldown → no second split.
        resources.reportSegmentLoadAsync(topicName, 1,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();
        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 3, "second split blocked by cooldown");
    }

    @Test
    public void testSplitCooldownSurvivesLeaderFailover() throws Exception {
        config.setScalableTopicSplitCooldownSeconds(3600);
        startController(2);
        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();
        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 3, "first split happens");

        // Leadership moves: close this controller and elect a fresh one. The new leader's
        // in-memory cooldown clocks must be re-seeded from the layout (the children's
        // createdAtMs records when the last split ran), not reset to "never".
        controller.close().join();
        controller = new ScalableTopicController(topicName, resources, brokerService,
                coordinationService);
        controller.initialize().get();

        resources.reportSegmentLoadAsync(topicName, 1,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();
        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 3,
                "split cooldown must survive failover via layout-derived seeding");
    }

    @Test
    public void testFailedSplitDoesNotBurnCooldown() throws Exception {
        config.setScalableTopicSplitCooldownSeconds(3600);
        startController(2);
        resources.reportSegmentLoadAsync(topicName, 0,
                new SegmentLoadStats(20_000, 0, 0, 0)).get();

        // First attempt fails at the segment-topic-creation step. The evaluation future
        // surfaces the failure (the production tick wrapper logs-and-swallows it).
        when(scalableTopics.createSegmentAsync(anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("injected")));
        assertThrows(java.util.concurrent.ExecutionException.class,
                () -> controller.evaluateAutoScaleForTest().get());
        assertEquals(activeSegmentCount(), 2, "failed split leaves the layout unchanged");

        // The failure must not have started the cooldown: once the transient error clears,
        // the next evaluation splits immediately instead of waiting out the hour.
        when(scalableTopics.createSegmentAsync(anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        controller.evaluateAutoScaleForTest().get();
        assertEquals(activeSegmentCount(), 3, "retry after a failed split is not cooldown-blocked");
    }
}
