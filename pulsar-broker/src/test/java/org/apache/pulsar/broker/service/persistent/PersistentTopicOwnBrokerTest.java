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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicFactory;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.broker.service.TopicPolicyListener;
import org.apache.pulsar.broker.testinterceptor.BrokerTestInterceptor;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterPolicies.ClusterUrl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.utils.TestLogAppender;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * {@link PersistentTopic} tests that change the broker's configuration or services, change the clusters or restart
 * the broker, so each one runs on a broker of its own. {@link PersistentTopicTest} has the tests that share one.
 */
@CustomLog
@Test(groups = "broker")
public class PersistentTopicOwnBrokerTest extends BrokerTestBase {

    private Function<BrokerService, BrokerService> brokerServiceCustomizer = Function.identity();

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        brokerServiceCustomizer = Function.identity();
        super.baseSetup();
    }

    @Override
    protected BrokerService customizeNewBrokerService(BrokerService brokerService) {
        return brokerServiceCustomizer.apply(brokerService);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setManagedLedgerCursorBackloggedThreshold(10);
    }

    @Test
    public void testDeleteNamespaceInfiniteRetry() throws Exception {
        //init namespace
        final String myNamespace = "prop/ns" + UUID.randomUUID();
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        final String topic = "persistent://" + myNamespace + "/testDeleteNamespaceInfiniteRetry";
        conf.setForceDeleteNamespaceAllowed(true);
        //init topic and policies
        pulsarClient.newProducer().topic(topic).create().close();
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 0);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(()
                -> admin.namespaces().getMaxConsumersPerTopic(myNamespace) == 0);

        PersistentTopic persistentTopic =
                spy((PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get());

        Policies policies = new Policies();
        policies.deleted = true;
        persistentTopic.onPoliciesUpdate(policies);
        verify(persistentTopic, times(0)).checkReplicationAndRetryOnFailure();

        policies.deleted = false;
        persistentTopic.onPoliciesUpdate(policies);
        verify(persistentTopic, times(1)).checkReplicationAndRetryOnFailure();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteTopicFail() throws Exception {
        final String fullyTopicName = "persistent://prop/ns-abc/" + "tp_"
                + UUID.randomUUID().toString().replaceAll("-", "");
        // Mock topic.
        BrokerService brokerService = spy(pulsar.getBrokerService());
        doReturn(brokerService).when(pulsar).getBrokerService();

        // Create a sub, and send one message.
        Consumer<?> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(fullyTopicName).subscriptionName("sub1")
                .subscribe();
        consumer1.close();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(fullyTopicName).create();
        producer.send("1");
        producer.close();

        // Make a failed delete operation.
        AtomicBoolean makeDeletedFailed = new AtomicBoolean(true);
        PersistentTopic persistentTopic = (PersistentTopic) brokerService.getTopic(fullyTopicName, false).get().get();
        doAnswer(invocation -> {
            CompletableFuture future = (CompletableFuture) invocation.getArguments()[1];
            if (makeDeletedFailed.get()) {
                future.completeExceptionally(new RuntimeException("mock ex for test"));
            } else {
                future.complete(null);
            }
            return null;
        }).when(brokerService)
                .deleteTopicAuthenticationWithRetry(any(String.class), any(CompletableFuture.class), anyInt());
        try {
            persistentTopic.delete().get();
        } catch (Exception e) {
            org.testng.Assert.assertTrue(e instanceof ExecutionException);
            org.testng.Assert.assertTrue(e.getCause() instanceof java.lang.RuntimeException);
            org.testng.Assert.assertEquals(e.getCause().getMessage(), "mock ex for test");
        }

        // Assert topic works after deleting failure.
        Consumer<?> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(fullyTopicName).subscriptionName("sub1")
                .subscribe();
        org.testng.Assert.assertEquals("1", consumer2.receive(2, TimeUnit.SECONDS).getValue());
        consumer2.close();

        // Make delete success.
        makeDeletedFailed.set(false);
        persistentTopic.delete().get();
    }

    @DataProvider
    public Object[][] deletedTopicCacheRemoval() {
        return new Object[][] {{false}, {true}};
    }

    @DataProvider
    public Object[][] deletedTopicCleanupFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "deletedTopicCleanupFailures", timeOut = 60000)
    public void testDeleteRetainsFencedTopicUntilCleanupSucceeds(boolean failDispatchCleanup)
            throws Exception {
        String topicName = "persistent://prop/ns-abc/delete-cleanup-failure-" + UUID.randomUUID();
        TopicName parsedTopic = TopicName.get(topicName);
        conf.setDispatchThrottlingRatePerTopicInMsg(1);
        DispatchRateLimiter limiter = mock(DispatchRateLimiter.class);
        brokerServiceCustomizer = service -> {
            BrokerService brokerService = spy(service);
            DispatchRateLimiterFactory factory = spy(brokerService.getDispatchRateLimiterFactory());
            doAnswer(invocation -> {
                PersistentTopic topic = invocation.getArgument(0);
                return topic.getName().equals(topicName) ? limiter : invocation.callRealMethod();
            }).when(factory).createTopicDispatchRateLimiter(any(PersistentTopic.class));
            doReturn(factory).when(brokerService).getDispatchRateLimiterFactory();
            return brokerService;
        };
        restartBroker();
        BrokerService brokerService = pulsar.getBrokerService();
        assertThat(brokerService).isSameAs(pulsarTestContext.getBrokerService());
        TopicPoliciesService policiesService = spy(pulsar.getTopicPoliciesService());
        RuntimeException failure = new RuntimeException("Injected deleted-topic cleanup failure");
        AtomicBoolean failPolicyCleanup = new AtomicBoolean(!failDispatchCleanup);
        doAnswer(invocation -> {
            if (failPolicyCleanup.get()) {
                throw failure;
            }
            return invocation.callRealMethod();
        }).when(policiesService).unregisterListener(eq(parsedTopic), any(TopicPolicyListener.class));
        pulsar.setTopicPoliciesService(policiesService);
        if (failDispatchCleanup) {
            doThrow(failure).when(limiter).close();
        }
        PersistentTopic topic = (PersistentTopic) brokerService.getTopic(topicName, true)
                .get(10, TimeUnit.SECONDS).orElseThrow();
        assertThat(topic.getDispatchRateLimiter()).contains(limiter);
        try {
            CompletableFuture<Void> deletion = brokerService.deleteTopic(topicName, false);
            assertThatThrownBy(() -> deletion.get(10, TimeUnit.SECONDS)).hasRootCause(failure);
            assertThat(deletion).isCompletedExceptionally();
            assertThat(brokerService.getTopicReference(topicName)).contains(topic);
            assertThat(topic.isFenced()).isTrue();
            assertThat(pulsar.getPulsarResources().getTopicResources().persistentTopicExists(parsedTopic)
                    .get(10, TimeUnit.SECONDS)).isFalse();
        } finally {
            if (failDispatchCleanup) {
                doNothing().when(limiter).close();
            } else {
                doThrow(new IllegalStateException("The successfully closed limiter must not be closed again"))
                        .when(limiter).close();
            }
            failPolicyCleanup.set(false);
        }
        if (!failDispatchCleanup) {
            doThrow(new IllegalStateException("The closed limiter must not be updated by a late policy event"))
                    .when(limiter).updateDispatchRate();
            topic.updateDispatchRateLimiter();
        }
        // A normal admin retry must still reach the old instance and finish its remaining cleanup.
        brokerService.deleteTopic(topicName, false).get(10, TimeUnit.SECONDS);
        verify(limiter, times(failDispatchCleanup ? 2 : 1)).close();
        verify(policiesService, times(failDispatchCleanup ? 1 : 2))
                .unregisterListener(eq(parsedTopic), any(TopicPolicyListener.class));
        doNothing().when(limiter).close();
        doNothing().when(limiter).updateDispatchRate();
        assertThat(brokerService.getTopicReference(topicName)).isEmpty();
        PersistentTopic recreated = (PersistentTopic) brokerService.getTopic(topicName, true)
                .get(10, TimeUnit.SECONDS).orElseThrow();
        assertThat(recreated).isNotSameAs(topic);
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create()) {
            producer.send("after cleanup retry");
        }
    }

    @Test(dataProvider = "deletedTopicCacheRemoval", timeOut = 60000)
    public void testDeleteWaitsForCacheRemovalAndRetriesAfterFailure(boolean failRemoval) throws Exception {
        brokerServiceCustomizer = service -> spy(service);
        restartBroker(configuration -> configuration.setTopicFactoryClassName(
                InitializationTopicFactory.class.getName()));
        String topicName = "persistent://prop/ns-abc/delete-cache-removal-" + UUID.randomUUID();
        TopicName parsedTopic = TopicName.get(topicName);
        BrokerService brokerService = pulsar.getBrokerService();
        assertThat(brokerService).isSameAs(pulsarTestContext.getBrokerService());
        CompletableFuture<Void> reconnectAdmission = new CompletableFuture<>();
        AtomicReference<PersistentTopic> observedTargetTopic = new AtomicReference<>();
        BrokerTestInterceptor.INSTANCE.setPersistentTopicDecorator(loadedTopic -> {
            if (!loadedTopic.getName().equals(topicName)) {
                return loadedTopic;
            }
            PersistentTopic observedTopic = spy(loadedTopic);
            doAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                CompletableFuture<Optional<Long>> admission =
                        (CompletableFuture<Optional<Long>>) invocation.callRealMethod();
                admission.whenComplete((epoch, error) -> {
                    if (error == null) {
                        reconnectAdmission.complete(null);
                    } else {
                        reconnectAdmission.completeExceptionally(error);
                    }
                });
                return admission;
            }).when(observedTopic).addProducer(any(), any());
            observedTargetTopic.set(observedTopic);
            return observedTopic;
        });
        PersistentTopic topic;
        try {
            topic = (PersistentTopic) brokerService.getTopic(topicName, true)
                    .get(10, TimeUnit.SECONDS).orElseThrow();
        } finally {
            BrokerTestInterceptor.INSTANCE.reset();
        }
        assertThat(topic).isSameAs(observedTargetTopic.get());
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
        NamespaceService namespaceService = pulsar.getNamespaceService();
        NamespaceBundle bundle = namespaceService.getBundleAsync(parsedTopic).get(10, TimeUnit.SECONDS);
        CompletableFuture<NamespaceBundle> cacheRemoval = new CompletableFuture<>();
        CompletableFuture<NamespaceBundle> retryCacheRemoval = new CompletableFuture<>();
        AtomicReference<CompletableFuture<NamespaceBundle>> pendingCacheRemoval = new AtomicReference<>(cacheRemoval);
        CompletableFuture<Void> cacheRemovalStarted = new CompletableFuture<>();
        AtomicBoolean interceptCacheRemoval = new AtomicBoolean(true);
        AtomicInteger cacheRemovalCalls = new AtomicInteger();
        when(namespaceService.getBundleAsync(parsedTopic)).thenAnswer(invocation -> {
            if (!interceptCacheRemoval.get()) {
                return invocation.callRealMethod();
            }
            cacheRemovalStarted.complete(null);
            cacheRemovalCalls.incrementAndGet();
            return pendingCacheRemoval.get();
        });
        try {
            CompletableFuture<Void> deletion = topic.delete();
            cacheRemovalStarted.get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(pulsar.getPulsarResources().getTopicResources().persistentTopicExists(parsedTopic)
                            .get(10, TimeUnit.SECONDS)).isFalse());
            assertThat(ledger.getState()).isEqualTo(ManagedLedgerImpl.State.FencedForDeletion);
            assertThat(deletion).as("deletion must wait for the deleted topic to leave the broker cache")
                    .isNotDone();

            if (failRemoval) {
                RuntimeException failure = new RuntimeException("Injected bundle lookup failure during cache removal");
                cacheRemoval.completeExceptionally(failure);
                assertThatThrownBy(() -> deletion.get(10, TimeUnit.SECONDS)).hasRootCause(failure);
                assertThat(brokerService.getTopicReference(topicName)).contains(topic);
                interceptCacheRemoval.set(false);
                // A real client must not reconnect to the cached instance whose ledger has already been deleted.
                try (PulsarClient reconnectingClient = PulsarClient.builder()
                        .serviceUrl(pulsar.getBrokerServiceUrl()).build()) {
                    reconnectingClient.newProducer().topic(topicName).createAsync();
                    assertThatThrownBy(() -> reconnectAdmission.get(10, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(BrokerServiceException.TopicFencedException.class);
                    assertThat(topic.getProducers()).isEmpty();
                    assertThat(topic.isFenced()).isTrue();
                }
                pendingCacheRemoval.set(retryCacheRemoval);
                cacheRemovalCalls.set(0);
                interceptCacheRemoval.set(true);
                CompletableFuture<Void> retry = brokerService.deleteTopic(topicName, false);
                Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(cacheRemovalCalls).hasValue(1));
                CompletableFuture<Void> concurrentRetry = brokerService.deleteTopic(topicName, false);
                assertThat(concurrentRetry).isSameAs(retry);
                CompletableFuture<Void> closing = topic.close();
                assertThat(closing).isNotDone();
                assertThat(retry).isNotDone();
                assertThat(cacheRemovalCalls).hasValue(1);
                retryCacheRemoval.complete(bundle);
                retry.get(10, TimeUnit.SECONDS);
                concurrentRetry.get(10, TimeUnit.SECONDS);
                closing.get(10, TimeUnit.SECONDS);
                interceptCacheRemoval.set(false);
            } else {
                cacheRemoval.complete(bundle);
                deletion.get(10, TimeUnit.SECONDS);
            }
            // Retrying local cleanup must not reenter the metadata-deletion pipeline.
            verify(brokerService, times(1)).deleteTopicAuthenticationWithRetry(eq(topicName), any(), anyInt());
            assertThat(brokerService.getTopicReference(topicName)).isEmpty();
            assertThat(brokerService.getTopics()).doesNotContainKey(topicName);
            PersistentTopic recreated = (PersistentTopic) brokerService.getTopic(topicName, true)
                    .get(10, TimeUnit.SECONDS).orElseThrow();
            assertThat(recreated).isNotSameAs(topic);
            assertThat(pulsar.getPulsarResources().getTopicResources().persistentTopicExists(parsedTopic)
                    .get(10, TimeUnit.SECONDS)).isTrue();
            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create()) {
                producer.send("after cache removal retry");
            }
        } finally {
            interceptCacheRemoval.set(false);
            cacheRemoval.complete(bundle);
            retryCacheRemoval.complete(bundle);
        }
    }

    @DataProvider(name = "topicLevelPolicy")
    public static Object[][] topicLevelPolicy() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "topicLevelPolicy")
    public void testCreateTopicWithZombieReplicatorCursor(boolean topicLevelPolicy) throws Exception {
        final String namespace = "prop/ns-abc";
        final String topicName = "persistent://" + namespace
                + "/testCreateTopicWithZombieReplicatorCursor" + topicLevelPolicy;
        final String remoteCluster = "remote";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, conf.getReplicatorPrefix() + "." + remoteCluster,
                MessageId.earliest, true);

        admin.clusters().createCluster(remoteCluster, ClusterData.builder()
                .serviceUrl("http://localhost:11112")
                .brokerServiceUrl("pulsar://localhost:11111")
                .build());
        TenantInfo tenantInfo = admin.tenants().getTenantInfo("prop");
        tenantInfo.getAllowedClusters().add(remoteCluster);
        admin.tenants().updateTenant("prop", tenantInfo);

        if (topicLevelPolicy) {
            admin.topics().setReplicationClusters(topicName, Arrays.asList("test", remoteCluster));
        } else {
            try {
                admin.namespaces().setNamespaceReplicationClustersAsync(
                        namespace, Sets.newHashSet("test", remoteCluster), false).get();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("Failed to validate remote-side"));
                return;
            }
        }

        final PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false)
                .get(3, TimeUnit.SECONDS).orElse(null);
        assertNotNull(topic);

        final Supplier<Set<String>> getCursors = () -> {
            final Set<String> cursors = new HashSet<>();
            final Iterable<ManagedCursor> iterable = topic.getManagedLedger().getCursors();
            iterable.forEach(c -> cursors.add(c.getName()));
            return cursors;
        };
        assertEquals(getCursors.get(), Collections.singleton(conf.getReplicatorPrefix() + "." + remoteCluster));

        // PersistentTopics#onPoliciesUpdate might happen in different threads, so there might be a race between two
        // updates of the replication clusters. So here we sleep for a while to reduce the flakiness.
        Thread.sleep(100);

        // Configure the local cluster to avoid the topic being deleted in PersistentTopics#checkReplication
        if (topicLevelPolicy) {
            admin.topics().setReplicationClusters(topicName, Collections.singletonList("test"));
        } else {
            admin.namespaces()
                .setNamespaceReplicationClustersAsync(namespace, Collections.singleton("test"), false).get();
        }
        admin.clusters().deleteCluster(remoteCluster);
        // Now the cluster and its related policy has been removed but the replicator cursor still exists

        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
            log.info("Before initialize...");
            try {
                topic.initialize().get(3, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                log.warn().exceptionMessage(e.getCause()).log("Failed to initialize");
            }
            return !topic.getManagedLedger().getCursors().iterator().hasNext();
        });
    }

    /**
     * A replicator cursor for a remote cluster whose name contains a dot must not be mistaken for an orphan.
     *
     * <p>{@code PersistentTopic#removeOrphanReplicationCursors()} used to derive the remote cluster by taking
     * the cursor-name segment after the last dot, so the live cursor {@code pulsar.repl.remote.east} of the
     * (legal) cluster {@code remote.east} resolved to {@code east}, which is not among the topic's replication
     * clusters. The topic then tried to delete the non-existent cursor {@code pulsar.repl.east}, whose
     * {@code CursorNotFoundException} failed the {@code PersistentTopic#initialize()} chain on every load of
     * the topic. The live cursor survived only because the name the sweep reconstructed was wrong too.
     */
    @Test
    public void testReplicatorCursorOfClusterWithDotInNameIsNotTreatedAsOrphan() throws Exception {
        final String namespace = "prop/ns-dotted-remote-cluster";
        final String topicName = "persistent://" + namespace + "/testDottedRemoteCluster-" + UUID.randomUUID();
        // A dot is a legal cluster-name character: NamedEntity#NAMED_ENTITY_PATTERN allows "-=:." plus \w.
        final String remoteCluster = "remote.east";
        final String replicatorCursor = conf.getReplicatorPrefix() + "." + remoteCluster;

        admin.clusters().createCluster(remoteCluster, ClusterData.builder()
                .serviceUrl("http://localhost:11112")
                .brokerServiceUrl("pulsar://localhost:11111")
                .build());
        TenantInfo tenantInfo = admin.tenants().getTenantInfo("prop");
        tenantInfo.getAllowedClusters().add(remoteCluster);
        admin.tenants().updateTenant("prop", tenantInfo);

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, replicatorCursor, MessageId.earliest, true);

        final PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false)
                .get(10, TimeUnit.SECONDS).orElseThrow();

        // Written straight to the namespace policies so that initialize() below reads them back synchronously,
        // and to skip the admin API's remote-side validation of an intentionally unreachable cluster.
        pulsar.getPulsarResources().getNamespaceResources()
                .setPolicies(NamespaceName.get(namespace), policies -> {
                    policies.replication_clusters = Sets.newHashSet("test", remoteCluster);
                    return policies;
                });

        // A live replicator must never reach the orphan-removal path, even if deleting its cursor would succeed.
        @Cleanup
        final TestLogAppender logAppender = TestLogAppender.create(PersistentTopic.class);

        topic.initialize().get(30, TimeUnit.SECONDS);

        final List<String> orphanWarnings = logAppender.getEvents().stream()
                .map(event -> event.getMessage().getFormattedMessage())
                .filter(message -> message.contains("Remove the orphan replicator"))
                .toList();
        assertTrue(orphanWarnings.isEmpty(),
                "the live replicator of cluster " + remoteCluster + " was treated as an orphan: "
                        + orphanWarnings);

        final Set<String> cursors = new HashSet<>();
        topic.getManagedLedger().getCursors().forEach(c -> cursors.add(c.getName()));
        assertTrue(cursors.contains(replicatorCursor),
                "the live replicator cursor was swept as an orphan, remaining cursors: " + cursors);
    }

    @Test
    public void testCheckPersistencePolicies() throws Exception {
        final String myNamespace = "prop/ns";
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        final String topic = "persistent://" + myNamespace + "/testConfig" + UUID.randomUUID();
        conf.setForceDeleteNamespaceAllowed(true);
        pulsarClient.newProducer().topic(topic).create().close();
        RetentionPolicies retentionPolicies = new RetentionPolicies(1, 1);
        PersistentTopic persistentTopic = spy((PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topic).get().get());
        TopicPoliciesService policiesService = spy(pulsar.getTopicPoliciesService());
        doReturn(policiesService).when(pulsar).getTopicPoliciesService();
        TopicPolicies policies = new TopicPolicies();
        policies.setRetentionPolicies(retentionPolicies);
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(policiesService)
                .getTopicPoliciesAsync(TopicName.get(topic), TopicPoliciesService.GetType.LOCAL_ONLY);
        persistentTopic.onUpdate(policies);
        verify(persistentTopic, times(1)).checkPersistencePolicies();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(persistentTopic.getManagedLedger().getConfig().getRetentionSizeInMB(), 1L);
            assertEquals(persistentTopic.getManagedLedger().getConfig().getRetentionTimeMillis(),
                    TimeUnit.MINUTES.toMillis(1));
        });
        // throw exception
        doReturn(CompletableFuture.failedFuture(new RuntimeException()))
                .when(persistentTopic).checkPersistencePolicies();
        policies.setRetentionPolicies(new RetentionPolicies(2, 2));
        persistentTopic.onUpdate(policies);
        assertEquals(persistentTopic.getManagedLedger().getConfig().getRetentionSizeInMB(), 1L);
        assertEquals(persistentTopic.getManagedLedger().getConfig().getRetentionTimeMillis(),
                TimeUnit.MINUTES.toMillis(1));
    }

    public static class InitializationTopicFactory implements TopicFactory {
        @Override
        public <T extends Topic> T create(String topic, ManagedLedger ledger, BrokerService brokerService,
                                         Class<T> topicClass) {
            return topicClass.cast(BrokerTestInterceptor.INSTANCE.getPersistentTopicDecorator()
                    .apply(new PersistentTopic(topic, ledger, brokerService)));
        }
    }

    @DataProvider
    public Object[][] initializationFailureStages() {
        return new Object[][] {{"namespace"}, {"topic"}, {"replication-cursor"}};
    }

    @Test(dataProvider = "initializationFailureStages", timeOut = 60000)
    public void testInitializationFailureClosesTopicAndAllowsRetry(String failureStage) throws Exception {
        restartBroker(configuration -> configuration.setTopicFactoryClassName(
                InitializationTopicFactory.class.getName()));
        final String topicName = "persistent://prop/ns-abc/initialization-failure-" + UUID.randomUUID();
        final TopicName parsedTopicName = TopicName.get(topicName);
        pulsar.getPulsarResources().getNamespaceResources()
                .setPolicies(parsedTopicName.getNamespaceObject(), policies -> {
                    policies.encryption_required = true;
                    return policies;
                });
        PersistentTopic initialTopic = (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, true)
                .get(10, TimeUnit.SECONDS).orElseThrow();
        assertThat(initialTopic.isEncryptionRequired()).isTrue();
        if (failureStage.equals("replication-cursor")) {
            initialTopic.getManagedLedger().openCursor(conf.getReplicatorPrefix() + ".removed-cluster");
        }
        initialTopic.close().get(10, TimeUnit.SECONDS);

        RuntimeException injectedFailure = new RuntimeException("Injected " + failureStage + " initialization failure");
        AtomicBoolean initializing = new AtomicBoolean();
        AtomicReference<PersistentTopic> failedTopic = new AtomicReference<>();
        PulsarResources originalResources = pulsar.getPulsarResources();
        TopicPoliciesService originalPoliciesService = pulsar.getTopicPoliciesService();
        BrokerTestInterceptor.INSTANCE.setPersistentTopicDecorator(topic -> {
            if (!topic.getName().equals(topicName)) {
                return topic;
            }
            initializing.set(true);
            PersistentTopic decorated = spy(topic);
            if (failureStage.equals("replication-cursor")) {
                doReturn(CompletableFuture.failedFuture(injectedFailure)).when(decorated)
                        .removeReplicator("removed-cluster");
            }
            failedTopic.set(decorated);
            return decorated;
        });
        try {
            if (failureStage.equals("namespace")) {
                PulsarResources resources = spy(originalResources);
                NamespaceResources namespaceResources = spy(originalResources.getNamespaceResources());
                doReturn(resources).when(pulsar).getPulsarResources();
                doReturn(namespaceResources).when(resources).getNamespaceResources();
                doAnswer(invocation -> initializing.compareAndSet(true, false)
                        ? CompletableFuture.failedFuture(injectedFailure) : invocation.callRealMethod())
                        .when(namespaceResources).getPoliciesAsync(parsedTopicName.getNamespaceObject());
            } else if (failureStage.equals("topic")) {
                TopicPoliciesService policiesService = spy(originalPoliciesService);
                doReturn(policiesService).when(pulsar).getTopicPoliciesService();
                doAnswer(invocation -> initializing.compareAndSet(true, false)
                        ? CompletableFuture.failedFuture(injectedFailure) : invocation.callRealMethod())
                        .when(policiesService).getTopicPoliciesAsync(parsedTopicName,
                                TopicPoliciesService.GetType.LOCAL_ONLY);
            }

            assertThatThrownBy(() -> pulsar.getBrokerService().getTopic(topicName, false).get(10, TimeUnit.SECONDS))
                    .hasRootCause(injectedFailure);
            assertThat(failedTopic.get()).as("failure must occur after the topic is constructed").isNotNull();
            assertThat(pulsar.getBrokerService().getTopics()).doesNotContainKey(topicName);
            assertThat(((ManagedLedgerImpl) failedTopic.get().getManagedLedger()).getState())
                    .isEqualTo(ManagedLedgerImpl.State.Closed);
        } finally {
            BrokerTestInterceptor.INSTANCE.reset();
            doReturn(originalResources).when(pulsar).getPulsarResources();
            doReturn(originalPoliciesService).when(pulsar).getTopicPoliciesService();
        }

        PersistentTopic recoveredTopic = (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false)
                .get(10, TimeUnit.SECONDS).orElseThrow();
        assertThat(recoveredTopic).isNotSameAs(failedTopic.get());
        assertThat(recoveredTopic.isEncryptionRequired()).isTrue();
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl())
                .operationTimeout(1, TimeUnit.SECONDS).build();
        assertThatThrownBy(() -> client.newProducer().topic(topicName).createAsync().get(10, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(PulsarClientException.BrokerMetadataException.class)
                .hasMessageContaining("Encryption is required");
        recoveredTopic.close().get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testTopicPolicyListenerForwardsLiveUpdatesAfterInitialLoadFailure() throws Exception {
        class RecordingPersistentTopic extends PersistentTopic {
            final List<TopicPolicies> receivedUpdates = new ArrayList<>();

            RecordingPersistentTopic(String topic, ManagedLedger ledger, BrokerService brokerService) {
                super(topic, ledger, brokerService);
            }

            @Override
            public void onUpdate(TopicPolicies policies) {
                receivedUpdates.add(policies);
            }

            // initTopicPolicy() moved to AbstractTopic (a different package), so widen it to public here to keep
            // this same-package test able to invoke it directly.
            @Override
            public CompletableFuture<Void> initTopicPolicy() {
                return super.initTopicPolicy();
            }
        }

        final String topic = "persistent://prop/ns-abc/testTopicPolicyInitFailure-" + UUID.randomUUID();
        ManagedLedger ledger = mock(ManagedLedger.class);
        doReturn(new ManagedLedgerConfig()).when(ledger).getConfig();
        doReturn(Collections.emptyMap()).when(ledger).getProperties();

        TopicPoliciesService policiesService = mock(TopicPoliciesService.class);
        doReturn(policiesService).when(pulsar).getTopicPoliciesService();
        doReturn(CompletableFuture.completedFuture(true)).when(policiesService)
                .registerListenerAsync(any(TopicName.class), any(TopicPolicyListener.class));
        doReturn(CompletableFuture.failedFuture(new RuntimeException("initial topic policy load failed")))
                .when(policiesService).getTopicPoliciesAsync(any(TopicName.class),
                        any(TopicPoliciesService.GetType.class));

        RecordingPersistentTopic persistentTopic =
                new RecordingPersistentTopic(topic, ledger, pulsar.getBrokerService());
        persistentTopic.initTopicPolicy().handle((ignored, ex) -> null).get(3, TimeUnit.SECONDS);

        ArgumentCaptor<TopicPolicyListener> listenerCaptor = ArgumentCaptor.forClass(TopicPolicyListener.class);
        verify(policiesService).registerListenerAsync(any(TopicName.class), listenerCaptor.capture());

        TopicPolicies livePolicies = new TopicPolicies();
        livePolicies.setIsGlobal(false);
        livePolicies.setMaxConsumerPerTopic(10);
        listenerCaptor.getValue().onUpdate(livePolicies);

        assertEquals(persistentTopic.receivedUpdates, Collections.singletonList(livePolicies));
    }

    @Test
    public void testDynamicConfigurationAutoSkipNonRecoverableData() throws Exception {
        pulsar.getConfiguration().setAutoSkipNonRecoverableData(false);
        final String topicName = "persistent://prop/ns-abc/testAutoSkipNonRecoverableData";
        final String subName = "test_sub";

        Consumer<byte[]> subscribe = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        PersistentSubscription subscription = persistentTopic.getSubscription(subName);

        assertFalse(persistentTopic.ledger.getConfig().isAutoSkipNonRecoverableData());
        assertFalse(subscription.getExpiryMonitor().isAutoSkipNonRecoverableData());

        String key = "autoSkipNonRecoverableData";
        admin.brokers().updateDynamicConfiguration(key, "true");
        Awaitility.await()
                .untilAsserted(() -> assertEquals(admin.brokers().getAllDynamicConfigurations().get(key), "true"));

        assertTrue(persistentTopic.ledger.getConfig().isAutoSkipNonRecoverableData());
        assertTrue(subscription.getExpiryMonitor().isAutoSkipNonRecoverableData());

        subscribe.close();
        admin.topics().delete(topicName);
    }

    @Test
    public void testNonDurableSubscriptionRejectedBeforeDurableSubscriptionPublication() throws Exception {
        restartBroker(BrokerTestInterceptor.INSTANCE::configure);
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        String topicName = "persistent://prop/ns-abc/pending-subscription-publication";
        String subscriptionName = "mixed-subscription";
        CompletableFuture<ManagedCursor> cursorCached = new CompletableFuture<>();
        CompletableFuture<Void> nonDurableAdmissionChecked = new CompletableFuture<>();
        CountDownLatch publishSubscription = new CountDownLatch(1);
        BrokerTestInterceptor.INSTANCE.setManagedLedgerDecorator(created -> {
            ManagedLedgerImpl decorated = spy(created);
            try {
                doAnswer(invocation -> {
                    try {
                        return invocation.callRealMethod();
                    } finally {
                        nonDurableAdmissionChecked.complete(null);
                    }
                }).when(decorated).newNonDurableCursor(any(), eq(subscriptionName), any(), anyBoolean());
            } catch (ManagedLedgerException e) {
                throw new AssertionError(e);
            }
            return decorated;
        });
        BrokerTestInterceptor.INSTANCE.setPersistentTopicDecorator(created -> {
            if (!topicName.equals(created.getName())) {
                return created;
            }
            PersistentTopic decorated = spy(created);
            doAnswer(invocation -> {
                cursorCached.complete(invocation.getArgument(1));
                assertThat(publishSubscription.await(60, TimeUnit.SECONDS)).isTrue();
                return invocation.callRealMethod();
            }).when(decorated).createPersistentSubscription(eq(subscriptionName), any(), any(), any());
            return decorated;
        });
        Consumer<byte[]> nonDurableConsumer = null;
        try {
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                    .getOrCreateTopic(topicName).get(30, TimeUnit.SECONDS);
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
            CompletableFuture<?> durableCreation = topic.createSubscription(subscriptionName,
                    CommandSubscribe.InitialPosition.Earliest, false, Collections.emptyMap());
            ManagedCursor durableCursor = cursorCached.get(30, TimeUnit.SECONDS);
            assertThat(ledger.getCursors().get(Codec.encode(subscriptionName))).isSameAs(durableCursor);
            assertThat(topic.getSubscription(subscriptionName)).isNull();

            AtomicReference<Throwable> nonDurableFailure = new AtomicReference<>();
            CompletableFuture<Consumer<byte[]>> nonDurableCreation = client.newConsumer().topic(topicName)
                    .subscriptionName(subscriptionName).subscriptionMode(SubscriptionMode.NonDurable)
                    .subscribeAsync().handle((consumer, error) -> {
                        nonDurableFailure.set(FutureUtil.unwrapCompletionException(error));
                        nonDurableAdmissionChecked.complete(null);
                        return consumer;
                    });
            // Either admission rejects the request or the real cursor lookup has already happened.
            nonDurableAdmissionChecked.get(30, TimeUnit.SECONDS);
            publishSubscription.countDown();
            Object durableSubscription = durableCreation.get(30, TimeUnit.SECONDS);
            nonDurableConsumer = nonDurableCreation.get(30, TimeUnit.SECONDS);
            SoftAssertions softly = new SoftAssertions();
            softly.assertThat(nonDurableFailure.get()).as("a cached durable cursor still reserves the name")
                    .isInstanceOf(PulsarClientException.NotAllowedException.class);
            softly.assertThat(topic.getSubscription(subscriptionName))
                    .as("the durable subscription must retain ownership after publication")
                    .isSameAs(durableSubscription);
            softly.assertAll();
        } finally {
            publishSubscription.countDown();
            try {
                if (nonDurableConsumer != null) {
                    nonDurableConsumer.close();
                }
            } finally {
                BrokerTestInterceptor.INSTANCE.reset();
            }
        }
    }

    @Test
    public void testDurableSubscriptionRejectedDuringNonDurableCursorRetirement() throws Exception {
        restartBroker(configuration -> configuration.setTopicFactoryClassName(
                InitializationTopicFactory.class.getName()));

        final String topicName = "persistent://" + newTopicName();
        final String subscriptionName = "shared-subscription";
        CountDownLatch removalStarted = new CountDownLatch(1);
        CompletableFuture<Void> allowRemoval = new CompletableFuture<>();
        AtomicReference<PersistentTopic> decoratedTopic = new AtomicReference<>();
        AtomicReference<PersistentSubscription> retiringSubscription = new AtomicReference<>();

        BrokerTestInterceptor.INSTANCE.setPersistentTopicDecorator(topic -> {
            if (!topic.getName().equals(topicName)) {
                return topic;
            }

            PersistentTopic decorated = spy(topic);
            doAnswer(invocation -> {
                String removedSubscriptionName = invocation.getArgument(0);
                PersistentSubscription expected = invocation.getArgument(1);
                if (subscriptionName.equals(removedSubscriptionName)
                        && expected == retiringSubscription.get()) {
                    removalStarted.countDown();
                    return allowRemoval.thenComposeAsync(ignored -> {
                        try {
                            @SuppressWarnings("unchecked")
                            CompletableFuture<Void> result = (CompletableFuture<Void>) invocation.callRealMethod();
                            return result;
                        } catch (Throwable error) {
                            return CompletableFuture.failedFuture(error);
                        }
                    }, pulsar.getExecutor());
                }
                return invocation.callRealMethod();
            }).when(decorated).removeSubscription(any(String.class), any(PersistentSubscription.class));
            decoratedTopic.set(decorated);
            return decorated;
        });

        PulsarClient client = null;
        Consumer<byte[]> consumer = null;
        try {
            client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
            consumer = client.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                    .subscriptionMode(SubscriptionMode.NonDurable).subscribe();

            PersistentTopic topic = decoratedTopic.get();
            assertThat(topic).as("the broker must use the decorated persistent topic").isNotNull();
            PersistentSubscription subscription = topic.getSubscription(subscriptionName);
            assertThat(subscription).isNotNull();
            retiringSubscription.set(subscription);
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();

            consumer.closeAsync().get(10, TimeUnit.SECONDS);
            assertThat(removalStarted.await(10, TimeUnit.SECONDS))
                    .as("the real non-durable cursor deletion must reach the map-removal boundary")
                    .isTrue();
            assertThat(ledger.getCursors().get(subscriptionName))
                    .as("non-durable cursor deletion has completed before map removal")
                    .isNull();
            assertThat(topic.getSubscription(subscriptionName))
                    .as("the retiring subscription remains reserved until map removal")
                    .isSameAs(subscription);

            CompletableFuture<?> durableAdmission = topic.createSubscription(subscriptionName,
                    CommandSubscribe.InitialPosition.Earliest, false, Collections.emptyMap());
            assertThatThrownBy(() -> durableAdmission.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(BrokerServiceException.NotAllowedException.class);
            assertThat(ledger.getCursors().get(subscriptionName))
                    .as("rejected durable admission must not leave an orphaned cursor")
                    .isNull();
        } finally {
            allowRemoval.complete(null);
            try {
                if (consumer != null) {
                    consumer.close();
                }
                if (client != null) {
                    client.close();
                }
            } finally {
                BrokerTestInterceptor.INSTANCE.reset();
            }
        }
    }

    @Test
    public void testMigrationPropagatesTenantCreationFailure() throws Exception {
        PulsarAdmin pulsarAdmin = mockMigrationAdmin();
        Topic topic = prepareTopicForMigration("http://destination.invalid", "orders");
        CompletableFuture<Void> createTenant = new CompletableFuture<>();
        when(pulsarAdmin.tenants().createTenantAsync(eq("prop"), any())).thenReturn(createTenant);

        CompletableFuture<Void> migration = topic.checkClusterMigration();
        verify(pulsarAdmin.tenants(), timeout(5000)).createTenantAsync(eq("prop"), any());
        assertCreationFailurePropagates(createTenant, migration);
        verifyNoInteractions(pulsarAdmin.namespaces(), pulsarAdmin.topics());
    }

    @Test
    public void testMigrationPropagatesCancelledTenantCreation() throws Exception {
        PulsarAdmin pulsarAdmin = mockMigrationAdmin();
        Topic topic = prepareTopicForMigration("http://destination.invalid", "orders");
        CompletableFuture<Void> createTenant = new CompletableFuture<>();
        when(pulsarAdmin.tenants().createTenantAsync(eq("prop"), any())).thenReturn(createTenant);

        CompletableFuture<Void> migration = topic.checkClusterMigration();
        verify(pulsarAdmin.tenants(), timeout(5000)).createTenantAsync(eq("prop"), any());
        assertThat(createTenant.cancel(false)).isTrue();

        ExecutionException thrown = Assert.expectThrows(ExecutionException.class,
                () -> migration.get(5, TimeUnit.SECONDS));
        assertThat(thrown.getCause()).isInstanceOf(CancellationException.class);
        verifyNoInteractions(pulsarAdmin.namespaces(), pulsarAdmin.topics());
    }

    @Test
    public void testMigrationPreservesForbiddenTenantCreationResponse() throws Exception {
        @Cleanup("stop")
        WireMockServer destination = new WireMockServer(0);
        destination.start();
        destination.stubFor(WireMock.put("/admin/v2/tenants/prop").willReturn(WireMock.forbidden()));
        Topic topic = prepareTopicForMigration(destination.baseUrl());

        ExecutionException thrown = Assert.expectThrows(ExecutionException.class,
                () -> topic.checkClusterMigration().get(5, TimeUnit.SECONDS));
        assertThat(thrown.getCause()).isInstanceOfSatisfying(NotAuthorizedException.class,
                failure -> assertThat(failure.getStatusCode()).isEqualTo(403));
    }

    @Test
    public void testMigrationContinuesAfterWrappedTenantConflict() throws Exception {
        PulsarAdmin pulsarAdmin = mockMigrationAdmin();
        Topic topic = prepareTopicForMigration("http://destination.invalid");
        CompletableFuture<Void> tenantConflict =
                CompletableFuture.failedFuture(new ConflictException(null, "Tenant already exists", 409));
        // A dependent stage propagates the conflict as a CompletionException.
        when(pulsarAdmin.tenants().createTenantAsync(eq("prop"), any()))
                .thenReturn(tenantConflict.thenApply(value -> value));
        when(pulsarAdmin.namespaces().createNamespaceAsync(eq("prop/ns-abc"), any(Policies.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        topic.checkClusterMigration().get(5, TimeUnit.SECONDS);

        verify(pulsarAdmin.tenants()).createTenantAsync(eq("prop"), any());
        verify(pulsarAdmin.namespaces()).createNamespaceAsync(eq("prop/ns-abc"), any(Policies.class));
        assertThat(topic.shouldProducerMigrate()).isTrue();
    }

    @Test
    public void testMigrationPropagatesNamespaceCreationFailure() throws Exception {
        PulsarAdmin pulsarAdmin = mockMigrationAdmin();
        Topic topic = prepareTopicForMigration("http://destination.invalid", "orders");
        when(pulsarAdmin.tenants().createTenantAsync(eq("prop"), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Void> createNamespace = new CompletableFuture<>();
        when(pulsarAdmin.namespaces().createNamespaceAsync(eq("prop/ns-abc"), any(Policies.class)))
                .thenReturn(createNamespace);

        CompletableFuture<Void> migration = topic.checkClusterMigration();
        verify(pulsarAdmin.namespaces(), timeout(5000)).createNamespaceAsync(eq("prop/ns-abc"), any(Policies.class));
        assertCreationFailurePropagates(createNamespace, migration);
        verifyNoInteractions(pulsarAdmin.topics());
    }

    @Test
    public void testMigrationPropagatesSubscriptionFailureWhileAnotherIsPending() throws Exception {
        PulsarAdmin pulsarAdmin = mockMigrationAdmin();
        Topic topic = prepareTopicForMigration("http://destination.invalid", "orders", "pending");
        when(pulsarAdmin.tenants().createTenantAsync(eq("prop"), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(pulsarAdmin.namespaces().createNamespaceAsync(eq("prop/ns-abc"), any(Policies.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Void> createOrders = new CompletableFuture<>();
        CompletableFuture<Void> createPending = new CompletableFuture<>();
        when(pulsarAdmin.topics().createSubscriptionAsync(topic.getName(), "orders", MessageId.earliest))
                .thenReturn(createOrders);
        when(pulsarAdmin.topics().createSubscriptionAsync(topic.getName(), "pending", MessageId.earliest))
                .thenReturn(createPending);

        CompletableFuture<Void> migration = topic.checkClusterMigration();
        verify(pulsarAdmin.topics(), timeout(5000))
                .createSubscriptionAsync(topic.getName(), "orders", MessageId.earliest);
        verify(pulsarAdmin.topics(), timeout(5000))
                .createSubscriptionAsync(topic.getName(), "pending", MessageId.earliest);
        assertCreationFailurePropagates(createOrders, migration);
        assertThat(createPending).isNotDone();
        assertThat(topic.shouldProducerMigrate()).isFalse();
    }

    private Topic prepareTopicForMigration(String destinationUrl, String... subscriptions) throws Exception {
        String topic = "persistent://" + newTopicName();
        admin.topics().createNonPartitionedTopic(topic);
        for (String subscription : subscriptions) {
            admin.topics().createSubscription(topic, subscription, MessageId.earliest);
        }
        conf.setClusterMigrationAutoResourceCreation(true);
        admin.clusters().updateClusterMigration(conf.getClusterName(), true,
                new ClusterUrl(destinationUrl, null, null, null));
        return pulsar.getBrokerService().getTopicReference(topic).orElseThrow();
    }

    private PulsarAdmin mockMigrationAdmin() {
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class, RETURNS_DEEP_STUBS);
        BrokerService broker = spy(pulsar.getBrokerService());
        doReturn(pulsarAdmin).when(broker).getClusterPulsarAdmin(eq("migration-cluster"), any());
        doReturn(broker).when(pulsar).getBrokerService();
        return pulsarAdmin;
    }

    private static void assertCreationFailurePropagates(CompletableFuture<Void> create,
                                                        CompletableFuture<Void> migration) {
        PulsarAdminException failure = new PulsarAdminException("Resource creation failed");
        assertThat(failure.getCause()).isNull();
        assertThat(create.completeExceptionally(failure)).isTrue();

        ExecutionException thrown = Assert.expectThrows(ExecutionException.class,
                () -> migration.get(5, TimeUnit.SECONDS));
        assertThat(thrown.getCause()).isSameAs(failure);
    }
}
