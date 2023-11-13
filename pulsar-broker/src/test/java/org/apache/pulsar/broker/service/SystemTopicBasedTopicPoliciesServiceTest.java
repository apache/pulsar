/**
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
package org.apache.pulsar.broker.service;

import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicPoliciesCacheNotInitException;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.BackoffBuilder;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
@Slf4j
public class SystemTopicBasedTopicPoliciesServiceTest extends MockedPulsarServiceBaseTest {

    private static final String NAMESPACE1 = "system-topic/namespace-1";
    private static final String NAMESPACE2 = "system-topic/namespace-2";
    private static final String NAMESPACE3 = "system-topic/namespace-3";

    private static final TopicName TOPIC1 = TopicName.get("persistent", NamespaceName.get(NAMESPACE1), "topic-1");
    private static final TopicName TOPIC2 = TopicName.get("persistent", NamespaceName.get(NAMESPACE1), "topic-2");
    private static final TopicName TOPIC3 = TopicName.get("persistent", NamespaceName.get(NAMESPACE2), "topic-1");
    private static final TopicName TOPIC4 = TopicName.get("persistent", NamespaceName.get(NAMESPACE2), "topic-2");
    private static final TopicName TOPIC5 = TopicName.get("persistent", NamespaceName.get(NAMESPACE3), "topic-1");
    private static final TopicName TOPIC6 = TopicName.get("persistent", NamespaceName.get(NAMESPACE3), "topic-2");

    private SystemTopicBasedTopicPoliciesService systemTopicBasedTopicPoliciesService;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        super.internalSetup();
        prepareData();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConcurrentlyRegisterUnregisterListeners() throws ExecutionException, InterruptedException {
        TopicName topicName = TopicName.get("test");
        class TopicPolicyListenerImpl implements TopicPolicyListener<TopicPolicies> {

            @Override
            public void onUpdate(TopicPolicies data) {
                //no op.
            }
        }

        CompletableFuture<Void> f = CompletableFuture.completedFuture(null).thenRunAsync(() -> {
            for (int i = 0; i < 100; i++) {
                TopicPolicyListener<TopicPolicies> listener = new TopicPolicyListenerImpl();
                systemTopicBasedTopicPoliciesService.registerListener(topicName, listener);
                Assert.assertNotNull(systemTopicBasedTopicPoliciesService.listeners.get(topicName));
                Assert.assertTrue(systemTopicBasedTopicPoliciesService.listeners.get(topicName).size() >= 1);
                systemTopicBasedTopicPoliciesService.unregisterListener(topicName, listener);
            }
        });

        for (int i = 0; i < 100; i++) {
            TopicPolicyListener<TopicPolicies> listener = new TopicPolicyListenerImpl();
            systemTopicBasedTopicPoliciesService.registerListener(topicName, listener);
            Assert.assertNotNull(systemTopicBasedTopicPoliciesService.listeners.get(topicName));
            Assert.assertTrue(systemTopicBasedTopicPoliciesService.listeners.get(topicName).size() >= 1);
            systemTopicBasedTopicPoliciesService.unregisterListener(topicName, listener);
        }

        f.get();
        //Some system topics will be added to the listeners. Just check if it contains topicName.
        Assert.assertFalse(systemTopicBasedTopicPoliciesService.listeners.containsKey(topicName));
    }

    @Test
    public void testGetPolicy() throws ExecutionException, InterruptedException, TopicPoliciesCacheNotInitException {

        // Init topic policies
        TopicPolicies initPolicy = TopicPolicies.builder()
                .maxConsumerPerTopic(10)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, initPolicy).get();

        // Wait for all topic policies updated.
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(systemTopicBasedTopicPoliciesService
                        .getPoliciesCacheInit(TOPIC1.getNamespaceObject()).isDone()));

        // Assert broker is cache all topic policies
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC1)
                        .getMaxConsumerPerTopic().intValue(), 10));

        // Update policy for TOPIC1
        TopicPolicies policies1 = TopicPolicies.builder()
                .maxProducerPerTopic(1)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1).get();

        // Update policy for TOPIC2
        TopicPolicies policies2 = TopicPolicies.builder()
                .maxProducerPerTopic(2)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2).get();

        // Update policy for TOPIC3
        TopicPolicies policies3 = TopicPolicies.builder()
                .maxProducerPerTopic(3)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC3, policies3).get();

        // Update policy for TOPIC4
        TopicPolicies policies4 = TopicPolicies.builder()
                .maxProducerPerTopic(4)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC4, policies4).get();

        // Update policy for TOPIC5
        TopicPolicies policies5 = TopicPolicies.builder()
                .maxProducerPerTopic(5)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC5, policies5).get();

        // Update policy for TOPIC6
        TopicPolicies policies6 = TopicPolicies.builder()
                .maxProducerPerTopic(6)
                .build();
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC6, policies6).get();

        Awaitility.await().untilAsserted(() -> {
            TopicPolicies policiesGet1 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC1);
            TopicPolicies policiesGet2 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC2);
            TopicPolicies policiesGet3 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC3);
            TopicPolicies policiesGet4 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC4);
            TopicPolicies policiesGet5 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC5);
            TopicPolicies policiesGet6 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC6);

            Assert.assertEquals(policiesGet1, policies1);
            Assert.assertEquals(policiesGet2, policies2);
            Assert.assertEquals(policiesGet3, policies3);
            Assert.assertEquals(policiesGet4, policies4);
            Assert.assertEquals(policiesGet5, policies5);
            Assert.assertEquals(policiesGet6, policies6);
        });

        // Remove reader cache will remove policies cache
        Assert.assertEquals(systemTopicBasedTopicPoliciesService.getPoliciesCacheSize(), 6);

        // Check reader cache is correct.
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE1)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE2)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE3)));

        policies1.setMaxProducerPerTopic(101);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1);
        policies2.setMaxProducerPerTopic(102);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2);
        policies2.setMaxProducerPerTopic(103);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2);
        policies1.setMaxProducerPerTopic(104);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1);
        policies2.setMaxProducerPerTopic(105);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, policies2);
        policies1.setMaxProducerPerTopic(106);
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, policies1);

        // reader for NAMESPACE1 will back fill the reader cache
        Awaitility.await().untilAsserted(() -> {
            TopicPolicies policiesGet1 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC1);
            TopicPolicies policiesGet2 = systemTopicBasedTopicPoliciesService.getTopicPolicies(TOPIC2);
            Assert.assertEquals(policies1, policiesGet1);
            Assert.assertEquals(policies2, policiesGet2);
        });

        // Check reader cache is correct.
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE2)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE1)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(NamespaceName.get(NAMESPACE3)));

        // Check get without cache
        TopicPolicies policiesGet1 = systemTopicBasedTopicPoliciesService.getTopicPoliciesBypassCacheAsync(TOPIC1).get();
        Assert.assertEquals(policies1, policiesGet1);
    }

    @Test
    public void testCacheCleanup() throws Exception {
        final String topic = "persistent://" + NAMESPACE1 + "/test" + UUID.randomUUID();
        TopicName topicName = TopicName.get(topic);
        admin.topics().createPartitionedTopic(topic, 3);
        pulsarClient.newProducer().topic(topic).create().close();
        admin.topics().setMaxConsumers(topic, 1000);
        Awaitility.await().untilAsserted(() ->
                assertNotNull(admin.topics().getMaxConsumers(topic)));
        Map<TopicName, TopicPolicies> map = systemTopicBasedTopicPoliciesService.getPoliciesCache();
        Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> listMap =
                systemTopicBasedTopicPoliciesService.getListeners();
        assertNotNull(map.get(topicName));
        assertEquals(map.get(topicName).getMaxConsumerPerTopic().intValue(), 1000);
        assertNotNull(listMap.get(topicName).get(0));

        admin.topics().deletePartitionedTopic(topic, true);
        admin.namespaces().unload(NAMESPACE1);
        assertNull(map.get(topicName));
        assertNull(listMap.get(topicName));
    }

    @Test
    public void testListenerCleanupByPartition() throws Exception {
        final String topic = "persistent://" + NAMESPACE1 + "/test" + UUID.randomUUID();
        TopicName topicName = TopicName.get(topic);
        admin.topics().createPartitionedTopic(topic, 3);
        pulsarClient.newProducer().topic(topic).create().close();

        Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> listMap =
                systemTopicBasedTopicPoliciesService.getListeners();
        Awaitility.await().untilAsserted(() -> {
            // all 3 topic partition have registered the topic policy listeners.
            assertEquals(listMap.get(topicName).size(), 3);
        });

        admin.topics().unload(topicName.getPartition(0).toString());
        assertEquals(listMap.get(topicName).size(), 2);
        admin.topics().unload(topicName.getPartition(1).toString());
        assertEquals(listMap.get(topicName).size(), 1);
        admin.topics().unload(topicName.getPartition(2).toString());
        assertNull(listMap.get(topicName));
    }



    private void prepareData() throws PulsarAdminException {
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("system-topic",
                new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.namespaces().createNamespace(NAMESPACE2);
        admin.namespaces().createNamespace(NAMESPACE3);
        admin.lookups().lookupTopic(TOPIC1.toString());
        admin.lookups().lookupTopic(TOPIC2.toString());
        admin.lookups().lookupTopic(TOPIC3.toString());
        admin.lookups().lookupTopic(TOPIC4.toString());
        admin.lookups().lookupTopic(TOPIC5.toString());
        admin.lookups().lookupTopic(TOPIC6.toString());
        systemTopicBasedTopicPoliciesService = (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
    }

    @Test
    public void testGetPolicyTimeout() throws Exception {
        SystemTopicBasedTopicPoliciesService service = (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        Awaitility.await().untilAsserted(() -> assertTrue(service.policyCacheInitMap.get(TOPIC1.getNamespaceObject()).isDone()));
        service.policyCacheInitMap.put(TOPIC1.getNamespaceObject(), new CompletableFuture<>());
        long start = System.currentTimeMillis();
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(500, TimeUnit.MILLISECONDS)
                .setMandatoryStop(5000, TimeUnit.MILLISECONDS)
                .setMax(1000, TimeUnit.MILLISECONDS)
                .create();
        try {
            service.getTopicPoliciesAsyncWithRetry(TOPIC1, backoff, pulsar.getExecutor(), false).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TopicPoliciesCacheNotInitException);
        }
        long cost = System.currentTimeMillis() - start;
        assertTrue("actual:" + cost, cost >= 5000 - 1000);
    }

    @Test
    public void testGetTopicPoliciesWithRetry() throws Exception {
        Field initMapField = SystemTopicBasedTopicPoliciesService.class.getDeclaredField("policyCacheInitMap");
        initMapField.setAccessible(true);
        Map<NamespaceName, Boolean> initMap = (Map)initMapField.get(systemTopicBasedTopicPoliciesService);
        initMap.remove(NamespaceName.get(NAMESPACE1));
        Field readerCaches = SystemTopicBasedTopicPoliciesService.class.getDeclaredField("readerCaches");
        readerCaches.setAccessible(true);
        Map<NamespaceName, CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>> readers = (Map)readerCaches.get(systemTopicBasedTopicPoliciesService);
        readers.remove(NamespaceName.get(NAMESPACE1));
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(500, TimeUnit.MILLISECONDS)
                .setMandatoryStop(5000, TimeUnit.MILLISECONDS)
                .setMax(1000, TimeUnit.MILLISECONDS)
                .create();
        TopicPolicies initPolicy = TopicPolicies.builder()
                .maxConsumerPerTopic(10)
                .build();
        ScheduledExecutorService executors = Executors.newScheduledThreadPool(1);
        executors.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, initPolicy).get();
                } catch (Exception ignore) {}
            }
        }, 2000, TimeUnit.MILLISECONDS);
        Awaitility.await().untilAsserted(() -> {
            Optional<TopicPolicies> topicPolicies = systemTopicBasedTopicPoliciesService
                    .getTopicPoliciesAsyncWithRetry(TOPIC1, backoff, pulsar.getExecutor(), false).get();
            Assert.assertTrue(topicPolicies.isPresent());
            if (topicPolicies.isPresent()) {
                Assert.assertEquals(topicPolicies.get(), initPolicy);
            }
        });
    }

    @Test
    public void testHandleNamespaceBeingDeleted() throws Exception {
        SystemTopicBasedTopicPoliciesService service = (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        pulsar.getPulsarResources().getNamespaceResources().setPolicies(NamespaceName.get(NAMESPACE1),
                old -> {
                    old.deleted = true;
                    return old;
        });
        service.deleteTopicPoliciesAsync(TOPIC1).get();
    }

    @Test
    public void testGetTopicPoliciesWithCleanCache() throws Exception {
        final String topic = "persistent://" + NAMESPACE1 + "/test" + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();

        SystemTopicBasedTopicPoliciesService topicPoliciesService =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();

        ConcurrentHashMap<TopicName, TopicPolicies> spyPoliciesCache = spy(new ConcurrentHashMap<TopicName, TopicPolicies>());
        FieldUtils.writeDeclaredField(topicPoliciesService, "policiesCache", spyPoliciesCache, true);

            Awaitility.await().untilAsserted(() -> {
                Assertions.assertThat(topicPoliciesService.getTopicPolicies(TopicName.get(topic))).isNull();
        });

        admin.topicPolicies().setMaxConsumersPerSubscription(topic, 1);
        Awaitility.await().untilAsserted(() -> {
                Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic))).isNotNull();
            });

        Map<NamespaceName, CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>> readers =
                (Map<NamespaceName, CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>>)
                        FieldUtils.readDeclaredField(topicPoliciesService, "readerCaches", true);

        Mockito.doAnswer(invocation -> {
            Thread.sleep(1000);
            return invocation.callRealMethod();
        }).when(spyPoliciesCache).get(Mockito.any());

        CompletableFuture<Void> result = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            TopicPolicies topicPolicies;
            for (int i = 0; i < 10; i++) {
                try {
                    topicPolicies = topicPoliciesService.getTopicPolicies(TopicName.get(topic));
                    Assert.assertNotNull(topicPolicies);
                    Thread.sleep(500);
                } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
                    log.warn("topic policies cache not init, retry...");
                } catch (Throwable e) {
                    log.error("ops: ", e);
                    result.completeExceptionally(e);
                    return;
                }
            }
            result.complete(null);
        });

        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerCompletableFuture =
                        readers.get(TopicName.get(topic).getNamespaceObject());
                if (readerCompletableFuture != null) {
                    readerCompletableFuture.join().closeAsync().join();
                }
            }
        });

        thread.start();
        thread2.start();

        thread.join();
        thread2.join();

        result.join();
    }
}
