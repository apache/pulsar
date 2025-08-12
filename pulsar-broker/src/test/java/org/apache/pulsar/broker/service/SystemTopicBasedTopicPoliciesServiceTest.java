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
package org.apache.pulsar.broker.service;

import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Schema;
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

    private static final String NAMESPACE4 = "system-topic/namespace-4";

    private static final String NAMESPACE5 = "system-topic/namespace-5";

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
        class TopicPolicyListenerImpl implements TopicPolicyListener {

            @Override
            public void onUpdate(TopicPolicies data) {
                //no op.
            }
        }

        CompletableFuture<Void> f = CompletableFuture.completedFuture(null).thenRunAsync(() -> {
            for (int i = 0; i < 100; i++) {
                TopicPolicyListener listener = new TopicPolicyListenerImpl();
                systemTopicBasedTopicPoliciesService.registerListener(topicName, listener);
                Assert.assertNotNull(systemTopicBasedTopicPoliciesService.listeners.get(topicName));
                Assert.assertTrue(systemTopicBasedTopicPoliciesService.listeners.get(topicName).size() >= 1);
                systemTopicBasedTopicPoliciesService.unregisterListener(topicName, listener);
            }
        });

        for (int i = 0; i < 100; i++) {
            TopicPolicyListener listener = new TopicPolicyListenerImpl();
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
    public void testGetPolicy() throws Exception {

        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(10)).get();

        // Wait for all topic policies updated.
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(systemTopicBasedTopicPoliciesService
                        .getPoliciesCacheInit(TOPIC1.getNamespaceObject()).isDone()));

        // Assert broker is cache all topic policies
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(TopicPolicyTestUtils.getTopicPolicies(systemTopicBasedTopicPoliciesService, TOPIC1)
                        .getMaxConsumerPerTopic().intValue(), 10));

        // Update policy for TOPIC1
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(1)).get();

        // Update policy for TOPIC2
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(2)).get();

        // Update policy for TOPIC3
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC3, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(3)).get();

        // Update policy for TOPIC4
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC4, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(4)).get();

        // Update policy for TOPIC5
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC5, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(5)).get();

        // Update policy for TOPIC6
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC6, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(6)).get();

        Awaitility.await().untilAsserted(() -> {
            TopicPolicies policiesGet1 = TopicPolicyTestUtils.getTopicPolicies(systemTopicBasedTopicPoliciesService,
                    TOPIC1);
            TopicPolicies policiesGet2 = TopicPolicyTestUtils.getTopicPolicies(systemTopicBasedTopicPoliciesService,
                    TOPIC2);
            TopicPolicies policiesGet3 = TopicPolicyTestUtils.getTopicPolicies(systemTopicBasedTopicPoliciesService,
                    TOPIC3);
            TopicPolicies policiesGet4 = TopicPolicyTestUtils.getTopicPolicies(systemTopicBasedTopicPoliciesService,
                    TOPIC4);
            TopicPolicies policiesGet5 = TopicPolicyTestUtils.getTopicPolicies(systemTopicBasedTopicPoliciesService,
                    TOPIC5);
            TopicPolicies policiesGet6 = TopicPolicyTestUtils.getTopicPolicies(systemTopicBasedTopicPoliciesService,
                    TOPIC6);

            Assert.assertEquals(policiesGet1.getMaxConsumerPerTopic(), 1);
            Assert.assertEquals(policiesGet2.getMaxConsumerPerTopic(), 2);
            Assert.assertEquals(policiesGet3.getMaxConsumerPerTopic(), 3);
            Assert.assertEquals(policiesGet4.getMaxConsumerPerTopic(), 4);
            Assert.assertEquals(policiesGet5.getMaxConsumerPerTopic(), 5);
            Assert.assertEquals(policiesGet6.getMaxConsumerPerTopic(), 6);
        });

        // Remove reader cache will remove policies cache
        Assert.assertEquals(systemTopicBasedTopicPoliciesService.getPoliciesCacheSize(), 6);

        // Check reader cache is correct.
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(
                NamespaceName.get(NAMESPACE1)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(
                NamespaceName.get(NAMESPACE2)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(
                NamespaceName.get(NAMESPACE3)));

        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(101));
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(102));
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(103));
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(104));
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC2, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(105));
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(106));

        // reader for NAMESPACE1 will back fill the reader cache
        Awaitility.await().untilAsserted(() -> {
            TopicPolicies policiesGet1 =
                    TopicPolicyTestUtils.getTopicPolicies(systemTopicBasedTopicPoliciesService, TOPIC1);
            TopicPolicies policiesGet2 =
                    TopicPolicyTestUtils.getTopicPolicies(systemTopicBasedTopicPoliciesService, TOPIC2);
            Assert.assertEquals(policiesGet1.getMaxConsumerPerTopic(), 106);
            Assert.assertEquals(policiesGet2.getMaxConsumerPerTopic(), 105);
        });

        // Check reader cache is correct.
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(
                NamespaceName.get(NAMESPACE2)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(
                NamespaceName.get(NAMESPACE1)));
        Assert.assertTrue(systemTopicBasedTopicPoliciesService.checkReaderIsCached(
                NamespaceName.get(NAMESPACE3)));

        TopicPolicies policies1 = TopicPolicyTestUtils.getTopicPolicies(systemTopicBasedTopicPoliciesService, TOPIC1);
        // Check get without cache
        TopicPolicies policiesGet1 = TopicPolicyTestUtils.getTopicPoliciesBypassCache(
                systemTopicBasedTopicPoliciesService, TOPIC1, false).orElseThrow();
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
        Map<TopicName, List<TopicPolicyListener>> listMap =
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

        Map<TopicName, List<TopicPolicyListener>> listMap =
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
        admin.clusters().createCluster("test", ClusterData.builder()
                .serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("system-topic",
                new TenantInfoImpl(new HashSet<>(), Set.of("test")));
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
    public void testHandleNamespaceBeingDeleted() throws Exception {
        SystemTopicBasedTopicPoliciesService service =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
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

        ConcurrentHashMap<TopicName, TopicPolicies> spyPoliciesCache =
                spy(new ConcurrentHashMap<TopicName, TopicPolicies>());
        FieldUtils.writeDeclaredField(topicPoliciesService, "policiesCache", spyPoliciesCache, true);

        Awaitility.await().untilAsserted(() -> Assertions.assertThat(
                TopicPolicyTestUtils.getTopicPolicies(topicPoliciesService, TopicName.get(topic))).isNull());

        admin.topicPolicies().setMaxConsumersPerSubscription(topic, 1);
        Awaitility.await().untilAsserted(() -> {
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                        TopicName.get(topic))).isNotNull();
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
            try {
                for (int i = 0; i < 10; i++) {
                    final var policies = TopicPolicyTestUtils.getTopicPolicies(topicPoliciesService,
                            TopicName.get(topic));
                    if (policies == null) {
                        throw new Exception("null policies for " + i + "th get");
                    }
                }
                result.complete(null);
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
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

    @Test
    public void testWriterCache() throws Exception {
        admin.namespaces().createNamespace(NAMESPACE4);
        for (int i = 1; i <= 5; i++) {
            final String topicName = "persistent://" + NAMESPACE4 + "/testWriterCache" + i;
            admin.topics().createNonPartitionedTopic(topicName);
            pulsarClient.newProducer(Schema.STRING).topic(topicName).create().close();
        }
        @Cleanup("shutdown")
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 1; i <= 5; i++) {
            int finalI = i;
            executorService.execute(() -> {
                final String topicName = "persistent://" + NAMESPACE4 + "/testWriterCache" + finalI;
                try {
                    admin.topicPolicies().setMaxConsumers(topicName, 2);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        SystemTopicBasedTopicPoliciesService service =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        Assert.assertNotNull(service.getWriterCaches().synchronous().get(NamespaceName.get(NAMESPACE4)));
        for (int i = 1; i <= 5; i++) {
            final String topicName = "persistent://" + NAMESPACE4 + "/testWriterCache" + i;
            admin.topics().delete(topicName);
        }
        admin.namespaces().deleteNamespace(NAMESPACE4);
        Assert.assertNull(service.getWriterCaches().synchronous().getIfPresent(NamespaceName.get(NAMESPACE4)));
    }

    @Test
    public void testPrepareInitPoliciesCacheAsyncWhenNamespaceBeingDeleted() throws Exception {
        SystemTopicBasedTopicPoliciesService service =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        admin.namespaces().createNamespace(NAMESPACE5);

        NamespaceName namespaceName = NamespaceName.get(NAMESPACE5);
        pulsar.getPulsarResources().getNamespaceResources().setPolicies(namespaceName,
                old -> {
                    old.deleted = true;
                    return old;
                });

        assertNull(service.getPoliciesCacheInit(namespaceName));
        service.prepareInitPoliciesCacheAsync(namespaceName).get();
        admin.namespaces().deleteNamespace(NAMESPACE5);
    }
}
