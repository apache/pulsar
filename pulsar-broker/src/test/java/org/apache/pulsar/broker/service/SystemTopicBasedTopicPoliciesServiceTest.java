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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.utils.TestLogAppender;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
@CustomLog
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
                systemTopicBasedTopicPoliciesService.registerListenerAsync(topicName, listener);
                Assert.assertNotNull(systemTopicBasedTopicPoliciesService.listeners.get(topicName));
                Assert.assertTrue(systemTopicBasedTopicPoliciesService.listeners.get(topicName).size() >= 1);
                systemTopicBasedTopicPoliciesService.unregisterListener(topicName, listener);
            }
        });

        for (int i = 0; i < 100; i++) {
            TopicPolicyListener listener = new TopicPolicyListenerImpl();
            systemTopicBasedTopicPoliciesService.registerListenerAsync(topicName, listener);
            Assert.assertNotNull(systemTopicBasedTopicPoliciesService.listeners.get(topicName));
            Assert.assertTrue(systemTopicBasedTopicPoliciesService.listeners.get(topicName).size() >= 1);
            systemTopicBasedTopicPoliciesService.unregisterListener(topicName, listener);
        }

        f.get();
        //Some system topics will be added to the listeners. Just check if it contains topicName.
        Assert.assertFalse(systemTopicBasedTopicPoliciesService.listeners.containsKey(topicName));
    }

    @Test
    public void testListenerNotificationRunsOffSharedReaderThread() throws Exception {
        // Regression test for #26037: topic-policy listener callbacks must not run on the single,
        // process-wide shared "broker-client-shared-internal-executor" reader thread. A slow or blocking
        // onUpdate there serializes — and can stall — topic-policy loading for every namespace. The
        // notification must instead be dispatched to the per-topic ordered executor ("broker-topic-workers").

        // Initialize the policy cache and start the change-events reader for the namespace.
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(10)).get();
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(systemTopicBasedTopicPoliciesService
                .getPoliciesCacheInit(TOPIC1.getNamespaceObject()).isDone()));

        // Register a listener that records the thread its onUpdate runs on.
        CompletableFuture<String> onUpdateThreadName = new CompletableFuture<>();
        TopicPolicyListener listener = data -> onUpdateThreadName.complete(Thread.currentThread().getName());
        systemTopicBasedTopicPoliciesService.registerListenerAsync(TOPIC1, listener).get();

        // A live policy update flows through readMorePoliciesAsync -> notifyListener -> listener.onUpdate.
        systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TOPIC1, false, false, topicPolicies ->
                topicPolicies.setMaxConsumerPerTopic(20)).get();

        String threadName = onUpdateThreadName.get(30, TimeUnit.SECONDS);
        assertFalse("listener.onUpdate must not run on the shared broker-client reader thread, but ran on: "
                        + threadName,
                threadName.contains("broker-client-shared-internal-executor"));
        assertTrue("listener.onUpdate should run on the per-topic ordered executor, but ran on: " + threadName,
                threadName.contains("broker-topic-workers"));
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

    @SuppressWarnings("deprecation")
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
    @SuppressWarnings("unchecked")
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

    @Test
    public void testCreateNamespaceEventsSystemTopicFactoryException() throws Exception {
        final String namespace = "system-topic/namespace-6";

        admin.namespaces().createNamespace(namespace);

        TopicName topicName = TopicName.get("persistent", NamespaceName.get(namespace), "topic-1");

        SystemTopicBasedTopicPoliciesService service =
            Mockito.spy((SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService());

        // inject exception when create NamespaceEventsSystemTopicFactory
        Mockito.doThrow(new RuntimeException("test exception")).when(service)
            .getNamespaceEventsSystemTopicFactory();

        CompletableFuture<Optional<TopicPolicies>> topicPoliciesFuture;
        Optional<TopicPolicies> topicPoliciesOptional;
        try {
            topicPoliciesFuture =
            service.getTopicPoliciesAsync(topicName, TopicPoliciesService.GetType.LOCAL_ONLY);
            topicPoliciesOptional = topicPoliciesFuture.join();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("test exception"));
        }

        Mockito.reset(service);

        service.updateTopicPoliciesAsync(topicName, false, false, topicPolicies ->
            topicPolicies.setMaxConsumerPerTopic(10)).get();

        topicPoliciesFuture =
            service.getTopicPoliciesAsync(topicName, TopicPoliciesService.GetType.LOCAL_ONLY);
        topicPoliciesOptional = topicPoliciesFuture.join();

        Assert.assertNotNull(topicPoliciesOptional);
        Assert.assertTrue(topicPoliciesOptional.isPresent());

        TopicPolicies topicPolicies = topicPoliciesOptional.get();
        Assert.assertNotNull(topicPolicies);
        Assert.assertEquals(topicPolicies.getMaxConsumerPerTopic(), 10);
    }

    @Test
    public void testPrepareInitPoliciesCacheAsyncThrowExceptionAfterCreateReader() throws Exception {
        // catch the log output
        @Cleanup
        TestLogAppender testLogAppender = TestLogAppender.create(log);

        // create namespace-5 and topic
        pulsar.getTopicPoliciesService().close();
        SystemTopicBasedTopicPoliciesService spyService =
                Mockito.spy(new SystemTopicBasedTopicPoliciesService(pulsar));
        FieldUtils.writeField(pulsar, "topicPoliciesService", spyService, true);

        admin.namespaces().createNamespace(NAMESPACE5);
        final String topic = "persistent://" + NAMESPACE5 + "/test" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 1);

        CompletableFuture<Void> future = spyService.getPoliciesCacheInit(NamespaceName.get(NAMESPACE5));
        Assert.assertNull(future);

        // mock readerCache and new a reader, then put this reader in readerCache.
        // when new reader, would trigger __change_event topic of namespace-5 created
        // and would trigger prepareInitPoliciesCacheAsync()
        ConcurrentHashMap<NamespaceName, CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>>
                spyReaderCaches = new ConcurrentHashMap<>();
        CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerCompletableFuture =
                spyService.createSystemTopicClient(NamespaceName.get(NAMESPACE5));
        spyReaderCaches.put(NamespaceName.get(NAMESPACE5), readerCompletableFuture);
        FieldUtils.writeDeclaredField(spyService, "readerCaches", spyReaderCaches, true);

        // set topic policy. create producer for __change_event topic
        admin.topicPolicies().setMaxConsumersPerSubscription(topic, 1);
        future = spyService.getPoliciesCacheInit(NamespaceName.get(NAMESPACE5));
        Assert.assertNotNull(future);

        // trigger close reader of __change_event directly, simulate that reader
        // is closed for some reason, such as topic unload or broker restart.
        // since prepareInitPoliciesCacheAsync() has been executed, it would go into readMorePoliciesAsync(),
        // throw exception, output "Closing the topic policies reader for" and do cleanPoliciesCacheInitMap()
        SystemTopicClient.Reader<PulsarEvent> reader = readerCompletableFuture.get();
        reader.close();
        log.info("successfully close spy reader");
        Awaitility.await().untilAsserted(() -> {
            boolean logFound = testLogAppender.getEvents().stream().anyMatch(logEvent ->
                    logEvent.getMessage().toString().contains("Closing the topic policies reader for"));
            assertTrue(logFound);
        });

        // The reader.close() above drives readMorePoliciesAsync() into cleanPoliciesCacheInitMap(), which logs
        // "Closing the topic policies reader for" and removes the namespace from policyCacheInitMap and readerCaches.
        // Now exercise a follow-up prepareInitPoliciesCacheAsync() that reuses a closed reader and must fail in
        // initPolicesCache(). Two timing hazards made this flaky (#25081), so both are pinned deterministically:
        //  1) A real closed reader's hasMoreEventsAsync() (Reader.hasMessageAvailableAsync()) can answer from cached
        //     state instead of failing with AlreadyClosedException, in which case initPolicesCache() reaches the end
        //     of the topic and completes successfully and the expected exception never happens. Use a spy whose
        //     hasMoreEventsAsync() always fails with AlreadyClosedException.
        //  2) A background topic load can re-run prepareInitPoliciesCacheAsync() for the namespace and leave a
        //     completed init future behind; the next call would then short-circuit through the existing-future
        //     branch and never re-initialize. Drop any init future right before the call so it re-initializes, and
        //     stub createSystemTopicClient() so every reader created for this namespace is the failing spy — then
        //     whichever initialization wins the race fails in the same (asserted) way.
        SystemTopicClient.Reader<PulsarEvent> closedReader = Mockito.spy(reader);
        Mockito.doReturn(CompletableFuture.failedFuture(
                        new PulsarClientException.AlreadyClosedException("Reader is already closed")))
                .when(closedReader).hasMoreEventsAsync();
        Mockito.doReturn(CompletableFuture.completedFuture(closedReader))
                .when(spyService).createSystemTopicClient(NamespaceName.get(NAMESPACE5));
        spyReaderCaches.put(NamespaceName.get(NAMESPACE5), CompletableFuture.completedFuture(closedReader));
        FieldUtils.writeDeclaredField(spyService, "readerCaches", spyReaderCaches, true);
        spyService.policyCacheInitMap.remove(NamespaceName.get(NAMESPACE5));

        CompletableFuture<Boolean> prepareFuture = new CompletableFuture<>();
        try {
            prepareFuture = spyService.prepareInitPoliciesCacheAsync(NamespaceName.get(NAMESPACE5));
            prepareFuture.get();
            Assert.fail();
        } catch (Exception e) {
            // that is ok
        }

        // since prepareInitPoliciesCacheAsync() throw exception when initPolicesCache(),
        // would clean readerCache and policyCacheInitMap.
        Assert.assertTrue(prepareFuture.isCompletedExceptionally());
        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<Void> future1 = spyService.getPoliciesCacheInit(NamespaceName.get(NAMESPACE5));
            Assert.assertNull(future1);
            CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerCompletableFuture1 =
                    spyReaderCaches.get(NamespaceName.get(NAMESPACE5));
            Assert.assertNull(readerCompletableFuture1);
        });

        // Cleanup must run exactly once per trigger and not repeat recursively (in older code it ran 3 times).
        // Two failures are triggered here, and both tear down through the identity-guarded cleanupFailedPolicyCacheInit
        // (2x): the reader.close() above drives readMorePoliciesAsync's AlreadyClosed branch into it, and the second
        // prepareInitPoliciesCacheAsync fails in initPolicesCache and is torn down by it as well. The namespace-keyed
        // cleanPoliciesCacheInitMap must not be reached from the reader-close path, otherwise a superseded reader could
        // clobber a newer generation's init future.
        boolean logFound = testLogAppender.getEvents().stream().anyMatch(logEvent ->
                logEvent.getMessage().toString().contains("Failed to create reader on __change_events topic"));
        assertFalse(logFound);
        boolean logFound2 = testLogAppender.getEvents().stream().anyMatch(logEvent ->
                logEvent.getMessage().toString().contains("Failed to check the move events for the system topic"));
        assertTrue(logFound2);
        verify(spyService, times(0)).cleanPoliciesCacheInitMap(any());
        verify(spyService, times(2)).cleanupFailedPolicyCacheInit(any(), any(), anyBoolean());

        // make sure not occur Recursive update
        boolean logFound3 = testLogAppender.getEvents().stream().anyMatch(logEvent ->
                logEvent.getMessage().toString().contains("Recursive update"));
        assertFalse(logFound3);
    }

    @Test
    public void testPrepareInitPoliciesCacheAsyncThrowExceptionInCreateReader() throws Exception {
        // catch the log output
        @Cleanup
        TestLogAppender testLogAppender = TestLogAppender.create(log);

        // create namespace-5 and topic
        pulsar.getTopicPoliciesService().close();
        SystemTopicBasedTopicPoliciesService spyService =
                Mockito.spy(new SystemTopicBasedTopicPoliciesService(pulsar));
        FieldUtils.writeField(pulsar, "topicPoliciesService", spyService, true);

        admin.namespaces().createNamespace(NAMESPACE5);
        final String topic = "persistent://" + NAMESPACE5 + "/test" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 1);

        CompletableFuture<Void> future = spyService.getPoliciesCacheInit(NamespaceName.get(NAMESPACE5));
        Assert.assertNull(future);

        // mock readerCache and put a failed readerCreateFuture in readerCache.
        // simulate that when trigger prepareInitPoliciesCacheAsync(),
        // it would use this failed readerFuture and go into corresponding logic
        ConcurrentHashMap<NamespaceName, CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>>
                spyReaderCaches = new ConcurrentHashMap<>();
        CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerCompletableFuture = new CompletableFuture<>();
        readerCompletableFuture.completeExceptionally(new Exception("create reader fail"));
        spyReaderCaches.put(NamespaceName.get(NAMESPACE5), readerCompletableFuture);
        FieldUtils.writeDeclaredField(spyService, "readerCaches", spyReaderCaches, true);

        // trigger prepareInitPoliciesCacheAsync()
        CompletableFuture<Boolean> prepareFuture = new CompletableFuture<>();
        try {
            prepareFuture = spyService.prepareInitPoliciesCacheAsync(NamespaceName.get(NAMESPACE5));
            prepareFuture.get();
            Assert.fail();
        } catch (Exception e) {
            // that is ok
        }

        // since prepareInitPoliciesCacheAsync() throw exception when createReader,
        // would clean readerCache and policyCacheInitMap.
        Assert.assertTrue(prepareFuture.isCompletedExceptionally());
        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<Void> future1 = spyService.getPoliciesCacheInit(NamespaceName.get(NAMESPACE5));
            Assert.assertNull(future1);
            CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerCompletableFuture1 =
                    spyReaderCaches.get(NamespaceName.get(NAMESPACE5));
            Assert.assertNull(readerCompletableFuture1);
        });

        // Reader creation fails, so the single cleanup runs once via the identity-guarded
        // cleanupFailedPolicyCacheInit (the reader-creation-failure branch no longer goes through
        // cleanPoliciesCacheInitMap), and must not run more than once.
        boolean logFound = testLogAppender.getEvents().stream().anyMatch(logEvent ->
                logEvent.getMessage().toString().contains("Failed to create reader on __change_events topic"));
        assertTrue(logFound);
        boolean logFound2 = testLogAppender.getEvents().stream().anyMatch(logEvent ->
                logEvent.getMessage().toString().contains("Failed to check the move events for the system topic")
                        || logEvent.getMessage().toString().contains("Failed to read event from the system topic"));
        assertFalse(logFound2);
        verify(spyService, times(1)).cleanupFailedPolicyCacheInit(any(), any(), anyBoolean());
        verify(spyService, times(0)).cleanPoliciesCacheInitMap(any());
    }

    @Test(timeOut = 60_000)
    public void testPrepareInitPoliciesCacheAsyncTimesOutWhenReaderStuck() throws Exception {
        // Bound the policy-cache initialization to a short timeout for the test.
        pulsar.getConfiguration().setTopicPoliciesCacheInitTimeoutSeconds(3);

        pulsar.getTopicPoliciesService().close();
        SystemTopicBasedTopicPoliciesService spyService =
                Mockito.spy(new SystemTopicBasedTopicPoliciesService(pulsar));
        FieldUtils.writeField(pulsar, "topicPoliciesService", spyService, true);

        admin.namespaces().createNamespace(NAMESPACE5);
        final NamespaceName namespace = NamespaceName.get(NAMESPACE5);

        // Create a real __change_events reader, then spy it so that it reports more events but never delivers one —
        // i.e. a reader that reconnected but is stuck (issue #25294). initPolicesCache would otherwise never complete.
        SystemTopicClient.Reader<PulsarEvent> stuckReader = Mockito.spy(spyService.createSystemTopicClient(namespace)
                .get(30, TimeUnit.SECONDS));
        Mockito.doReturn(CompletableFuture.completedFuture(true)).when(stuckReader).hasMoreEventsAsync();
        Mockito.doReturn(new CompletableFuture<Message<PulsarEvent>>()).when(stuckReader).readNextAsync();
        Mockito.doReturn(CompletableFuture.completedFuture(stuckReader))
                .when(spyService).createSystemTopicClient(namespace);

        // Without the timeout the returned future never completes and topic loading for the namespace hangs forever.
        CompletableFuture<Boolean> prepareFuture = spyService.prepareInitPoliciesCacheAsync(namespace);
        try {
            prepareFuture.get(15, TimeUnit.SECONDS);
            Assert.fail("Expected the topic policies cache initialization to time out");
        } catch (ExecutionException e) {
            assertTrue("Expected a TimeoutException cause but got " + e.getCause(),
                    e.getCause() instanceof TimeoutException);
        }

        // The poisoned cache entry must be cleared and the stuck reader closed so a subsequent load can retry with a
        // fresh reader instead of being pinned until the broker restarts.
        Awaitility.await().untilAsserted(() -> assertNull(spyService.getPoliciesCacheInit(namespace)));
        Mockito.verify(stuckReader, Mockito.atLeastOnce()).closeAsync();
    }

    @Test
    public void testCleanPoliciesCacheInitMapCompletesPendingInitFuture() {
        SystemTopicBasedTopicPoliciesService service =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        final NamespaceName namespace = NamespaceName.get(NAMESPACE1);

        // Dropping the cached init future (e.g. on a namespace-bundle unload) must complete it so the topic loads
        // awaiting it fail fast and retry, instead of hanging until the broker restarts (issue #25294).
        CompletableFuture<Void> pendingInitFuture = new CompletableFuture<>();
        service.policyCacheInitMap.put(namespace, pendingInitFuture);
        service.cleanPoliciesCacheInitMap(namespace);
        assertTrue(pendingInitFuture.isCompletedExceptionally());
        assertNull(service.getPoliciesCacheInit(namespace));

        // An already-completed init future must not be overwritten/disturbed.
        CompletableFuture<Void> alreadyDone = CompletableFuture.completedFuture(null);
        service.policyCacheInitMap.put(namespace, alreadyDone);
        service.cleanPoliciesCacheInitMap(namespace);
        assertFalse(alreadyDone.isCompletedExceptionally());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCleanupFailedPolicyCacheInitIsIdentityGuarded() {
        SystemTopicBasedTopicPoliciesService service =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        final NamespaceName namespace = NamespaceName.get(NAMESPACE1);

        // A newer init attempt (B) already owns the namespace with a fresh future and reader.
        CompletableFuture<Void> newerInitFuture = new CompletableFuture<>();
        SystemTopicClient.Reader<PulsarEvent> newerReader = Mockito.mock(SystemTopicClient.Reader.class);
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(newerReader).closeAsync();
        service.policyCacheInitMap.put(namespace, newerInitFuture);
        service.getReaderCaches().put(namespace, CompletableFuture.completedFuture(newerReader));

        // A stale init attempt (A) whose reader was already torn down (e.g. by the timeout cleanup) fires its failure
        // callback late. Cleaning it up by identity must be a no-op: it must not clobber B's future or close B's reader
        // (issue #25294 follow-up).
        CompletableFuture<Void> staleInitFuture = new CompletableFuture<>();
        service.cleanupFailedPolicyCacheInit(namespace, staleInitFuture, true);
        assertSame(newerInitFuture, service.getPoliciesCacheInit(namespace));
        assertFalse(newerInitFuture.isDone());
        assertNotNull(service.getReaderCaches().get(namespace));
        Mockito.verify(newerReader, Mockito.never()).closeAsync();

        // Cleaning up the owning attempt does drop its future and close its reader.
        service.cleanupFailedPolicyCacheInit(namespace, newerInitFuture, true);
        assertNull(service.getPoliciesCacheInit(namespace));
        assertTrue(newerInitFuture.isCompletedExceptionally());
        assertNull(service.getReaderCaches().get(namespace));
        Mockito.verify(newerReader, Mockito.times(1)).closeAsync();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testClosedSupersededReaderDoesNotAbortReloadedInit() throws Exception {
        // Reproduces the race behind the flaky AdminApi2Test.testGetInternalStatsWithProperties: a namespace unload
        // closes the __change_events reader while a reload (e.g. getTopic right after unload) installs a fresh reader
        // and init future for the same namespace. The old reader's close only surfaces later, on the pulsar-client
        // executor, as an AlreadyClosedException in readMorePoliciesAsync. That late cleanup must NOT clobber the newer
        // generation and abort its init future ("...aborted because the cached state was cleared"), which would fail
        // the reloading topic load.
        @Cleanup
        TestLogAppender testLogAppender = TestLogAppender.create(log);

        pulsar.getTopicPoliciesService().close();
        SystemTopicBasedTopicPoliciesService spyService =
                Mockito.spy(new SystemTopicBasedTopicPoliciesService(pulsar));
        FieldUtils.writeField(pulsar, "topicPoliciesService", spyService, true);

        final NamespaceName namespace = NamespaceName.get(NAMESPACE5);
        admin.namespaces().createNamespace(NAMESPACE5);

        // A real reader, spied so its background read loop is fully controllable: it reports "no more events" so the
        // initialization completes and readMorePoliciesAsync starts, then parks on a read future we complete by hand.
        SystemTopicClient.Reader<PulsarEvent> oldReader =
                Mockito.spy(spyService.createSystemTopicClient(namespace).get(30, TimeUnit.SECONDS));
        CompletableFuture<Message<PulsarEvent>> parkedRead = new CompletableFuture<>();
        Mockito.doReturn(CompletableFuture.completedFuture(false)).when(oldReader).hasMoreEventsAsync();
        Mockito.doReturn(parkedRead).when(oldReader).readNextAsync();
        Mockito.doReturn(CompletableFuture.completedFuture(oldReader))
                .when(spyService).createSystemTopicClient(namespace);
        spyService.getReaderCaches().put(namespace, CompletableFuture.completedFuture(oldReader));

        // Drive initialization: readMorePoliciesAsync(oldReader, <old init future>) is now looping, parked on
        // parkedRead, having registered its whenComplete callback.
        assertTrue(spyService.prepareInitPoliciesCacheAsync(namespace).get(30, TimeUnit.SECONDS));
        Mockito.verify(oldReader, Mockito.atLeastOnce()).readNextAsync();

        // Simulate the concurrent unload+reload having already replaced the generation: a fresh reader and a fresh,
        // still-pending init future that a reloading topic is awaiting.
        SystemTopicClient.Reader<PulsarEvent> reloadReader = Mockito.mock(SystemTopicClient.Reader.class);
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(reloadReader).closeAsync();
        CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> reloadReaderFuture =
                CompletableFuture.completedFuture(reloadReader);
        CompletableFuture<Void> reloadInitFuture = new CompletableFuture<>();
        spyService.getReaderCaches().put(namespace, reloadReaderFuture);
        spyService.policyCacheInitMap.put(namespace, reloadInitFuture);

        // The old reader finally observes it was closed; this runs readMorePoliciesAsync's AlreadyClosed cleanup
        // synchronously on this thread.
        parkedRead.completeExceptionally(new PulsarClientException.AlreadyClosedException("reader is already closed"));

        // The cleanup ran (it logged), but being identity-guarded on the init future it left the newer generation
        // untouched. Before the fix it cleared readerCaches/policyCacheInitMap by namespace key and aborted the reload.
        assertTrue(testLogAppender.getEvents().stream().anyMatch(e ->
                e.getMessage().toString().contains("Closing the topic policies reader for")));
        assertFalse("the reload's init future must not be aborted by the superseded reader's late close",
                reloadInitFuture.isCompletedExceptionally());
        assertFalse(reloadInitFuture.isDone());
        assertSame("the reload's reader must remain cached", reloadReaderFuture,
                spyService.getReaderCaches().get(namespace));
        assertSame(reloadInitFuture, spyService.getPoliciesCacheInit(namespace));
        Mockito.verify(reloadReader, Mockito.never()).closeAsync();
    }

    @Test
    public void testReplayTopicPolicyListenersNotifiesOnlyNamespaceScopedLocalAndGlobalPolicies() throws Exception {
        SystemTopicBasedTopicPoliciesService service =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        final NamespaceName namespaceA = NamespaceName.get(NAMESPACE1);
        final NamespaceName namespaceB = NamespaceName.get(NAMESPACE2);
        final TopicName localTopicA = TopicName.get("persistent", namespaceA, "replay-local-a");
        final TopicName globalTopicA = TopicName.get("persistent", namespaceA, "replay-global-a");
        final TopicName topicB = TopicName.get("persistent", namespaceB, "replay-b");

        // Seed the caches: namespace A has one topic with a cached local policy and another with a cached global
        // policy; namespace B has a topic with a cached local policy that must not be replayed for namespace A.
        service.policiesCache.put(localTopicA, TopicPolicies.builder().isGlobal(false).build());
        service.globalPoliciesCache.put(globalTopicA, TopicPolicies.builder().isGlobal(true).build());
        service.policiesCache.put(topicB, TopicPolicies.builder().isGlobal(false).build());

        final Map<TopicName, List<TopicPolicies>> received = new ConcurrentHashMap<>();
        for (TopicName topicName : List.of(localTopicA, globalTopicA, topicB)) {
            final List<TopicPolicies> updates = new CopyOnWriteArrayList<>();
            received.put(topicName, updates);
            service.registerListenerAsync(topicName, updates::add).get();
        }

        service.replayTopicPolicyListeners(namespaceA).get(30, TimeUnit.SECONDS);

        // Only namespace A's topics are notified, once each, and both the local and the global cache are replayed.
        Assertions.assertThat(received.get(localTopicA)).hasSize(1);
        Assertions.assertThat(received.get(globalTopicA)).hasSize(1);
        // Namespace B is left untouched. The pre-fix code iterated the whole cache and replayed every namespace.
        Assertions.assertThat(received.get(topicB)).isEmpty();
    }

    @Test
    public void testTopicPolicyListenerReplayDisabledByDefault() {
        Assertions.assertThat(new ServiceConfiguration().isTopicPolicyListenerReplayEnabled()).isFalse();
    }

    @Test
    public void testChangeEventsTopicPolicyLoadDoesNotRecurse() throws Exception {
        SystemTopicBasedTopicPoliciesService service =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        final String namespaceStr = "system-topic/change-events-recursion";
        admin.namespaces().createNamespace(namespaceStr);
        final NamespaceName namespace = NamespaceName.get(namespaceStr);
        final TopicName changeEvents =
                TopicName.get("persistent", namespace, SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);

        // The __change_events system topic must not load topic-level policies: that would create a policy-cache
        // reader on __change_events while __change_events is still loading -- a recursive, deadlocking dependency.
        // isSelf() guards getTopicPoliciesAsync so it returns empty for the __change_events topic without ever
        // creating a reader. AbstractTopic#initTopicPolicy (now called for persistent AND non-persistent topics)
        // relies on this short-circuit when a __change_events topic itself is loaded.
        Assertions.assertThat(service.getTopicPoliciesAsync(changeEvents, TopicPoliciesService.GetType.LOCAL_ONLY)
                .get(30, TimeUnit.SECONDS)).isEmpty();
        Assertions.assertThat(service.getTopicPoliciesAsync(changeEvents, TopicPoliciesService.GetType.GLOBAL_ONLY)
                .get(30, TimeUnit.SECONDS)).isEmpty();
        // No policy-cache reader was created as a side effect, which is what would recurse.
        Assertions.assertThat(service.getReaderCaches()).doesNotContainKey(namespace);
    }
}
