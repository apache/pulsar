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

import static org.apache.pulsar.common.naming.SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN;
import static org.apache.pulsar.common.naming.SystemTopicNames.TRANSACTION_COORDINATOR_LOG;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.NonAppendableLedgerOffloader;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService.TopicLoadingContext;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.PulsarMetadataEventSynchronizer.State;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsServlet;
import org.apache.pulsar.broker.stats.prometheus.PrometheusRawMetricsProvider;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.compaction.Compactor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MockZooKeeper;
import org.awaitility.Awaitility;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class BrokerServiceTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setSystemTopicEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    // method for resetting state explicitly
    // this is required since setup & cleanup are using BeforeClass & AfterClass
    private void resetState() throws Exception {
        cleanup();
        setup();
    }

    @Test
    public void testShutDownWithMaxConcurrentUnload() throws Exception {
        int bundleNum = 3;
        cleanup();
        conf.setDefaultNumberOfNamespaceBundles(bundleNum);
        setup();
        final String topic = "persistent://prop/ns-abc/successTopic";
        admin.topics().createPartitionedTopic(topic, 12);
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES).topic(topic).create();

        BundlesData bundlesData = admin.namespaces().getBundles("prop/ns-abc");
        assertEquals(bundlesData.getNumBundles(), bundleNum);
        List<String> list = admin.brokers().getActiveBrokers("test");
        assertEquals(list.size(), 1);
        admin.brokers().shutDownBrokerGracefully(1, false);
        //We can only unload one bundle per second, so it takes at least 2 seconds.
        Awaitility.await().atLeast(bundleNum - 1, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(pulsar.getBrokerService().getTopics().size(), 0);
        });
        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            assertNull(pulsar.getBrokerService());
            assertEquals(pulsar.getState(), PulsarService.State.Closed);
        });
        try {
            producer.send("1".getBytes(StandardCharsets.UTF_8));
            fail("sending msg should timeout, because broker is down and there is only one broker");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.TimeoutException);
        }

        pulsar = null;

        producer.close();
        resetState();
    }

    @Test
    public void testOwnedNsCheck() throws Exception {
        final String topic = "persistent://prop/ns-abc/successTopic";
        BrokerService service = pulsar.getBrokerService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        service.getOrCreateTopic(topic).thenAccept(t -> {
            latch1.countDown();
            fail("should fail as NS is not owned");
        }).exceptionally(exception -> {
            assertTrue(exception.getCause() instanceof IOException);
            latch1.countDown();
            return null;
        });
        latch1.await();

        admin.lookups().lookupTopic(topic);

        final CountDownLatch latch2 = new CountDownLatch(1);
        service.getOrCreateTopic(topic).thenAccept(t -> {
            try {
                assertNotNull(service.getTopicReference(topic));
            } catch (Exception e) {
                fail("should not fail");
            }
            latch2.countDown();
        }).exceptionally(exception -> {
            latch2.countDown();
            fail("should not fail");
            return null;
        });
        latch2.await();
    }

    @Test
    public void testBrokerServicePersistentTopicStats() throws Exception {
        // this test might fail if there are stats from other tests
        resetState();

        final String topicName = "persistent://prop/ns-abc/successTopic";
        final String subName = "successSub";

        TopicStats stats;
        SubscriptionStats subStats;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false, false, false);
        subStats = stats.getSubscriptions().values().iterator().next();

        // subscription stats
        assertEquals(stats.getSubscriptions().keySet().size(), 1);
        assertEquals(subStats.getMsgBacklog(), 0);
        assertEquals(subStats.getConsumers().size(), 1);

        // storage stats
        assertEquals(stats.getOffloadedStorageSize(), 0);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false, false, false);
        subStats = stats.getSubscriptions().values().iterator().next();

        // publisher stats
        assertEquals(subStats.getMsgBacklog(), 10);
        assertEquals(stats.getPublishers().size(), 1);
        assertTrue(stats.getPublishers().get(0).getMsgRateIn() > 0.0);
        assertTrue(stats.getPublishers().get(0).getMsgThroughputIn() > 0.0);
        assertTrue(stats.getPublishers().get(0).getAverageMsgSize() > 0.0);
        assertNotNull(stats.getPublishers().get(0).getClientVersion());

        // aggregated publish stats
        assertEquals(stats.getMsgRateIn(), stats.getPublishers().get(0).getMsgRateIn());
        assertEquals(stats.getMsgThroughputIn(), stats.getPublishers().get(0).getMsgThroughputIn());
        double diff = stats.getAverageMsgSize() - stats.getPublishers().get(0).getAverageMsgSize();
        assertTrue(Math.abs(diff) < 0.000001);

        // consumer stats
        assertTrue(subStats.getConsumers().get(0).getMsgRateOut() > 0.0);
        assertTrue(subStats.getConsumers().get(0).getMsgThroughputOut() > 0.0);

        // aggregated consumer stats
        assertEquals(subStats.getMsgRateOut(), subStats.getConsumers().get(0).getMsgRateOut());
        assertEquals(subStats.getMsgThroughputOut(), subStats.getConsumers().get(0).getMsgThroughputOut());
        assertEquals(stats.getMsgRateOut(), subStats.getConsumers().get(0).getMsgRateOut());
        assertEquals(stats.getMsgThroughputOut(), subStats.getConsumers().get(0).getMsgThroughputOut());
        assertNotNull(subStats.getConsumers().get(0).getClientVersion());
        assertEquals(stats.getOffloadedStorageSize(), 0);

        Message<byte[]> msg;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }
        consumer.close();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false, false, false);
        subStats = stats.getSubscriptions().values().iterator().next();
        assertEquals(stats.getOffloadedStorageSize(), 0);

        assertEquals(subStats.getMsgBacklog(), 0);
    }

    @Test
    public void testConnectionController() throws Exception {
        cleanup();
        conf.setBrokerMaxConnections(3);
        conf.setBrokerMaxConnectionsPerIp(2);
        setup();
        final String topicName = "persistent://prop/ns-abc/connection" + UUID.randomUUID();
        List<PulsarClient> clients = new ArrayList<>();
        ClientBuilder clientBuilder =
                PulsarClient.builder().operationTimeout(1, TimeUnit.DAYS)
                        .connectionTimeout(1, TimeUnit.DAYS)
                        .serviceUrl(brokerUrl.toString());
        long startTime = System.currentTimeMillis();
        clients.add(createNewConnection(topicName, clientBuilder));
        clients.add(createNewConnection(topicName, clientBuilder));
        createNewConnectionAndCheckFail(topicName, clientBuilder);
        assertTrue(System.currentTimeMillis() - startTime < 20 * 1000);
        cleanClient(clients);
        clients.clear();

        cleanup();
        conf.setBrokerMaxConnections(2);
        conf.setBrokerMaxConnectionsPerIp(3);
        setup();
        startTime = System.currentTimeMillis();
        clientBuilder.serviceUrl(brokerUrl.toString());
        clients.add(createNewConnection(topicName, clientBuilder));
        clients.add(createNewConnection(topicName, clientBuilder));
        createNewConnectionAndCheckFail(topicName, clientBuilder);
        assertTrue(System.currentTimeMillis() - startTime < 20 * 1000);
        cleanClient(clients);
        clients.clear();
    }

    @Test
    public void testConnectionController2() throws Exception {
        cleanup();
        conf.setBrokerMaxConnections(0);
        conf.setBrokerMaxConnectionsPerIp(1);
        setup();
        final String topicName = "persistent://prop/ns-abc/connection" + UUID.randomUUID();
        List<PulsarClient> clients = new ArrayList<>();
        ClientBuilder clientBuilder =
                PulsarClient.builder().operationTimeout(1, TimeUnit.DAYS)
                        .connectionTimeout(1, TimeUnit.DAYS)
                        .serviceUrl(brokerUrl.toString());
        long startTime = System.currentTimeMillis();
        clients.add(createNewConnection(topicName, clientBuilder));
        createNewConnectionAndCheckFail(topicName, clientBuilder);
        assertTrue(System.currentTimeMillis() - startTime < 20 * 1000);
        cleanClient(clients);
        clients.clear();

        cleanup();
        conf.setBrokerMaxConnections(1);
        conf.setBrokerMaxConnectionsPerIp(0);
        setup();
        startTime = System.currentTimeMillis();
        clientBuilder.serviceUrl(brokerUrl.toString());
        clients.add(createNewConnection(topicName, clientBuilder));
        createNewConnectionAndCheckFail(topicName, clientBuilder);
        assertTrue(System.currentTimeMillis() - startTime < 20 * 1000);
        cleanClient(clients);
        clients.clear();

        cleanup();
        conf.setBrokerMaxConnections(1);
        conf.setBrokerMaxConnectionsPerIp(1);
        setup();
        startTime = System.currentTimeMillis();
        clientBuilder.serviceUrl(brokerUrl.toString());
        clients.add(createNewConnection(topicName, clientBuilder));
        createNewConnectionAndCheckFail(topicName, clientBuilder);
        assertTrue(System.currentTimeMillis() - startTime < 20 * 1000);
        cleanClient(clients);
        clients.clear();

        cleanup();
        conf.setBrokerMaxConnections(0);
        conf.setBrokerMaxConnectionsPerIp(0);
        setup();
        clientBuilder.serviceUrl(brokerUrl.toString());
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            clients.add(createNewConnection(topicName, clientBuilder));
        }
        assertTrue(System.currentTimeMillis() - startTime < 20 * 1000);
        cleanClient(clients);
        clients.clear();

    }

    private void createNewConnectionAndCheckFail(String topicName, ClientBuilder builder) throws Exception {
        try {
            createNewConnection(topicName, builder);
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Reached the maximum number of connections"));
        }
    }

    private PulsarClient createNewConnection(String topicName, ClientBuilder clientBuilder) throws PulsarClientException {
        PulsarClient client1 = clientBuilder.build();
        client1.newProducer().topic(topicName).create().close();
        return client1;
    }

    private void cleanClient(List<PulsarClient> clients) throws Exception {
        for (PulsarClient client : clients) {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    public void testStatsOfStorageSizeWithSubscription() throws Exception {
        final String topicName = "persistent://prop/ns-abc/no-subscription";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        assertNotNull(topicRef);
        assertEquals(topicRef.getStats(false, false, false).storageSize, 0);

        for (int i = 0; i < 10; i++) {
            producer.send(new byte[10]);
        }

        assertTrue(topicRef.getStats(false, false, false).storageSize > 0);
    }

    @Test
    public void testBrokerServicePersistentRedeliverTopicStats() throws Exception {
        // this test might fail if there are stats from other tests
        resetState();

        final String topicName = "persistent://prop/ns-abc/successSharedTopic";
        final String subName = "successSharedSub";

        TopicStats stats;
        SubscriptionStats subStats;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false, false, false);
        subStats = stats.getSubscriptions().values().iterator().next();

        // subscription stats
        assertEquals(stats.getSubscriptions().keySet().size(), 1);
        assertEquals(subStats.getMsgBacklog(), 0);
        assertEquals(subStats.getConsumers().size(), 1);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false, false, false);
        subStats = stats.getSubscriptions().values().iterator().next();

        // publisher stats
        assertEquals(subStats.getMsgBacklog(), 10);
        assertEquals(stats.getPublishers().size(), 1);
        assertTrue(stats.getPublishers().get(0).getMsgRateIn() > 0.0);
        assertTrue(stats.getPublishers().get(0).getMsgThroughputIn() > 0.0);
        assertTrue(stats.getPublishers().get(0).getAverageMsgSize() > 0.0);

        // aggregated publish stats
        assertEquals(stats.getMsgRateIn(), stats.getPublishers().get(0).getMsgRateIn());
        assertEquals(stats.getMsgThroughputIn(), stats.getPublishers().get(0).getMsgThroughputIn());
        double diff = stats.getAverageMsgSize() - stats.getPublishers().get(0).getAverageMsgSize();
        assertTrue(Math.abs(diff) < 0.000001);

        // consumer stats
        assertTrue(subStats.getConsumers().get(0).getMsgRateOut() > 0.0);
        assertTrue(subStats.getConsumers().get(0).getMsgThroughputOut() > 0.0);
        assertEquals(subStats.getMsgRateRedeliver(), 0.0);
        assertEquals(subStats.getConsumers().get(0).getUnackedMessages(), 10);

        // aggregated consumer stats
        assertEquals(subStats.getMsgRateOut(), subStats.getConsumers().get(0).getMsgRateOut());
        assertEquals(subStats.getMsgThroughputOut(), subStats.getConsumers().get(0).getMsgThroughputOut());
        assertEquals(subStats.getMsgRateRedeliver(), subStats.getConsumers().get(0).getMsgRateRedeliver());
        assertEquals(stats.getMsgRateOut(), subStats.getConsumers().get(0).getMsgRateOut());
        assertEquals(stats.getMsgThroughputOut(), subStats.getConsumers().get(0).getMsgThroughputOut());
        assertEquals(subStats.getMsgRateRedeliver(), subStats.getConsumers().get(0).getMsgRateRedeliver());
        assertEquals(subStats.getUnackedMessages(), subStats.getConsumers().get(0).getUnackedMessages());

        consumer.redeliverUnacknowledgedMessages();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false, false, false);
        subStats = stats.getSubscriptions().values().iterator().next();
        assertTrue(subStats.getMsgRateRedeliver() > 0.0);
        assertEquals(subStats.getMsgRateRedeliver(), subStats.getConsumers().get(0).getMsgRateRedeliver());

        Message<byte[]> msg;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }
        consumer.close();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false, false, false);
        subStats = stats.getSubscriptions().values().iterator().next();

        assertEquals(subStats.getMsgBacklog(), 0);
    }

    @Test
    public void testBrokerStatsMetrics() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic";
        final String subName = "newSub";
        BrokerStats brokerStatsClient = admin.brokerStats();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }
        consumer.close();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        String json = brokerStatsClient.getMetrics();
        JsonArray metrics = new Gson().fromJson(json, JsonArray.class);

        // these metrics seem to be arriving in different order at different times...
        // is the order really relevant here?
        boolean namespaceDimensionFound = false;
        boolean topicLoadTimesDimensionFound = false;
        for (int i = 0; i < metrics.size(); i++) {
            try {
                String data = metrics.get(i).getAsJsonObject().get("dimensions").toString();
                if (!namespaceDimensionFound && data.contains("prop/ns-abc")) {
                    namespaceDimensionFound = true;
                }
                if (!topicLoadTimesDimensionFound && data.contains("prop/ns-abc")) {
                    topicLoadTimesDimensionFound = true;
                }
            } catch (Exception e) {
                /* it's possible there's no dimensions */ }
        }

        assertTrue(namespaceDimensionFound && topicLoadTimesDimensionFound);

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
    }

    @Test
    public void testBrokerServiceNamespaceStats() throws Exception {
        // this test fails if there is state from other tests
        resetState();

        final int numBundles = 4;
        final String ns1 = "prop/stats1";
        final String ns2 = "prop/stats2";

        List<String> nsList = List.of(ns1, ns2);
        List<Producer<byte[]>> producerList = new ArrayList<>();

        BrokerStats brokerStatsClient = admin.brokerStats();

        for (String ns : nsList) {
            admin.namespaces().createNamespace(ns, numBundles);
            admin.namespaces().setNamespaceReplicationClusters(ns, Sets.newHashSet("test"));
            String topic1 = String.format("persistent://%s/topic1", ns);
            producerList.add(pulsarClient.newProducer().topic(topic1).create());
            String topic2 = String.format("persistent://%s/topic2", ns);
            producerList.add(pulsarClient.newProducer().topic(topic2).create());
        }

        rolloverPerIntervalStats();
        String json = brokerStatsClient.getTopics();
        JsonObject topicStats = new Gson().fromJson(json, JsonObject.class);
        assertEquals(topicStats.size(), 2, topicStats.toString());

        for (String ns : nsList) {
            JsonObject nsObject = topicStats.getAsJsonObject(ns);
            List<String> topicList = admin.namespaces().getTopics(ns);
            for (String topic : topicList) {
                NamespaceBundle bundle = (NamespaceBundle) pulsar.getNamespaceService().getBundle(TopicName.get(topic));
                JsonObject bundleObject = nsObject.getAsJsonObject(bundle.getBundleRange());
                JsonObject topicObject = bundleObject.getAsJsonObject("persistent");
                AtomicBoolean topicPresent = new AtomicBoolean();
                topicObject.entrySet().iterator().forEachRemaining(persistentTopic -> {
                    if (persistentTopic.getKey().equals(topic)) {
                        topicPresent.set(true);
                    }
                });
                assertTrue(topicPresent.get());
            }
        }

        for (Producer<?> producer : producerList) {
            producer.close();
        }

        for (String ns : nsList) {
            List<String> topics = admin.namespaces().getTopics(ns);
            for (String dest : topics) {
                admin.topics().delete(dest);
            }
            admin.namespaces().deleteNamespace(ns);
        }
    }

    @Test
    public void testTlsDisabled() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic";
        final String subName = "newSub";
        PulsarClient pulsarClient = null;

        conf.setAuthenticationEnabled(false);
        restartBroker();

        // Case 1: Access without TLS
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(brokerUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();
        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }

        // Case 2: Access with TLS
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(brokerUrlTls.toString()).enableTls(true)
                    .statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();

            fail("TLS connection should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("ConnectException"));
        } finally {
            pulsarClient.close();
        }
    }

    @Test
    public void testTlsEnabled() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic";
        final String subName = "newSub";

        conf.setAuthenticationEnabled(false);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setNumExecutorThreadPoolSize(5);
        restartBroker();

        // Case 1: Access without TLS
        PulsarClient pulsarClient = null;
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(brokerUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();
        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }

        // Case 2: Access with TLS (Allow insecure TLS connection)
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(brokerUrlTls.toString()).enableTls(true)
                    .allowTlsInsecureConnection(true).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();

        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }

        // Case 3: Access with TLS (Disallow insecure TLS connection)
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(brokerUrlTls.toString()).enableTls(true)
                    .allowTlsInsecureConnection(false).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();

            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("General OpenSslEngine problem"));
        } finally {
            pulsarClient.close();
        }

        // Case 4: Access with TLS (Use trusted certificates)
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(brokerUrlTls.toString()).enableTls(true)
                    .allowTlsInsecureConnection(false).tlsTrustCertsFilePath(BROKER_CERT_FILE_PATH)
                    .statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();
        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }
    }

    @Test
    public void testTlsEnabledWithoutNonTlsServicePorts() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic";
        final String subName = "newSub";

        conf.setAuthenticationEnabled(false);
        conf.setBrokerServicePort(Optional.empty());
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePort(Optional.empty());
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setNumExecutorThreadPoolSize(5);
        restartBroker();

        // Access with TLS (Allow insecure TLS connection)
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(brokerUrlTls.toString()).enableTls(true)
                    .allowTlsInsecureConnection(true).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();

        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }
        resetState();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testTlsAuthAllowInsecure() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic";
        final String subName = "newSub";
        Authentication auth;

        Set<String> providers = new HashSet<>();
        providers.add("org.apache.pulsar.broker.authentication.AuthenticationProviderTls");

        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(providers);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(true);
        conf.setNumExecutorThreadPoolSize(5);
        restartBroker();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", getTlsFileForClient("admin.cert"));
        authParams.put("tlsKeyFile", getTlsFileForClient("admin.key-pk8"));

        PulsarClient pulsarClient = null;

        // Case 1: Access without client certificate
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(brokerUrlTls.toString()).enableTls(true)
                    .allowTlsInsecureConnection(true).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();

            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unauthorized"));
        } finally {
            pulsarClient.close();
        }

        // Case 2: Access with client certificate
        try {
            auth = new AuthenticationTls();
            auth.configure(authParams);

            pulsarClient = PulsarClient.builder().authentication(auth).serviceUrl(brokerUrlTls.toString())
                    .enableTls(true).allowTlsInsecureConnection(true).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();

        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testTlsAuthDisallowInsecure() throws Exception {
        final String topicName = "persistent://prop/my-ns/newTopic";
        final String subName = "newSub";
        Authentication auth;

        Set<String> providers = new HashSet<>();
        providers.add("org.apache.pulsar.broker.authentication.AuthenticationProviderTls");

        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(providers);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(false);
        conf.setNumExecutorThreadPoolSize(5);
        restartBroker();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", getTlsFileForClient("admin.cert"));
        authParams.put("tlsKeyFile", getTlsFileForClient("admin.key-pk8"));

        PulsarClient pulsarClient = null;

        // Case 1: Access without client certificate
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(brokerUrlTls.toString()).enableTls(true)
                    .allowTlsInsecureConnection(true).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();

            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unauthorized"));
        } finally {
            pulsarClient.close();
        }

        // Case 2: Access with client certificate
        try {
            auth = new AuthenticationTls();
            auth.configure(authParams);
            pulsarClient = PulsarClient.builder().authentication(auth).serviceUrl(brokerUrlTls.toString())
                    .enableTls(true).allowTlsInsecureConnection(true).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unauthorized"));
        } finally {
            pulsarClient.close();
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testTlsAuthUseTrustCert() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic";
        final String subName = "newSub";
        Authentication auth;

        Set<String> providers = new HashSet<>();
        providers.add("org.apache.pulsar.broker.authentication.AuthenticationProviderTls");

        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(providers);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(false);
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        conf.setNumExecutorThreadPoolSize(5);
        restartBroker();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", getTlsFileForClient("admin.cert"));
        authParams.put("tlsKeyFile", getTlsFileForClient("admin.key-pk8"));

        PulsarClient pulsarClient = null;

        // Case 1: Access without client certificate
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(brokerUrlTls.toString()).enableTls(true)
                    .allowTlsInsecureConnection(true).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unauthorized"));
        } finally {
            pulsarClient.close();
        }

        // Case 2: Access with client certificate
        try {
            auth = new AuthenticationTls();
            auth.configure(authParams);
            pulsarClient = PulsarClient.builder().authentication(auth).serviceUrl(brokerUrlTls.toString())
                    .enableTls(true).allowTlsInsecureConnection(true).statsInterval(0, TimeUnit.SECONDS)
                    .operationTimeout(1000, TimeUnit.MILLISECONDS).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();
        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }
    }

    /**
     * Verifies: client side throttling.
     *
     * @throws Exception
     */
    @Test
    public void testLookupThrottlingForClientByClient() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic";

        PulsarServiceNameResolver resolver = new PulsarServiceNameResolver();
        resolver.updateServiceUrl(pulsar.getBrokerServiceUrl());
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setConcurrentLookupRequest(1);
        conf.setMaxLookupRequest(2);

        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(20, false,
                new DefaultThreadFactory("test-pool", Thread.currentThread().isDaemon()));
        long reqId = 0xdeadbeef;
        // Using an AtomicReference in order to reset a new CountDownLatch
        AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();
        latchRef.set(new CountDownLatch(1));
        try (ConnectionPool pool = new ConnectionPool(conf, eventLoop, () -> new ClientCnx(conf, eventLoop) {
            @Override
            protected void handleLookupResponse(CommandLookupTopicResponse lookupResult) {
                try {
                    latchRef.get().await();
                } catch (InterruptedException e) {
                    // ignore
                }
                super.handleLookupResponse(lookupResult);
            }

            @Override
            protected void handlePartitionResponse(CommandPartitionedTopicMetadataResponse lookupResult) {
                try {
                    latchRef.get().await();
                } catch (InterruptedException e) {
                    // ignore
                }
                super.handlePartitionResponse(lookupResult);
            }
        })) {
            // for PMR
            // 2 lookup will succeed
            long reqId1 = reqId++;
            ByteBuf request1 = Commands.newPartitionMetadataRequest(topicName, reqId1, true);
            CompletableFuture<?> f1 = pool.getConnection(resolver.resolveHost())
                .thenCompose(clientCnx -> clientCnx.newLookup(request1, reqId1));

            long reqId2 = reqId++;
            ByteBuf request2 = Commands.newPartitionMetadataRequest(topicName, reqId2, true);
            CompletableFuture<?> f2 = pool.getConnection(resolver.resolveHost())
                .thenCompose(clientCnx -> {
                    CompletableFuture<?> future = clientCnx.newLookup(request2, reqId2);
                    // pending other responses in `ClientCnx` until now
                    latchRef.get().countDown();
                    return future;
                });

            f1.get();
            f2.get();

            // 3 lookup will fail
            latchRef.set(new CountDownLatch(1));
            long reqId3 = reqId++;
            ByteBuf request3 = Commands.newPartitionMetadataRequest(topicName, reqId3, true);
            f1 = pool.getConnection(resolver.resolveHost())
                .thenCompose(clientCnx -> clientCnx.newLookup(request3, reqId3));

            long reqId4 = reqId++;
            ByteBuf request4 = Commands.newPartitionMetadataRequest(topicName, reqId4, true);
            f2 = pool.getConnection(resolver.resolveHost())
                .thenCompose(clientCnx -> clientCnx.newLookup(request4, reqId4));

            long reqId5 = reqId++;
            ByteBuf request5 = Commands.newPartitionMetadataRequest(topicName, reqId5, true);
            CompletableFuture<?> f3 = pool.getConnection(resolver.resolveHost())
                .thenCompose(clientCnx -> {
                    CompletableFuture<?> future = clientCnx.newLookup(request5, reqId5);
                    // pending other responses in `ClientCnx` until now
                    latchRef.get().countDown();
                    return future;
                    });

            f1.get();
            f2.get();
            try {
                f3.get();
                fail("At least one should fail");
            } catch (ExecutionException e) {
                Throwable rootCause = e;
                while (rootCause instanceof ExecutionException) {
                    rootCause = rootCause.getCause();
                }
                if (!(rootCause instanceof
                      org.apache.pulsar.client.api.PulsarClientException.TooManyRequestsException)) {
                    throw e;
                }
            }

            // for Lookup
            // 2 lookup will succeed
            latchRef.set(new CountDownLatch(1));
            long reqId6 = reqId++;
            ByteBuf request6 = Commands.newLookup(topicName, true, reqId6);
            f1 = pool.getConnection(resolver.resolveHost())
                .thenCompose(clientCnx -> clientCnx.newLookup(request6, reqId6));

            long reqId7 = reqId++;
            ByteBuf request7 = Commands.newLookup(topicName, true, reqId7);
            f2 = pool.getConnection(resolver.resolveHost())
                .thenCompose(clientCnx -> {
                    CompletableFuture<?> future = clientCnx.newLookup(request7, reqId7);
                    // pending other responses in `ClientCnx` until now
                    latchRef.get().countDown();
                    return future;
                });

            f1.get();
            f2.get();

            // 3 lookup will fail
            latchRef.set(new CountDownLatch(1));
            long reqId8 = reqId++;
            ByteBuf request8 = Commands.newLookup(topicName, true, reqId8);
            f1 = pool.getConnection(resolver.resolveHost())
                .thenCompose(clientCnx -> clientCnx.newLookup(request8, reqId8));

            long reqId9 = reqId++;
            ByteBuf request9 = Commands.newLookup(topicName, true, reqId9);
            f2 = pool.getConnection(resolver.resolveHost())
                .thenCompose(clientCnx -> clientCnx.newLookup(request9, reqId9));

            long reqId10 = reqId++;
            ByteBuf request10 = Commands.newLookup(topicName, true, reqId10);
            f3 = pool.getConnection(resolver.resolveHost())
                .thenCompose(clientCnx -> {
                    CompletableFuture<?> future = clientCnx.newLookup(request10, reqId10);
                    // pending other responses in `ClientCnx` until now
                    latchRef.get().countDown();
                    return future;
                });

            f1.get();
            f2.get();
            try {
                f3.get();
                fail("At least one should fail");
            } catch (ExecutionException e) {
                Throwable rootCause = e;
                while (rootCause instanceof ExecutionException) {
                    rootCause = rootCause.getCause();
                }
                if (!(rootCause instanceof
                      org.apache.pulsar.client.api.PulsarClientException.TooManyRequestsException)) {
                    throw e;
                }
            }
        }
    }

    @Test
    public void testTopicLoadingOnDisableNamespaceBundle() throws Exception {
        final String namespace = "prop/disableBundle";
        try {
            admin.namespaces().createNamespace(namespace);
        } catch (PulsarAdminException.ConflictException e) {
            // Ok.. (if test fails intermittently and namespace is already created)
        }
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("test"));

        // own namespace bundle
        final String topicName = "persistent://" + namespace + "/my-topic";
        TopicName topic = TopicName.get(topicName);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();

        // disable namespace-bundle
        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(topic);
        pulsar.getNamespaceService().getOwnershipCache().updateBundleState(bundle, false).join();

        // try to create topic which should fail as bundle is disable
        TopicLoadingContext topicLoadingContext =
                TopicLoadingContext.builder().topic(topicName).createIfMissing(true).properties(null).build();
        CompletableFuture<Optional<Topic>> futureResult = pulsar.getBrokerService()
                .loadOrCreatePersistentTopic(topicLoadingContext);

        try {
            futureResult.get();
            fail("Topic creation should fail due to disable bundle");
        } catch (Exception e) {
            if (!(e.getCause() instanceof BrokerServiceException.ServiceUnitNotReadyException)) {
                fail("Topic creation should fail with ServiceUnitNotReadyException");
            }

        }
    }

    @Test
    public void testCheckInactiveSubscriptionsShouldNotDeleteCompactionCursor() throws Exception {
        String namespace = "prop/test";

        // set up broker set compaction threshold.
        cleanup();
        conf.setBrokerServiceCompactionThresholdInBytes(8);
        setup();

        try {
            admin.namespaces().createNamespace(namespace);
        } catch (PulsarAdminException.ConflictException e) {
            // Ok.. (if test fails intermittently and namespace is already created)
        }

        // set enable subscription expiration.
        admin.namespaces().setSubscriptionExpirationTime(namespace, 1);

        String compactionInactiveTestTopic = "persistent://prop/test/testCompactionCursorShouldNotDelete";

        admin.topics().createNonPartitionedTopic(compactionInactiveTestTopic);

        CompletableFuture<Optional<Topic>> topicCf =
                pulsar.getBrokerService().getTopic(compactionInactiveTestTopic, true);

        Optional<Topic> topicOptional = topicCf.get();
        assertTrue(topicOptional.isPresent());

        PersistentTopic topic = (PersistentTopic) topicOptional.get();

        PersistentSubscription sub = (PersistentSubscription) topic.getSubscription(Compactor.COMPACTION_SUBSCRIPTION);
        assertNotNull(sub);

        topic.checkCompaction();

        Field currentCompaction = PersistentTopic.class.getDeclaredField("currentCompaction");
        currentCompaction.setAccessible(true);
        CompletableFuture<Long> compactionFuture = (CompletableFuture<Long>)currentCompaction.get(topic);

        compactionFuture.get();

        ManagedCursorImpl cursor = (ManagedCursorImpl) sub.getCursor();

        // make cursor last active time to very small to check if it will be deleted
        Field cursorLastActiveField = ManagedCursorImpl.class.getDeclaredField("lastActive");
        cursorLastActiveField.setAccessible(true);
        cursorLastActiveField.set(cursor, 0);

        // replace origin object. so we can check if subscription is deleted.
        PersistentSubscription spySubscription = Mockito.spy(sub);
        topic.getSubscriptions().put(Compactor.COMPACTION_SUBSCRIPTION, spySubscription);

        // trigger inactive check.
        topic.checkInactiveSubscriptions();

        // Compaction subscription should not call delete method.
        Mockito.verify(spySubscription, Mockito.never()).delete();

        // check if the subscription exist.
        assertNotNull(topic.getSubscription(Compactor.COMPACTION_SUBSCRIPTION));

    }

    /**
     * Verifies brokerService should not have deadlock and successfully remove topic from topicMap on topic-failure and
     * it should not introduce deadlock while performing it.
     *
     */
    @Test(timeOut = 3000)
    public void testTopicFailureShouldNotHaveDeadLock() {
        final String namespace = "prop/ns-abc";
        final String deadLockTestTopic = "persistent://" + namespace + "/deadLockTestTopic";

        // let this broker own this namespace bundle by creating a topic
        try {
            final String successfulTopic = "persistent://" + namespace + "/ownBundleTopic";
            Producer<byte[]> producer = pulsarClient.newProducer().topic(successfulTopic).create();
            producer.close();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        BrokerService service = spy(pulsar.getBrokerService());
        // create topic will fail to get managedLedgerConfig
        CompletableFuture<ManagedLedgerConfig> failedManagedLedgerConfig = new CompletableFuture<>();
        failedManagedLedgerConfig.completeExceptionally(new NullPointerException("failed to peristent policy"));
        doReturn(failedManagedLedgerConfig).when(service).getManagedLedgerConfig(any());

        CompletableFuture<Void> topicCreation = new CompletableFuture<Void>();

        // create topic async and wait on the future completion
        executor.submit(() -> {
            service.getOrCreateTopic(deadLockTestTopic).thenAccept(topic -> topicCreation.complete(null)).exceptionally(e -> {
                topicCreation.completeExceptionally(e.getCause());
                return null;
            });
        });

        // future-result should be completed with exception
        try {
            topicCreation.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException | InterruptedException e) {
            fail("there is a dead-lock and it should have been prevented");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NullPointerException);
        }
    }

    @Test
    public void testLedgerOpenFailureShouldNotHaveDeadLock() throws Exception {
        final String namespace = "prop/ns-abc";
        final String deadLockTestTopic = "persistent://" + namespace + "/deadLockTestTopic";

        // let this broker own this namespace bundle by creating a topic
        try {
            final String successfulTopic = "persistent://" + namespace + "/ownBundleTopic";
            Producer<byte[]> producer = pulsarClient.newProducer().topic(successfulTopic).create();
            producer.close();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        BrokerService service = spy(pulsar.getBrokerService());
        // create topic will fail to get managedLedgerConfig
        CompletableFuture<ManagedLedgerConfig> failedManagedLedgerConfig = new CompletableFuture<>();
        failedManagedLedgerConfig.complete(new ManagedLedgerConfig());
        doReturn(failedManagedLedgerConfig).when(service).getManagedLedgerConfig(any());

        CompletableFuture<Void> topicCreation = new CompletableFuture<Void>();
        // fail managed-ledger future
        Field ledgerField = ManagedLedgerFactoryImpl.class.getDeclaredField("ledgers");
        ledgerField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = (ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>>) ledgerField
                .get(pulsar.getManagedLedgerFactory());
        CompletableFuture<ManagedLedgerImpl> future = new CompletableFuture<>();
        future.completeExceptionally(new ManagedLedgerException("ledger opening failed"));
        ledgers.put(namespace + "/persistent/deadLockTestTopic", future);

        // create topic async and wait on the future completion
        executor.submit(() -> {
            service.getOrCreateTopic(deadLockTestTopic).thenAccept(topic -> topicCreation.complete(null)).exceptionally(e -> {
                topicCreation.completeExceptionally(e.getCause());
                return null;
            });
        });

        // future-result should be completed with exception
        try {
            topicCreation.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException | InterruptedException e) {
            fail("there is a dead-lock and it should have been prevented");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), PersistenceException.class);
        } finally {
            ledgers.clear();
        }
    }

    /**
     * It verifies that policiesCache() copies global-policy data into local-policy data and returns combined result
     *
     * @throws Exception
     */
    @Test
    public void testCreateNamespacePolicy() throws Exception {
        final String namespace = "prop/testPolicy";
        final int totalBundle = 3;
        System.err.println("----------------");
        admin.namespaces().createNamespace(namespace, BundlesData.builder().numBundles(totalBundle).build());
        admin.topics().createNonPartitionedTopic(namespace + "/test");

        Optional<LocalPolicies> policy = pulsar.getPulsarResources().getLocalPolicies().getLocalPolicies(
                NamespaceName.get(namespace));
        assertTrue(policy.isPresent());
        assertEquals(policy.get().bundles.getNumBundles(), totalBundle);
    }

    /**
     * It verifies that unloading bundle gracefully closes managed-ledger before removing ownership to avoid bad-zk
     * version.
     *
     * @throws Exception
     */
    @Test
    public void testStuckTopicUnloading() throws Exception {
        final String namespace = "prop/ns-abc";
        final String topicName = "persistent://" + namespace + "/unoadTopic";
        final String topicMlName = namespace + "/persistent/unoadTopic";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscribe();
        consumer.close();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName).sendTimeout(5,
                TimeUnit.SECONDS);

        Producer<byte[]> producer = producerBuilder.create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        ManagedLedgerFactoryImpl mlFactory = (ManagedLedgerFactoryImpl) pulsar.getManagedLedgerClientFactory()
                .getManagedLedgerFactory();
        Field ledgersField = ManagedLedgerFactoryImpl.class.getDeclaredField("ledgers");
        ledgersField.setAccessible(true);
        ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = (ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>>) ledgersField
                .get(mlFactory);
        assertNotNull(ledgers.get(topicMlName));

        org.apache.pulsar.broker.service.Producer prod = (org.apache.pulsar.broker.service.Producer) spy(topic.producers.values().toArray()[0]);
        topic.producers.clear();
        topic.producers.put(prod.getProducerName(), prod);
        CompletableFuture<Void> waitFuture = new CompletableFuture<Void>();
        doReturn(waitFuture).when(prod).disconnect();
        Set<NamespaceBundle> bundles = pulsar.getNamespaceService().getOwnedServiceUnits();
        for (NamespaceBundle bundle : bundles) {
            String ns = bundle.getNamespaceObject().toString();
            System.out.println();
            if (namespace.equals(ns)) {
                pulsar.getNamespaceService().unloadNamespaceBundle(bundle, 2, TimeUnit.SECONDS);
            }
        }
        assertNull(ledgers.get(topicMlName));
    }

    @Test
    public void testMetricsProvider() throws IOException {
        PrometheusRawMetricsProvider rawMetricsProvider = stream -> stream.write("test_metrics{label1=\"xyz\"} 10 \n");
        getPulsar().addPrometheusRawMetricsProvider(rawMetricsProvider);
        HttpClient httpClient = HttpClientBuilder.create().build();
        final String metricsEndPoint = getPulsar().getWebServiceAddress() + "/metrics";
        HttpResponse response = httpClient.execute(new HttpGet(metricsEndPoint));
        assertEquals(response.getEntity().getContentType().getValue(),
                PrometheusMetricsServlet.PROMETHEUS_CONTENT_TYPE_004);
        InputStream inputStream = response.getEntity().getContent();
        InputStreamReader isReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(isReader);
        StringBuffer sb = new StringBuffer();
        String str;
        while((str = reader.readLine()) != null){
            sb.append(str);
        }
        Assert.assertTrue(sb.toString().contains("test_metrics"));
    }

    @Test
    public void testPublishRateLimiterMonitor() {
        BrokerService.PublishRateLimiterMonitor monitor = new BrokerService.PublishRateLimiterMonitor("test");
        AtomicInteger checkCnt = new AtomicInteger(0);
        AtomicInteger refreshCnt = new AtomicInteger(0);
        monitor.startOrUpdate(100, checkCnt::incrementAndGet, refreshCnt::incrementAndGet);
        Assert.assertEquals(monitor.getTickTimeMs(), 100);
        Awaitility.await().until(() -> checkCnt.get() > 0);
        Awaitility.await().until(() -> refreshCnt.get() > 0);

        monitor.startOrUpdate(500, checkCnt::incrementAndGet, refreshCnt::incrementAndGet);
        Assert.assertEquals(monitor.getTickTimeMs(), 500);
        checkCnt.set(0);
        refreshCnt.set(0);
        Awaitility.await().until(() -> checkCnt.get() > 0);
        Awaitility.await().until(() -> refreshCnt.get() > 0);

        monitor.stop();
        Assert.assertEquals(monitor.getTickTimeMs(), 0);
    }

    @Test
    public void testDynamicBrokerPublisherThrottlingTickTimeMillis() throws Exception {
        cleanup();
        conf.setBrokerPublisherThrottlingMaxMessageRate(1000);
        conf.setBrokerPublisherThrottlingTickTimeMillis(100);
        setup();

        int prevTickMills = 100;
        BrokerService.PublishRateLimiterMonitor monitor = pulsar.getBrokerService().brokerPublishRateLimiterMonitor;
        Awaitility.await().until(() -> monitor.getTickTimeMs() == prevTickMills);

        int newTickMills = prevTickMills * 2;
        admin.brokers().updateDynamicConfiguration("brokerPublisherThrottlingTickTimeMillis",
                String.valueOf(newTickMills));
        Awaitility.await().until(() -> monitor.getTickTimeMs() == newTickMills);

        admin.brokers().updateDynamicConfiguration("brokerPublisherThrottlingTickTimeMillis",
                String.valueOf(0));
        Awaitility.await().until(() -> monitor.getTickTimeMs() == 0);

        admin.brokers().updateDynamicConfiguration("brokerPublisherThrottlingTickTimeMillis",
                String.valueOf(prevTickMills));
        Awaitility.await().until(() -> monitor.getTickTimeMs() == prevTickMills);
    }

    @Test
    public void testDynamicTopicPublisherThrottlingTickTimeMillis() throws Exception {
        cleanup();
        conf.setPreciseTopicPublishRateLimiterEnable(false);
        conf.setMaxPublishRatePerTopicInMessages(1000);
        conf.setTopicPublisherThrottlingTickTimeMillis(100);
        setup();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://prop/ns-abc/test-topic").create();

        int prevTickMills = 100;
        BrokerService.PublishRateLimiterMonitor monitor = pulsar.getBrokerService().topicPublishRateLimiterMonitor;
        Awaitility.await().until(() -> monitor.getTickTimeMs() == prevTickMills);

        int newTickMills = prevTickMills * 2;
        admin.brokers().updateDynamicConfiguration("topicPublisherThrottlingTickTimeMillis",
                String.valueOf(newTickMills));
        Awaitility.await().until(() -> monitor.getTickTimeMs() == newTickMills);

        admin.brokers().updateDynamicConfiguration("topicPublisherThrottlingTickTimeMillis",
                String.valueOf(0));
        Awaitility.await().until(() -> monitor.getTickTimeMs() == 0);

        admin.brokers().updateDynamicConfiguration("topicPublisherThrottlingTickTimeMillis",
                String.valueOf(prevTickMills));
        Awaitility.await().until(() -> monitor.getTickTimeMs() == prevTickMills);
    }

    @Test
    public void shouldNotPreventCreatingTopicWhenNonexistingTopicIsCached() throws Exception {
        // run multiple iterations to increase the chance of reproducing a race condition in the topic cache
        for (int i = 0; i < 100; i++) {
            final String topicName = "persistent://prop/ns-abc/topic-caching-test-topic" + i;
            CountDownLatch latch = new CountDownLatch(1);
            Thread getStatsThread = new Thread(() -> {
                try {
                    latch.countDown();
                    // create race condition with a short delay
                    // the bug might not reproduce in all environments, this works at least on i7-10750H CPU
                    Thread.sleep(1);
                    admin.topics().getStats(topicName);
                    fail("The topic should not exist yet.");
                } catch (PulsarAdminException.NotFoundException e) {
                    // expected exception
                } catch (PulsarAdminException | InterruptedException e) {
                    log.error("Exception in {}", Thread.currentThread().getName(), e);
                }
            }, "getStatsThread#" + i);
            getStatsThread.start();
            latch.await();
            @Cleanup
            Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
            assertNotNull(producer);
            getStatsThread.join();
        }
    }

    @Test
    public void testIsSystemTopic() {
        BrokerService brokerService = pulsar.getBrokerService();
        assertFalse(brokerService.isSystemTopic(TopicName.get("test")));
        assertFalse(brokerService.isSystemTopic(TopicName.get("public/default/test")));
        assertFalse(brokerService.isSystemTopic(TopicName.get("healthcheck")));
        assertFalse(brokerService.isSystemTopic(TopicName.get("public/default/healthcheck")));
        assertFalse(brokerService.isSystemTopic(TopicName.get("persistent://public/default/test")));
        assertFalse(brokerService.isSystemTopic(TopicName.get("non-persistent://public/default/test")));

        assertTrue(brokerService.isSystemTopic(TopicName.get("__change_events")));
        assertTrue(brokerService.isSystemTopic(TopicName.get("__change_events-partition-0")));
        assertTrue(brokerService.isSystemTopic(TopicName.get("__change_events-partition-1")));
        assertTrue(brokerService.isSystemTopic(TopicName.get("__transaction_buffer_snapshot")));
        assertTrue(brokerService.isSystemTopic(TopicName.get("__transaction_buffer_snapshot-partition-0")));
        assertTrue(brokerService.isSystemTopic(TopicName.get("__transaction_buffer_snapshot-partition-1")));
        assertTrue(brokerService.isSystemTopic(TopicName
                .get("topicxxx-partition-0-multiTopicsReader-f433329d68__transaction_pending_ack")));
        assertTrue(brokerService.isSystemTopic(
                TopicName.get("topicxxx-multiTopicsReader-f433329d68__transaction_pending_ack")));

        assertTrue(brokerService.isSystemTopic(TRANSACTION_COORDINATOR_ASSIGN));
        assertTrue(brokerService.isSystemTopic(TRANSACTION_COORDINATOR_LOG));
        NamespaceName heartbeatNamespaceV1 = NamespaceService
                .getHeartbeatNamespace(pulsar.getBrokerId(), pulsar.getConfig());
        NamespaceName heartbeatNamespaceV2 = NamespaceService
                .getHeartbeatNamespaceV2(pulsar.getBrokerId(), pulsar.getConfig());
        assertTrue(brokerService.isSystemTopic("persistent://" + heartbeatNamespaceV1.toString() + "/healthcheck"));
        assertTrue(brokerService.isSystemTopic(heartbeatNamespaceV2.toString() + "/healthcheck"));
    }

    @Test
    public void testGetTopic() throws Exception {
        final String ns = "prop/ns-test";
        admin.namespaces().createNamespace(ns, 2);
        final String topicName = ns + "/topic-1";
        admin.topics().createNonPartitionedTopic(String.format("persistent://%s", topicName));
        Producer<String> producer1 = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        producer1.close();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopic(topicName.toString(), false).get().get();
        persistentTopic.close().join();
        List<String> topics = new ArrayList<>(pulsar.getBrokerService().getTopics().keys());
        topics.removeIf(item -> item.contains(SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME));
        Assert.assertEquals(topics.size(), 0);
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub-1")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
    }

    @Test
    public void testDynamicConfigurationsForceDeleteNamespaceAllowed() throws Exception {
        cleanup();
        conf.setForceDeleteNamespaceAllowed(false);
        setup();
        admin.brokers()
                .updateDynamicConfiguration("forceDeleteNamespaceAllowed", "true");
        Awaitility.await().untilAsserted(()->{
            assertTrue(conf.isForceDeleteNamespaceAllowed());
        });
    }

    @Test
    public void testDynamicConfigurationsForceDeleteTenantAllowed() throws Exception {
        cleanup();
        conf.setForceDeleteTenantAllowed(false);
        setup();
        admin.brokers()
                .updateDynamicConfiguration("forceDeleteTenantAllowed", "true");
        Awaitility.await().untilAsserted(()->{
            assertTrue(conf.isForceDeleteTenantAllowed());
        });
    }

    @Test
    public void testIsSystemTopicAllowAutoTopicCreationAsync() throws Exception {
        BrokerService brokerService = pulsar.getBrokerService();
        assertFalse(brokerService.isAllowAutoTopicCreationAsync(
                ServiceUnitStateChannelImpl.TOPIC).get());
        assertTrue(brokerService.isAllowAutoTopicCreationAsync(
                "persistent://pulsar/system/my-system-topic").get());
    }

    @Test
    public void testDuplicateAcknowledgement() throws Exception {
        final String ns = "prop/ns-test";

        admin.namespaces().createNamespace(ns, 2);
        final String topicName = "persistent://prop/ns-test/duplicated-acknowledgement-test";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("sub-1")
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Shared)
                .isAckReceiptEnabled(true)
                .subscribe();
        producer.send("1".getBytes(StandardCharsets.UTF_8));
        Message<byte[]> message = consumer1.receive();
        consumer1.acknowledge(message);
        consumer1.acknowledge(message);
        assertEquals(admin.topics().getStats(topicName).getSubscriptions()
                .get("sub-1").getUnackedMessages(), 0);
    }

    @Test
    public void testBlockedConsumerOnUnackedMsgs() throws Exception {
        final String ns = "prop/ns-test";
        admin.namespaces().createNamespace(ns, 2);
        admin.namespaces().setMaxUnackedMessagesPerConsumer(ns, 1);

        final String topicName = "persistent://prop/ns-test/testBlockedConsumerOnUnackedMsgs";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("sub-test")
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Shared)
                .isAckReceiptEnabled(true)
                .receiverQueueSize(0)
                .subscribe();

        producer.send("1".getBytes(StandardCharsets.UTF_8));
        producer.send("2".getBytes(StandardCharsets.UTF_8));

        // 1. receive message
        Message<byte[]> message = consumer.receive();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        SubscriptionStats subscriptionStats = admin.topics().getStats(topicName).getSubscriptions().get("sub-test");
        assertEquals(subscriptionStats.getUnackedMessages(), 1);
        assertTrue(subscriptionStats.getConsumers().get(0).isBlockedConsumerOnUnackedMsgs());

        // 2、ack this message
        consumer.acknowledge(message);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        subscriptionStats = admin.topics().getStats(topicName).getSubscriptions().get("sub-test");
        assertEquals(subscriptionStats.getUnackedMessages(), 0);
        assertFalse(subscriptionStats.getConsumers().get(0).isBlockedConsumerOnUnackedMsgs());
    }

    @Test
    public void testUnsubscribeNonDurableSub() throws Exception {
        final String ns = "prop/ns-test";
        final String topic = ns + "/testUnsubscribeNonDurableSub";

        admin.namespaces().createNamespace(ns, 2);
        admin.topics().createPartitionedTopic(String.format("persistent://%s", topic), 1);

        pulsarClient.newProducer(Schema.STRING).topic(topic).create().close();
        @Cleanup
        Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("sub1")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
        try {
            consumer.unsubscribe();
        } catch (Exception ex) {
            fail("Unsubscribe failed");
        }
    }

    @Test
    public void testMetricsPersistentTopicLoadFails() throws Exception {
        final String namespace = "prop/" + UUID.randomUUID().toString().replaceAll("-", "");
        String topic = "persistent://" + namespace + "/topic1_" + UUID.randomUUID();
        admin.namespaces().createNamespace(namespace);
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().unload(topic);

        // Inject an error that makes the topic load fails.
        AtomicBoolean failMarker = new AtomicBoolean(true);
        mockZooKeeper.failConditional(KeeperException.Code.NODEEXISTS, (op, path) -> {
            if (failMarker.get() && op.equals(MockZooKeeper.Op.SET) &&
                    path.endsWith(TopicName.get(topic).getPersistenceNamingEncoding())) {
                return true;
            }
            return false;
        });

        // Do test
        CompletableFuture<Producer<byte[]>> producer = pulsarClient.newProducer().topic(topic).createAsync();
        JerseyClient httpClient = JerseyClientBuilder.createClient();
        Awaitility.await().until(() -> {
            String response = httpClient.target(pulsar.getWebServiceAddress()).path("/metrics/")
                    .request().get(String.class);
            BufferedReader reader = new BufferedReader(new StringReader(response));
            String line;
            String metricsLine = null;
            while ((line = reader.readLine()) != null) {
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                if (line.startsWith("#")) {
                    continue;
                }
                if (line.contains("topic_load_failed")) {
                    metricsLine = line;
                    break;
                }
            }
            log.info("topic_load_failed: {}", metricsLine);
            if (metricsLine == null) {
                return false;
            }
            reader.close();
            String[] parts = metricsLine.split(" ");
            Double value = Double.valueOf(parts[parts.length - 1]);
            return value >= 1D;
        });

        // Remove the injection.
        failMarker.set(false);
        // cleanup.
        httpClient.close();
        producer.join().close();
        admin.topics().delete(topic);
        admin.namespaces().deleteNamespace(namespace);
    }

    @Test
    public void testMetricsNonPersistentTopicLoadFails() throws Exception {
        final String namespace = "prop/" + UUID.randomUUID().toString().replaceAll("-", "");
        String topic = "non-persistent://" + namespace + "/topic1_" + UUID.randomUUID();
        admin.namespaces().createNamespace(namespace);

        // Inject an error that makes the topic load fails.
        // Since we did not set a topic factory name, the "topicFactory" variable is null, inject a mocked
        // "topicFactory".
        Field fieldTopicFactory = BrokerService.class.getDeclaredField("topicFactory");
        fieldTopicFactory.setAccessible(true);
        TopicFactory originalTopicFactory = (TopicFactory) fieldTopicFactory.get(pulsar.getBrokerService());
        assertNull(originalTopicFactory);
        TopicFactory mockedTopicFactory = mock(TopicFactory.class);
        when(mockedTopicFactory.create(anyString(), any(), any(), any()))
                .thenThrow(new RuntimeException("mocked error"));
        fieldTopicFactory.set(pulsar.getBrokerService(), mockedTopicFactory);

        // Do test.
        CompletableFuture<Producer<byte[]>> producer = pulsarClient.newProducer().topic(topic).createAsync();
        JerseyClient httpClient = JerseyClientBuilder.createClient();
        Awaitility.await().until(() -> {
            String response = httpClient.target(pulsar.getWebServiceAddress()).path("/metrics/")
                    .request().get(String.class);
            BufferedReader reader = new BufferedReader(new StringReader(response));
            String line;
            String metricsLine = null;
            while ((line = reader.readLine()) != null) {
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                if (line.startsWith("#")) {
                    continue;
                }
                if (line.contains("topic_load_failed")) {
                    metricsLine = line;
                    break;
                }
            }
            log.info("topic_load_failed: {}", metricsLine);
            if (metricsLine == null) {
                return false;
            }
            reader.close();
            String[] parts = metricsLine.split(" ");
            Double value = Double.valueOf(parts[parts.length - 1]);
            return value >= 1D;
        });

        // Remove the injection.
        fieldTopicFactory.set(pulsar.getBrokerService(), null);

        // cleanup.
        httpClient.close();
        producer.join().close();
        admin.topics().delete(topic);
        admin.namespaces().deleteNamespace(namespace);
    }


    @Test
    public void testOffloadConfShouldNotAppliedForSystemTopic() throws PulsarAdminException {
        final String driver = "aws-s3";
        final String region = "test-region";
        final String bucket = "test-bucket";
        final String role = "test-role";
        final String roleSessionName = "test-role-session-name";
        final String credentialId = "test-credential-id";
        final String credentialSecret = "test-credential-secret";
        final String endPoint = "test-endpoint";
        final Integer maxBlockSizeInBytes = 5;
        final Integer readBufferSizeInBytes = 2;
        final Long offloadThresholdInBytes = 10L;
        final Long offloadThresholdInSeconds = 1000L;
        final Long offloadDeletionLagInMillis = 5L;

        final OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create(
                driver,
                region,
                bucket,
                endPoint,
                role,
                roleSessionName,
                credentialId,
                credentialSecret,
                maxBlockSizeInBytes,
                readBufferSizeInBytes,
                offloadThresholdInBytes,
                offloadThresholdInSeconds,
                offloadDeletionLagInMillis,
                OffloadedReadPriority.TIERED_STORAGE_FIRST
        );

        var fakeOffloader = new LedgerOffloader() {
            @Override
            public String getOffloadDriverName() {
                return driver;
            }

            @Override
            public CompletableFuture<Void> offload(ReadHandle ledger, UUID uid, Map<String, String> extraMetadata) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid, Map<String, String> offloadDriverMetadata) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid, Map<String, String> offloadDriverMetadata) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public OffloadPoliciesImpl getOffloadPolicies() {
                return offloadPolicies;
            }

            @Override
            public void close() {
            }
        };

        final BrokerService brokerService = pulsar.getBrokerService();
        final String namespace = "prop/" + UUID.randomUUID();
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setOffloadPolicies(namespace, offloadPolicies);
        Awaitility.await().untilAsserted(() -> {
            Object policiesGot = admin.namespaces().getOffloadPolicies(namespace);
            assertNotNull(policiesGot);
        });

        // Inject the cache to avoid real load off-loader jar
        final Map<NamespaceName, LedgerOffloader> ledgerOffloaderMap = pulsar.getLedgerOffloaderMap();
        ledgerOffloaderMap.put(NamespaceName.get(namespace), fakeOffloader);

        // (1) test normal topic
        final String normalTopic = "persistent://" + namespace + "/" + UUID.randomUUID();
        var managedLedgerConfig = brokerService.getManagedLedgerConfig(TopicName.get(normalTopic)).join();

        Assert.assertEquals(managedLedgerConfig.getLedgerOffloader(), fakeOffloader);

        // (2) test system topic
        for (String eventTopicName : SystemTopicNames.EVENTS_TOPIC_NAMES) {
            boolean offloadPoliciesExists = false;
            try {
                Object policiesGot =
                        admin.namespaces().getOffloadPolicies(TopicName.get(eventTopicName).getNamespace());
                offloadPoliciesExists = policiesGot != null;
            } catch (PulsarAdminException.NotFoundException notFoundException) {
                offloadPoliciesExists = false;
            }
            var managedLedgerConfig2 = brokerService.getManagedLedgerConfig(TopicName.get(eventTopicName)).join();
            if (offloadPoliciesExists) {
                Assert.assertTrue(managedLedgerConfig2.getLedgerOffloader() instanceof NonAppendableLedgerOffloader);
            } else {
                Assert.assertEquals(managedLedgerConfig2.getLedgerOffloader(), NullLedgerOffloader.INSTANCE);
            }
        }
    }

    @Test
    public void testTlsWithAuthParams() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic";
        final String subName = "newSub";
        Authentication auth;

        Set<String> providers = new HashSet<>();
        providers.add("org.apache.pulsar.broker.authentication.AuthenticationProviderTls");

        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(providers);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(false);
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        conf.setNumExecutorThreadPoolSize(5);
        restartBroker();

        String authParam = String.format("tlsCertFile:%s,tlsKeyFile:%s", getTlsFileForClient("admin.cert"),
                getTlsFileForClient("admin.key-pk8"));
        String authClassName = "org.apache.pulsar.client.impl.auth.AuthenticationTls";
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(brokerUrlTls.toString());
        conf.setAuthParams(authParam);
        conf.setAuthPluginClassName(authClassName);
        conf.setTlsAllowInsecureConnection(true);

        PulsarClient pulsarClient = null;
        try {
            pulsarClient = (new ClientBuilderImpl(conf)).build();

            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscribe();
        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }
    }

    @Test
    public void testPulsarMetadataEventSyncProducerCreation() throws Exception {
        final String topicName = "persistent://prop/usw/my-ns/syncTopic";
        pulsar.getConfiguration().setMetadataSyncEventTopic(topicName);
        PulsarMetadataEventSynchronizer sync = new PulsarMetadataEventSynchronizer(pulsar, topicName);
        // set invalid client for retry
        @Cleanup
        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder().serviceUrl("http://invalidhost:8080")
                .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
        sync.client = client;
        sync.STATE_UPDATER.set(sync, State.Starting_Producer);
        sync.startProducer();
        assertNull(sync.getProducer());
        // update valid client which will set the producer
        sync.client = (PulsarClientImpl) pulsarClient;
        retryStrategically((test) -> sync.getProducer() != null, 1000, 10);
        assertNotNull(sync.getProducer());
    }
}

