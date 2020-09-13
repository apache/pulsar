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

import static org.apache.pulsar.broker.cache.LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT;
import static org.apache.pulsar.broker.web.PulsarWebResource.joinPath;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import lombok.Cleanup;

/**
 */
public class BrokerServiceTest extends BrokerTestBase {

    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/certificate/client.crt";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/certificate/client.key";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
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
        stats = topicRef.getStats(false);
        subStats = stats.subscriptions.values().iterator().next();

        // subscription stats
        assertEquals(stats.subscriptions.keySet().size(), 1);
        assertEquals(subStats.msgBacklog, 0);
        assertEquals(subStats.consumers.size(), 1);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false);
        subStats = stats.subscriptions.values().iterator().next();

        // publisher stats
        assertEquals(subStats.msgBacklog, 10);
        assertEquals(stats.publishers.size(), 1);
        assertTrue(stats.publishers.get(0).msgRateIn > 0.0);
        assertTrue(stats.publishers.get(0).msgThroughputIn > 0.0);
        assertTrue(stats.publishers.get(0).averageMsgSize > 0.0);
        assertNotNull(stats.publishers.get(0).getClientVersion());

        // aggregated publish stats
        assertEquals(stats.msgRateIn, stats.publishers.get(0).msgRateIn);
        assertEquals(stats.msgThroughputIn, stats.publishers.get(0).msgThroughputIn);
        double diff = stats.averageMsgSize - stats.publishers.get(0).averageMsgSize;
        assertTrue(Math.abs(diff) < 0.000001);

        // consumer stats
        assertTrue(subStats.consumers.get(0).msgRateOut > 0.0);
        assertTrue(subStats.consumers.get(0).msgThroughputOut > 0.0);

        // aggregated consumer stats
        assertEquals(subStats.msgRateOut, subStats.consumers.get(0).msgRateOut);
        assertEquals(subStats.msgThroughputOut, subStats.consumers.get(0).msgThroughputOut);
        assertEquals(stats.msgRateOut, subStats.consumers.get(0).msgRateOut);
        assertEquals(stats.msgThroughputOut, subStats.consumers.get(0).msgThroughputOut);
        assertNotNull(subStats.consumers.get(0).getClientVersion());

        Message<byte[]> msg;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }
        consumer.close();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false);
        subStats = stats.subscriptions.values().iterator().next();

        assertEquals(subStats.msgBacklog, 0);
    }

    @Test
    public void testStatsOfStorageSizeWithSubscription() throws Exception {
        final String topicName = "persistent://prop/ns-abc/no-subscription";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        assertNotNull(topicRef);
        assertEquals(topicRef.getStats(false).storageSize, 0);

        for (int i = 0; i < 10; i++) {
            producer.send(new byte[10]);
        }

        assertTrue(topicRef.getStats(false).storageSize > 0);
    }

    @Test
    public void testBrokerServicePersistentRedeliverTopicStats() throws Exception {
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
        stats = topicRef.getStats(false);
        subStats = stats.subscriptions.values().iterator().next();

        // subscription stats
        assertEquals(stats.subscriptions.keySet().size(), 1);
        assertEquals(subStats.msgBacklog, 0);
        assertEquals(subStats.consumers.size(), 1);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false);
        subStats = stats.subscriptions.values().iterator().next();

        // publisher stats
        assertEquals(subStats.msgBacklog, 10);
        assertEquals(stats.publishers.size(), 1);
        assertTrue(stats.publishers.get(0).msgRateIn > 0.0);
        assertTrue(stats.publishers.get(0).msgThroughputIn > 0.0);
        assertTrue(stats.publishers.get(0).averageMsgSize > 0.0);

        // aggregated publish stats
        assertEquals(stats.msgRateIn, stats.publishers.get(0).msgRateIn);
        assertEquals(stats.msgThroughputIn, stats.publishers.get(0).msgThroughputIn);
        double diff = stats.averageMsgSize - stats.publishers.get(0).averageMsgSize;
        assertTrue(Math.abs(diff) < 0.000001);

        // consumer stats
        assertTrue(subStats.consumers.get(0).msgRateOut > 0.0);
        assertTrue(subStats.consumers.get(0).msgThroughputOut > 0.0);
        assertEquals(subStats.msgRateRedeliver, 0.0);
        assertEquals(subStats.consumers.get(0).unackedMessages, 10);

        // aggregated consumer stats
        assertEquals(subStats.msgRateOut, subStats.consumers.get(0).msgRateOut);
        assertEquals(subStats.msgThroughputOut, subStats.consumers.get(0).msgThroughputOut);
        assertEquals(subStats.msgRateRedeliver, subStats.consumers.get(0).msgRateRedeliver);
        assertEquals(stats.msgRateOut, subStats.consumers.get(0).msgRateOut);
        assertEquals(stats.msgThroughputOut, subStats.consumers.get(0).msgThroughputOut);
        assertEquals(subStats.msgRateRedeliver, subStats.consumers.get(0).msgRateRedeliver);
        assertEquals(subStats.unackedMessages, subStats.consumers.get(0).unackedMessages);

        consumer.redeliverUnacknowledgedMessages();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false);
        subStats = stats.subscriptions.values().iterator().next();
        assertTrue(subStats.msgRateRedeliver > 0.0);
        assertEquals(subStats.msgRateRedeliver, subStats.consumers.get(0).msgRateRedeliver);

        Message<byte[]> msg;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }
        consumer.close();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats(false);
        subStats = stats.subscriptions.values().iterator().next();

        assertEquals(subStats.msgBacklog, 0);
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
        JsonArray metrics = brokerStatsClient.getMetrics();

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
        final int numBundles = 4;
        final String ns1 = "prop/stats1";
        final String ns2 = "prop/stats2";

        List<String> nsList = Lists.newArrayList(ns1, ns2);
        List<Producer<byte[]>> producerList = Lists.newArrayList();

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
        JsonObject topicStats = brokerStatsClient.getTopics();
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
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
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
                    .allowTlsInsecureConnection(false).tlsTrustCertsFilePath(TLS_SERVER_CERT_FILE_PATH)
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
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(true);
        conf.setNumExecutorThreadPoolSize(5);
        restartBroker();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);

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
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(false);
        conf.setNumExecutorThreadPoolSize(5);
        restartBroker();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);

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
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(false);
        conf.setTlsTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH);
        conf.setNumExecutorThreadPoolSize(5);
        restartBroker();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);

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

        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .maxConcurrentLookupRequests(1)
                .maxLookupRequests(2)
                .build();

        // 2 lookup will success.
        try {
            CompletableFuture<Consumer<byte[]>> consumer1 = pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub1").subscribeAsync();
            CompletableFuture<Consumer<byte[]>> consumer2 = pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub2").subscribeAsync();

            consumer1.get().close();
            consumer2.get().close();
        } catch (Exception e) {
            fail("Subscribe should success with 2 requests");
        }

        // 3 lookup will fail
        try {
            CompletableFuture<Consumer<byte[]>> consumer1 = pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub11").subscribeAsync();
            CompletableFuture<Consumer<byte[]>> consumer2 = pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub22").subscribeAsync();
            CompletableFuture<Consumer<byte[]>> consumer3 = pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub33").subscribeAsync();

            consumer1.get().close();
            consumer2.get().close();
            consumer3.get().close();
            fail("It should fail as throttling should only receive 2 requests");
        } catch (Exception e) {
            if (!(e.getCause() instanceof
                org.apache.pulsar.client.api.PulsarClientException.TooManyRequestsException)) {
                fail("Subscribe should fail with TooManyRequestsException");
            }
        }
    }

    @Test
    public void testTopicLoadingOnDisableNamespaceBundle() throws Exception {
        final String namespace = "prop/disableBundle";
        admin.namespaces().createNamespace(namespace);
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
        CompletableFuture<Optional<Topic>> futureResult = pulsar.getBrokerService()
                .loadOrCreatePersistentTopic(topicName, true);

        try {
            futureResult.get();
            fail("Topic creation should fail due to disable bundle");
        } catch (Exception e) {
            if (!(e.getCause() instanceof BrokerServiceException.ServiceUnitNotReadyException)) {
                fail("Topic creation should fail with ServiceUnitNotReadyException");
            }

        }
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
        } finally {
            executor.shutdownNow();
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
            executor.shutdownNow();
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
        admin.namespaces().createNamespace(namespace, new BundlesData(totalBundle));

        String globalPath = joinPath(LOCAL_POLICIES_ROOT, namespace);
        pulsar.getLocalZkCacheService().policiesCache().clear();
        Optional<LocalPolicies> policy = pulsar.getLocalZkCacheService().policiesCache().get(globalPath);
        assertTrue(policy.isPresent());
        assertEquals(policy.get().bundles.numBundles, totalBundle);
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
}