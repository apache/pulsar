/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.yahoo.pulsar.broker.service.persistent.PersistentTopic;
import com.yahoo.pulsar.client.admin.BrokerStats;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.client.impl.auth.AuthenticationTls;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.policies.data.PersistentSubscriptionStats;
import com.yahoo.pulsar.common.policies.data.PersistentTopicStats;

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
        final String topic = "persistent://prop/use/ns-abc/successTopic";
        BrokerService service = pulsar.getBrokerService();

        final CountDownLatch latch1 = new CountDownLatch(1);
        service.getTopic(topic).thenAccept(t -> {
            latch1.countDown();
            fail("should fail as NS is not owned");
        }).exceptionally(exception -> {
            assertTrue(exception.getCause() instanceof IOException);
            latch1.countDown();
            return null;
        });
        latch1.await();

        admin.lookups().lookupDestination(topic);

        final CountDownLatch latch2 = new CountDownLatch(1);
        service.getTopic(topic).thenAccept(t -> {
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
        final String topicName = "persistent://prop/use/ns-abc/successTopic";
        final String subName = "successSub";

        PersistentTopicStats stats;
        PersistentSubscriptionStats subStats;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topicRef);

        rolloverPerIntervalStats();
        stats = topicRef.getStats();
        subStats = stats.subscriptions.values().iterator().next();

        // subscription stats
        assertEquals(stats.subscriptions.keySet().size(), 1);
        assertEquals(subStats.msgBacklog, 0);
        assertEquals(subStats.consumers.size(), 1);

        Producer producer = pulsarClient.createProducer(topicName);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats();
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

        // aggregated consumer stats
        assertEquals(subStats.msgRateOut, subStats.consumers.get(0).msgRateOut);
        assertEquals(subStats.msgThroughputOut, subStats.consumers.get(0).msgThroughputOut);
        assertEquals(stats.msgRateOut, subStats.consumers.get(0).msgRateOut);
        assertEquals(stats.msgThroughputOut, subStats.consumers.get(0).msgThroughputOut);

        Message msg;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }
        consumer.close();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats();
        subStats = stats.subscriptions.values().iterator().next();

        assertEquals(subStats.msgBacklog, 0);
    }

    @Test
    public void testBrokerServicePersistentRedeliverTopicStats() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/successSharedTopic";
        final String subName = "successSharedSub";

        PersistentTopicStats stats;
        PersistentSubscriptionStats subStats;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topicRef);

        rolloverPerIntervalStats();
        stats = topicRef.getStats();
        subStats = stats.subscriptions.values().iterator().next();

        // subscription stats
        assertEquals(stats.subscriptions.keySet().size(), 1);
        assertEquals(subStats.msgBacklog, 0);
        assertEquals(subStats.consumers.size(), 1);

        Producer producer = pulsarClient.createProducer(topicName);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats();
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
        stats = topicRef.getStats();
        subStats = stats.subscriptions.values().iterator().next();
        assertTrue(subStats.msgRateRedeliver > 0.0);
        assertEquals(subStats.msgRateRedeliver, subStats.consumers.get(0).msgRateRedeliver);

        Message msg;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }
        consumer.close();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        rolloverPerIntervalStats();
        stats = topicRef.getStats();
        subStats = stats.subscriptions.values().iterator().next();

        assertEquals(subStats.msgBacklog, 0);
    }

    @Test
    public void testBrokerStatsMetrics() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/newTopic";
        final String subName = "newSub";
        BrokerStats brokerStatsClient = admin.brokerStats();

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe(topicName, subName, conf);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        Producer producer = pulsarClient.createProducer(topicName);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        Message msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            consumer.acknowledge(msg);
        }
        consumer.close();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        JsonArray metrics = brokerStatsClient.getMetrics();
        assertEquals(metrics.size(), 4, metrics.toString());

        // these metrics seem to be arriving in different order at different times...
        // is the order really relevant here?
        boolean namespaceDimensionFound = false;
        boolean topicLoadTimesDimensionFound = false;
        for ( int i=0; i<metrics.size(); i++ ) {
            try {
                String data = metrics.get(i).getAsJsonObject().get("dimensions").toString();
                if (!namespaceDimensionFound && data.contains("prop/use/ns-abc")) {
                    namespaceDimensionFound = true;
                }
                if (!topicLoadTimesDimensionFound && data.contains("prop/use/ns-abc")) {
                    topicLoadTimesDimensionFound = true;
                }
            } catch (Exception e) { /* it's possible there's no dimensions */ }
        }

        assertTrue(namespaceDimensionFound && topicLoadTimesDimensionFound);

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBrokerServiceNamespaceStats() throws Exception {
        final int numBundles = 4;
        final String ns1 = "prop/use/stats1";
        final String ns2 = "prop/use/stats2";

        List<String> nsList = Lists.newArrayList(ns1, ns2);
        List<Producer> producerList = Lists.newArrayList();

        BrokerStats brokerStatsClient = admin.brokerStats();

        for (String ns : nsList) {
            admin.namespaces().createNamespace(ns, numBundles);
            String topic1 = String.format("persistent://%s/topic1", ns);
            producerList.add(pulsarClient.createProducer(topic1));
            String topic2 = String.format("persistent://%s/topic2", ns);
            producerList.add(pulsarClient.createProducer(topic2));
        }

        rolloverPerIntervalStats();
        JsonObject destinationStats = brokerStatsClient.getDestinations();
        assertEquals(destinationStats.size(), 2, destinationStats.toString());

        for (String ns : nsList) {
            JsonObject nsObject = destinationStats.getAsJsonObject(ns);
            List<String> topicList = admin.namespaces().getDestinations(ns);
            for (String topic : topicList) {
                NamespaceBundle bundle = (NamespaceBundle) pulsar.getNamespaceService()
                        .getBundle(DestinationName.get(topic));
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

        for (Producer producer : producerList) {
            producer.close();
        }
        for (String ns : nsList) {
            List<String> destinations = admin.namespaces().getDestinations(ns);
            for (String dest : destinations) {
                admin.persistentTopics().delete(dest);
            }
            admin.namespaces().deleteNamespace(ns);
        }
    }

    @Test
    public void testTlsDisabled() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/newTopic";
        final String subName = "newSub";
        ClientConfiguration clientConfig;
        ConsumerConfiguration consumerConfig;
        Consumer consumer;
        PulsarClient pulsarClient = null;

        conf.setAuthenticationEnabled(false);
        conf.setTlsEnabled(false);
        restartBroker();

        // Case 1: Access without TLS
        try {
            clientConfig = new ClientConfiguration();
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrl.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }

        // Case 2: Access with TLS
        try {
            clientConfig = new ClientConfiguration();
            clientConfig.setUseTls(true);
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrlTls.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
            fail("TLS connection should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("ConnectException"));
        } finally {
            pulsarClient.close();
        }
    }

    @Test
    public void testTlsEnabled() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/newTopic";
        final String subName = "newSub";
        ClientConfiguration clientConfig;
        ConsumerConfiguration consumerConfig;
        Consumer consumer;

        conf.setAuthenticationEnabled(false);
        conf.setTlsEnabled(true);
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        restartBroker();

        // Case 1: Access without TLS
        PulsarClient pulsarClient = null;
        try {
            clientConfig = new ClientConfiguration();
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrl.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }

        // Case 2: Access with TLS (Allow insecure TLS connection)
        try {
            clientConfig = new ClientConfiguration();
            clientConfig.setUseTls(true);
            clientConfig.setTlsAllowInsecureConnection(true);
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrlTls.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }

        // Case 3: Access with TLS (Disallow insecure TLS connection)
        try {
            clientConfig = new ClientConfiguration();
            clientConfig.setUseTls(true);
            clientConfig.setTlsAllowInsecureConnection(false);
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrlTls.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("General SSLEngine problem"));
        } finally {
            pulsarClient.close();
        }

        // Case 4: Access with TLS (Use trusted certificates)
        try {
            clientConfig = new ClientConfiguration();
            clientConfig.setUseTls(true);
            clientConfig.setTlsAllowInsecureConnection(false);
            clientConfig.setTlsTrustCertsFilePath(TLS_SERVER_CERT_FILE_PATH);
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrlTls.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }
    }

    @Test
    public void testTlsAuthAllowInsecure() throws Exception {
        final String topicName = "persistent://prop/usw/my-ns/newTopic";
        final String subName = "newSub";
        ClientConfiguration clientConfig;
        ConsumerConfiguration consumerConfig;
        Consumer consumer;
        Authentication auth;

        Set<String> providers = new HashSet<>();
        providers.add("com.yahoo.pulsar.broker.authentication.AuthenticationProviderTls");

        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(providers);
        conf.setTlsEnabled(true);
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(true);
        restartBroker();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);

        PulsarClient pulsarClient = null;

        // Case 1: Access without client certificate
        try {
            clientConfig = new ClientConfiguration();
            clientConfig.setUseTls(true);
            clientConfig.setTlsAllowInsecureConnection(true);
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrlTls.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Authentication required"));
        } finally {
            pulsarClient.close();
        }

        // Case 2: Access with client certificate
        try {
            auth = new AuthenticationTls();
            auth.configure(authParams);
            clientConfig = new ClientConfiguration();
            clientConfig.setAuthentication(auth);
            clientConfig.setUseTls(true);
            clientConfig.setTlsAllowInsecureConnection(true);
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrlTls.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
        } catch (Exception e) {
            fail("should not fail");
        } finally {
            pulsarClient.close();
        }
    }

    @Test
    public void testTlsAuthDisallowInsecure() throws Exception {
        final String topicName = "persistent://prop/usw/my-ns/newTopic";
        final String subName = "newSub";
        ClientConfiguration clientConfig;
        ConsumerConfiguration consumerConfig;
        Consumer consumer;
        Authentication auth;

        Set<String> providers = new HashSet<>();
        providers.add("com.yahoo.pulsar.broker.authentication.AuthenticationProviderTls");

        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(providers);
        conf.setTlsEnabled(true);
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(false);
        restartBroker();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);

        PulsarClient pulsarClient = null;

        // Case 1: Access without client certificate
        try {
            clientConfig = new ClientConfiguration();
            clientConfig.setUseTls(true);
            clientConfig.setTlsAllowInsecureConnection(true);
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrlTls.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Authentication required"));
        } finally {
            pulsarClient.close();
        }

        // Case 2: Access with client certificate
        try {
            auth = new AuthenticationTls();
            auth.configure(authParams);
            clientConfig = new ClientConfiguration();
            clientConfig.setAuthentication(auth);
            clientConfig.setUseTls(true);
            clientConfig.setTlsAllowInsecureConnection(true);
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrlTls.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Authentication required"));
        } finally {
            pulsarClient.close();
        }
    }

    @Test
    public void testTlsAuthUseTrustCert() throws Exception {
        final String topicName = "persistent://prop/usw/my-ns/newTopic";
        final String subName = "newSub";
        ClientConfiguration clientConfig;
        ConsumerConfiguration consumerConfig;
        Consumer consumer;
        Authentication auth;

        Set<String> providers = new HashSet<>();
        providers.add("com.yahoo.pulsar.broker.authentication.AuthenticationProviderTls");

        conf.setAuthenticationEnabled(true);
        conf.setAuthenticationProviders(providers);
        conf.setTlsEnabled(true);
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(false);
        conf.setTlsTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH);
        restartBroker();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);

        PulsarClient pulsarClient = null;

        // Case 1: Access without client certificate
        try {
            clientConfig = new ClientConfiguration();
            clientConfig.setUseTls(true);
            clientConfig.setTlsAllowInsecureConnection(true);
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrlTls.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Authentication required"));
        } finally {
            pulsarClient.close();
        }

        // Case 2: Access with client certificate
        try {
            auth = new AuthenticationTls();
            auth.configure(authParams);
            clientConfig = new ClientConfiguration();
            clientConfig.setAuthentication(auth);
            clientConfig.setUseTls(true);
            clientConfig.setTlsAllowInsecureConnection(true);
            clientConfig.setStatsInterval(0, TimeUnit.SECONDS);
            pulsarClient = PulsarClient.create(brokerUrlTls.toString(), clientConfig);
            consumerConfig = new ConsumerConfiguration();
            consumerConfig.setSubscriptionType(SubscriptionType.Exclusive);
            consumer = pulsarClient.subscribe(topicName, subName, consumerConfig);
            consumer.close();
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
        final String topicName = "persistent://prop/usw/my-ns/newTopic";

        com.yahoo.pulsar.client.api.ClientConfiguration clientConf = new com.yahoo.pulsar.client.api.ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        clientConf.setConcurrentLookupRequest(0);
        String lookupUrl = new URI("pulsar://localhost:" + BROKER_PORT).toString();
        PulsarClient pulsarClient = PulsarClient.create(lookupUrl, clientConf);

        try {
            Consumer consumer = pulsarClient.subscribe(topicName, "mysub", new ConsumerConfiguration());
            fail("It should fail as throttling should not receive any request");
        } catch (com.yahoo.pulsar.client.api.PulsarClientException.TooManyLookupRequestException e) {
            // ok as throttling set to 0
        }
    }
}