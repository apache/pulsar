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
package org.apache.pulsar.websocket.proxy;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.service.WebSocketServiceStarter;
import org.apache.pulsar.websocket.stats.ProxyTopicStat;
import org.apache.pulsar.websocket.stats.ProxyTopicStat.ConsumerStats;
import org.apache.pulsar.websocket.stats.ProxyTopicStat.ProducerStats;
import org.awaitility.Awaitility;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeException;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

@Test(groups = "websocket")
public class ProxyPublishConsumeTest extends ProducerConsumerBase {
    protected String methodName;

    private ProxyServer proxyServer;
    private WebSocketService service;

    private static final int TIME_TO_CHECK_BACKLOG_QUOTA = 5;

    @BeforeMethod
    public void setup() throws Exception {
        conf.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);

        super.internalSetup();
        super.producerBaseSetup();

        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(Optional.of(0));
        config.setClusterName("test");
        config.setConfigurationStoreServers(GLOBAL_DUMMY_VALUE);
        service = spy(new WebSocketService(config));
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(service).createMetadataStore(anyString(), anyInt());
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.resetConfig();
        super.internalCleanup();
        if (service != null) {
            service.close();
        }
        if (proxyServer != null) {
            proxyServer.stop();
        }
        log.info("Finished Cleaning Up Test setup");
    }

    @Test(timeOut = 10000)
    public void socketTest() throws Exception {
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/consumer/persistent/my-property/my-ns/my-topic1/my-sub1?subscriptionType=Failover";
        String readerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/reader/persistent/my-property/my-ns/my-topic1";
        String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/persistent/my-property/my-ns/my-topic1/";

        URI consumeUri = URI.create(consumerUri);
        URI readUri = URI.create(readerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        WebSocketClient consumeClient2 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket2 = new SimpleConsumerSocket();
        WebSocketClient readClient = new WebSocketClient();
        SimpleConsumerSocket readSocket = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            consumeClient1.start();
            consumeClient2.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            ClientUpgradeRequest consumeRequest2 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            Future<Session> consumerFuture2 = consumeClient2.connect(consumeSocket2, consumeUri, consumeRequest2);
            log.info("Connecting to : {}", consumeUri);

            readClient.start();
            ClientUpgradeRequest readRequest = new ClientUpgradeRequest();
            Future<Session> readerFuture = readClient.connect(readSocket, readUri, readRequest);
            log.info("Connecting to : {}", readUri);

            // let it connect
            assertTrue(consumerFuture1.get().isOpen());
            assertTrue(consumerFuture2.get().isOpen());
            assertTrue(readerFuture.get().isOpen());

            // Also make sure subscriptions and reader are already created
            Thread.sleep(500);

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            assertTrue(producerFuture.get().isOpen());

            int retry = 0;
            int maxRetry = 400;
            while ((consumeSocket1.getReceivedMessagesCount() < 10 && consumeSocket2.getReceivedMessagesCount() < 10)
                    || readSocket.getReceivedMessagesCount() < 10) {
                Thread.sleep(10);
                if (retry++ > maxRetry) {
                    final String msg = String.format("Consumer still has not received the message after %s ms",
                            (maxRetry * 10));
                    log.warn(msg);
                    throw new IllegalStateException(msg);
                }
            }

            // if the subscription type is exclusive (default), either of the consumer sessions has already been closed
            assertTrue(consumerFuture1.get().isOpen());
            assertTrue(consumerFuture2.get().isOpen());
            assertTrue(produceSocket.getBuffer().size() > 0);

            if (consumeSocket1.getBuffer().size() > consumeSocket2.getBuffer().size()) {
                assertEquals(produceSocket.getBuffer(), consumeSocket1.getBuffer());
            } else {
                assertEquals(produceSocket.getBuffer(), consumeSocket2.getBuffer());
            }
            assertEquals(produceSocket.getBuffer(), readSocket.getBuffer());
        } finally {
            stopWebSocketClient(consumeClient1, consumeClient2, readClient, produceClient);
        }
    }

    @Test(timeOut = 10000)
    public void socketTestEndOfTopic() throws Exception {
        final String topic = "my-property/my-ns/my-topic8";
        final String subscription = "my-sub";
        final String consumerUri = String.format(
                "ws://localhost:%d/ws/v2/consumer/persistent/%s/%s?pullMode=true&subscriptionType=Shared",
                proxyServer.getListenPortHTTP().get(), topic, subscription
        );
        final String producerUri = String.format("ws://localhost:%d/ws/v2/producer/persistent/%s", proxyServer.getListenPortHTTP().get(), topic);

        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient = new WebSocketClient();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            log.info("Connecting to : {}", consumeUri);

            // let it connect
            assertTrue(consumerFuture.get().isOpen());

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            assertTrue(producerFuture.get().isOpen());
            // Send 30 message in total.
            produceSocket.sendMessage(20);
            // Send 10 permits, should receive 10 message
            consumeSocket.sendPermits(10);
            Awaitility.await().untilAsserted(() ->
                    assertEquals(consumeSocket.getReceivedMessagesCount(), 10));
            consumeSocket.isEndOfTopic();
            // Wait till get response
            Awaitility.await().untilAsserted(() ->
                    assertEquals(consumeSocket.getBuffer().size(), 11));
            // Assert not reach end of topic yet
            assertEquals(consumeSocket.getBuffer().get(consumeSocket.getBuffer().size() - 1), "{\"endOfTopic\":false}");

            // Send 20 more permits, should receive all message
            consumeSocket.sendPermits(20);
            // 31 includes previous of end of topic request.
            Awaitility.await().untilAsserted(() ->
                    assertEquals(consumeSocket.getReceivedMessagesCount(), 31));
            consumeSocket.isEndOfTopic();
            // Wait till get response
            Awaitility.await().untilAsserted(() ->
                    assertEquals(consumeSocket.getReceivedMessagesCount(), 32));
            // Assert not reached end of topic.
            assertEquals(consumeSocket.getBuffer().get(consumeSocket.getBuffer().size() - 1), "{\"endOfTopic\":false}");

            admin.topics().terminateTopicAsync(topic).get();
            consumeSocket.isEndOfTopic();
            // Wait till get response
            Awaitility.await().untilAsserted(() ->
                    assertEquals(consumeSocket.getReceivedMessagesCount(), 33));
            // Assert reached end of topic.
            assertEquals(consumeSocket.getBuffer().get(consumeSocket.getBuffer().size() - 1), "{\"endOfTopic\":true}");
        } finally {
            stopWebSocketClient(consumeClient, produceClient);
        }
    }

    @Test
    public void unsubscribeTest() throws Exception {
        final String namespace = "my-property/my-ns";
        final String topic = namespace + "/" + "my-topic7";
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + topic);
        admin.topics().createPartitionedTopic(topicName, 3);

        final String subscription = "my-sub";
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/persistent/" + topic + "/" + subscription;

        URI consumeUri = URI.create(consumerUri);
        WebSocketClient consumeClient = new WebSocketClient();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        Thread.sleep(500);

        try {
            // setup a consumer
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            consumerFuture.get();
            List<String> subs = admin.topics().getSubscriptions(topic);
            Assert.assertEquals(subs.size(), 1);
            Assert.assertEquals(subs.get(0), subscription);
            // do unsubscribe
            consumeSocket.unsubscribe();
            //wait for delete
            Thread.sleep(1000);
            subs = admin.topics().getSubscriptions(topic);
            Assert.assertEquals(subs.size(), 0);
        } finally {
            stopWebSocketClient(consumeClient);
        }
    }

    @Test(timeOut = 10000)
    public void emptySubscriptionConsumerTest() {

        // Empty subscription name
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/consumer/persistent/my-property/my-ns/my-topic2/?subscriptionType=Exclusive";
        URI consumeUri = URI.create(consumerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();

        try {
            consumeClient1.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            consumerFuture1.get();
            fail("should fail: empty subscription");
        } catch (Exception e) {
            // Expected
            assertTrue(e.getCause() instanceof UpgradeException);
            assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
                    HttpServletResponse.SC_BAD_REQUEST);
        } finally {
            stopWebSocketClient(consumeClient1);
        }
    }

    @Test(timeOut = 10000)
    public void conflictingConsumerTest() throws Exception {
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/consumer/persistent/my-property/my-ns/my-topic3/sub1?subscriptionType=Exclusive";
        URI consumeUri = URI.create(consumerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        WebSocketClient consumeClient2 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        SimpleConsumerSocket consumeSocket2 = new SimpleConsumerSocket();

        try {
            consumeClient1.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            consumerFuture1.get();

            try {
                consumeClient2.start();
                ClientUpgradeRequest consumeRequest2 = new ClientUpgradeRequest();
                Future<Session> consumerFuture2 = consumeClient2.connect(consumeSocket2, consumeUri, consumeRequest2);
                consumerFuture2.get();
                fail("should fail: conflicting subscription name");
            } catch (Exception e) {
                // Expected
                assertTrue(e.getCause() instanceof UpgradeException);
                assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
                        HttpServletResponse.SC_CONFLICT);
            } finally {
                stopWebSocketClient(consumeClient2);
            }
        } finally {
            stopWebSocketClient(consumeClient1);
        }
    }

    @Test(timeOut = 10000)
    public void conflictingProducerTest() throws Exception {
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/producer/persistent/my-property/my-ns/my-topic4?producerName=my-producer";
        URI produceUri = URI.create(producerUri);

        WebSocketClient produceClient1 = new WebSocketClient();
        WebSocketClient produceClient2 = new WebSocketClient();
        SimpleProducerSocket produceSocket1 = new SimpleProducerSocket();
        SimpleProducerSocket produceSocket2 = new SimpleProducerSocket();

        try {
            produceClient1.start();
            ClientUpgradeRequest produceRequest1 = new ClientUpgradeRequest();
            Future<Session> producerFuture1 = produceClient1.connect(produceSocket1, produceUri, produceRequest1);
            producerFuture1.get();

            try {
                produceClient2.start();
                ClientUpgradeRequest produceRequest2 = new ClientUpgradeRequest();
                Future<Session> producerFuture2 = produceClient2.connect(produceSocket2, produceUri, produceRequest2);
                producerFuture2.get();
                fail("should fail: conflicting producer name");
            } catch (Exception e) {
                // Expected
                assertTrue(e.getCause() instanceof UpgradeException);
                assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
                        HttpServletResponse.SC_CONFLICT);
            } finally {
                stopWebSocketClient(produceClient2);
            }
        } finally {
            stopWebSocketClient(produceClient1);
        }
    }

    @Test// (timeOut = 30000)
    public void producerBacklogQuotaExceededTest() throws Exception {
        String namespace = "my-property/ns-ws-quota";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("test"));
        admin.namespaces().setBacklogQuota(namespace,
                BacklogQuota.builder()
                        .limitSize(10)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                        .build());

        final String topic = namespace + "/my-topic5";
        final String subscription = "my-sub";
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/persistent/" + topic + "/" + subscription;
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/persistent/" + topic;

        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient = new WebSocketClient();
        WebSocketClient produceClient1 = new WebSocketClient();
        WebSocketClient produceClient2 = new WebSocketClient();

        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        SimpleProducerSocket produceSocket1 = new SimpleProducerSocket();
        SimpleProducerSocket produceSocket2 = new SimpleProducerSocket();

        // Create subscription
        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            consumerFuture.get();
        } finally {
            stopWebSocketClient(consumeClient);
        }

        // Fill the backlog
        try {
            produceClient1.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient1.connect(produceSocket1, produceUri, produceRequest);
            producerFuture.get();
            produceSocket1.sendMessage(100);
        } finally {
            stopWebSocketClient(produceClient1);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

        // New producer fails to connect
        try {
            produceClient2.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient2.connect(produceSocket2, produceUri, produceRequest);
            producerFuture.get();
            fail("should fail: backlog quota exceeded");
        } catch (Exception e) {
            // Expected
            assertTrue(e.getCause() instanceof UpgradeException);
            assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
                    HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        } finally {
            stopWebSocketClient(produceClient2);
            admin.topics().skipAllMessages("persistent://" + topic, subscription);
            admin.topics().delete("persistent://" + topic);
            admin.namespaces().removeBacklogQuota(namespace);
            admin.namespaces().deleteNamespace(namespace);
        }
    }

    @Test(timeOut = 10000)
    public void topicDoesNotExistTest() throws Exception {
        final String namespace = "my-property/ns-topic-creation-not-allowed";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("test"));
        admin.namespaces().setAutoTopicCreation(namespace,
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(false)
                        .topicType(TopicType.NON_PARTITIONED.toString())
                        .build());

        final String topic = namespace + "/my-topic";
        final String subscription = "my-sub";
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/producer/persistent/" + topic;
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/consumer/persistent/" + topic + "/" + subscription;

        URI produceUri = URI.create(producerUri);
        URI consumeUri = URI.create(consumerUri);

        WebSocketClient produceClient = new WebSocketClient();
        WebSocketClient consumeClient = new WebSocketClient();

        SimpleProducerSocket produceSocket = new SimpleProducerSocket();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();

        try {
            produceClient.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            producerFuture.get();
            fail("should fail: topic does not exist");
        } catch (Exception e) {
            // Expected
            assertTrue(e.getCause() instanceof UpgradeException);
            assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(), HttpServletResponse.SC_NOT_FOUND);
        } finally {
            stopWebSocketClient(produceClient);
        }

        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            consumerFuture.get();
            fail("should fail: topic does not exist");
        } catch (Exception e) {
            // Expected
            assertTrue(e.getCause() instanceof UpgradeException);
            assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(), HttpServletResponse.SC_NOT_FOUND);
        } finally {
            stopWebSocketClient(consumeClient);
        }

        admin.namespaces().deleteNamespace(namespace);
    }

    @Test(timeOut = 10000)
    public void producerFencedTest() throws Exception {
        final String topic = "my-property/my-ns/producer-fenced-test";
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://" + topic)
                .accessMode(ProducerAccessMode.Exclusive).create();

        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/producer/persistent/" + topic;
        URI produceUri = URI.create(producerUri);

        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            produceClient.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            producerFuture.get();
            fail("should fail: producer fenced");
        } catch (Exception e) {
            // Expected
            assertTrue(e.getCause() instanceof UpgradeException);
            assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(), HttpServletResponse.SC_CONFLICT);
        } finally {
            stopWebSocketClient(produceClient);
            producer.close();
        }
    }

    @Test(timeOut = 10000)
    public void topicTerminatedTest() throws Exception {
        final String topic = "my-property/my-ns/topic-terminated-test";
        admin.topics().createNonPartitionedTopic("persistent://" + topic);
        admin.topics().terminateTopic("persistent://" + topic);

        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/producer/persistent/" + topic;
        URI produceUri = URI.create(producerUri);

        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            produceClient.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            producerFuture.get();
            fail("should fail: topic terminated");
        } catch (Exception e) {
            // Expected
            assertTrue(e.getCause() instanceof UpgradeException);
            assertEquals(((UpgradeException) e.getCause()).getResponseStatusCode(),
                    HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        } finally {
            stopWebSocketClient(produceClient);
            admin.topics().delete("persistent://" + topic);
        }
    }

    /**
     * It verifies proxy topic-stats and proxy-metrics api
     *
     * @throws Exception
     */
    @Test(timeOut = 10000)
    public void testProxyStats() throws Exception {
        final String topic = "my-property/my-ns/my-topic6";
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/persistent/" + topic
                + "/my-sub?subscriptionType=Failover";
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/persistent/" + topic + "/";
        final String readerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/reader/persistent/" + topic;
        System.out.println(consumerUri + ", " + producerUri);
        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);
        URI readUri = URI.create(readerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();
        WebSocketClient readClient = new WebSocketClient();
        SimpleConsumerSocket readSocket = new SimpleConsumerSocket();

        try {
            consumeClient1.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            log.info("Connecting to : {}", consumeUri);

            readClient.start();
            ClientUpgradeRequest readRequest = new ClientUpgradeRequest();
            Future<Session> readerFuture = readClient.connect(readSocket, readUri, readRequest);
            log.info("Connecting to : {}", readUri);

            assertTrue(consumerFuture1.get().isOpen());
            assertTrue(readerFuture.get().isOpen());

            Thread.sleep(500);

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            // let it connect

            assertTrue(producerFuture.get().isOpen());

            // sleep so, proxy can deliver few messages to consumers for stats
            int retry = 0;
            int maxRetry = 400;
            while (consumeSocket1.getReceivedMessagesCount() < 2) {
                Thread.sleep(10);
                if (retry++ > maxRetry) {
                    final String msg = String.format("Consumer still has not received the message after %s ms", (maxRetry * 10));
                    log.warn(msg);
                    break;
                }
            }

            Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));
            final String baseUrl = pulsar.getSafeWebServiceAddress()
                    .replace(Integer.toString(pulsar.getConfiguration().getWebServicePort().get()),
                            (Integer.toString(proxyServer.getListenPortHTTP().get())))
                    + "/admin/v2/proxy-stats/";

            // verify proxy metrics
            verifyProxyMetrics(client, baseUrl);

            // verify proxy stats
            verifyProxyStats(client, baseUrl, topic);

            // verify topic stat
            verifyTopicStat(client, baseUrl + "persistent/", topic);

        } finally {
            stopWebSocketClient(consumeClient1, produceClient, readClient);
        }
    }

    @Test(timeOut = 10000)
    public void consumeMessagesInPartitionedTopicTest() throws Exception {
        final String namespace = "my-property/my-ns";
        final String topic = namespace + "/" + "my-topic7";
        admin.topics().createPartitionedTopic("persistent://" + topic, 3);

        final String subscription = "my-sub";
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/persistent/" + topic + "/" + subscription;
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/persistent/" + topic;

        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient = new WebSocketClient();
        WebSocketClient produceClient = new WebSocketClient();

        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            produceClient.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            producerFuture.get();
            produceSocket.sendMessage(100);
        } finally {
            stopWebSocketClient(produceClient);
        }

        Thread.sleep(500);

        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            consumerFuture.get();
        } finally {
            stopWebSocketClient(consumeClient);
        }
    }

    @Test(timeOut = 10000)
    public void socketPullModeTest() throws Exception {
        final String topic = "my-property/my-ns/my-topic8";
        final String subscription = "my-sub";
        final String consumerUri = String.format(
                "ws://localhost:%d/ws/v2/consumer/persistent/%s/%s?pullMode=true&subscriptionType=Shared",
                proxyServer.getListenPortHTTP().get(), topic, subscription
        );
        final String producerUri = String.format("ws://localhost:%d/ws/v2/producer/persistent/%s", proxyServer.getListenPortHTTP().get(), topic);

        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        WebSocketClient consumeClient2 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket2 = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            consumeClient1.start();
            consumeClient2.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            ClientUpgradeRequest consumeRequest2 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            Future<Session> consumerFuture2 = consumeClient2.connect(consumeSocket2, consumeUri, consumeRequest2);
            log.info("Connecting to : {}", consumeUri);

            // let it connect
            assertTrue(consumerFuture1.get().isOpen());
            assertTrue(consumerFuture2.get().isOpen());

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            assertTrue(producerFuture.get().isOpen());
            produceSocket.sendMessage(100);

            Thread.sleep(500);

            // Verify no messages received despite production
            assertEquals(consumeSocket1.getReceivedMessagesCount(), 0);
            assertEquals(consumeSocket2.getReceivedMessagesCount(), 0);

            consumeSocket1.sendPermits(3);
            consumeSocket2.sendPermits(2);
            consumeSocket2.sendPermits(2);
            consumeSocket2.sendPermits(2);

            Thread.sleep(500);

            assertEquals(consumeSocket1.getReceivedMessagesCount(), 3);
            assertEquals(consumeSocket2.getReceivedMessagesCount(), 6);

        } finally {
            stopWebSocketClient(consumeClient1, consumeClient2, produceClient);
        }
    }

    @Test(timeOut = 20000)
    public void nackMessageTest() throws Exception {
        final String subscription = "my-sub";
        final String dlqTopic = "my-property/my-ns/nack-msg-dlq-" + UUID.randomUUID();
        final String consumerTopic = "my-property/my-ns/nack-msg-" + UUID.randomUUID();

        final String dlqUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() +
          "/ws/v2/consumer/persistent/" +
          dlqTopic + "/" + subscription +
          "?subscriptionType=Shared";

        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() +
          "/ws/v2/consumer/persistent/" +
          consumerTopic + "/" + subscription +
          "?deadLetterTopic=" + dlqTopic +
          "&maxRedeliverCount=1&subscriptionType=Shared&negativeAckRedeliveryDelay=1000";

        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() +
          "/ws/v2/producer/persistent/" + consumerTopic;

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        WebSocketClient consumeClient2 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket2 = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket(0);

        consumeSocket1.setMessageHandler((id, data) -> {
            JsonObject nack = new JsonObject();
            nack.add("messageId", new JsonPrimitive(id));
            nack.add("type", new JsonPrimitive("negativeAcknowledge"));
            return nack.toString();
        });

        try {
            consumeClient1.start();
            consumeClient2.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            ClientUpgradeRequest consumeRequest2 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, URI.create(consumerUri), consumeRequest1);
            Future<Session> consumerFuture2 = consumeClient2.connect(consumeSocket2, URI.create(dlqUri), consumeRequest2);

            assertTrue(consumerFuture1.get().isOpen());
            assertTrue(consumerFuture2.get().isOpen());

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, URI.create(producerUri), produceRequest);
            assertTrue(producerFuture.get().isOpen());

            assertEquals(consumeSocket1.getReceivedMessagesCount(), 0);
            assertEquals(consumeSocket2.getReceivedMessagesCount(), 0);

            produceSocket.sendMessage(1);

            // Main topic
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(consumeSocket1.getReceivedMessagesCount(), 2));

            // DLQ
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(consumeSocket2.getReceivedMessagesCount(), 1));
        } finally {
            stopWebSocketClient(consumeClient1, consumeClient2, produceClient);
        }
    }

    @Test(timeOut = 20000)
    public void nackRedeliveryDelayTest() throws Exception {
        final String uriBase = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2";
        final String topic = "my-property/my-ns/nack-redelivery-delay-" + UUID.randomUUID();
        final String sub = "my-sub";
        final int delayTime = 5000;

        final String consumerUri = String.format("%s/consumer/persistent/%s/%s?negativeAckRedeliveryDelay=%d", uriBase,
                topic, sub, delayTime);

        final String producerUri = String.format("%s/producer/persistent/%s", uriBase, topic);

        final WebSocketClient consumeClient = new WebSocketClient();
        final SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();

        final WebSocketClient produceClient = new WebSocketClient();
        final SimpleProducerSocket produceSocket = new SimpleProducerSocket(0);

        consumeSocket.setMessageHandler((mid, data) -> {
            JsonObject nack = new JsonObject();
            nack.add("type", new JsonPrimitive("negativeAcknowledge"));
            nack.add("messageId", new JsonPrimitive(mid));
            return nack.toString();
        });

        try {
            consumeClient.start();
            final ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            final Future<Session> consumerFuture = consumeClient.connect(consumeSocket, URI.create(consumerUri),
                    consumeRequest);
            assertTrue(consumerFuture.get().isOpen());

            produceClient.start();
            final ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            final Future<Session> producerFuture = produceClient.connect(produceSocket, URI.create(producerUri),
                    produceRequest);
            assertTrue(producerFuture.get().isOpen());

            assertEquals(consumeSocket.getReceivedMessagesCount(), 0);

            produceSocket.sendMessage(1);

            Awaitility.await().atMost(delayTime - 1000, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> assertEquals(consumeSocket.getReceivedMessagesCount(), 1));

            // Nacked message should be redelivered after 5 seconds
            Thread.sleep(delayTime);

            Awaitility.await().atMost(delayTime - 1000, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> assertEquals(consumeSocket.getReceivedMessagesCount(), 2));
        } finally {
            stopWebSocketClient(consumeClient, produceClient);
        }
    }

    @Test(timeOut = 20000)
    public void ackBatchMessageTest() throws Exception {
        final String subscription = "my-sub";
        final String topic = "my-property/my-ns/ack-batch-message" + UUID.randomUUID();
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() +
                "/ws/v2/consumer/persistent/" + topic + "/" + subscription;
        final int messages = 10;

        WebSocketClient consumerClient = new WebSocketClient();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        try {
            consumerClient.start();
            ClientUpgradeRequest consumerRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumerClient.connect(consumeSocket, URI.create(consumerUri), consumerRequest);

            assertTrue(consumerFuture.get().isOpen());
            assertEquals(consumeSocket.getReceivedMessagesCount(), 0);

            for (int i = 0; i < messages; i++) {
                producer.sendAsync(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            }

            producer.flush();
            consumeSocket.sendPermits(messages);
            Awaitility.await().untilAsserted(() ->
                    Assert.assertEquals(consumeSocket.getReceivedMessagesCount(), messages));

            // The message should not be acked since we only acked 1 message of the batch message
            Awaitility.await().untilAsserted(() ->
                    Assert.assertEquals(admin.topics().getStats(topic).getSubscriptions()
                            .get(subscription).getMsgBacklog(), 0));

        } finally {
            stopWebSocketClient(consumerClient);
        }
    }

    private void verifyTopicStat(Client client, String baseUrl, String topic) {
        String statUrl = baseUrl + topic + "/stats";
        WebTarget webTarget = client.target(statUrl);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = (Response) invocationBuilder.get();
        String responseStr = response.readEntity(String.class);
        final Gson gson = new Gson();
        final ProxyTopicStat data = gson.fromJson(responseStr, ProxyTopicStat.class);
        assertFalse(data.producerStats.isEmpty());
        assertFalse(data.consumerStats.isEmpty());
    }

    private void verifyProxyMetrics(Client client, String baseUrl) {
        // generate metrics
        service.getProxyStats().generate();
        // collect metrics
        String statUrl = baseUrl + "metrics";
        WebTarget webTarget = client.target(statUrl);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        String responseStr = response.readEntity(String.class);
        final Gson gson = new Gson();
        List<Metrics> data = gson.fromJson(responseStr, new TypeToken<List<Metrics>>() {
        }.getType());
        assertFalse(data.isEmpty());
        // re-generate metrics
        service.getProxyStats().generate();
        invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        response = invocationBuilder.get();
        responseStr = response.readEntity(String.class);
        data = gson.fromJson(responseStr, new TypeToken<List<Metrics>>() {
        }.getType());
        assertFalse(data.isEmpty());
    }

    private void verifyProxyStats(Client client, String baseUrl, String topic) {

        String statUrl = baseUrl + "stats";
        WebTarget webTarget = client.target(statUrl);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        String responseStr = response.readEntity(String.class);
        final Gson gson = new Gson();
        final Map<String, ProxyTopicStat> data = gson.fromJson(responseStr,
                new TypeToken<Map<String, ProxyTopicStat>>() {
                }.getType());
        // number of topic is loaded = 1
        assertEquals(data.size(), 1);
        Entry<String, ProxyTopicStat> entry = data.entrySet().iterator().next();
        assertEquals(entry.getKey(), "persistent://" + topic);
        ProxyTopicStat stats = entry.getValue();
        // number of consumers are connected = 2 (one is reader)
        assertEquals(stats.consumerStats.size(), 2);
        ConsumerStats consumerStats = stats.consumerStats.iterator().next();
        // Assert.assertTrue(consumerStats.numberOfMsgDelivered > 0);
        assertNotNull(consumerStats.remoteConnection);

        // number of producers are connected = 1
        assertEquals(stats.producerStats.size(), 1);
        ProducerStats producerStats = stats.producerStats.iterator().next();
        // Assert.assertTrue(producerStats.numberOfMsgPublished > 0);
        assertNotNull(producerStats.remoteConnection);
    }

    private void stopWebSocketClient(WebSocketClient... clients) {
        @Cleanup("shutdownNow")
        ExecutorService executor = newFixedThreadPool(1);
        try {
            executor.submit(() -> {
                for (WebSocketClient client : clients) {
                    try {
                        client.stop();
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }
                log.info("proxy clients are stopped successfully");
            }).get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("failed to close proxy clients", e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyPublishConsumeTest.class);
}
