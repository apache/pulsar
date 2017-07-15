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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.service.WebSocketServiceStarter;
import org.apache.pulsar.websocket.stats.ProxyTopicStat;
import org.apache.pulsar.websocket.stats.ProxyTopicStat.ConsumerStats;
import org.apache.pulsar.websocket.stats.ProxyTopicStat.ProducerStats;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeException;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.filter.LoggingFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ProxyPublishConsumeTest extends ProducerConsumerBase {
    protected String methodName;
    private int port;

    private ProxyServer proxyServer;
    private WebSocketService service;

    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        port = PortManager.nextFreePort();
        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(port);
        config.setClusterName("use");
        config.setGlobalZookeeperServers("dummy-zk-servers");
        service = spy(new WebSocketService(config));
        doReturn(mockZooKeeperClientFactory).when(service).getZooKeeperClientFactory();
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
        service.close();
        proxyServer.stop();
        log.info("Finished Cleaning Up Test setup");
    }

    @Test(timeOut = 10000)
    public void socketTest() throws Exception {
        String consumerUri = "ws://localhost:" + port
                + "/ws/consumer/persistent/my-property/use/my-ns/my-topic1/my-sub1?subscriptionType=Failover";
        String producerUri = "ws://localhost:" + port + "/ws/producer/persistent/my-property/use/my-ns/my-topic1/";

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

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            // let it connect
            Assert.assertTrue(consumerFuture1.get().isOpen());
            Assert.assertTrue(consumerFuture2.get().isOpen());
            Assert.assertTrue(producerFuture.get().isOpen());

            int retry = 0;
            int maxRetry = 400;
            while (consumeSocket1.getReceivedMessagesCount() < 10 && consumeSocket2.getReceivedMessagesCount() < 10) {
                Thread.sleep(10);
                if (retry++ > maxRetry) {
                    final String msg = String.format("Consumer still has not received the message after %s ms",
                            (maxRetry * 10));
                    log.warn(msg);
                    throw new IllegalStateException(msg);
                }
            }

            // if the subscription type is exclusive (default), either of the consumer sessions has already been closed
            Assert.assertTrue(consumerFuture1.get().isOpen());
            Assert.assertTrue(consumerFuture2.get().isOpen());
            Assert.assertTrue(produceSocket.getBuffer().size() > 0);

            if (consumeSocket1.getBuffer().size() > consumeSocket2.getBuffer().size()) {
                Assert.assertEquals(produceSocket.getBuffer(), consumeSocket1.getBuffer());
            } else {
                Assert.assertEquals(produceSocket.getBuffer(), consumeSocket2.getBuffer());
            }
        } finally {
            ExecutorService executor = newFixedThreadPool(1);
            try {
                executor.submit(() -> {
                    try {
                        consumeClient1.stop();
                        consumeClient2.stop();
                        produceClient.stop();
                        log.info("proxy clients are stopped successfully");
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }).get(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("failed to close clients ", e);
            }
            executor.shutdownNow();
        }
    }

    @Test(timeOut = 10000)
    public void badConsumerTest() throws Exception {

        // Empty subcription name
        String consumerUri = "ws://localhost:" + port
                + "/ws/consumer/persistent/my-property/use/my-ns/my-topic1/?subscriptionType=Exclusive";
        URI consumeUri = URI.create(consumerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();

        try {
            consumeClient1.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            consumerFuture1.get();
            Assert.fail("should fail: empty subscription");
        } catch (Exception e) {
            // Expected
            Assert.assertTrue(e.getCause() instanceof UpgradeException);
        } finally {
            ExecutorService executor = newFixedThreadPool(1);
            try {
                executor.submit(() -> {
                    try {
                        consumeClient1.stop();
                        log.info("proxy clients are stopped successfully");
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }).get(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("failed to close clients ", e);
            }
            executor.shutdownNow();
        }
    }

    /**
     * It verifies proxy topic-stats and proxy-metrics api
     * 
     * @throws Exception
     */
    @Test(timeOut = 10000)
    public void testProxyStats() throws Exception {
        final String topic = "my-property/use/my-ns/my-topic2";
        final String consumerUri = "ws://localhost:" + port + "/ws/consumer/persistent/" + topic
                + "/my-sub?subscriptionType=Failover";
        final String producerUri = "ws://localhost:" + port + "/ws/producer/persistent/" + topic + "/";
        System.out.println(consumerUri+", "+producerUri);
        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient1 = new WebSocketClient();
        SimpleConsumerSocket consumeSocket1 = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            consumeClient1.start();
            ClientUpgradeRequest consumeRequest1 = new ClientUpgradeRequest();
            Future<Session> consumerFuture1 = consumeClient1.connect(consumeSocket1, consumeUri, consumeRequest1);
            log.info("Connecting to : {}", consumeUri);

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            // let it connect
            Assert.assertTrue(consumerFuture1.get().isOpen());
            Assert.assertTrue(producerFuture.get().isOpen());

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

            Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFilter.class));
            final String baseUrl = pulsar.getWebServiceAddress()
                    .replace(Integer.toString(pulsar.getConfiguration().getWebServicePort()), (Integer.toString(port)))
                    + "/admin/proxy-stats/";

            // verify proxy metrics
            verifyProxyMetrics(client, baseUrl);

            // verify proxy stats
            verifyProxyStats(client, baseUrl, topic);

            // verify topic stat
            verifyTopicStat(client, baseUrl, topic);

        } finally {
            consumeClient1.stop();
            produceClient.stop();
            log.info("proxy clients are stopped successfully");
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
        Assert.assertFalse(data.producerStats.isEmpty());
        Assert.assertFalse(data.consumerStats.isEmpty());
    }

    private void verifyProxyMetrics(Client client, String baseUrl) {
        // generate metrics
        service.getProxyStats().generate();
        // collect metrics
        String statUrl = baseUrl + "metrics";
        WebTarget webTarget = client.target(statUrl);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = (Response) invocationBuilder.get();
        String responseStr = response.readEntity(String.class);
        final Gson gson = new Gson();
        final List<Metrics> data = gson.fromJson(responseStr, new TypeToken<List<Metrics>>() {
        }.getType());
        Assert.assertFalse(data.isEmpty());
    }

    private void verifyProxyStats(Client client, String baseUrl, String topic) {

        String statUrl = baseUrl + "stats";
        WebTarget webTarget = client.target(statUrl);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = (Response) invocationBuilder.get();
        String responseStr = response.readEntity(String.class);
        final Gson gson = new Gson();
        final Map<String, ProxyTopicStat> data = gson.fromJson(responseStr,
                new TypeToken<Map<String, ProxyTopicStat>>() {
                }.getType());
        // number of topic is loaded = 1
        Assert.assertEquals(data.size(), 1);
        Entry<String, ProxyTopicStat> entry = data.entrySet().iterator().next();
        Assert.assertEquals(entry.getKey(), "persistent://" + topic);
        ProxyTopicStat stats = entry.getValue();
        // number of consumers are connected = 1
        Assert.assertEquals(stats.consumerStats.size(), 1);
        ConsumerStats consumerStats = stats.consumerStats.iterator().next();
        // Assert.assertTrue(consumerStats.numberOfMsgDelivered > 0);
        Assert.assertNotNull(consumerStats.remoteConnection);

        // number of producers are connected = 1
        Assert.assertEquals(stats.producerStats.size(), 1);
        ProducerStats producerStats = stats.producerStats.iterator().next();
        // Assert.assertTrue(producerStats.numberOfMsgPublished > 0);
        Assert.assertNotNull(producerStats.remoteConnection);
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyPublishConsumeTest.class);
}
