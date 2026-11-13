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
package org.apache.pulsar.websocket.proxy;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Future;
import lombok.CustomLog;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.service.WebSocketServiceStarter;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.exceptions.UpgradeException;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * WebSocket proxy tests that need a broker setting read only at startup (a short backlog quota check interval), so
 * they run on a broker of their own. {@link ProxyPublishConsumeTest} has the tests that share one.
 */
@Test(groups = "websocket")
@CustomLog
public class ProxyPublishConsumeOwnBrokerTest extends ProducerConsumerBase {

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
        config.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        service = spyWithClassAndConstructorArgs(WebSocketService.class, config);
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(service)
                .createConfigMetadataStore(anyString(), anyInt(), anyBoolean());
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        // Stop the proxy before the broker: the broker would wait for the proxy's connections to close when it stops
        // its web service
        if (service != null) {
            service.close();
        }
        if (proxyServer != null) {
            proxyServer.stop();
        }
        super.internalCleanup();
        log.info("Finished Cleaning Up Test setup");
    }

    @Test// (timeOut = 30000)
    public void producerBacklogQuotaExceededTest() throws Exception {
        String namespace = "my-property/ns-ws-quota";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("test"), false);
        admin.namespaces().setBacklogQuota(namespace,
                BacklogQuota.builder()
                        .limitSize(10)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                        .build());

        final String topic = namespace + "/my-topic5";
        final String subscription = "my-sub";
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/consumer/persistent/" + topic + "/" + subscription;
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/producer/persistent/" + topic;

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
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest(consumeUri);
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeRequest);
            consumerFuture.get();
        } finally {
            stopWebSocketClient(consumeClient);
        }

        // Fill the backlog
        try {
            produceClient1.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest(produceUri);
            Future<Session> producerFuture = produceClient1.connect(produceSocket1, produceRequest);
            producerFuture.get();
            produceSocket1.sendMessage(100);
        } finally {
            stopWebSocketClient(produceClient1);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

        // New producer fails to connect
        try {
            produceClient2.setIdleTimeout(Duration.ofSeconds(5));
            produceClient2.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest(produceUri);
            Future<Session> producerFuture = produceClient2.connect(produceSocket2, produceRequest);
            producerFuture.get();
            fail("should fail: backlog quota exceeded");
        } catch (Exception e) {
            // Expected
            assertTrue(e.getCause() instanceof UpgradeException);
        } finally {
            stopWebSocketClient(produceClient2);
            admin.topics().skipAllMessages("persistent://" + topic, subscription);
            admin.topics().delete("persistent://" + topic);
            admin.namespaces().removeBacklogQuota(namespace);
            admin.namespaces().deleteNamespace(namespace);
        }
    }

    private void stopWebSocketClient(WebSocketClient... clients) {
        for (WebSocketClient client : clients) {
            try {
                client.stop();
            } catch (Exception e) {
                log.error().exception(e).log("Error stopping client");
            }
        }
        log.info("proxy clients are stopped successfully");
    }
}
