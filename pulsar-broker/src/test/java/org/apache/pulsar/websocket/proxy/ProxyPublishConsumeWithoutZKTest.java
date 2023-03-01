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
import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.service.WebSocketServiceStarter;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "websocket")
public class ProxyPublishConsumeWithoutZKTest extends ProducerConsumerBase {
    protected String methodName;
    private ProxyServer proxyServer;
    private WebSocketService service;

    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(Optional.of(0));
        config.setClusterName("test");
        config.setServiceUrl(pulsar.getSafeWebServiceAddress());
        config.setServiceUrlTls(pulsar.getWebServiceAddressTls());
        service = spyWithClassAndConstructorArgs(WebSocketService.class, config);
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(service)
                .createConfigMetadataStore(anyString(), anyInt(), anyBoolean());
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (service != null) {
            service.close();
        }
        if (proxyServer != null) {
            proxyServer.stop();
        }
        log.info("Finished Cleaning Up Test setup");
    }

    @Test(timeOut = 30000)
    public void socketTest() throws Exception {

        String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/persistent/my-property/my-ns/my-topic/my-sub";
        String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/persistent/my-property/my-ns/my-topic/";

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

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            // let it connect
            Assert.assertTrue(consumerFuture.get().isOpen());
            Assert.assertTrue(producerFuture.get().isOpen());

            while (consumeSocket.getReceivedMessagesCount() < 10) {
                Thread.sleep(10);
            }
            Assert.assertTrue(produceSocket.getBuffer().size() > 0);
            Assert.assertEquals(produceSocket.getBuffer(), consumeSocket.getBuffer());
        } finally {
            @Cleanup("shutdownNow")
            ExecutorService executor = newFixedThreadPool(1);
            try {
                executor.submit(() -> {
                    try {
                        consumeClient.stop();
                        produceClient.stop();
                        log.info("proxy clients are stopped successfully");
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }).get(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("failed to close clients ", e);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyPublishConsumeWithoutZKTest.class);
}
