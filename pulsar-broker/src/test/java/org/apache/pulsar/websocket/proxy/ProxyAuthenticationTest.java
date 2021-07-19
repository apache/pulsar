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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyInt;

import com.google.common.collect.Sets;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
public class ProxyAuthenticationTest extends ProducerConsumerBase {

    private ProxyServer proxyServer;
    private WebSocketService service;
    private WebSocketClient consumeClient;
    private WebSocketClient produceClient;

    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(Optional.of(0));
        config.setClusterName("test");
        config.setAuthenticationEnabled(true);
        // If this is not set, 500 error occurs.
        config.setConfigurationStoreServers(GLOBAL_DUMMY_VALUE);
        config.setSuperUserRoles(Sets.newHashSet("pulsar.super_user"));

        if (methodName.equals("authenticatedSocketTest") || methodName.equals("statsTest")) {
            config.setAuthenticationProviders(Sets.newHashSet("org.apache.pulsar.websocket.proxy.MockAuthenticationProvider"));
        } else {
            config.setAuthenticationProviders(Sets.newHashSet("org.apache.pulsar.websocket.proxy.MockUnauthenticationProvider"));
        }
        if (methodName.equals("anonymousSocketTest")) {
            config.setAnonymousUserRole("anonymousUser");
        }

        service = spy(new WebSocketService(config));
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(service).createMetadataStore(anyString(), anyInt());
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
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

        super.internalCleanup();
        if (service != null) {
            service.close();
        }
        if (proxyServer != null) {
            proxyServer.stop();
        }
        log.info("Finished Cleaning Up Test setup");
    }

    private void checkSocket() throws Exception {
        final String topic = "my-property/my-ns/my-topic1";
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/persistent/" + topic + "/my-sub";
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/persistent/" + topic;
        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        consumeClient = new WebSocketClient();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        consumeClient.start();
        ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
        Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
        log.info("Connecting to : {}", consumeUri);

        ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
        produceClient.start();
        Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
        Assert.assertTrue(consumerFuture.get().isOpen());
        Assert.assertTrue(producerFuture.get().isOpen());

        consumeSocket.awaitClose(1, TimeUnit.SECONDS);
        produceSocket.awaitClose(1, TimeUnit.SECONDS);
        Assert.assertTrue(produceSocket.getBuffer().size() > 0);
        Assert.assertEquals(produceSocket.getBuffer(), consumeSocket.getBuffer());
    }

    @Test(timeOut = 10000)
    public void authenticatedSocketTest() throws Exception {
        checkSocket();
    }

    @Test(timeOut = 10000)
    public void anonymousSocketTest() throws Exception {
        checkSocket();
    }

    @Test(timeOut = 10000)
    public void unauthenticatedSocketTest() {
        Exception exception = null;
        try {
            checkSocket();
        } catch (Exception e) {
            exception = e;
        }
        Assert.assertTrue(exception instanceof java.util.concurrent.ExecutionException);
    }

    @Test(timeOut = 10000)
    public void statsTest() throws Exception {
        final String topic = "persistent/my-property/my-ns/my-topic2";
        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/consumer/" + topic + "/my-sub";
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get() + "/ws/v2/producer/" + topic;
        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient consumeClient = new WebSocketClient();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        final String baseUrl = "http://localhost:" + proxyServer.getListenPortHTTP().get() + "/admin/v2/proxy-stats/";
        Client client = ClientBuilder.newClient();

        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            Assert.assertTrue(consumerFuture.get().isOpen());

            produceClient.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            Assert.assertTrue(producerFuture.get().isOpen());

            int retry = 0;
            int maxRetry = 500;
            while (consumeSocket.getReceivedMessagesCount() < 3) {
                Thread.sleep(10);
                if (retry++ > maxRetry) {
                    break;
                }
            }

            service.getProxyStats().generate();

            verifyResponseStatus(client, baseUrl + "metrics");
            verifyResponseStatus(client, baseUrl + "stats");
            verifyResponseStatus(client, baseUrl + topic + "/stats");
        } finally {
            consumeClient.stop();
            produceClient.stop();
            client.close();
        }
    }

    private void verifyResponseStatus(Client client, String url) {
        WebTarget webTarget = client.target(url);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        Assert.assertEquals(response.getStatus(), 200);
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyAuthenticationTest.class);
}
