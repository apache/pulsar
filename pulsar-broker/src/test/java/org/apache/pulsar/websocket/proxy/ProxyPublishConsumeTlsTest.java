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
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.client.api.TlsProducerConsumerBase;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.service.WebSocketServiceStarter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProxyPublishConsumeTlsTest extends TlsProducerConsumerBase {
    protected String methodName;
    private int port;
    private int tlsPort;

    private ProxyServer proxyServer;
    private WebSocketService service;

    @BeforeMethod
    public void setup() throws Exception {
        super.setup();
        super.internalSetUpForNamespace();

        port = PortManager.nextFreePort();
        tlsPort = PortManager.nextFreePort();
        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(Optional.of(port));
        config.setWebServicePortTls(Optional.of(tlsPort));
        config.setBrokerClientTlsEnabled(true);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        config.setBrokerClientTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        config.setClusterName("use");
        config.setConfigurationStoreServers("dummy-zk-servers");
        config.setBrokerClientAuthenticationParameters("tlsCertFile:" + TLS_CLIENT_CERT_FILE_PATH + ",tlsKeyFile:" + TLS_CLIENT_KEY_FILE_PATH);
        config.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        String lookupUrl = new URI("pulsar://localhost:" + BROKER_PORT_TLS).toString();
        service = spy(new WebSocketService(config));
        doReturn(mockZooKeeperClientFactory).when(service).getZooKeeperClientFactory();
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        super.cleanup();
        service.close();
        proxyServer.stop();
        log.info("Finished Cleaning Up Test setup");

    }

    @Test(timeOut = 30000)
    public void socketTest() throws InterruptedException, GeneralSecurityException {
        String consumerUri =
                "wss://localhost:" + tlsPort + "/ws/consumer/persistent/my-property/use/my-ns/my-topic/my-sub";
        String producerUri = "wss://localhost:" + tlsPort + "/ws/producer/persistent/my-property/use/my-ns/my-topic/";
        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setSslContext(SecurityUtility
                .createSslContext(false, SecurityUtility.loadCertificatesFromPemFile(TLS_TRUST_CERT_FILE_PATH)));

        WebSocketClient consumeClient = new WebSocketClient(sslContextFactory);
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient(sslContextFactory);

        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            log.info("Connecting to : {}", consumeUri);
            Assert.assertTrue(consumerFuture.get().isOpen());

            SimpleProducerSocket produceSocket = new SimpleProducerSocket();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);

            Assert.assertTrue(producerFuture.get().isOpen());

            consumeSocket.awaitClose(1, TimeUnit.SECONDS);
            produceSocket.awaitClose(1, TimeUnit.SECONDS);
            Assert.assertTrue(produceSocket.getBuffer().size() > 0);
            Assert.assertEquals(produceSocket.getBuffer(), consumeSocket.getBuffer());
        } catch (Throwable t) {
            log.error(t.getMessage());
            Assert.fail(t.getMessage());
        } finally {
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
            executor.shutdownNow();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyPublishConsumeTlsTest.class);
}
