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
package com.yahoo.pulsar.websocket.proxy;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import com.yahoo.pulsar.websocket.WebSocketService;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class ProxyPublishConsumeTls extends MockedPulsarServiceBaseTest {
    protected String methodName;
    private static final String CONSUME_URI = "wss://localhost:6090/ws/consumer/persistent/my-property/cluster1/my-ns/my-topic/my-sub";
    private static final String PRODUCE_URI = "wss://localhost:6090/ws/producer/persistent/my-property/cluster1/my-ns/my-topic/";
    private static final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private static final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";
    private static final int TEST_PORT = 6080;
    private static final int TLS_TEST_PORT = 6090;

    private WebSocketService service;

    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();

        ServiceConfiguration config = new ServiceConfiguration();
        config.setWebServicePort(TEST_PORT);
        config.setWebServicePortTls(TLS_TEST_PORT);
        config.setTlsEnabled(true);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);

        service = spy(new WebSocketService(config));
        doReturn(mockZooKeeperClientFactory).when(service).getZooKeeperClientFactory();
        service.start();

        log.info("Proxy Server Started");
    }

    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        service.close();
        log.info("Finished Cleaning Up Test setup");

    }

    @Test
    public void socketTest() throws InterruptedException, NoSuchAlgorithmException, KeyManagementException {
        URI consumeUri = URI.create(CONSUME_URI);
        URI produceUri = URI.create(PRODUCE_URI);

        KeyManager[] keyManagers = null;
        TrustManager[] trustManagers = InsecureTrustManagerFactory.INSTANCE.getTrustManagers();
        SSLContext sslCtx = SSLContext.getInstance("TLS");
        sslCtx.init(keyManagers, trustManagers, new SecureRandom());

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setSslContext(sslCtx);

        WebSocketClient consumeClient = new WebSocketClient(sslContextFactory);
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        WebSocketClient produceClient = new WebSocketClient(sslContextFactory);
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            log.info("Connecting to : %s%n", consumeUri);

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            produceClient.start();
            produceClient.connect(produceSocket, produceUri, produceRequest);

            consumeSocket.awaitClose(1, TimeUnit.SECONDS);
            produceSocket.awaitClose(1, TimeUnit.SECONDS);

            Assert.assertEquals(produceSocket.getBuffer(), consumeSocket.getBuffer());
        } catch (Throwable t) {
            log.error(t.getMessage());
        } finally {
            try {
                consumeClient.stop();
                produceClient.stop();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyPublishConsumeTls.class);
}
