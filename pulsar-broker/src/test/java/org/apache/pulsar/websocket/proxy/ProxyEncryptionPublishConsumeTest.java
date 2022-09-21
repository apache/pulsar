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
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.websocket.CryptoKeyReaderFactory;
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

import lombok.Cleanup;

@Test(groups = "websocket")
public class ProxyEncryptionPublishConsumeTest extends ProducerConsumerBase {
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
        config.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        config.setCryptoKeyReaderFactoryClassName(CryptoKeyReaderFactoryImpl.class.getName());
        WebSocketService service = spy(new WebSocketService(config));
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(service).createConfigMetadataStore(anyString(), anyInt());
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
        String readerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/reader/persistent/my-property/my-ns/my-topic1";
        String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/producer/persistent/my-property/my-ns/my-topic1?encryptionKeys=client-ecdsa.pem";

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

            // if the subscription type is exclusive (default), either of the
            // consumer
            // sessions has already been closed
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

    public static class CryptoKeyReaderFactoryImpl implements CryptoKeyReaderFactory {

        private static final EncKeyReader reader = new EncKeyReader();

        @Override
        public CryptoKeyReader create() {
            return reader;
        }

    }

    public static class EncKeyReader implements CryptoKeyReader {

        final EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
            String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
            if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                    return keyInfo;
                } catch (IOException e) {
                    Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                }
            } else {
                Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
            }
            return null;
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
            String CERT_FILE_PATH = "./src/test/resources/certificate/private-key." + keyName;
            if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                    return keyInfo;
                } catch (IOException e) {
                    Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                }
            } else {
                Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
            }
            return null;
        }
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

    private static final Logger log = LoggerFactory.getLogger(ProxyEncryptionPublishConsumeTest.class);
}
