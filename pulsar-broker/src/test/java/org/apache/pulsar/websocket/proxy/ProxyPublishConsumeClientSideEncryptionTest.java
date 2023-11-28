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

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.data.ConsumerMessage;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.service.WebSocketServiceStarter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "websocket")
public class ProxyPublishConsumeClientSideEncryptionTest extends ProducerConsumerBase {
    private static final int TIME_TO_CHECK_BACKLOG_QUOTA = 5;
    private ScheduledExecutorService executor;
    private static final Charset charset = Charset.defaultCharset();

    private ProxyServer proxyServer;
    private WebSocketService service;

    @BeforeClass
    public void setup() throws Exception {
        executor = Executors.newScheduledThreadPool(1);

        conf.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);

        super.internalSetup();
        super.producerBaseSetup();

        WebSocketProxyConfiguration config = new WebSocketProxyConfiguration();
        config.setWebServicePort(Optional.of(0));
        config.setClusterName("test");
        config.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        service = spy(new WebSocketService(config));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(service)
                .createConfigMetadataStore(anyString(), anyInt(), anyBoolean());
        proxyServer = new ProxyServer(config);
        WebSocketServiceStarter.start(proxyServer, service);
        log.info("Proxy Server Started");
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (service != null) {
            service.close();
        }
        if (proxyServer != null) {
            proxyServer.stop();
        }
        executor.shutdownNow();
        log.info("Finished Cleaning Up Test setup");
    }

    @DataProvider(name = "encryptKeyNames")
    public Object[][] encryptKeyNames() {
        return new Object[][]{
            {"client-ecdsa.pem"},
            {"client-rsa.pem"}
        };
    }

    @Test(dataProvider = "encryptKeyNames")
    public void testWssSendAndJavaConsumeWithEncryption(String keyName) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("public/default/tp_");
        final String subscriptionName = "s1";
        final String producerName = "wss-p1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);

        // Create wss producer.
        final String webSocketProxyHost = "localhost";
        final int webSocketProxyPort = proxyServer.getListenPortHTTP().get();
        final CryptoKeyReader cryptoKeyReader = new CryptoKeyReaderForTest();
        ClientSideEncryptionWssProducer producer = new ClientSideEncryptionWssProducer(webSocketProxyHost,
                webSocketProxyPort, topicName, producerName, cryptoKeyReader, keyName, executor);
        producer.start();

        // Send message.
        String msgPayloadBeforeEncrypt = "msg-123";
        ProducerMessage messageSent = new ProducerMessage();
        messageSent.key = "k";
        messageSent.payload = msgPayloadBeforeEncrypt;
        MessageIdData messageIdData = producer.sendMessage(messageSent);
        log.info("send success: {}", messageIdData.toString());

        // Consume.
        Consumer consumer = pulsarClient.newConsumer().cryptoKeyReader(cryptoKeyReader)
                .topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        Message msgReceived = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(msgReceived.getData(), charset), msgPayloadBeforeEncrypt);

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topicName);
    }

    @DataProvider(name = "compressionTypes")
    public Object[][] compressionTypes() {
        return new Object[][]{
                {CompressionType.NONE},
                {CompressionType.LZ4},
                {CompressionType.ZLIB},
                {CompressionType.SNAPPY},
                {CompressionType.ZSTD}
        };
    }

    @Test(dataProvider = "compressionTypes")
    public void testWssSendAndJavaConsumeWithEncryptionAndCompression(CompressionType compressionType)
            throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("public/default/tp_");
        final String subscriptionName = "s1";
        final String producerName = "wss-p1";
        final String keyName = "client-ecdsa.pem";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);

        // Create wss producer.
        final String webSocketProxyHost = "localhost";
        final int webSocketProxyPort = proxyServer.getListenPortHTTP().get();
        final CryptoKeyReader cryptoKeyReader = new CryptoKeyReaderForTest();
        ClientSideEncryptionWssProducer producer = new ClientSideEncryptionWssProducer(webSocketProxyHost,
                webSocketProxyPort, topicName, producerName, cryptoKeyReader, keyName, executor);
        producer.start();

        // Send message.
        String originalPayload = "msg-123";
        ProducerMessage messageSent = new ProducerMessage();
        messageSent.key = "k";
        messageSent.payload = originalPayload;
        messageSent.compressionType = compressionType;
        MessageIdData messageIdData = producer.sendMessage(messageSent);
        log.info("send success: {}", messageIdData.toString());

        // Consume.
        Consumer consumer = pulsarClient.newConsumer().cryptoKeyReader(cryptoKeyReader)
                .topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        Message msgReceived = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(msgReceived.getData(), charset), originalPayload);

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topicName);
    }

    @Test(dataProvider = "encryptKeyNames")
    public void testJavaSendAndWssConsumeWithEncryption(String keyName) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("public/default/tp_");
        final String subscriptionName = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
        final CryptoKeyReader cryptoKeyReader = new CryptoKeyReaderForTest();

        final String originalPayload = "msg-123";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName).addEncryptionKey(keyName)
                .cryptoKeyReader(cryptoKeyReader).create();
        producer.send(originalPayload.getBytes(StandardCharsets.UTF_8));

        // Create wss consumer.
        final String webSocketProxyHost = "localhost";
        final int webSocketProxyPort = proxyServer.getListenPortHTTP().get();

        ClientSideEncryptionWssConsumer consumer = new ClientSideEncryptionWssConsumer(webSocketProxyHost,
                webSocketProxyPort, topicName, subscriptionName, SubscriptionType.Shared, cryptoKeyReader);
        consumer.start();

        // Receive message.
        ConsumerMessage message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(message.payload, originalPayload);

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topicName);
    }

    @DataProvider(name = "compressionTypesForJ")
    public Object[][] compressionTypesForJ() {
        return new Object[][]{
                {org.apache.pulsar.client.api.CompressionType.NONE},
                {org.apache.pulsar.client.api.CompressionType.LZ4},
                {org.apache.pulsar.client.api.CompressionType.ZLIB},
                {org.apache.pulsar.client.api.CompressionType.SNAPPY},
                {org.apache.pulsar.client.api.CompressionType.ZSTD}
        };
    }

    @Test(dataProvider = "compressionTypesForJ")
    public void testJavaSendAndWssConsumeWithEncryptionAndCompression(org.apache.pulsar.client.api.CompressionType
                                                                                  compressionType)
                                                                        throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("public/default/tp_");
        final String subscriptionName = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
        final CryptoKeyReader cryptoKeyReader = new CryptoKeyReaderForTest();
        final String keyName = "client-ecdsa.pem";

        final String originalPayload = "msg-123";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName).addEncryptionKey(keyName).compressionType(compressionType)
                .cryptoKeyReader(cryptoKeyReader).create();
        producer.send(originalPayload.getBytes(StandardCharsets.UTF_8));

        // Create wss consumer.
        final String webSocketProxyHost = "localhost";
        final int webSocketProxyPort = proxyServer.getListenPortHTTP().get();

        ClientSideEncryptionWssConsumer consumer = new ClientSideEncryptionWssConsumer(webSocketProxyHost,
                webSocketProxyPort, topicName, subscriptionName, SubscriptionType.Shared, cryptoKeyReader);
        consumer.start();

        // Receive message.
        ConsumerMessage message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(message.payload, originalPayload);

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topicName);
    }

    @Test
    public void testJavaSendAndWssConsumeWithEncryptionAndCompressionAndBatch() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("public/default/tp_");
        final String subscriptionName = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
        final CryptoKeyReader cryptoKeyReader = new CryptoKeyReaderForTest();
        final String keyName = "client-ecdsa.pem";

        final HashSet<String> messagesSent = new HashSet<>();
        Producer<byte[]> producer = pulsarClient.newProducer().enableBatching(true)
                .batchingMaxMessages(1000)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .topic(topicName).addEncryptionKey(keyName)
                .compressionType(org.apache.pulsar.client.api.CompressionType.LZ4)
                .cryptoKeyReader(cryptoKeyReader).create();
        for (int i = 0; i < 10; i++) {
            String payload = "msg-" + i;
            messagesSent.add(payload);
            producer.sendAsync(payload.getBytes(StandardCharsets.UTF_8));
        }
        producer.flush();

        // Create wss consumer.
        final String webSocketProxyHost = "localhost";
        final int webSocketProxyPort = proxyServer.getListenPortHTTP().get();

        ClientSideEncryptionWssConsumer consumer = new ClientSideEncryptionWssConsumer(webSocketProxyHost,
                webSocketProxyPort, topicName, subscriptionName, SubscriptionType.Shared, cryptoKeyReader);
        consumer.start();

        // Receive message.
        final HashSet<String> messagesReceived = new HashSet<>();
        while (true) {
            ConsumerMessage message = consumer.receive(2, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            messagesReceived.add(message.payload);
        }
        assertEquals(messagesReceived, messagesSent);

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topicName);
    }



    @Test(dataProvider = "encryptKeyNames")
    public void testWssSendAndWssConsumeWithEncryption(String keyName) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("public/default/tp_");
        final String subscriptionName = "s1";
        final String producerName = "wss-p1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);

        // Create wss producer.
        final String webSocketProxyHost = "localhost";
        final int webSocketProxyPort = proxyServer.getListenPortHTTP().get();
        final CryptoKeyReader cryptoKeyReader = new CryptoKeyReaderForTest();
        ClientSideEncryptionWssProducer producer = new ClientSideEncryptionWssProducer(webSocketProxyHost,
                webSocketProxyPort, topicName, producerName, cryptoKeyReader, keyName, executor);
        producer.start();

        // Send message.
        String msgPayloadBeforeEncrypt = "msg-123";
        ProducerMessage messageSent = new ProducerMessage();
        messageSent.key = "k";
        messageSent.payload = msgPayloadBeforeEncrypt;
        MessageIdData messageIdData = producer.sendMessage(messageSent);
        log.info("send success: {}", messageIdData.toString());

        // Consume.
        ClientSideEncryptionWssConsumer consumer = new ClientSideEncryptionWssConsumer(webSocketProxyHost,
                webSocketProxyPort, topicName, subscriptionName, SubscriptionType.Shared, cryptoKeyReader);
        consumer.start();
        ConsumerMessage msgReceived = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(msgReceived.payload, msgPayloadBeforeEncrypt);
        assertEquals(msgReceived.encryptionContext.getKeys().get(keyName).getMetadata(),
                CryptoKeyReaderForTest.RANDOM_METADATA);

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topicName);
    }
}
