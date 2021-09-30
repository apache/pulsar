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
package org.apache.pulsar.tests.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;
import org.apache.pulsar.shade.io.netty.buffer.Unpooled;
import org.apache.pulsar.tests.TestRetrySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SimpleProducerConsumerTest extends TestRetrySupport {
    private static final Logger log = LoggerFactory.getLogger(SimpleProducerConsumerTest.class);

    private PulsarContainer pulsarContainer;
    private URI lookupUrl;
    private PulsarClient pulsarClient;

    @Override
    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        incrementSetupNumber();
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

        pulsarContainer = new PulsarContainer();
        pulsarContainer.start();
        pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarContainer.getPlainTextPulsarBrokerUrl())
                .build();
        lookupUrl = new URI(pulsarContainer.getPlainTextPulsarBrokerUrl());

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarContainer.getPulsarAdminUrl()).build();
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(new HashSet<>(Arrays.asList("appid1", "appid2")), Collections.singleton("standalone")));
        admin.namespaces().createNamespace("my-property/my-ns");
        admin.namespaces().setNamespaceReplicationClusters("my-property/my-ns", Collections.singleton("standalone"));
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        if (pulsarClient != null) {
            pulsarClient.close();
            pulsarClient = null;
        }
        if (pulsarContainer != null) {
            pulsarContainer.stop();
            pulsarContainer.close();
            pulsarContainer = null;
        }
    }

    private PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(url).statsInterval(intervalInSecs, TimeUnit.SECONDS).build();
    }

    @Test
    public void testRSAEncryption() throws Exception {

        String topicName = "persistent://my-property/my-ns/myrsa-topic1-" + System.currentTimeMillis();

        class EncKeyReader implements CryptoKeyReader {

            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

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

        final int totalMsg = 10;

        Set<String> messageSet = new HashSet<>();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/myrsa-topic1")
                .subscriptionName("my-subscriber-name").cryptoKeyReader(new EncKeyReader()).subscribe();
        Consumer<byte[]> normalConsumer = pulsarClient.newConsumer()
                .topic(topicName).subscriptionName("my-subscriber-name-normal")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/myrsa-topic1")
                .addEncryptionKey("client-rsa.pem").cryptoKeyReader(new EncKeyReader()).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic("persistent://my-property/my-ns/myrsa-topic1")
                .addEncryptionKey("client-rsa.pem").cryptoKeyReader(new EncKeyReader()).create();

        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        for (int i = totalMsg; i < totalMsg * 2; i++) {
            String message = "my-message-" + i;
            producer2.send(message.getBytes());
        }

        MessageImpl<byte[]> msg = null;

        msg = (MessageImpl<byte[]>) normalConsumer.receive(500, TimeUnit.MILLISECONDS);
        // should not able to read message using normal message.
        assertNull(msg);

        for (int i = 0; i < totalMsg * 2; i++) {
            msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
            // verify that encrypted message contains encryption-context
            msg.getEncryptionCtx()
                    .orElseThrow(() -> new IllegalStateException("encryption-ctx not present for encrypted message"));
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
    }

    protected <T> void testMessageOrderAndDuplicates(Set<T> messagesReceived, T receivedMessage,
                                                     T expectedMessage) {
        // Make sure that messages are received in order
        assertEquals(receivedMessage, expectedMessage,
                "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);

        // Make sure that there are no duplicates
        assertTrue(messagesReceived.add(receivedMessage), "Received duplicate message " + receivedMessage);
    }

    @Test
    public void testRedeliveryOfFailedMessages() throws Exception {

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarContainer.getPlainTextPulsarBrokerUrl())
                .build();

        final String encryptionKeyName = "client-rsa.pem";
        final String encryptionKeyVersion = "1.0";
        Map<String, String> metadata = new HashMap<>();
        metadata.put("version", encryptionKeyVersion);
        class EncKeyReader implements CryptoKeyReader {
            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        keyInfo.setMetadata(metadata);
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
                        keyInfo.setMetadata(metadata);
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

        class InvalidKeyReader implements CryptoKeyReader {
            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                return null;
            }

            @Override
            public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
                return null;
            }
        }

        /*
         * Redelivery functionality guarantees that customer will get a chance to process the message again.
         * In case of shared subscription eventually every client will get a chance to process the message, till one of them acks it.
         *
         * For client with Encryption enabled where in cases like a new production rollout or a buggy client configuration, we might have a mismatch of consumers
         * - few which can decrypt, few which can't (due to errors or cryptoReader not configured).
         *
         * In that case eventually all messages should be acked as long as there is a single consumer who can decrypt the message.
         *
         * Consumer 1 - Can decrypt message
         * Consumer 2 - Has invalid Reader configured.
         * Consumer 3 - Has no reader configured.
         *
         */

        String topicName = "persistent://my-property/my-ns/myrsa-topic2";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .addEncryptionKey(encryptionKeyName).compressionType(CompressionType.LZ4)
                .cryptoKeyReader(new EncKeyReader()).create();

        @Cleanup
        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer1 = newPulsarClient.newConsumer().topicsPattern(topicName)
                .subscriptionName("my-subscriber-name").cryptoKeyReader(new EncKeyReader())
                .subscriptionType(SubscriptionType.Shared).ackTimeout(1, TimeUnit.SECONDS).subscribe();

        @Cleanup
        PulsarClient newPulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer2 = newPulsarClient1.newConsumer().topicsPattern(topicName)
                .subscriptionName("my-subscriber-name").cryptoKeyReader(new InvalidKeyReader())
                .subscriptionType(SubscriptionType.Shared).ackTimeout(1, TimeUnit.SECONDS).subscribe();

        @Cleanup
        PulsarClient newPulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer3 = newPulsarClient2.newConsumer().topicsPattern(topicName)
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared).ackTimeout(1, TimeUnit.SECONDS).subscribe();

        int numberOfMessages = 100;
        String message = "my-message";
        Set<String> messages = new HashSet(); // Since messages are in random order
        for (int i = 0; i < numberOfMessages; i++) {
            producer.send((message + i).getBytes());
        }

        // Consuming from consumer 2 and 3
        // no message should be returned since they can't decrypt the message
        Message m = consumer2.receive(3, TimeUnit.SECONDS);
        assertNull(m);
        m = consumer3.receive(3, TimeUnit.SECONDS);
        assertNull(m);

        for (int i = 0; i < numberOfMessages; i++) {
            // All messages would be received by consumer 1
            m = consumer1.receive();
            messages.add(new String(m.getData()));
            consumer1.acknowledge(m);
        }

        // Consuming from consumer 2 and 3 again just to be sure
        // no message should be returned since they can't decrypt the message
        m = consumer2.receive(3, TimeUnit.SECONDS);
        assertNull(m);
        m = consumer3.receive(3, TimeUnit.SECONDS);
        assertNull(m);

        // checking if all messages were received
        for (int i = 0; i < numberOfMessages; i++) {
            assertTrue(messages.contains((message + i)));
        }

        consumer1.close();
        consumer2.close();
        consumer3.close();
        newPulsarClient.close();
        newPulsarClient1.close();
        newPulsarClient2.close();
    }

    @Test
    public void testEncryptionFailure() throws Exception {

        class EncKeyReader implements CryptoKeyReader {

            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        return keyInfo;
                    } catch (IOException e) {
                        log.error("Failed to read certificate from {}", CERT_FILE_PATH);
                    }
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
                        log.error("Failed to read certificate from {}", CERT_FILE_PATH);
                    }
                }
                return null;
            }
        }

        final int totalMsg = 10;

        MessageImpl<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/myenc-ns/myenc-topic1").subscriptionName("my-subscriber-name")
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        // 1. Invalid key name
        try {
            pulsarClient.newProducer().topic("persistent://my-property/use/myenc-ns/myenc-topic1")
                    .addEncryptionKey("client-non-existant-rsa.pem").cryptoKeyReader(new EncKeyReader()).create();
            Assert.fail("Producer creation should not suceed if failing to read key");
        } catch (Exception e) {
            // ok
        }

        // 2. Producer with valid key name
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/myenc-ns/myenc-topic1")
                .addEncryptionKey("client-rsa.pem")
                .cryptoKeyReader(new EncKeyReader())
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // 3. KeyReder is not set by consumer
        // Receive should fail since key reader is not setup
        msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
        assertNull(msg, "Receive should have failed with no keyreader");

        // 4. Set consumer config to consume even if decryption fails
        consumer.close();
        consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/myenc-ns/myenc-topic1")
                .subscriptionName("my-subscriber-name").cryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        int msgNum = 0;
        try {
            // Receive should proceed and deliver encrypted message
            msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + msgNum++;
            Assert.assertNotEquals(receivedMessage, expectedMessage, "Received encrypted message " + receivedMessage
                    + " should not match the expected message " + expectedMessage);
            consumer.acknowledgeCumulative(msg);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to receive message even after ConsumerCryptoFailureAction.CONSUME is set.");
        }

        // 5. Set keyreader and failure action
        consumer.close();
        // Set keyreader
        consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/myenc-ns/myenc-topic1")
                .subscriptionName("my-subscriber-name").cryptoFailureAction(ConsumerCryptoFailureAction.FAIL)
                .cryptoKeyReader(new EncKeyReader()).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        for (int i = msgNum; i < totalMsg - 1; i++) {
            msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
            // verify that encrypted message contains encryption-context
            msg.getEncryptionCtx()
                    .orElseThrow(() -> new IllegalStateException("encryption-ctx not present for encrypted message"));
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

        // 6. Set consumer config to discard if decryption fails
        consumer.close();
        consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/myenc-ns/myenc-topic1")
                .subscriptionName("my-subscriber-name").cryptoFailureAction(ConsumerCryptoFailureAction.DISCARD)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        // Receive should proceed and discard encrypted messages
        msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
        assertNull(msg, "Message received even aftet ConsumerCryptoFailureAction.DISCARD is set.");
    }

    @Test
    public void testEncryptionConsumerWithoutCryptoReader() throws Exception {

        final String encryptionKeyName = "client-rsa.pem";
        final String encryptionKeyVersion = "1.0";
        Map<String, String> metadata = new HashMap<>();
        metadata.put("version", encryptionKeyVersion);
        class EncKeyReader implements CryptoKeyReader {
            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        keyInfo.setMetadata(metadata);
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
                        keyInfo.setMetadata(metadata);
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

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/myrsa-topic3")
                .addEncryptionKey(encryptionKeyName).compressionType(CompressionType.LZ4)
                .cryptoKeyReader(new EncKeyReader()).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topicsPattern("persistent://my-property/my-ns/myrsa-topic3")
                .subscriptionName("my-subscriber-name").cryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
                .subscribe();

        String message = "my-message";
        producer.send(message.getBytes());

        TopicMessageImpl<byte[]> msg = (TopicMessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);

        String receivedMessage = decryptMessage(msg, encryptionKeyName, new EncKeyReader());
        assertEquals(message, receivedMessage);

        consumer.close();
    }

    private String decryptMessage(TopicMessageImpl<byte[]> msg, String encryptionKeyName, CryptoKeyReader reader)
            throws Exception {
        Optional<EncryptionContext> ctx = msg.getEncryptionCtx();
        assertTrue(ctx.isPresent());
        EncryptionContext encryptionCtx = ctx
                .orElseThrow(() -> new IllegalStateException("encryption-ctx not present for encrypted message"));

        Map<String, EncryptionContext.EncryptionKey> keys = encryptionCtx.getKeys();
        assertEquals(keys.size(), 1);
        EncryptionContext.EncryptionKey encryptionKey = keys.get(encryptionKeyName);
        byte[] dataKey = encryptionKey.getKeyValue();
        Map<String, String> metadata = encryptionKey.getMetadata();
        String version = metadata.get("version");
        assertEquals(version, "1.0");

        CompressionType compressionType = encryptionCtx.getCompressionType();
        int uncompressedSize = encryptionCtx.getUncompressedMessageSize();
        byte[] encrParam = encryptionCtx.getParam();
        String encAlgo = encryptionCtx.getAlgorithm();
        int batchSize = encryptionCtx.getBatchSize().orElse(0);

        ByteBuffer payloadBuf = ByteBuffer.wrap(msg.getData());
        // try to decrypt use default MessageCryptoBc
        MessageCrypto crypto = new MessageCryptoBc("test", false);
        MessageMetadata msgMetadata = new MessageMetadata()
                .setEncryptionParam(encrParam)
                .setProducerName("test")
                .setSequenceId(123)
                .setPublishTime(12333453454L)
                .setCompression(CompressionCodecProvider.convertToWireProtocol(compressionType))
                .setUncompressedSize(uncompressedSize);

        if (encAlgo != null) {
            msgMetadata.setEncryptionAlgo(encAlgo);
        }

        msgMetadata.addEncryptionKey()
                .setKey(encryptionKeyName)
                .setValue(dataKey);

        ByteBuffer decryptedPayload = ByteBuffer.allocate(crypto.getMaxOutputSize(payloadBuf.remaining()));
        crypto.decrypt(() -> msgMetadata, payloadBuf, decryptedPayload, reader);

        // try to uncompress
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        ByteBuf uncompressedPayload = codec.decode(Unpooled.wrappedBuffer(decryptedPayload), uncompressedSize);

        if (batchSize > 0) {
            SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
            uncompressedPayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                    singleMessageMetadata, 0, batchSize);
        }

        byte[] data = new byte[uncompressedPayload.readableBytes()];
        uncompressedPayload.readBytes(data);
        uncompressedPayload.release();
        return new String(data);
    }

}
