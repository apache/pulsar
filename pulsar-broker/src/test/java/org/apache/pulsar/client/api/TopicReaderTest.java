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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Cleanup;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsReaderImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.RelativeTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "flaky")
public class TopicReaderTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(TopicReaderTest.class);

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider
    public static Object[][] variationsForExpectedPos() {
        return new Object[][] {
                // batching / start-inclusive / num-of-messages
                {true, true, 10 },
                {true, false, 10 },
                {false, true, 10 },
                {false, false, 10 },

                {true, true, 100 },
                {true, false, 100 },
                {false, true, 100 },
                {false, false, 100 },
        };
    }

    @DataProvider
    public static Object[][] variationsForResetOnLatestMsg() {
        return new Object[][] {
                // start-inclusive / num-of-messages
                {true, 20},
                {false, 20}
        };
    }

    @DataProvider
    public static Object[][] variationsForHasMessageAvailable() {
        return new Object[][] {
                // batching / start-inclusive
                {true,  true},
                {true,  false},
                {false, true},
                {false, false},
        };
    }

    @Test
    public void testSimpleReader() throws Exception {
        Reader<byte[]> reader = pulsarClient.newReader().topic("persistent://my-property/my-ns/testSimpleReader")
                .startMessageId(MessageId.earliest).create();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/testSimpleReader")
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = reader.readNext(1, TimeUnit.SECONDS);

            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Acknowledge the consumption of all messages at once
        reader.close();
        producer.close();
    }

    @Test
    public void testSimpleMultiReader() throws Exception {
        String topic = "persistent://my-property/my-ns/testSimpleMultiReader";
        admin.topics().createPartitionedTopic(topic, 3);

        Reader<byte[]> reader = pulsarClient.newReader().topic(topic)
                .startMessageId(MessageId.earliest).create();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = reader.readNext(1, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            assertTrue(messageSet.add(receivedMessage));
        }

        reader.close();
        producer.close();
    }

    @Test
    public void testReaderAfterMessagesWerePublished() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/testReaderAfterMessagesWerePublished")
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Reader<byte[]> reader = pulsarClient.newReader()
                .topic("persistent://my-property/my-ns/testReaderAfterMessagesWerePublished")
                .startMessageId(MessageId.earliest).create();

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = reader.readNext(1, TimeUnit.SECONDS);

            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Acknowledge the consumption of all messages at once
        reader.close();
        producer.close();
    }

    @Test
    public void testMultiReaderAfterMessagesWerePublished() throws Exception {
        String topic = "persistent://my-property/my-ns/testMultiReaderAfterMessagesWerePublished";
        admin.topics().createPartitionedTopic(topic, 3);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Reader<byte[]> reader = pulsarClient.newReader().topic(topic)
                .startMessageId(MessageId.earliest).create();

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = reader.readNext(1, TimeUnit.SECONDS);

            String receivedMessage = new String(msg.getData());
            assertTrue(messageSet.add(receivedMessage));
        }

        reader.close();
        producer.close();
    }

    @Test
    public void testMultipleReaders() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/testMultipleReaders")
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Reader<byte[]> reader1 = pulsarClient.newReader().topic("persistent://my-property/my-ns/testMultipleReaders")
                .startMessageId(MessageId.earliest).create();

        Reader<byte[]> reader2 = pulsarClient.newReader().topic("persistent://my-property/my-ns/testMultipleReaders")
                .startMessageId(MessageId.earliest).create();

        Message<byte[]> msg = null;
        Set<String> messageSet1 = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = reader1.readNext(1, TimeUnit.SECONDS);

            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet1, receivedMessage, expectedMessage);
        }

        Set<String> messageSet2 = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = reader2.readNext(1, TimeUnit.SECONDS);

            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet2, receivedMessage, expectedMessage);
        }

        reader1.close();
        reader2.close();
        producer.close();
    }

    @Test
    public void testMultiMultipleReaders() throws Exception {
        final String topic = "persistent://my-property/my-ns/testMultiMultipleReaders";
        admin.topics().createPartitionedTopic(topic, 3);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Reader<byte[]> reader1 = pulsarClient.newReader().topic(topic)
                .startMessageId(MessageId.earliest).create();

        Reader<byte[]> reader2 = pulsarClient.newReader().topic(topic)
                .startMessageId(MessageId.earliest).create();

        Message<byte[]> msg = null;
        Set<String> messageSet1 = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = reader1.readNext(1, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            assertTrue(messageSet1.add(receivedMessage));
        }

        Set<String> messageSet2 = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = reader2.readNext(1, TimeUnit.SECONDS);

            String receivedMessage = new String(msg.getData());
            assertTrue(messageSet2.add(receivedMessage));
        }

        reader1.close();
        reader2.close();
        producer.close();
    }

    @Test
    public void testTopicStats() throws Exception {
        String topicName = "persistent://my-property/my-ns/testTopicStats";

        Reader<byte[]> reader1 = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();

        Reader<byte[]> reader2 = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();

        TopicStats stats = admin.topics().getStats(topicName);
        assertEquals(stats.getSubscriptions().size(), 2);

        reader1.close();
        stats = admin.topics().getStats(topicName);
        assertEquals(stats.getSubscriptions().size(), 1);

        reader2.close();

        stats = admin.topics().getStats(topicName);
        assertEquals(stats.getSubscriptions().size(), 0);
    }

    @Test
    public void testMultiTopicStats() throws Exception {
        String topicName = "persistent://my-property/my-ns/testMultiTopicStats";
        admin.topics().createPartitionedTopic(topicName, 3);

        Reader<byte[]> reader1 = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();

        Reader<byte[]> reader2 = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();

        TopicStats stats = admin.topics().getPartitionedStats(topicName,true);
        assertEquals(stats.getSubscriptions().size(), 2);

        reader1.close();
        stats = admin.topics().getPartitionedStats(topicName, true);
        assertEquals(stats.getSubscriptions().size(), 1);

        reader2.close();

        stats = admin.topics().getPartitionedStats(topicName, true);
        assertEquals(stats.getSubscriptions().size(), 0);
    }

    @Test(dataProvider = "variationsForResetOnLatestMsg")
    public void testReaderOnLatestMessage(boolean startInclusive, int numOfMessages) throws Exception {
        final String topicName = "persistent://my-property/my-ns/ReaderOnLatestMessage";
        final int halfOfMsgs = numOfMessages / 2;

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        for (int i = 0; i < halfOfMsgs; i++) {
            producer.send(String.format("my-message-%d", i).getBytes());
        }

        ReaderBuilder<byte[]> readerBuilder = pulsarClient.newReader()
                .topic(topicName)
                .startMessageId(MessageId.latest);

        if (startInclusive) {
            readerBuilder.startMessageIdInclusive();
        }

        Reader<byte[]> reader = readerBuilder.create();

        for (int i = halfOfMsgs; i < numOfMessages; i++) {
            producer.send(String.format("my-message-%d", i).getBytes());
        }

        // Publish more messages and verify the readers only sees new messages
        Set<String> messageSet = Sets.newHashSet();
        for (int i = halfOfMsgs; i < numOfMessages; i++) {
            Message<byte[]> message = reader.readNext();
            String receivedMessage = new String(message.getData());
            String expectedMessage = String.format("my-message-%d", i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        assertTrue(reader.isConnected());
        assertEquals(((ReaderImpl) reader).getConsumer().numMessagesInQueue(), 0);
        assertEquals(messageSet.size(), halfOfMsgs);

        // Acknowledge the consumption of all messages at once
        reader.close();
        producer.close();
    }

    @Test(dataProvider = "variationsForResetOnLatestMsg")
    public void testMultiReaderOnLatestMessage(boolean startInclusive, int numOfMessages) throws Exception {
        final String topicName = "persistent://my-property/my-ns/testMultiReaderOnLatestMessage" + System.currentTimeMillis();
        admin.topics().createPartitionedTopic(topicName, 3);
        final int halfOfMsgs = numOfMessages / 2;

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        Set<byte[]> oldMessage = new HashSet<>();
        for (int i = 0; i < halfOfMsgs; i++) {
            byte[] message = String.format("my-message-%d", i).getBytes();
            producer.send(message);
            oldMessage.add(message);
        }

        ReaderBuilder<byte[]> readerBuilder = pulsarClient.newReader()
                .topic(topicName)
                .startMessageId(MessageId.latest);

        if (startInclusive) {
            readerBuilder.startMessageIdInclusive();
        }

        Reader<byte[]> reader = readerBuilder.create();

        for (int i = halfOfMsgs; i < numOfMessages; i++) {
            producer.send(String.format("my-message-%d", i).getBytes());
        }

        // Publish more messages and verify the readers only sees new messages
        Set<String> messageSet = Sets.newHashSet();
        for (int i = halfOfMsgs; i < numOfMessages; i++) {
            Message<byte[]> message = reader.readNext();
            assertFalse(oldMessage.contains(message));
            String receivedMessage = new String(message.getData());
            assertTrue(messageSet.add(receivedMessage));
        }

        assertTrue(reader.isConnected());
        assertEquals(((MultiTopicsReaderImpl) reader).getMultiTopicsConsumer().numMessagesInQueue(), 0);
        assertEquals(messageSet.size(), halfOfMsgs);

        producer.close();
        reader.close();
    }

    @Test
    public void testReaderOnSpecificMessage() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/testReaderOnSpecificMessage")
                .create();
        List<MessageId> messageIds = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            messageIds.add(producer.send(message.getBytes()));
        }

        Reader<byte[]> reader = pulsarClient.newReader().topic("persistent://my-property/my-ns/testReaderOnSpecificMessage")
                .startMessageId(messageIds.get(4)).create();

        // Publish more messages and verify the readers only sees messages starting from the intended message
        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 5; i < 10; i++) {
            msg = reader.readNext(1, TimeUnit.SECONDS);

            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Acknowledge the consumption of all messages at once
        reader.close();
        producer.close();
    }

    @Test
    public void testReaderOnSpecificMessageWithBatches() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/testReaderOnSpecificMessageWithBatches").enableBatching(true)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS).create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }

        // Write one sync message to ensure everything before got persistend
        producer.send("my-message-10".getBytes());
        Reader<byte[]> reader1 = pulsarClient.newReader()
                .topic("persistent://my-property/my-ns/testReaderOnSpecificMessageWithBatches")
                .startMessageId(MessageId.earliest).create();

        MessageId lastMessageId = null;
        for (int i = 0; i < 5; i++) {
            Message<byte[]> msg = reader1.readNext();
            lastMessageId = msg.getMessageId();
        }

        assertEquals(lastMessageId.getClass(), BatchMessageIdImpl.class);

        System.out.println("CREATING READER ON MSG ID: " + lastMessageId);

        Reader<byte[]> reader2 = pulsarClient.newReader()
                .topic("persistent://my-property/my-ns/testReaderOnSpecificMessageWithBatches")
                .startMessageId(lastMessageId).create();

        for (int i = 5; i < 11; i++) {
            Message<byte[]> msg = reader2.readNext(1, TimeUnit.SECONDS);

            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            assertEquals(receivedMessage, expectedMessage);
        }

        producer.close();
    }

    @Test
    public void testECDSAEncryption() throws Exception {
        log.info("-- Starting {} test --", methodName);

        class EncKeyReader implements CryptoKeyReader {

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

        final int totalMsg = 10;

        Set<String> messageSet = Sets.newHashSet();
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic("persistent://my-property/my-ns/test-reader-myecdsa-topic1").startMessageId(MessageId.latest)
                .cryptoKeyReader(new EncKeyReader()).create();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/test-reader-myecdsa-topic1")
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader()).create();
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;

        for (int i = 0; i < totalMsg; i++) {
            msg = reader.readNext(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        producer.close();
        reader.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testMultiReaderECDSAEncryption() throws Exception {
        log.info("-- Starting {} test --", methodName);

        class EncKeyReader implements CryptoKeyReader {

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

        final int totalMsg = 10;

        Set<String> messageSet = Sets.newHashSet();
        String topic = "persistent://my-property/my-ns/test-multi-reader-myecdsa-topic1";
        admin.topics().createPartitionedTopic(topic, 3);
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic).startMessageId(MessageId.latest)
                .cryptoKeyReader(new EncKeyReader()).create();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader()).create();
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;

        for (int i = 0; i < totalMsg; i++) {
            msg = reader.readNext(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            Assert.assertTrue(messageSet.add(receivedMessage), "Received duplicate message " + receivedMessage);
        }
        producer.close();
        reader.close();
    }

    @Test
    public void testDefaultCryptoKeyReader() throws Exception {
        final String topic = "persistent://my-property/my-ns/test-reader-default-crypto-key-reader"
                + System.currentTimeMillis();
        final String ecdsaPublicKeyFile = "file:./src/test/resources/certificate/public-key.client-ecdsa.pem";
        final String ecdsaPrivateKeyFile = "file:./src/test/resources/certificate/private-key.client-ecdsa.pem";
        final String ecdsaPublicKeyData = "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUlIS01JR2pCZ2NxaGtqT1BRSUJNSUdYQWdFQk1Cd0dCeXFHU000OUFRRUNFUUQvLy8vOS8vLy8vLy8vLy8vLwovLy8vTURzRUVQLy8vLzMvLy8vLy8vLy8vLy8vLy93RUVPaDFlY0VRZWZROTJDU1pQQ3p1WHRNREZRQUFEZzFOCmFXNW5hSFZoVVhVTXdEcEVjOUEyZVFRaEJCWWY5MUtMaVpzdERDaGdmS1VzVzRiUFdzZzVXNi9yRThBdG9wTGQKN1hxREFoRUEvLy8vL2dBQUFBQjFvdzBia0RpaEZRSUJBUU1pQUFUcktqNlJQSEdQTktjWktJT2NjTjR0Z0VOTQpuMWR6S2pMck1aVGtKNG9BYVE9PQotLS0tLUVORCBQVUJMSUMgS0VZLS0tLS0K";
        final String ecdsaPrivateKeyData = "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBFQyBQQVJBTUVURVJTLS0tLS0KTUlHWEFnRUJNQndHQnlxR1NNNDlBUUVDRVFELy8vLzkvLy8vLy8vLy8vLy8vLy8vTURzRUVQLy8vLzMvLy8vLwovLy8vLy8vLy8vd0VFT2gxZWNFUWVmUTkyQ1NaUEN6dVh0TURGUUFBRGcxTmFXNW5hSFZoVVhVTXdEcEVjOUEyCmVRUWhCQllmOTFLTGlac3REQ2hnZktVc1c0YlBXc2c1VzYvckU4QXRvcExkN1hxREFoRUEvLy8vL2dBQUFBQjEKb3cwYmtEaWhGUUlCQVE9PQotLS0tLUVORCBFQyBQQVJBTUVURVJTLS0tLS0KLS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1JSFlBZ0VCQkJEZXU5aGM4a092TDNwbCtMWVNqTHE5b0lHYU1JR1hBZ0VCTUJ3R0J5cUdTTTQ5QVFFQ0VRRC8KLy8vOS8vLy8vLy8vLy8vLy8vLy9NRHNFRVAvLy8vMy8vLy8vLy8vLy8vLy8vL3dFRU9oMWVjRVFlZlE5MkNTWgpQQ3p1WHRNREZRQUFEZzFOYVc1bmFIVmhVWFVNd0RwRWM5QTJlUVFoQkJZZjkxS0xpWnN0RENoZ2ZLVXNXNGJQCldzZzVXNi9yRThBdG9wTGQ3WHFEQWhFQS8vLy8vZ0FBQUFCMW93MGJrRGloRlFJQkFhRWtBeUlBQk9zcVBwRTgKY1k4MHB4a29nNXh3M2kyQVEweWZWM01xTXVzeGxPUW5pZ0JwCi0tLS0tRU5EIEVDIFBSSVZBVEUgS0VZLS0tLS0K";
        final String rsaPublicKeyFile = "file:./src/test/resources/certificate/public-key.client-rsa.pem";
        final String rsaPrivateKeyFile = "file:./src/test/resources/certificate/private-key.client-rsa.pem";
        final String rsaPublicKeyData = "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF0S1d3Z3FkblRZck9DditqMU1rVApXZlNIMHdDc0haWmNhOXdBVzNxUDR1dWhsQnZuYjEwSmNGZjVaanpQOUJTWEsrdEhtSTh1b04zNjh2RXY2eWhVClJITTR5dVhxekN4enVBd2tRU28zOXJ6WDhQR0M3cWRqQ043TERKM01ucWlCSXJVc1NhRVAxd3JOc0Ixa0krbzkKRVIxZTVPL3VFUEFvdFA5MzNoSFEwSjJoTUVla0hxTDdzQmxKOThoNk5tc2ljRWFVa2FyZGswVE9YcmxrakMrYwpNZDhaYkdTY1BxSTlNMzhibW4zT0x4RlRuMXZ0aHB2blhMdkNtRzRNKzZ4dFl0RCtucGNWUFp3MWkxUjkwZk1zCjdwcFpuUmJ2OEhjL0RGZE9LVlFJZ2FtNkNEZG5OS2dXN2M3SUJNclAwQUVtMzdIVHUwTFNPalAyT0hYbHZ2bFEKR1FJREFRQUIKLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg==";
        final String rsaPrivateKeyData = "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFb3dJQkFBS0NBUUVBdEtXd2dxZG5UWXJPQ3YrajFNa1RXZlNIMHdDc0haWmNhOXdBVzNxUDR1dWhsQnZuCmIxMEpjRmY1Wmp6UDlCU1hLK3RIbUk4dW9OMzY4dkV2NnloVVJITTR5dVhxekN4enVBd2tRU28zOXJ6WDhQR0MKN3FkakNON0xESjNNbnFpQklyVXNTYUVQMXdyTnNCMWtJK285RVIxZTVPL3VFUEFvdFA5MzNoSFEwSjJoTUVlawpIcUw3c0JsSjk4aDZObXNpY0VhVWthcmRrMFRPWHJsa2pDK2NNZDhaYkdTY1BxSTlNMzhibW4zT0x4RlRuMXZ0Cmhwdm5YTHZDbUc0TSs2eHRZdEQrbnBjVlBadzFpMVI5MGZNczdwcFpuUmJ2OEhjL0RGZE9LVlFJZ2FtNkNEZG4KTktnVzdjN0lCTXJQMEFFbTM3SFR1MExTT2pQMk9IWGx2dmxRR1FJREFRQUJBb0lCQUFhSkZBaTJDN3UzY05yZgpBc3RZOXZWRExvTEl2SEZabGtCa3RqS1pEWW1WSXNSYitoU0NWaXdWVXJXTEw2N1I2K0l2NGVnNERlVE9BeDAwCjhwbmNYS2daVHcyd0liMS9RalIvWS9SamxhQzhsa2RtUldsaTd1ZE1RQ1pWc3lodVNqVzZQajd2cjhZRTR3b2oKRmhOaWp4RUdjZjl3V3JtTUpyemRuVFdRaVhCeW8rZVR2VVE5QlBnUEdyUmpzTVptVGtMeUFWSmZmMkRmeE81YgpJV0ZEWURKY3lZQU1DSU1RdTd2eXMvSTUwb3U2aWxiMUNPNlFNNlo3S3BQZU9vVkZQd3R6Ymg4Y2Y5eE04VU5TCmo2Si9KbWRXaGdJMzRHUzNOQTY4eFRRNlBWN3pqbmhDYytpY2NtM0pLeXpHWHdhQXBBWitFb2NlLzlqNFdLbXUKNUI0emlSMENnWUVBM2wvOU9IYmwxem15VityUnhXT0lqL2kyclR2SHp3Qm5iblBKeXVlbUw1Vk1GZHBHb2RRMwp2d0h2eVFtY0VDUlZSeG1Yb2pRNFF1UFBIczNxcDZ3RUVGUENXeENoTFNUeGxVYzg1U09GSFdVMk85OWpWN3pJCjcrSk9wREsvTXN0c3g5bkhnWGR1SkYrZ2xURnRBM0xIOE9xeWx6dTJhRlBzcHJ3S3VaZjk0UThDZ1lFQXovWngKYWtFRytQRU10UDVZUzI4Y1g1WGZqc0lYL1YyNkZzNi9zSDE2UWpVSUVkZEU1VDRmQ3Vva3hDalNpd1VjV2htbApwSEVKNVM1eHAzVllSZklTVzNqUlczcXN0SUgxdHBaaXBCNitTMHpUdUptTEpiQTNJaVdFZzJydE10N1gxdUp2CkEvYllPcWUwaE9QVHVYdVpkdFZaMG5NVEtrN0dHOE82VmtCSTdGY0NnWUVBa0RmQ21zY0pnczdKYWhsQldIbVgKekg5cHdlbStTUEtqSWMvNE5CNk4rZGdpa3gyUHAwNWhwUC9WaWhVd1lJdWZ2cy9MTm9nVllOUXJ0SGVwVW5yTgoyK1RtYkhiWmdOU3YxTGR4dDgyVWZCN3kwRnV0S3U2bGhtWEh5TmVjaG8zRmk4c2loMFYwYWlTV21ZdUhmckFICkdhaXNrRVpLbzFpaVp2UVhKSXg5TzJNQ2dZQVRCZjByOWhUWU10eXh0YzZIMy9zZGQwMUM5dGhROGdEeTB5alAKMFRxYzBkTVNKcm9EcW1JV2tvS1lldzkvYmhGQTRMVzVUQ25Xa0NBUGJIbU50RzRmZGZiWXdta0gvaGRuQTJ5MApqS2RscGZwOEdYZVVGQUdIR3gxN0ZBM3NxRnZnS1VoMGVXRWdSSFVMN3ZkUU1WRkJnSlM5M283elFNOTRmTGdQCjZjT0I4d0tCZ0ZjR1Y0R2pJMld3OWNpbGxhQzU1NE12b1NqZjhCLyswNGtYekRPaDhpWUlJek85RVVpbDFqaksKSnZ4cDRobkx6VEtXYnV4M01FV3F1ckxrWWFzNkdwS0JqdytpTk9DYXI2WWRxV0dWcU0zUlV4N1BUVWFad2tLeApVZFA2M0lmWTdpWkNJVC9RYnlIUXZJVWUyTWFpVm5IK3VseGRrSzZZNWU3Z3hjYmNrSUg0Ci0tLS0tRU5EIFJTQSBQUklWQVRFIEtFWS0tLS0tCg==";
        final int numMsg = 10;

        Map<String, String> privateKeyFileMap = Maps.newHashMap();
        privateKeyFileMap.put("client-ecdsa.pem", ecdsaPrivateKeyFile);
        privateKeyFileMap.put("client-rsa.pem", rsaPrivateKeyFile);
        Map<String, String> privateKeyDataMap = Maps.newHashMap();
        privateKeyDataMap.put("client-ecdsa.pem", ecdsaPrivateKeyData);
        privateKeyDataMap.put("client-rsa.pem", rsaPrivateKeyData);

        Reader<byte[]> reader1 = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                .defaultCryptoKeyReader(ecdsaPrivateKeyFile).create();
        Reader<byte[]> reader2 = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                .defaultCryptoKeyReader(ecdsaPrivateKeyData).create();
        Reader<byte[]> reader3 = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                .defaultCryptoKeyReader(privateKeyFileMap).create();
        Reader<byte[]> reader4 = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                .defaultCryptoKeyReader(privateKeyDataMap).create();

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topic).addEncryptionKey("client-ecdsa.pem")
                .defaultCryptoKeyReader(ecdsaPublicKeyFile).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topic).addEncryptionKey("client-ecdsa.pem")
                .defaultCryptoKeyReader(ecdsaPublicKeyData).create();

        for (int i = 0; i < numMsg; i++) {
            producer1.send(("my-message-" + i).getBytes());
        }
        for (int i = numMsg; i < numMsg * 2; i++) {
            producer2.send(("my-message-" + i).getBytes());
        }

        producer1.close();
        producer2.close();

        for (Reader<byte[]> reader : (List<Reader<byte[]>>) Lists.newArrayList(reader1, reader2)) {
            for (int i = 0; i < numMsg * 2; i++) {
                MessageImpl<byte[]> msg = (MessageImpl<byte[]>) reader.readNext(5, TimeUnit.SECONDS);
                // verify that encrypted message contains encryption-context
                msg.getEncryptionCtx().orElseThrow(
                        () -> new IllegalStateException("encryption-ctx not present for encrypted message"));
                assertEquals(new String(msg.getData()), "my-message-" + i);
            }
        }

        reader1.close();
        reader2.close();

        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topic).addEncryptionKey("client-rsa.pem")
                .defaultCryptoKeyReader(rsaPublicKeyFile).create();
        Producer<byte[]> producer4 = pulsarClient.newProducer().topic(topic).addEncryptionKey("client-rsa.pem")
                .defaultCryptoKeyReader(rsaPublicKeyData).create();

        for (int i = numMsg * 2; i < numMsg * 3; i++) {
            producer3.send(("my-message-" + i).getBytes());
        }
        for (int i = numMsg * 3; i < numMsg * 4; i++) {
            producer4.send(("my-message-" + i).getBytes());
        }

        producer3.close();
        producer4.close();

        for (Reader<byte[]> reader : (List<Reader<byte[]>>) Lists.newArrayList(reader3, reader4)) {
            for (int i = 0; i < numMsg * 4; i++) {
                MessageImpl<byte[]> msg = (MessageImpl<byte[]>) reader.readNext(5, TimeUnit.SECONDS);
                // verify that encrypted message contains encryption-context
                msg.getEncryptionCtx().orElseThrow(
                        () -> new IllegalStateException("encryption-ctx not present for encrypted message"));
                assertEquals(new String(msg.getData()), "my-message-" + i);
            }
        }

        reader3.close();
        reader4.close();
    }

    @Test
    public void testSimpleReaderReachEndOfTopic() throws Exception {
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic("persistent://my-property/my-ns/testSimpleReaderReachEndOfTopic")
                .startMessageId(MessageId.earliest).create();
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/testSimpleReaderReachEndOfTopic")
                .create();

        // no data write, should return false
        assertFalse(reader.hasMessageAvailable());

        // produce message 0 -- 99
        for (int i = 0; i < 100; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        MessageImpl<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        int index = 0;

        // read message till end.
        while (reader.hasMessageAvailable()) {
            msg = (MessageImpl<byte[]>) reader.readNext(1, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + (index++);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        assertEquals(index, 100);
        // readNext should return null, after reach the end of topic.
        assertNull(reader.readNext(1, TimeUnit.SECONDS));

        // produce message again.
        for (int i = 100; i < 200; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // read message till end again.
        while (reader.hasMessageAvailable()) {
            msg = (MessageImpl<byte[]>) reader.readNext(1, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + (index++);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        assertEquals(index, 200);
        // readNext should return null, after reach the end of topic.
        assertNull(reader.readNext(1, TimeUnit.SECONDS));

        reader.close();
        producer.close();
    }

    @Test
    public void testSimpleMultiReaderReachEndOfTopic() throws Exception {
        String topic = "persistent://my-property/my-ns/testSimpleMultiReaderReachEndOfTopic";
        admin.topics().createPartitionedTopic(topic,3);
        Reader<byte[]> reader = pulsarClient.newReader().topic(topic).startMessageId(MessageId.earliest).create();
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        // no data write, should return false
        assertFalse(reader.hasMessageAvailable());

        // produce message 0 -- 99
        for (int i = 0; i < 100; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        TopicMessageImpl<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        int index = 0;

        // read message till end.
        while (reader.hasMessageAvailable()) {
            msg = (TopicMessageImpl<byte[]>) reader.readNext(1, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            index++;
            Assert.assertTrue(messageSet.add(receivedMessage), "Received duplicate message " + receivedMessage);
        }

        assertEquals(index, 100);
        // readNext should return null, after reach the end of topic.
        assertNull(reader.readNext(1, TimeUnit.SECONDS));

        // produce message again.
        for (int i = 100; i < 200; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // read message till end again.
        while (reader.hasMessageAvailable()) {
            msg = (TopicMessageImpl<byte[]>) reader.readNext(1, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            index++;
            Assert.assertTrue(messageSet.add(receivedMessage), "Received duplicate message " + receivedMessage);
        }

        assertEquals(index, 200);
        // readNext should return null, after reach the end of topic.
        assertNull(reader.readNext(1, TimeUnit.SECONDS));

        reader.close();
        producer.close();
    }

    @Test
    public void testReaderReachEndOfTopicOnMessageWithBatches() throws Exception {
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic("persistent://my-property/my-ns/testReaderReachEndOfTopicOnMessageWithBatches")
                .startMessageId(MessageId.earliest).create();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/testReaderReachEndOfTopicOnMessageWithBatches")
                .enableBatching(true).batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS).create();

        // no data write, should return false
        assertFalse(reader.hasMessageAvailable());

        for (int i = 0; i < 100; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }

        // Write one sync message to ensure everything before got persistend
        producer.send("my-message-10".getBytes());

        MessageId lastMessageId = null;
        int index = 0;
        assertTrue(reader.hasMessageAvailable());

        if (reader.hasMessageAvailable()) {
            Message<byte[]> msg = reader.readNext();
            lastMessageId = msg.getMessageId();
            assertEquals(lastMessageId.getClass(), BatchMessageIdImpl.class);

            while (msg != null) {
                index++;
                msg = reader.readNext(100, TimeUnit.MILLISECONDS);
            }
            assertEquals(index, 101);
        }

        assertFalse(reader.hasMessageAvailable());

        reader.close();
        producer.close();
    }

    @Test
    public void testMultiReaderReachEndOfTopicOnMessageWithBatches() throws Exception {
        String topic = "persistent://my-property/my-ns/testMultiReaderReachEndOfTopicOnMessageWithBatches";
        admin.topics().createPartitionedTopic(topic, 3);
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest).create();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(true).batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS).create();

        // no data write, should return false
        assertFalse(reader.hasMessageAvailable());

        for (int i = 0; i < 100; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }

        // Write one sync message to ensure everything before got persistend
        producer.send("my-message-10".getBytes());

        MessageId lastMessageId = null;
        int index = 0;
        assertTrue(reader.hasMessageAvailable());

        if (reader.hasMessageAvailable()) {
            Message<byte[]> msg = reader.readNext();
            lastMessageId = msg.getMessageId();
            assertEquals(lastMessageId.getClass(), TopicMessageIdImpl.class);

            while (msg != null) {
                index++;
                msg = reader.readNext(100, TimeUnit.MILLISECONDS);
            }
            assertEquals(index, 101);
        }

        assertFalse(reader.hasMessageAvailable());

        reader.close();
        producer.close();
    }

    @Test
    public void testMessageAvailableAfterRestart() throws Exception {
        String topic = "persistent://my-property/use/my-ns/testMessageAvailableAfterRestart";
        String content = "my-message-1";

        // stop retention from cleaning up
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic)
                .startMessageId(MessageId.earliest).create()) {
            assertFalse(reader.hasMessageAvailable());
        }

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create()) {
            producer.send(content.getBytes());
        }

        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic)
                .startMessageId(MessageId.earliest).create()) {
            assertTrue(reader.hasMessageAvailable());
        }

        // cause broker to drop topic. Will be loaded next time we access it
        pulsar.getBrokerService().getTopicReference(topic).get().close(false).get();

        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic)
                .startMessageId(MessageId.earliest).create()) {
            assertTrue(reader.hasMessageAvailable());

            String readOut = new String(reader.readNext().getData());
            assertEquals(content, readOut);
            assertFalse(reader.hasMessageAvailable());
        }

    }

    @Test
    public void testMultiReaderMessageAvailableAfterRestart() throws Exception {
        String topic = "persistent://my-property/use/my-ns/testMessageAvailableAfterRestart2";
        String content = "my-message-1";
        admin.topics().createPartitionedTopic(topic, 3);
        // stop retention from cleaning up
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub2").subscribe().close();

        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic)
                .startMessageId(MessageId.earliest).create()) {
            assertFalse(reader.hasMessageAvailable());
        }

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create()) {
            producer.send(content.getBytes());
        }

        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic)
                .startMessageId(MessageId.earliest).create()) {
            assertTrue(reader.hasMessageAvailable());
        }

        // cause broker to drop topic. Will be loaded next time we access it
        pulsar.getBrokerService().getTopics().keys().forEach(topicName -> {
            try {
                pulsar.getBrokerService().getTopicReference(topicName).get().close(false).get();
            } catch (Exception e) {
                fail();
            }
        });

        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic)
                .startMessageId(MessageId.earliest).create()) {
            assertTrue(reader.hasMessageAvailable());

            String readOut = new String(reader.readNext().getData());
            assertEquals(content, readOut);
            assertFalse(reader.hasMessageAvailable());
        }

    }

    @Test(dataProvider = "variationsForHasMessageAvailable")
    public void testHasMessageAvailable(boolean enableBatch, boolean startInclusive) throws Exception {
        final String topicName = "persistent://my-property/my-ns/HasMessageAvailable";
        final int numOfMessage = 100;

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic(topicName);

        if (enableBatch) {
            producerBuilder
                    .enableBatching(true)
                    .batchingMaxMessages(10);
        } else {
            producerBuilder
                    .enableBatching(false);
        }

        Producer<byte[]> producer = producerBuilder.create();

        CountDownLatch latch = new CountDownLatch(numOfMessage);

        List<MessageId> allIds = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numOfMessage; i++) {
            producer.sendAsync(String.format("msg num %d", i).getBytes()).whenComplete((mid, e) -> {
                if (e != null) {
                    Assert.fail();
                } else {
                    allIds.add(mid);
                }
                latch.countDown();
            });
        }

        latch.await();

        allIds.sort(null); // make sure the largest mid appears at last.

        for (MessageId id : allIds) {
            Reader<byte[]> reader;

            if (startInclusive) {
                reader = pulsarClient.newReader().topic(topicName)
                        .startMessageId(id).startMessageIdInclusive().create();
            } else {
                reader = pulsarClient.newReader().topic(topicName)
                        .startMessageId(id).create();
            }
            if (startInclusive) {
                assertTrue(reader.hasMessageAvailable());
            } else if (id != allIds.get(allIds.size() - 1)) {
                assertTrue(reader.hasMessageAvailable());
            } else {
                assertFalse(reader.hasMessageAvailable());
            }
            reader.close();
        }

        producer.close();
    }

    @Test(timeOut = 20000)
    public void testHasMessageAvailableWithBatch() throws Exception {
        final String topicName = "persistent://my-property/my-ns/testHasMessageAvailableWithBatch";
        final int numOfMessage = 10;

        Producer<byte[]> producer = pulsarClient.newProducer()
                .enableBatching(true)
                .batchingMaxMessages(10)
                .batchingMaxPublishDelay(2,TimeUnit.SECONDS)
                .topic(topicName).create();

        //For batch-messages with single message, the type of client messageId should be the same as that of broker
        MessageIdImpl messageId = (MessageIdImpl) producer.send("msg".getBytes());
        assertTrue(messageId instanceof MessageIdImpl);
        ReaderImpl<byte[]> reader = (ReaderImpl<byte[]>)pulsarClient.newReader().topic(topicName)
                .startMessageId(messageId).startMessageIdInclusive().create();
        MessageIdImpl lastMsgId = (MessageIdImpl) reader.getConsumer().getLastMessageId();
        assertTrue(messageId instanceof BatchMessageIdImpl);
        assertEquals(lastMsgId.getLedgerId(), messageId.getLedgerId());
        assertEquals(lastMsgId.getEntryId(), messageId.getEntryId());
        reader.close();

        CountDownLatch latch = new CountDownLatch(numOfMessage);
        List<MessageId> allIds = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < numOfMessage; i++) {
            producer.sendAsync(String.format("msg num %d", i).getBytes()).whenComplete((mid, e) -> {
                if (e != null) {
                    Assert.fail();
                } else {
                    allIds.add(mid);
                }
                latch.countDown();
            });
        }
        producer.flush();
        latch.await();
        producer.close();

        //For batch-message with multi messages, the type of client messageId should be the same as that of broker
        for (MessageId id : allIds) {
            reader = (ReaderImpl<byte[]>) pulsarClient.newReader().topic(topicName)
                    .startMessageId(id).startMessageIdInclusive().create();
            if (id instanceof BatchMessageIdImpl) {
                MessageId lastMessageId = reader.getConsumer().getLastMessageId();
                assertTrue(lastMessageId instanceof BatchMessageIdImpl);
                log.info("id {} instance of BatchMessageIdImpl",id);
            } else {
                assertTrue(id instanceof MessageIdImpl);
                MessageId lastMessageId = reader.getConsumer().getLastMessageId();
                assertTrue(lastMessageId instanceof MessageIdImpl);
                log.info("id {} instance of MessageIdImpl",id);
            }
            reader.close();
        }
        //For non-batch message, the type of client messageId should be the same as that of broker
        producer = pulsarClient.newProducer()
                .enableBatching(false).topic(topicName).create();
        messageId = (MessageIdImpl) producer.send("non-batch".getBytes());
        assertFalse(messageId instanceof BatchMessageIdImpl);
        assertTrue(messageId instanceof MessageIdImpl);
        reader = (ReaderImpl<byte[]>) pulsarClient.newReader().topic(topicName)
                .startMessageId(messageId).create();
        MessageId lastMessageId = reader.getConsumer().getLastMessageId();
        assertFalse(lastMessageId instanceof BatchMessageIdImpl);
        assertTrue(lastMessageId instanceof MessageIdImpl);
        assertEquals(lastMessageId, messageId);
        producer.close();
        reader.close();
    }

    @Test
    public void testReaderNonDurableIsAbleToSeekRelativeTime() throws Exception {
        final int numOfMessage = 10;
        final String topicName = "persistent://my-property/my-ns/ReaderNonDurableIsAbleToSeekRelativeTime";

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName).create();

        for (int i = 0; i < numOfMessage; i++) {
            producer.send(String.format("msg num %d", i).getBytes());
        }

        Reader<byte[]> reader = pulsarClient.newReader().topic(topicName)
                .startMessageId(MessageId.earliest).create();
        assertTrue(reader.hasMessageAvailable());

        reader.seek(RelativeTimeUtil.parseRelativeTimeInSeconds("-1m"));

        assertTrue(reader.hasMessageAvailable());

        reader.close();
        producer.close();
    }

    @Test
    public void testMultiReaderNonDurableIsAbleToSeekRelativeTime() throws Exception {
        final int numOfMessage = 10;
        final String topicName = "persistent://my-property/my-ns/ReaderNonDurableIsAbleToSeekRelativeTime";
        admin.topics().createPartitionedTopic(topicName, 3);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        for (int i = 0; i < numOfMessage; i++) {
            producer.send(String.format("msg num %d", i).getBytes());
        }

        Reader<byte[]> reader = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();
        assertTrue(reader.hasMessageAvailable());

        reader.seek(RelativeTimeUtil.parseRelativeTimeInSeconds("-1m"));

        assertTrue(reader.hasMessageAvailable());

        reader.close();
        producer.close();
    }

    @Test
    public void testReaderIsAbleToSeekWithTimeOnBeginningOfTopic() throws Exception {
        final String topicName = "persistent://my-property/my-ns/ReaderSeekWithTimeOnBeginningOfTopic";
        final int numOfMessage = 10;

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName).create();

        for (int i = 0; i < numOfMessage; i++) {
            producer.send(String.format("msg num %d", i).getBytes());
        }

        Reader<byte[]> reader = pulsarClient.newReader().topic(topicName)
                .startMessageId(MessageId.earliest).create();

        assertTrue(reader.hasMessageAvailable());

        // Read all messages the first time
        Set<String> messageSetA = Sets.newHashSet();
        for (int i = 0; i < numOfMessage; i++) {
            Message<byte[]> message = reader.readNext();
            String receivedMessage = new String(message.getData());
            String expectedMessage = String.format("msg num %d", i);
            testMessageOrderAndDuplicates(messageSetA, receivedMessage, expectedMessage);
        }

        assertFalse(reader.hasMessageAvailable());

        // Perform cursor reset by time
        reader.seek(RelativeTimeUtil.parseRelativeTimeInSeconds("-1m"));

        // Read all messages a second time after seek()
        Set<String> messageSetB = Sets.newHashSet();
        for (int i = 0; i < numOfMessage; i++) {
            Message<byte[]> message = reader.readNext();
            String receivedMessage = new String(message.getData());
            String expectedMessage = String.format("msg num %d", i);
            testMessageOrderAndDuplicates(messageSetB, receivedMessage, expectedMessage);
        }

        // Reader should be finished
        assertTrue(reader.isConnected());
        assertFalse(reader.hasMessageAvailable());
        assertEquals(((ReaderImpl) reader).getConsumer().numMessagesInQueue(), 0);

        reader.close();
        producer.close();
    }

    @Test
    public void testMultiReaderIsAbleToSeekWithTimeOnBeginningOfTopic() throws Exception {
        final String topicName = "persistent://my-property/my-ns/MultiReaderSeekWithTimeOnBeginningOfTopic";
        final int numOfMessage = 10;
        admin.topics().createPartitionedTopic(topicName, 3);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        for (int i = 0; i < numOfMessage; i++) {
            producer.send(String.format("msg num %d", i).getBytes());
        }

        Reader<byte[]> reader = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();

        assertTrue(reader.hasMessageAvailable());

        // Read all messages the first time
        Set<String> messageSetA = Sets.newHashSet();
        for (int i = 0; i < numOfMessage; i++) {
            Message<byte[]> message = reader.readNext();
            String receivedMessage = new String(message.getData());
            Assert.assertTrue(messageSetA.add(receivedMessage), "Received duplicate message " + receivedMessage);
        }

        assertFalse(reader.hasMessageAvailable());

        // Perform cursor reset by time
        reader.seek(RelativeTimeUtil.parseRelativeTimeInSeconds("-1m"));

        // Read all messages a second time after seek()
        Set<String> messageSetB = Sets.newHashSet();
        for (int i = 0; i < numOfMessage; i++) {
            Message<byte[]> message = reader.readNext();
            String receivedMessage = new String(message.getData());
            Assert.assertTrue(messageSetB.add(receivedMessage), "Received duplicate message " + receivedMessage);
        }

        // Reader should be finished
        assertTrue(reader.isConnected());
        assertFalse(reader.hasMessageAvailable());
        assertEquals(((MultiTopicsReaderImpl) reader).getMultiTopicsConsumer().numMessagesInQueue(), 0);

        reader.close();
        producer.close();
    }

    @Test
    public void testReaderIsAbleToSeekWithMessageIdOnMiddleOfTopic() throws Exception {
        final String topicName = "persistent://my-property/my-ns/ReaderSeekWithMessageIdOnMiddleOfTopic";
        final int numOfMessage = 100;
        final int halfMessages = numOfMessage / 2;

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName).create();

        for (int i = 0; i < numOfMessage; i++) {
            producer.send(String.format("msg num %d", i).getBytes());
        }

        Reader<byte[]> reader = pulsarClient.newReader().topic(topicName)
                .startMessageId(MessageId.earliest).create();

        assertTrue(reader.hasMessageAvailable());

        // Read all messages the first time
        MessageId midmessageToSeek = null;
        Set<String> messageSetA = Sets.newHashSet();
        for (int i = 0; i < numOfMessage; i++) {
            Message<byte[]> message = reader.readNext();
            String receivedMessage = new String(message.getData());
            String expectedMessage = String.format("msg num %d", i);
            testMessageOrderAndDuplicates(messageSetA, receivedMessage, expectedMessage);

            if (i == halfMessages) {
                midmessageToSeek = message.getMessageId();
            }
        }

        assertFalse(reader.hasMessageAvailable());

        // Perform cursor reset by MessageId to half of the topic
        reader.seek(midmessageToSeek);

        // Read all halved messages after seek()
        Set<String> messageSetB = Sets.newHashSet();
        for (int i = halfMessages + 1; i < numOfMessage; i++) {
            Message<byte[]> message = reader.readNext();
            String receivedMessage = new String(message.getData());
            String expectedMessage = String.format("msg num %d", i);
            testMessageOrderAndDuplicates(messageSetB, receivedMessage, expectedMessage);
        }

        // Reader should be finished
        assertTrue(reader.isConnected());
        assertFalse(reader.hasMessageAvailable());
        assertEquals(((ReaderImpl) reader).getConsumer().numMessagesInQueue(), 0);

        reader.close();
        producer.close();
    }

    @Test
    public void testReaderIsAbleToSeekWithTimeOnMiddleOfTopic() throws Exception {
        final String topicName = "persistent://my-property/my-ns/ReaderIsAbleToSeekWithTimeOnMiddleOfTopic";
        final int numOfMessage = 10;
        final int halfMessages = numOfMessage / 2;

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName).create();

        long l = System.currentTimeMillis();
        for (int i = 0; i < numOfMessage; i++) {
            producer.send(String.format("msg num %d", i).getBytes());
            Thread.sleep(100);
        }

        Reader<byte[]> reader = pulsarClient.newReader().topic(topicName)
                .startMessageId(MessageId.earliest).create();

        int plusTime = (halfMessages + 1) * 100;
        reader.seek(l + plusTime);

        Set<String> messageSet = Sets.newHashSet();
        for (int i = halfMessages + 1; i < numOfMessage; i++) {
            Message<byte[]> message = reader.readNext();
            String receivedMessage = new String(message.getData());
            String expectedMessage = String.format("msg num %d", i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        reader.close();
        producer.close();
    }

    @Test
    public void testMultiReaderIsAbleToSeekWithTimeOnMiddleOfTopic() throws Exception {
        final String topicName = "persistent://my-property/my-ns/testMultiReaderIsAbleToSeekWithTimeOnMiddleOfTopic" + System.currentTimeMillis();
        final int numOfMessage = 10;
        final int halfMessages = numOfMessage / 2;
        admin.topics().createPartitionedTopic(topicName, 3);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        long halfTime = 0;
        for (int i = 0; i < numOfMessage; i++) {
            if (i == numOfMessage / 2) {
                halfTime = System.currentTimeMillis();
            }
            producer.send(String.format("msg num %d", i).getBytes());
        }
        Assert.assertTrue(halfTime != 0);
        Reader<byte[]> reader = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();

        reader.seek(halfTime);
        Set<String> messageSet = Sets.newHashSet();
        for (int i = halfMessages + 1; i < numOfMessage; i++) {
            Message<byte[]> message = reader.readNext(10, TimeUnit.SECONDS);
            String receivedMessage = new String(message.getData());
            Assert.assertTrue(messageSet.add(receivedMessage), "Received duplicate message " + receivedMessage);
        }
        reader.close();
        producer.close();
    }

    @Test(dataProvider = "variationsForExpectedPos")
    public void testReaderStartMessageIdAtExpectedPos(boolean batching, boolean startInclusive, int numOfMessages)
            throws Exception {
        final String topicName = "persistent://my-property/my-ns/ReaderStartMessageIdAtExpectedPos";
        final int resetIndex = new Random().nextInt(numOfMessages); // Choose some random index to reset
        final int firstMessage = startInclusive ? resetIndex : resetIndex + 1; // First message of reset

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(batching)
                .create();

        CountDownLatch latch = new CountDownLatch(numOfMessages);

        final AtomicReference<MessageId> resetPos = new AtomicReference<>();

        for (int i = 0; i < numOfMessages; i++) {

            final int j = i;

            producer.sendAsync(String.format("msg num %d", i).getBytes())
                    .thenCompose(messageId -> FutureUtils.value(Pair.of(j, messageId)))
                    .whenComplete((p, e) -> {
                        if (e != null) {
                            fail("send msg failed due to " + e.getMessage());
                        } else {
                            if (p.getLeft() == resetIndex) {
                                resetPos.set(p.getRight());
                            }
                        }
                        latch.countDown();
                    });
        }

        latch.await();

        ReaderBuilder<byte[]> readerBuilder = pulsarClient.newReader()
                .topic(topicName)
                .startMessageId(resetPos.get());

        if (startInclusive) {
            readerBuilder.startMessageIdInclusive();
        }

        Reader<byte[]> reader = readerBuilder.create();
        Set<String> messageSet = Sets.newHashSet();
        for (int i = firstMessage; i < numOfMessages; i++) {
            Message<byte[]> message = reader.readNext();
            String receivedMessage = new String(message.getData());
            String expectedMessage = String.format("msg num %d", i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        assertTrue(reader.isConnected());
        assertEquals(((ReaderImpl) reader).getConsumer().numMessagesInQueue(), 0);

        // Processed messages should be the number of messages in the range: [FirstResetMessage..TotalNumOfMessages]
        assertEquals(messageSet.size(), numOfMessages - firstMessage);

        reader.close();
        producer.close();
    }

    @Test
    public void testReaderBuilderConcurrentCreate() throws Exception {
        String topicName = "persistent://my-property/my-ns/testReaderBuilderConcurrentCreate_";
        int numTopic = 30;
        ReaderBuilder<byte[]> builder = pulsarClient.newReader().startMessageId(MessageId.earliest);

        List<CompletableFuture<Reader<byte[]>>> readers = Lists.newArrayListWithExpectedSize(numTopic);
        List<Producer<byte[]>> producers = Lists.newArrayListWithExpectedSize(numTopic);
        // create producer firstly
        for (int i = 0; i < numTopic; i++) {
            producers.add(pulsarClient.newProducer()
                .topic(topicName + i)
                .create());
        }

        // create reader concurrently
        for (int i = 0; i < numTopic; i++) {
            readers.add(builder.clone().topic(topicName + i).createAsync());
        }

        // verify readers config are different for topic name.
        for (int i = 0; i < numTopic; i++) {
            assertEquals(readers.get(i).get().getTopic(), topicName + i);
            readers.get(i).get().close();
            producers.get(i).close();
        }
    }

    @Test(timeOut = 10000)
    public void testMultiReaderBuilderConcurrentCreate() throws Exception {
        String topicName = "persistent://my-property/my-ns/testMultiReaderBuilderConcurrentCreate_";
        int numTopic = 30;
        ReaderBuilder<byte[]> builder = pulsarClient.newReader().startMessageId(MessageId.earliest);

        List<CompletableFuture<Reader<byte[]>>> readers = Lists.newArrayListWithExpectedSize(numTopic);
        List<Producer<byte[]>> producers = Lists.newArrayListWithExpectedSize(numTopic);
        // create producer firstly
        for (int i = 0; i < numTopic; i++) {
            admin.topics().createPartitionedTopic(topicName + i, 3);
            producers.add(pulsarClient.newProducer()
                .topic(topicName + i)
                .create());
        }

        // create reader concurrently
        for (int i = 0; i < numTopic; i++) {
            readers.add(builder.clone().topic(topicName + i).createAsync());
        }

        // verify readers config are different for topic name.
        for (int i = 0; i < numTopic; i++) {
            assertTrue(readers.get(i).get().getTopic().startsWith(MultiTopicsConsumerImpl.DUMMY_TOPIC_NAME_PREFIX));
            readers.get(i).get().close();
            producers.get(i).close();
        }
    }

    @Test
    public void testReaderStartInMiddleOfBatch() throws Exception {
        final String topicName = "persistent://my-property/my-ns/ReaderStartInMiddleOfBatch";
        final int numOfMessage = 100;

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxMessages(10)
                .create();

        CountDownLatch latch = new CountDownLatch(numOfMessage);

        List<MessageId> allIds = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numOfMessage; i++) {
            producer.sendAsync(String.format("msg num %d", i).getBytes()).whenComplete((mid, e) -> {
                if (e != null) {
                    fail();
                } else {
                    allIds.add(mid);
                }
                latch.countDown();
            });
        }

        latch.await();

        for (MessageId id : allIds) {
            Reader<byte[]> reader = pulsarClient.newReader().topic(topicName)
                    .startMessageId(id).startMessageIdInclusive().create();
            MessageId idGot = reader.readNext().getMessageId();
            assertEquals(idGot, id);
            reader.close();
        }

        producer.close();
    }

    @Test
    public void testHasMessageAvailableOnEmptyTopic() throws Exception {
        String topic = newTopicName();

        @Cleanup
        Reader<String> r1 = pulsarClient.newReader(Schema.STRING)
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create();

        @Cleanup
        Reader<String> r2 = pulsarClient.newReader(Schema.STRING)
                .topic(topic)
                .startMessageId(MessageId.latest)
                .create();

        @Cleanup
        Reader<String> r2Inclusive = pulsarClient.newReader(Schema.STRING)
                .topic(topic)
                .startMessageId(MessageId.latest)
                .startMessageIdInclusive()
                .create();

        // no data write, should return false
        assertFalse(r1.hasMessageAvailable());
        assertFalse(r2.hasMessageAvailable());
        assertFalse(r2Inclusive.hasMessageAvailable());

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        producer.send("hello-1");
        assertTrue(r1.hasMessageAvailable());
        assertTrue(r2.hasMessageAvailable());
        assertTrue(r2Inclusive.hasMessageAvailable());

        @Cleanup
        Reader<String> r3 = pulsarClient.newReader(Schema.STRING)
                .topic(topic)
                .startMessageId(MessageId.latest)
                .create();


        assertFalse(r3.hasMessageAvailable());

        producer.send("hello-2");

        assertTrue(r1.hasMessageAvailable());
        assertTrue(r2.hasMessageAvailable());
        assertTrue(r2Inclusive.hasMessageAvailable());
        assertTrue(r3.hasMessageAvailable());
    }
}
