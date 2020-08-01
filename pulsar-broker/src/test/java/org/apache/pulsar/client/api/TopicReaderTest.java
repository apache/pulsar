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
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.RelativeTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TopicReaderTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(TopicReaderTest.class);

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass
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
    public void testReaderAfterMessagesWerePublished() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/testReaderAfterMessagesWerePublished")
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Reader<byte[]> reader = pulsarClient.newReader().topic("persistent://my-property/my-ns/testReaderAfterMessagesWerePublished")
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
    public void testTopicStats() throws Exception {
        String topicName = "persistent://my-property/my-ns/testTopicStats";

        Reader<byte[]> reader1 = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();

        Reader<byte[]> reader2 = pulsarClient.newReader().topic(topicName).startMessageId(MessageId.earliest).create();

        TopicStats stats = admin.topics().getStats(topicName);
        assertEquals(stats.subscriptions.size(), 2);

        reader1.close();
        stats = admin.topics().getStats(topicName);
        assertEquals(stats.subscriptions.size(), 1);

        reader2.close();

        stats = admin.topics().getStats(topicName);
        assertEquals(stats.subscriptions.size(), 0);
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

    @Test(groups = "encryption")
    public void testECDSAEncryption() throws Exception {
        log.info("-- Starting {} test --", methodName);

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
    public void testSimpleReaderReachEndOfTopic() throws Exception {
        Reader<byte[]> reader = pulsarClient.newReader().topic("persistent://my-property/my-ns/testSimpleReaderReachEndOfTopic")
                .startMessageId(MessageId.earliest).create();
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/testSimpleReaderReachEndOfTopic")
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
}
