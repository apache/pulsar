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

import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicReaderTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(TopicReaderTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
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

    @Test
    public void testReaderOnLastMessage() throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/testReaderOnLastMessage")
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Reader<byte[]> reader = pulsarClient.newReader().topic("persistent://my-property/my-ns/testReaderOnLastMessage")
                .startMessageId(MessageId.latest).create();

        for (int i = 10; i < 20; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // Publish more messages and verify the readers only sees new messages

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 10; i < 20; i++) {
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
        pulsar.getBrokerService().getTopicReference(topic).get().close().get();

        try (Reader<byte[]> reader = pulsarClient.newReader().topic(topic)
            .startMessageId(MessageId.earliest).create()) {
            assertTrue(reader.hasMessageAvailable());

            String readOut = new String(reader.readNext().getData());
            assertTrue(readOut.equals(content));
            assertFalse(reader.hasMessageAvailable());
        }

    }
}
