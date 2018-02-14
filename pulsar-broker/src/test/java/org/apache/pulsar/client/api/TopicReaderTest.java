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
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
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
        ReaderConfiguration conf = new ReaderConfiguration();
        Reader reader = pulsarClient.createReader("persistent://my-property/use/my-ns/my-topic1", MessageId.earliest,
                conf);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message msg = null;
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
        ProducerConfiguration producerConf = new ProducerConfiguration();

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Reader reader = pulsarClient.createReader("persistent://my-property/use/my-ns/my-topic1", MessageId.earliest,
                new ReaderConfiguration());

        Message msg = null;
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
        ProducerConfiguration producerConf = new ProducerConfiguration();

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Reader reader1 = pulsarClient.createReader("persistent://my-property/use/my-ns/my-topic1", MessageId.earliest,
                new ReaderConfiguration());

        Reader reader2 = pulsarClient.createReader("persistent://my-property/use/my-ns/my-topic1", MessageId.earliest,
                new ReaderConfiguration());

        Message msg = null;
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
        String topicName = "persistent://my-property/use/my-ns/my-topic1";

        Reader reader1 = pulsarClient.createReader(topicName, MessageId.earliest, new ReaderConfiguration());

        Reader reader2 = pulsarClient.createReader(topicName, MessageId.earliest, new ReaderConfiguration());

        PersistentTopicStats stats = admin.persistentTopics().getStats(topicName);
        assertEquals(stats.subscriptions.size(), 2);

        reader1.close();
        stats = admin.persistentTopics().getStats(topicName);
        assertEquals(stats.subscriptions.size(), 1);

        reader2.close();

        stats = admin.persistentTopics().getStats(topicName);
        assertEquals(stats.subscriptions.size(), 0);
    }

    @Test
    public void testReaderOnLastMessage() throws Exception {
        ProducerConfiguration producerConf = new ProducerConfiguration();

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Reader reader = pulsarClient.createReader("persistent://my-property/use/my-ns/my-topic1", MessageId.latest,
                new ReaderConfiguration());

        for (int i = 10; i < 20; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // Publish more messages and verify the readers only sees new messages

        Message msg = null;
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
        ProducerConfiguration producerConf = new ProducerConfiguration();

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
        List<MessageId> messageIds = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            messageIds.add(producer.send(message.getBytes()));
        }

        Reader reader = pulsarClient.createReader("persistent://my-property/use/my-ns/my-topic1", messageIds.get(4),
                new ReaderConfiguration());

        // Publish more messages and verify the readers only sees messages starting from the intended message
        Message msg = null;
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
        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setBatchingEnabled(true);
        producerConf.setBatchingMaxPublishDelay(100, TimeUnit.MILLISECONDS);
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/testReaderOnSpecificMessageWithBatches", producerConf);
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }

        // Write one sync message to ensure everything before got persistend
        producer.send("my-message-10".getBytes());
        Reader reader1 = pulsarClient.createReader(
                "persistent://my-property/use/my-ns/testReaderOnSpecificMessageWithBatches", MessageId.earliest,
                new ReaderConfiguration());

        MessageId lastMessageId = null;
        for (int i = 0; i < 5; i++) {
            Message msg = reader1.readNext();
            lastMessageId = msg.getMessageId();
        }

        assertEquals(lastMessageId.getClass(), BatchMessageIdImpl.class);

        System.out.println("CREATING READER ON MSG ID: " + lastMessageId);

        Reader reader2 = pulsarClient.createReader(
                "persistent://my-property/use/my-ns/testReaderOnSpecificMessageWithBatches", lastMessageId,
                new ReaderConfiguration());

        for (int i = 5; i < 11; i++) {
            Message msg = reader2.readNext(1, TimeUnit.SECONDS);

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
        ReaderConfiguration conf = new ReaderConfiguration();
        conf.setCryptoKeyReader(new EncKeyReader());
        Reader reader = pulsarClient.createReader("persistent://my-property/use/my-ns/test-reader-myecdsa-topic1", MessageId.latest,
                conf);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.addEncryptionKey("client-ecdsa.pem");
        producerConf.setCryptoKeyReader(new EncKeyReader());

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/test-reader-myecdsa-topic1", producerConf);
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message msg = null;

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
    public void testSimpleReaderReachEndofTopic() throws Exception {
        ReaderConfiguration conf = new ReaderConfiguration();
        Reader reader = pulsarClient.createReader("persistent://my-property/use/my-ns/my-topic1", MessageId.earliest,
            conf);
        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);

        // no data write, should return false
        assertFalse(reader.hasMessageAvailable());

        // produce message 0 -- 99
        for (int i = 0; i < 100; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        MessageImpl msg = null;
        Set<String> messageSet = Sets.newHashSet();
        int index = 0;

        // read message till end.
        while (reader.hasMessageAvailable()) {
            msg = (MessageImpl) reader.readNext(1, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + (index ++);
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
            msg = (MessageImpl) reader.readNext(1, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + (index ++);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        assertEquals(index, 200);
        // readNext should return null, after reach the end of topic.
        assertNull(reader.readNext(1, TimeUnit.SECONDS));

        producer.close();
    }

    @Test
    public void testReaderReachEndofTopicOnMessageWithBatches() throws Exception {
        Reader reader = pulsarClient.createReader(
            "persistent://my-property/use/my-ns/testReaderReachEndofTopicOnMessageWithBatches", MessageId.earliest,
            new ReaderConfiguration());

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setBatchingEnabled(true);
        producerConf.setBatchingMaxPublishDelay(100, TimeUnit.MILLISECONDS);
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/testReaderReachEndofTopicOnMessageWithBatches", producerConf);

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
            Message msg = reader.readNext();
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
}
