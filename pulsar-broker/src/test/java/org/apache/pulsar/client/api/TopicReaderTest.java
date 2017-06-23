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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

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

}