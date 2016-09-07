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
package com.yahoo.pulsar.client.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.service.BrokerTestBase;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.PulsarClientException;

public class MessageIdTest extends BrokerTestBase {
    private static final Logger log = LoggerFactory.getLogger(MessageIdTest.class);

    @BeforeClass
    @Override
    public void setup() throws Exception {
        baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test(timeOut = 10000)
    public void producerSendAsync() throws PulsarClientException {
        // 1. Basic Config
        String key = "producerSendAsync";
        final String topicName = "persistent://property/cluster/namespace/topic-" + key;
        final String subscriptionName = "my-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int numberOfMessages = 30;

        // 2. Create Producer
        Producer producer = pulsarClient.createProducer(topicName);

        // 3. Create Consumer
        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);

        // 4. Publish message and get message id
        Set<MessageId> messageIds = new HashSet();
        List<Future<MessageId>> futures = new ArrayList();
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }

        MessageIdImpl previousMessageId = null;
        for (Future<MessageId> f : futures) {
            try {
                MessageIdImpl currentMessageId = (MessageIdImpl) f.get();
                if (previousMessageId != null) {
                    Assert.assertTrue(currentMessageId.compareTo(previousMessageId) > 0,
                            "Message Ids should be in ascending order");
                }
                messageIds.add(currentMessageId);
                previousMessageId = currentMessageId;
            } catch (Exception e) {
                Assert.fail("Failed to publish message, Exception: " + e.getMessage());
            }
        }

        // 4. Check if message Ids are correct
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), numberOfMessages, "Not all messages published successfully");

        for (int i = 0; i < numberOfMessages; i++) {
            Message message = consumer.receive();
            Assert.assertEquals(new String(message.getData()), messagePredicate + i);
            MessageId messageId = message.getMessageId();
            Assert.assertTrue(messageIds.remove(messageId), "Failed to receive message");
        }
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), 0, "Not all messages received successfully");
        consumer.unsubscribe();
    }

    @Test(timeOut = 10000)
    public void producerSend() throws PulsarClientException {
        // 1. Basic Config
        String key = "producerSend";
        final String topicName = "persistent://property/cluster/namespace/topic-" + key;
        final String subscriptionName = "my-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int numberOfMessages = 30;

        // 2. Create Producer
        Producer producer = pulsarClient.createProducer(topicName);

        // 3. Create Consumer
        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);

        // 4. Publish message and get message id
        Set<MessageId> messageIds = new HashSet();
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            messageIds.add(producer.send(message.getBytes()));
        }

        // 4. Check if message Ids are correct
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), numberOfMessages, "Not all messages published successfully");

        for (int i = 0; i < numberOfMessages; i++) {
            Assert.assertTrue(messageIds.remove(consumer.receive().getMessageId()), "Failed to receive Message");
        }
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), 0, "Not all messages received successfully");
        consumer.unsubscribe();
        ;
    }

    @Test(timeOut = 10000)
    public void partitionedProducerSendAsync() throws PulsarClientException, PulsarAdminException {
        // 1. Basic Config
        String key = "partitionedProducerSendAsync";
        final String topicName = "persistent://property/cluster/namespace/topic-" + key;
        final String subscriptionName = "my-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int numberOfMessages = 30;
        int numberOfPartitions = 3;
        admin.persistentTopics().createPartitionedTopic(topicName, numberOfPartitions);

        // 2. Create Producer
        Producer producer = pulsarClient.createProducer(topicName);

        // 3. Create Consumer
        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);

        // 4. Publish message and get message id
        Set<MessageId> messageIds = new HashSet();
        Set<Future<MessageId>> futures = new HashSet();
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }

        futures.forEach(f -> {
            try {
                messageIds.add(f.get());
            } catch (Exception e) {
                Assert.fail("Failed to publish message, Exception: " + e.getMessage());
            }
        });

        // 4. Check if message Ids are correct
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), numberOfMessages, "Not all messages published successfully");

        for (int i = 0; i < numberOfMessages; i++) {
            MessageId messageId = consumer.receive().getMessageId();
            log.info("Message ID Received = " + messageId);
            Assert.assertTrue(messageIds.remove(messageId), "Failed to receive Message");
        }
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), 0, "Not all messages received successfully");
        consumer.unsubscribe();
    }

    @Test(timeOut = 10000)
    public void partitionedProducerSend() throws PulsarClientException, PulsarAdminException {
        // 1. Basic Config
        String key = "partitionedProducerSend";
        final String topicName = "persistent://property/cluster/namespace/topic-" + key;
        final String subscriptionName = "my-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int numberOfMessages = 30;
        int numberOfPartitions = 7;
        admin.persistentTopics().createPartitionedTopic(topicName, numberOfPartitions);

        // 2. Create Producer
        Producer producer = pulsarClient.createProducer(topicName);

        // 3. Create Consumer
        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);

        // 4. Publish message and get message id
        Set<MessageId> messageIds = new HashSet();
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePredicate + i;
            messageIds.add(producer.send(message.getBytes()));
        }

        // 4. Check if message Ids are correct
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), numberOfMessages, "Not all messages published successfully");

        for (int i = 0; i < numberOfMessages; i++) {
            Assert.assertTrue(messageIds.remove(consumer.receive().getMessageId()), "Failed to receive Message");
        }
        log.info("Message IDs = " + messageIds);
        Assert.assertEquals(messageIds.size(), 0, "Not all messages received successfully");
        // TODO - this statement causes the broker to hang - need to look into
        // it
        // consumer.unsubscribe();;
    }
}
