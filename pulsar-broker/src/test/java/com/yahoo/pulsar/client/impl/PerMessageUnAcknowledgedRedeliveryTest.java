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

import com.yahoo.pulsar.broker.service.BrokerTestBase;
import com.yahoo.pulsar.client.api.*;
import com.yahoo.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class PerMessageUnAcknowledgedRedeliveryTest extends BrokerTestBase {
    private static final long testTimeout = 90000; // 1.5 min
    private static final Logger log = LoggerFactory.getLogger(PerMessageUnAcknowledgedRedeliveryTest.class);
    private final long ackTimeOutMillis = TimeUnit.SECONDS.toMillis(2);

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
    }

    @Override
    @AfterMethod
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = testTimeout)
    public void testSharedAckedNormalTopic() throws Exception {
        String key = "testSharedAckedNormalTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 15;

        // 1. producer connect
        Producer producer = pulsarClient.createProducer(topicName);

        // 2. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(50);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName, conf);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message, doesn't ack
        Message message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);

        // 5. producer publish more messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String m = messagePredicate + i;
            log.info("Producer produced: " + m);
            producer.send(m.getBytes());
        }

        // 6. Receiver receives the message, ack them
        message = consumer.receive();
        int received = 0;
        while (message != null) {
            received++;
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            consumer.acknowledge(message);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
        assertEquals(received, 5);

        // 7. Simulate ackTimeout
        ((ConsumerBase) consumer).getUnAckedMessageTracker().toggle();

        // 8. producer publish more messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String m = messagePredicate + i;
            log.info("Producer produced: " + m);
            producer.send(m.getBytes());
        }

        // 9. Receiver receives the message, doesn't ack
        message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 10);

        Thread.sleep(ackTimeOutMillis);

        // 10. Receiver receives redelivered messages
        message = consumer.receive();
        int redelivered = 0;
        while (message != null) {
            redelivered++;
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            consumer.acknowledge(message);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        assertEquals(redelivered, 5);
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
    }

    @Test(timeOut = testTimeout)
    public void testExclusiveAckedNormalTopic() throws Exception {
        String key = "testExclusiveAckedNormalTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 15;

        // 1. producer connect
        Producer producer = pulsarClient.createProducer(topicName);

        // 2. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(50);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName, conf);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message, doesn't ack
        Message message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);

        // 5. producer publish more messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String m = messagePredicate + i;
            log.info("Producer produced: " + m);
            producer.send(m.getBytes());
        }

        // 6. Receiver receives the message, ack them
        message = consumer.receive();
        int received = 0;
        while (message != null) {
            received++;
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            consumer.acknowledge(message);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
        assertEquals(received, 5);

        // 7. Simulate ackTimeout
        ((ConsumerBase) consumer).getUnAckedMessageTracker().toggle();

        // 8. producer publish more messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String m = messagePredicate + i;
            log.info("Producer produced: " + m);
            producer.send(m.getBytes());
        }

        // 9. Receiver receives the message, doesn't ack
        message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 10);

        Thread.sleep(ackTimeOutMillis);

        // 10. Receiver receives redelivered messages
        message = consumer.receive();
        int redelivered = 0;
        while (message != null) {
            redelivered++;
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            consumer.acknowledge(message);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        assertEquals(redelivered, 10);
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 0);
    }

    @Test(timeOut = testTimeout)
    public void testFailoverAckedNormalTopic() throws Exception {
        String key = "testFailoverAckedNormalTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 15;

        // 1. producer connect
        Producer producer = pulsarClient.createProducer(topicName);

        // 2. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(50);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        conf.setSubscriptionType(SubscriptionType.Failover);
        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName, conf);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message, doesn't ack
        Message message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);

        // 5. producer publish more messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String m = messagePredicate + i;
            log.info("Producer produced: " + m);
            producer.send(m.getBytes());
        }

        // 6. Receiver receives the message, ack them
        message = consumer.receive();
        int received = 0;
        while (message != null) {
            received++;
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            consumer.acknowledge(message);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
        assertEquals(received, 5);

        // 7. Simulate ackTimeout
        ((ConsumerBase) consumer).getUnAckedMessageTracker().toggle();

        // 8. producer publish more messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String m = messagePredicate + i;
            log.info("Producer produced: " + m);
            producer.send(m.getBytes());
        }

        // 9. Receiver receives the message, doesn't ack
        message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 10);

        Thread.sleep(ackTimeOutMillis);

        // 10. Receiver receives redelivered messages
        message = consumer.receive();
        int redelivered = 0;
        while (message != null) {
            redelivered++;
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            consumer.acknowledge(message);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        assertEquals(redelivered, 10);
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 0);
    }

    @Test(timeOut = testTimeout)
    public void testSharedAckedPartitionedTopic() throws Exception {
        String key = "testSharedAckedPartitionedTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 15;
        final int numberOfPartitions = 3;
        admin.persistentTopics().createPartitionedTopic(topicName, numberOfPartitions);

        // 1. producer connect
        ProducerConfiguration prodConfig = new ProducerConfiguration();
        prodConfig.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = pulsarClient.createProducer(topicName, prodConfig);

        // 2. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(50);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName, conf);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message, doesn't ack
        Message message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);

        // 5. producer publish more messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String m = messagePredicate + i;
            log.info("Producer produced: " + m);
            producer.send(m.getBytes());
        }

        // 6. Receiver receives the message, ack them
        message = consumer.receive();
        int received = 0;
        while (message != null) {
            received++;
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            consumer.acknowledge(message);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
        assertEquals(received, 5);

        // 7. Simulate ackTimeout
        ((ConsumerBase) consumer).getUnAckedMessageTracker().toggle();

        // 8. producer publish more messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String m = messagePredicate + i;
            log.info("Producer produced: " + m);
            producer.send(m.getBytes());
        }

        // 9. Receiver receives the message, doesn't ack
        message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 10);

        Thread.sleep(ackTimeOutMillis);

        // 10. Receiver receives redelivered messages
        message = consumer.receive();
        int redelivered = 0;
        while (message != null) {
            redelivered++;
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            consumer.acknowledge(message);
            message = consumer.receive(10, TimeUnit.MILLISECONDS);
        }
        assertEquals(redelivered, 5);
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
    }
}
