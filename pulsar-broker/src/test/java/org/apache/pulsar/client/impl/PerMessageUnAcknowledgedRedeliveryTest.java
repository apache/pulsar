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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
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
    @AfterMethod(alwaysRun = true)
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
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(50).ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message, doesn't ack
        Message<byte[]> message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
        assertEquals(received, 5);

        // 7. Simulate ackTimeout
        Thread.sleep(ackTimeOutMillis);

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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        assertEquals(redelivered, 5);
        size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
    }

    @Test(timeOut = testTimeout)
    public void testUnAckedMessageTrackerSize() throws Exception {
        String key = "testUnAckedMessageTrackerSize";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 15;

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 2. Create consumer,doesn't set the ackTimeout
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(50).subscriptionType(SubscriptionType.Shared).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message, doesn't ack
        Message<byte[]> message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        UnAckedMessageTracker unAckedMessageTracker = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker();
        long size = unAckedMessageTracker.size();
        log.info(key + " Unacked Message Tracker size is " + size);
        // 5. If ackTimeout is not set, UnAckedMessageTracker is a disabled method
        assertEquals(size, 0);
        assertTrue(unAckedMessageTracker.add(null));
        assertTrue(unAckedMessageTracker.remove(null));
        assertEquals(unAckedMessageTracker.removeMessagesTill(null), 0);
    }

    @Test(timeOut = testTimeout)
    public void testExclusiveAckedNormalTopic() throws Exception {
        String key = "testExclusiveAckedNormalTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 15;

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(50).ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscriptionType(SubscriptionType.Exclusive).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message, doesn't ack
        Message<byte[]> message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
        assertEquals(received, 5);

        // 7. Simulate ackTimeout
        Thread.sleep(ackTimeOutMillis);

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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        assertEquals(redelivered, 10);
        size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
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
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(50).ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscriptionType(SubscriptionType.Failover).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message, doesn't ack
        Message<byte[]> message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
        assertEquals(received, 5);

        // 7. Simulate ackTimeout
        Thread.sleep(ackTimeOutMillis);

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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        assertEquals(redelivered, 10);
        size = ((ConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 0);
    }

    private static long getUnackedMessagesCountInPartitionedConsumer(Consumer<byte[]> c) {
        MultiTopicsConsumerImpl<byte[]> pc = (MultiTopicsConsumerImpl<byte[]>) c;
        return pc.getUnAckedMessageTracker().size()
                + pc.getConsumers().stream().mapToLong(consumer -> consumer.getUnAckedMessageTracker().size()).sum();
    }

    @Test(timeOut = testTimeout)
    public void testSharedAckedPartitionedTopic() throws Exception {
        String key = "testSharedAckedPartitionedTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 15;
        final int numberOfPartitions = 3;
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName, numberOfPartitions);

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(50).ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message, doesn't ack
        Message<byte[]> message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.info("Consumer received : " + data);
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }

        long size = getUnackedMessagesCountInPartitionedConsumer(consumer);
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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        size = getUnackedMessagesCountInPartitionedConsumer(consumer);
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
        assertEquals(received, 5);

        // 7. Simulate ackTimeout
        Thread.sleep(ackTimeOutMillis);

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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        size = getUnackedMessagesCountInPartitionedConsumer(consumer);
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
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        assertEquals(redelivered, 5);
        size = getUnackedMessagesCountInPartitionedConsumer(consumer);
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 5);
    }
}
