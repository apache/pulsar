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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class UnAcknowledgedMessagesTimeoutTest extends BrokerTestBase {
    private static final Logger log = LoggerFactory.getLogger(UnAcknowledgedMessagesTimeoutTest.class);
    private final long ackTimeOutMillis = TimeUnit.SECONDS.toMillis(2);

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        super.baseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testExclusiveSingleAckedNormalTopic() throws Exception {
        String key = "testExclusiveSingleAckedNormalTopic";
        final String topicName = "persistent://prop/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(7).ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 2; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message
        Message<byte[]> message = consumer.receive();
        while (message != null) {
            log.info("Consumer received : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerImpl<?>) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, totalMessages / 2);

        // Blocking call, redeliver should kick in
        message = consumer.receive();
        log.info("Consumer received : " + new String(message.getData()));

        HashSet<String> hSet = new HashSet<>();
        for (int i = totalMessages / 2; i < totalMessages; i++) {
            String messageString = messagePredicate + i;
            producer.send(messageString.getBytes());
        }

        do {
            hSet.add(new String(message.getData()));
            consumer.acknowledge(message);
            log.info("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);

        size = ((ConsumerImpl<?>) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 0);
        assertEquals(hSet.size(), totalMessages);
    }

    @Test
    public void testExclusiveCumulativeAckedNormalTopic() throws Exception {
        String key = "testExclusiveCumulativeAckedNormalTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(7).ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message
        HashSet<String> hSet = new HashSet<>();
        Message<byte[]> message = consumer.receive();
        Message<byte[]> lastMessage = message;
        while (message != null) {
            lastMessage = message;
            hSet.add(new String(message.getData()));
            log.info("Consumer received " + new String(message.getData()));
            log.info("Message ID details " + message.getMessageId().toString());
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerImpl<?>) consumer).getUnAckedMessageTracker().size();
        assertEquals(size, totalMessages);
        log.info("Comulative Ack sent for " + new String(lastMessage.getData()));
        log.info("Message ID details " + lastMessage.getMessageId().toString());
        consumer.acknowledgeCumulative(lastMessage);
        size = ((ConsumerImpl<?>) consumer).getUnAckedMessageTracker().size();
        assertEquals(size, 0);
        message = consumer.receive((int) (2 * ackTimeOutMillis), TimeUnit.MILLISECONDS);
        assertNull(message);
    }

    @Test
    public void testSharedSingleAckedPartitionedTopic() throws Exception {
        String key = "testSharedSingleAckedPartitionedTopic";
        final String topicName = "persistent://prop/ns-abc/topic-" + key;
        final String subscriptionName = "my-shared-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 20;
        final int numberOfPartitions = 3;
        admin.topics().createPartitionedTopic(topicName, numberOfPartitions);
        // Special step to create partitioned topic

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        // 2. Create consumer
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(100).subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS).consumerName("Consumer-1").subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(100).subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS).consumerName("Consumer-2").subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            MessageId msgId = producer.send(message.getBytes());
            log.info("Message produced: {} -- msgId: {}", message, msgId);
        }

        // 4. Receive messages
        int messageCount1 = receiveAllMessage(consumer1, false /* no-ack */);
        int messageCount2 = receiveAllMessage(consumer2, true /* yes-ack */);
        int ackCount1 = 0;
        int ackCount2 = messageCount2;

        log.info(key + " messageCount1 = " + messageCount1);
        log.info(key + " messageCount2 = " + messageCount2);
        log.info(key + " ackCount1 = " + ackCount1);
        log.info(key + " ackCount2 = " + ackCount2);
        assertEquals(messageCount1 + messageCount2, totalMessages);

        // 5. Check if Messages redelivered again
        // Since receive is a blocking call hoping that timeout will kick in
        Thread.sleep((int) (ackTimeOutMillis * 1.1));
        log.info(key + " Timeout should be triggered now");
        messageCount1 = receiveAllMessage(consumer1, true);
        messageCount2 += receiveAllMessage(consumer2, false);

        ackCount1 = messageCount1;

        log.info(key + " messageCount1 = " + messageCount1);
        log.info(key + " messageCount2 = " + messageCount2);
        log.info(key + " ackCount1 = " + ackCount1);
        log.info(key + " ackCount2 = " + ackCount2);
        assertEquals(messageCount1 + messageCount2, totalMessages);
        assertEquals(ackCount1 + messageCount2, totalMessages);

        Thread.sleep((int) (ackTimeOutMillis * 1.1));

        // Since receive is a blocking call hoping that timeout will kick in
        log.info(key + " Timeout should be triggered again");
        ackCount1 += receiveAllMessage(consumer1, true);
        ackCount2 += receiveAllMessage(consumer2, true);
        log.info(key + " ackCount1 = " + ackCount1);
        log.info(key + " ackCount2 = " + ackCount2);
        assertEquals(ackCount1 + ackCount2, totalMessages);
    }

    private static int receiveAllMessage(Consumer<?> consumer, boolean ackMessages) throws Exception {
        int messagesReceived = 0;
        Message<?> msg = consumer.receive(1, TimeUnit.SECONDS);
        while (msg != null) {
            ++messagesReceived;
            log.info("Consumer received {}", new String(msg.getData()));

            if (ackMessages) {
                consumer.acknowledge(msg);
            }

            msg = consumer.receive(1, TimeUnit.SECONDS);
        }

        return messagesReceived;
    }

    @Test
    public void testFailoverSingleAckedPartitionedTopic() throws Exception {
        String key = "testFailoverSingleAckedPartitionedTopic";
        final String topicName = "persistent://prop/ns-abc/topic-" + key + UUID.randomUUID().toString();
        final String subscriptionName = "my-failover-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;
        final int numberOfPartitions = 3;
        admin.topics().createPartitionedTopic(topicName, numberOfPartitions);
        // Special step to create partitioned topic

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        // 2. Create consumer
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .receiverQueueSize(7)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .consumerName("Consumer-1")
                .subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .receiverQueueSize(7)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .consumerName("Consumer-2")
                .subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Message produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receive messages
        int messagesReceived = 0;
        while (true) {
            Message<byte[]> message1 = consumer1.receive(500, TimeUnit.MILLISECONDS);
            if (message1 == null) {
                break;
            }

            ++messagesReceived;
        }

        int ackCount = 0;
        while (true) {
            Message<byte[]> message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
            if (message2 == null) {
                break;
            }

            consumer2.acknowledge(message2);
            ++messagesReceived;
            ++ackCount;
        }

        assertEquals(messagesReceived, totalMessages);

        // 5. Check if Messages redelivered again
        Thread.sleep(ackTimeOutMillis);
        log.info(key + " Timeout should be triggered now");
        messagesReceived = 0;
        while (true) {
            Message<byte[]> message1 = consumer1.receive(500, TimeUnit.MILLISECONDS);
            if (message1 == null) {
                break;
            }

            ++messagesReceived;
        }

        while (true) {
            Message<byte[]> message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
            if (message2 == null) {
                break;
            }

            ++messagesReceived;
        }

        assertEquals(messagesReceived + ackCount, totalMessages);
    }

    @Test
    public void testAckTimeoutMinValue() {
        try {
            pulsarClient.newConsumer().ackTimeout(999, TimeUnit.MILLISECONDS);
            Assert.fail("Exception should have been thrown since the set timeout is less than min timeout.");
        } catch (Exception ex) {
            // Ok
        }
    }

    @Test
    public void testCheckUnAcknowledgedMessageTimer() throws PulsarClientException, InterruptedException {
        String key = "testCheckUnAcknowledgedMessageTimer";
        final String topicName = "persistent://prop/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 3;

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        // 2. Create consumer
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(7).subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        Thread.sleep((long) (ackTimeOutMillis * 1.1));

        for (int i = 0; i < totalMessages; i++) {
            Message<byte[]> msg = consumer.receive();
            if (i != totalMessages - 1) {
                consumer.acknowledge(msg);
            }
        }

        assertEquals(consumer.getUnAckedMessageTracker().size(), 1);

        Message<byte[]> msg = consumer.receive();
        consumer.acknowledge(msg);
        assertEquals(consumer.getUnAckedMessageTracker().size(), 0);

        Thread.sleep((long) (ackTimeOutMillis * 1.1));

        assertEquals(consumer.getUnAckedMessageTracker().size(), 0);
    }

    @Test
    public void testSingleMessageBatch() throws Exception {
        String topicName = "prop/ns-abc/topic-estSingleMessageBatch";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS)
                .create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("subscription")
                .ackTimeout(1, TimeUnit.HOURS)
                .subscribe();

        // Force the creation of a batch with a single message
        producer.sendAsync("hello");
        producer.flush();

        Message<String> message = consumer.receive();

        assertFalse(((ConsumerImpl<?>) consumer).getUnAckedMessageTracker().isEmpty());

        consumer.acknowledge(message);

        assertTrue(((ConsumerImpl<?>) consumer).getUnAckedMessageTracker().isEmpty());
    }
}
