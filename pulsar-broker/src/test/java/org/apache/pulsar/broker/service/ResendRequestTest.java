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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ResendRequestTest extends BrokerTestBase {
    private static final long testTimeout = 60000; // 1 min
    private static final Logger log = LoggerFactory.getLogger(ResendRequestTest.class);

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = testTimeout)
    public void testExclusiveSingleAckedNormalTopic() throws Exception {
        String key = "testExclusiveSingleAckedNormalTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;

        HashSet<MessageId> messageIdHashSet = new HashSet<>();
        HashSet<String> messageDataHashSet = new HashSet<>();

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(7).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }

        // 4. Receive messages
        Message<byte[]> message = consumer.receive();
        log.info("Message received " + new String(message.getData()));

        for (int i = 1; i < totalMessages; i++) {
            Message<byte[]> msg = consumer.receive();
            log.info("Message received " + new String(msg.getData()));
            messageDataHashSet.add(new String(msg.getData()));
        }
        printIncomingMessageQueue(consumer);

        // 5. Ack 1st message and ask for resend
        consumer.acknowledge(message);
        log.info("Message acked " + new String(message.getData()));
        messageIdHashSet.add(message.getMessageId());
        messageDataHashSet.add(new String(message.getData()));

        consumer.redeliverUnacknowledgedMessages();
        log.info("Resend Messages Request sent");

        // 6. Check if messages resent in correct order
        for (int i = 0; i < totalMessages - 1; i++) {
            message = consumer.receive();
            log.info("Message received " + new String(message.getData()));
            if (i < 2) {
                messageIdHashSet.add(message.getMessageId());
                consumer.acknowledge(message);
            }
            log.info("Message acked " + new String(message.getData()));
            assertTrue(messageDataHashSet.contains(new String(message.getData())));
        }
        assertEquals(messageIdHashSet.size(), 3);
        assertEquals(messageDataHashSet.size(), totalMessages);
        printIncomingMessageQueue(consumer);

        // 7. Request resend 2nd time - you should receive 4 messages
        consumer.redeliverUnacknowledgedMessages();
        log.info("Resend Messages Request sent");
        message = consumer.receive(2000, TimeUnit.MILLISECONDS);
        while (message != null) {
            log.info("Message received " + new String(message.getData()));
            consumer.acknowledge(message);
            log.info("Message acked " + new String(message.getData()));
            messageIdHashSet.add(message.getMessageId());
            messageDataHashSet.add(new String(message.getData()));
            message = consumer.receive(5000, TimeUnit.MILLISECONDS);
        }

        assertEquals(messageIdHashSet.size(), totalMessages);
        assertEquals(messageDataHashSet.size(), totalMessages);
        printIncomingMessageQueue(consumer);

        // 9. Calling resend after acking all messages - expectin 0 messages
        consumer.redeliverUnacknowledgedMessages();
        assertNull(consumer.receive(2000, TimeUnit.MILLISECONDS));

        // 10. Checking message contents
        for (int i = 0; i < totalMessages; i++) {
            assertTrue(messageDataHashSet.contains(messagePredicate + i));
        }
    }

    @Test(timeOut = testTimeout)
    public void testSharedSingleAckedNormalTopic() throws Exception {
        String key = "testSharedSingleAckedNormalTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-shared-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;
        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. Create consumer
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(totalMessages / 2)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer2 = newPulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(totalMessages / 2)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        // 3. Producer publishes messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
            log.info("Producer produced " + message);
        }

        // 4. Receive messages
        int receivedConsumer1 = 0, receivedConsumer2 = 0;
        Message<byte[]> message1 = consumer1.receive();
        Message<byte[]> message2 = consumer2.receive();
        do {
            if (message1 != null) {
                log.info("Consumer 1 Received: " + new String(message1.getData()));
                receivedConsumer1 += 1;
            }

            if (message2 != null) {
                log.info("Consumer 2 Received: " + new String(message2.getData()));
                receivedConsumer2 += 1;
            }
            message1 = consumer1.receive(100, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(100, TimeUnit.MILLISECONDS);
        } while (message1 != null || message2 != null);

        log.info("Consumer 1 receives = " + receivedConsumer1);
        log.info("Consumer 2 receives = " + receivedConsumer2);
        log.info("Total receives = " + (receivedConsumer2 + receivedConsumer1));
        assertEquals(receivedConsumer2 + receivedConsumer1, totalMessages);

        // 5. Send a resend request from Consumer 1
        log.info("Consumer 1 sent a resend request");
        consumer1.redeliverUnacknowledgedMessages();

        // 6. Consumer 1's unAcked messages should be sent to Consumer 1 or 2
        int receivedMessagesAfterRedelivery = 0;
        receivedConsumer1 = 0;
        message1 = consumer1.receive(100, TimeUnit.MILLISECONDS);
        message2 = consumer2.receive(100, TimeUnit.MILLISECONDS);
        do {
            if (message1 != null) {
                log.info("Consumer 1 Received: " + new String(message1.getData()));
                receivedConsumer1 += 1;
                ++receivedMessagesAfterRedelivery;
            }

            if (message2 != null) {
                log.info("Consumer 2 Received: " + new String(message2.getData()));
                receivedConsumer2 += 1;
                ++receivedMessagesAfterRedelivery;
            }
            message1 = consumer1.receive(200, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(200, TimeUnit.MILLISECONDS);
        } while (message1 != null || message2 != null);
        log.info("Additional received = " + receivedMessagesAfterRedelivery);
        newPulsarClient.close();
        assertTrue(receivedMessagesAfterRedelivery > 0);

        assertEquals(receivedConsumer1 + receivedConsumer2, totalMessages);
    }

    @Test(timeOut = testTimeout)
    public void testFailoverSingleAckedNormalTopic() throws Exception {
        String key = "testFailoverSingleAckedNormalTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-failover-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;
        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. Create consumer
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(10).subscriptionType(SubscriptionType.Failover)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS);
        Consumer<byte[]> consumer1 = consumerBuilder.clone().consumerName("consumer-1").subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.clone().consumerName("consumer-2").subscribe();

        // 3. Producer publishes messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
            log.info("Producer produced " + message);
        }

        // 4. Receive messages
        int receivedConsumer1 = 0, receivedConsumer2 = 0;
        Message<byte[]> message1;
        Message<byte[]> message2;
        do {
            message1 = consumer1.receive(500, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
            if (message1 != null) {
                log.info("Consumer 1 Received: " + new String(message1.getData()));
                receivedConsumer1 += 1;
            }
            if (message2 != null) {
                log.info("Consumer 2 Received: " + new String(message2.getData()));
                receivedConsumer2 += 1;
            }

        } while (message1 != null || message2 != null);

        log.info("Consumer 1 receives = " + receivedConsumer1);
        log.info("Consumer 2 receives = " + receivedConsumer2);
        log.info("Total receives = " + (receivedConsumer2 + receivedConsumer1));
        assertEquals(receivedConsumer2 + receivedConsumer1, totalMessages);
        // Consumer 2 is on Stand By
        assertEquals(receivedConsumer2, 0);

        // 5. Consumer 1 asks for resend
        consumer1.redeliverUnacknowledgedMessages();
        Thread.sleep(1000);

        // 6. Consumer 1 acknowledges a few messages
        receivedConsumer1 = receivedConsumer2 = 0;
        for (int i = 0; i < totalMessages / 2; i++) {
            message1 = consumer1.receive(500, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
            if (message1 != null) {
                log.info("Consumer 1 Received: " + new String(message1.getData()));
                receivedConsumer1 += 1;
                log.info("Consumer 1 Acknowledged: " + new String(message1.getData()));
                consumer1.acknowledge(message1);
            }

            if (message2 != null) {
                log.info("Consumer 2 Received: " + new String(message2.getData()));
                receivedConsumer2 += 1;
            }
        }
        assertEquals(receivedConsumer2 + receivedConsumer1, totalMessages / 2);

        // Consumer 2 is on Stand By
        assertEquals(receivedConsumer2, 0);

        // 7. Consumer 1 close
        consumer1.close();

        // 8. Checking if all messages are received by Consumer 2
        message2 = consumer2.receive();
        int acknowledgedMessages = 0;
        int unAcknowledgedMessages = 0;
        boolean flag = true;
        do {
            if (flag) {
                consumer2.acknowledge(message2);
                acknowledgedMessages += 1;
                log.info("Consumer 2 Acknowledged: " + new String(message2.getData()));
            } else {
                unAcknowledgedMessages += 1;
            }
            flag = !flag;
            log.info("Consumer 2 Received: " + new String(message2.getData()));
            message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
        } while (message2 != null);
        log.info("Consumer 2 receives = " + (unAcknowledgedMessages + acknowledgedMessages));
        log.info("Consumer 2 acknowledges = " + acknowledgedMessages);
        assertEquals(unAcknowledgedMessages + acknowledgedMessages, totalMessages - receivedConsumer1);

        // 9 .Consumer 2 asks for a resend
        consumer2.redeliverUnacknowledgedMessages();
        Thread.sleep(1000);
        message2 = consumer2.receive();
        receivedConsumer2 = 0;
        do {
            receivedConsumer2 += 1;
            message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
        } while (message2 != null);
        log.info("Consumer 2 receives = " + receivedConsumer2);
        assertEquals(unAcknowledgedMessages, receivedConsumer2);
    }

    @Test(timeOut = testTimeout)
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

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(7).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }

        // 4. Receive messages
        Message<byte[]> message = consumer.receive();
        log.info("Message received " + new String(message.getData()));
        for (int i = 0; i < 7; i++) {
            printIncomingMessageQueue(consumer);
            message = consumer.receive();
            log.info("Message received " + new String(message.getData()));
        }

        consumer.redeliverUnacknowledgedMessages();
        Thread.sleep(1000);
        consumer.acknowledgeCumulative(message);
        do {
            message = consumer.receive(1000, TimeUnit.MILLISECONDS);
        } while (message != null);

        log.info("Consumer Requests Messages");
        consumer.redeliverUnacknowledgedMessages();

        int numOfReceives = 0;
        message = consumer.receive();
        do {
            numOfReceives += 1;
            log.info("Message received " + new String(message.getData()));
            message = consumer.receive(1000, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(numOfReceives, 2);
    }

    @Test(timeOut = testTimeout)
    public void testExclusiveSingleAckedPartitionedTopic() throws Exception {
        String key = "testExclusiveSingleAckedPartitionedTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;
        final int numberOfPartitions = 4;
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName, numberOfPartitions);
        // Special step to create partitioned topic

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(7).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Message produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receive messages
        Message<byte[]> message = consumer.receive();
        int messageCount = 0;
        log.info("Message received " + new String(message.getData()));
        do {
            messageCount += 1;
            log.info("Message received " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageCount, totalMessages);

        // 5. Ask for redeliver
        consumer.redeliverUnacknowledgedMessages();

        // 6. Check if Messages redelivered again
        message = consumer.receive();
        messageCount = 0;
        log.info("Message received " + new String(message.getData()));
        do {
            messageCount += 1;
            log.info("Message received " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageCount, totalMessages);
    }

    @Test(timeOut = testTimeout)
    public void testSharedSingleAckedPartitionedTopic() throws Exception {
        String key = "testSharedSingleAckedPartitionedTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-shared-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;
        final int numberOfPartitions = 3;
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName, numberOfPartitions);
        Random rn = new Random();
        // Special step to create partitioned topic

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        // 2. Create consumer
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(7).subscriptionType(SubscriptionType.Shared).subscribe();

        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer2 = newPulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(7).subscriptionType(SubscriptionType.Shared).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Message produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receive messages
        Message<byte[]> message1 = consumer1.receive();
        Message<byte[]> message2 = consumer2.receive();
        int messageCount1 = 0;
        int messageCount2 = 0;
        int ackCount1 = 0;
        int ackCount2 = 0;
        do {
            if (message1 != null) {
                log.info("Consumer1 received " + new String(message1.getData()));
                messageCount1 += 1;
                if (rn.nextInt() % 3 == 0) {
                    consumer1.acknowledge(message1);
                    log.info("Consumer1 acked " + new String(message1.getData()));
                    ackCount1 += 1;
                }
            }
            if (message2 != null) {
                log.info("Consumer2 received " + new String(message2.getData()));
                messageCount2 += 1;
                if (rn.nextInt() % 3 == 0) {
                    consumer2.acknowledge(message2);
                    log.info("Consumer2 acked " + new String(message2.getData()));
                    ackCount2 += 1;
                }
            }
            message1 = consumer1.receive(500, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
        } while (message1 != null || message2 != null);
        log.info(key + " messageCount1 = " + messageCount1);
        log.info(key + " messageCount2 = " + messageCount2);
        log.info(key + " ackCount1 = " + ackCount1);
        log.info(key + " ackCount2 = " + ackCount2);
        assertEquals(messageCount1 + messageCount2, totalMessages);

        // 5. Ask for redeliver
        log.info(key + ": Sent a Redeliver Message Request");
        consumer1.redeliverUnacknowledgedMessages();
        if ((ackCount1 + ackCount2) == totalMessages) {
            return;
        }

        // 6. Check if Messages redelivered again
        message1 = consumer1.receive(5000, TimeUnit.MILLISECONDS);
        message2 = consumer2.receive(5000, TimeUnit.MILLISECONDS);
        messageCount1 = 0;
        do {
            if (message1 != null) {
                log.info("Consumer1 received " + new String(message1.getData()));
                messageCount1 += 1;
            }
            if (message2 != null) {
                log.info("Consumer2 received " + new String(message2.getData()));
                messageCount2 += 1;
            }
            message1 = consumer1.receive(1000, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(1000, TimeUnit.MILLISECONDS);
        } while (message1 != null || message2 != null);

        log.info(key + " messageCount1 = " + messageCount1);
        log.info(key + " messageCount2 = " + messageCount2);
        log.info(key + " ackCount1 = " + ackCount1);
        log.info(key + " ackCount2 = " + ackCount2);
        newPulsarClient.close();
        assertEquals(messageCount1 + messageCount2 + ackCount1, totalMessages);
    }

    @Test(timeOut = testTimeout)
    public void testFailoverSingleAckedPartitionedTopic() throws Exception {
        String key = "testFailoverSingleAckedPartitionedTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-failover-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;
        final int numberOfPartitions = 3;
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName, numberOfPartitions);
        Random rn = new Random();
        // Special step to create partitioned topic

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();

        // 2. Create consumer
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(7).subscriptionType(SubscriptionType.Failover);
        Consumer<byte[]> consumer1 = consumerBuilder.clone().consumerName("Consumer-1").subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.clone().consumerName("Consumer-2").subscribe();

        Thread.sleep(1000);
        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Message produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receive messages
        Message<byte[]> message1 = consumer1.receive();
        Message<byte[]> message2 = consumer2.receive();
        int messageCount1 = 0;
        int messageCount2 = 0;
        int ackCount1 = 0;
        int ackCount2 = 0;
        do {
            if (message1 != null) {
                log.info("Consumer1 received " + new String(message1.getData()));
                messageCount1 += 1;
                if (rn.nextInt() % 3 == 0) {
                    consumer1.acknowledge(message1);
                    ackCount1 += 1;
                }
            }
            if (message2 != null) {
                log.info("Consumer2 received " + new String(message2.getData()));
                messageCount2 += 1;
                if (rn.nextInt() % 3 == 0) {
                    consumer2.acknowledge(message2);
                    ackCount2 += 1;
                }
            }
            message1 = consumer1.receive(500, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
        } while (message1 != null || message2 != null);
        log.info(key + " messageCount1 = " + messageCount1);
        log.info(key + " messageCount2 = " + messageCount2);
        log.info(key + " ackCount1 = " + ackCount1);
        log.info(key + " ackCount2 = " + ackCount2);
        assertEquals(messageCount1 + messageCount2, totalMessages);
        if ((ackCount1 + ackCount2) == totalMessages) {
            return;
        }
        // 5. Ask for redeliver
        log.info(key + ": Sent a Redeliver Message Request");
        consumer1.redeliverUnacknowledgedMessages();
        consumer1.close();

        // 6. Check if Messages redelivered again
        message2 = consumer2.receive();
        messageCount1 = 0;
        do {
            if (message2 != null) {
                log.info("Consumer2 received " + new String(message2.getData()));
                messageCount2 += 1;
            }
            message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
        } while (message2 != null);
        log.info(key + " messageCount1 = " + messageCount1);
        log.info(key + " messageCount2 = " + messageCount2);
        log.info(key + " ackCount1 = " + ackCount1);
        log.info(key + " ackCount2 = " + ackCount2);
        assertEquals(messageCount2 + ackCount1, totalMessages);
    }

    @Test(timeOut = testTimeout)
    public void testFailoverInactiveConsumer() throws Exception {
        String key = "testFailoverInactiveConsumer";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-failover-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;
        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        // 2. Create consumer
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(10).subscriptionType(SubscriptionType.Failover);
        Consumer<byte[]> consumer1 = consumerBuilder.clone().consumerName("Consumer-1").subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.clone().consumerName("Consumer-2").subscribe();

        // 3. Producer publishes messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
            log.info("Producer produced " + message);
        }

        // 4. Receive messages
        int receivedConsumer1 = 0, receivedConsumer2 = 0;
        Message<byte[]> message1;
        Message<byte[]> message2;
        do {
            message1 = consumer1.receive(500, TimeUnit.MILLISECONDS);
            if (message1 != null) {
                log.info("Consumer 1 Received: " + new String(message1.getData()));
                receivedConsumer1 += 1;
            }
        } while (message1 != null);
        log.info("Consumer 1 receives = " + receivedConsumer1);
        log.info("Consumer 2 receives = " + receivedConsumer2);
        log.info("Total receives = " + (receivedConsumer2 + receivedConsumer1));
        assertEquals(receivedConsumer2 + receivedConsumer1, totalMessages);
        // Consumer 2 is on Stand By
        assertEquals(receivedConsumer2, 0);

        // 5. Consumer 2 asks for a redelivery but the request is ignored
        log.info("Consumer 2 asks for resend");
        consumer2.redeliverUnacknowledgedMessages();
        Thread.sleep(1000);

        message1 = consumer1.receive(500, TimeUnit.MILLISECONDS);
        message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
        assertNull(message1);
        assertNull(message2);
    }

    @SuppressWarnings("unchecked")
    private BlockingQueue<Message<byte[]>> printIncomingMessageQueue(Consumer<byte[]> consumer) throws Exception {
        GrowableArrayBlockingQueue<Message<byte[]>> imq = null;
        ConsumerBase<byte[]> c = (ConsumerBase<byte[]>) consumer;
        Field field = ConsumerBase.class.getDeclaredField("incomingMessages");
        field.setAccessible(true);
        imq = (GrowableArrayBlockingQueue<Message<byte[]>>) field.get(c);
        log.info("Incoming MEssage Queue: {}", imq.toList());
        return imq;
    }

}
