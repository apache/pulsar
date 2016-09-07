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

import static org.testng.Assert.assertEquals;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.service.BrokerTestBase;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.client.impl.ConsumerBase;
import com.yahoo.pulsar.client.impl.MessageIdImpl;

public class UnAcknowledgedMessagesTimeoutTest extends BrokerTestBase {
    private static final long testTimeout = 90000; // 1.5 min
    private static final Logger log = LoggerFactory.getLogger(UnAcknowledgedMessagesTimeoutTest.class);
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
    public void testExclusiveSingleAckedNormalTopic() throws Exception {
        String key = "testExclusiveSingleAckedNormalTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;

        // 1. producer connect
        Producer producer = pulsarClient.createProducer(topicName);

        // 2. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(7);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName, conf);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 2; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message
        Message message = consumer.receive();
        while (message != null) {
            log.info("Consumer received : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
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

        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 0);
        assertEquals(hSet.size(), totalMessages);
    }

    @Test(timeOut = testTimeout)
    public void testExclusiveCumulativeAckedNormalTopic() throws Exception {
        String key = "testExclusiveCumulativeAckedNormalTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;

        // 1. producer connect
        Producer producer = pulsarClient.createProducer(topicName);

        // 2. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(7);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName, conf);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message
        HashSet<String> hSet = new HashSet<>();
        Message message = consumer.receive();
        Message lastMessage = message;
        while (message != null) {
            lastMessage = message;
            hSet.add(new String(message.getData()));
            log.info("Consumer received " + new String(message.getData()));
            log.info("Message ID details " + ((MessageIdImpl) message.getMessageId()).toString());
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        }
        long size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        assertEquals(size, totalMessages);
        log.info("Comulative Ack sent for " + new String(lastMessage.getData()));
        log.info("Message ID details " + ((MessageIdImpl) lastMessage.getMessageId()).toString());
        consumer.acknowledgeCumulative(lastMessage);
        size = ((ConsumerBase) consumer).getUnAckedMessageTracker().size();
        assertEquals(size, 0);
        message = consumer.receive((int) (2 * ackTimeOutMillis), TimeUnit.MILLISECONDS);
        assertEquals(message, null);
    }

    @Test(timeOut = testTimeout)
    public void testSharedSingleAckedPartitionedTopic() throws Exception {
        String key = "testSharedSingleAckedPartitionedTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-shared-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 20;
        final int numberOfPartitions = 3;
        admin.persistentTopics().createPartitionedTopic(topicName, numberOfPartitions);
        // Special step to create partitioned topic

        // 1. producer connect
        ProducerConfiguration prodConfig = new ProducerConfiguration();
        prodConfig.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = pulsarClient.createProducer(topicName, prodConfig);

        // 2. Create consumer
        ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
        consumerConfig.setReceiverQueueSize(100);
        consumerConfig.setSubscriptionType(SubscriptionType.Shared);
        consumerConfig.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        consumerConfig.setConsumerName("Consumer-1");
        Consumer consumer1 = pulsarClient.subscribe(topicName, subscriptionName, consumerConfig);
        consumerConfig.setConsumerName("Consumer-2");
        Consumer consumer2 = pulsarClient.subscribe(topicName, subscriptionName, consumerConfig);

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

    private static int receiveAllMessage(Consumer consumer, boolean ackMessages) throws Exception {
        int messagesReceived = 0;
        Message msg = consumer.receive(1, TimeUnit.SECONDS);
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

    @Test(timeOut = testTimeout)
    public void testFailoverSingleAckedPartitionedTopic() throws Exception {
        String key = "testFailoverSingleAckedPartitionedTopic";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-failover-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 10;
        final int numberOfPartitions = 3;
        admin.persistentTopics().createPartitionedTopic(topicName, numberOfPartitions);
        // Special step to create partitioned topic

        // 1. producer connect
        ProducerConfiguration prodConfig = new ProducerConfiguration();
        prodConfig.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = pulsarClient.createProducer(topicName, prodConfig);

        // 2. Create consumer
        ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
        consumerConfig.setReceiverQueueSize(7);
        consumerConfig.setSubscriptionType(SubscriptionType.Failover);
        consumerConfig.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        consumerConfig.setConsumerName("Consumer-1");
        Consumer consumer1 = pulsarClient.subscribe(topicName, subscriptionName, consumerConfig);
        consumerConfig.setConsumerName("Consumer-2");
        Consumer consumer2 = pulsarClient.subscribe(topicName, subscriptionName, consumerConfig);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Message produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receive messages
        Message message1 = consumer1.receive();
        Message message2 = consumer2.receive();
        int messageCount1 = 0;
        int messageCount2 = 0;
        int ackCount1 = 0;
        int ackCount2 = 0;
        do {
            if (message1 != null) {
                log.info("Consumer1 received " + new String(message1.getData()));
                messageCount1 += 1;
            }
            if (message2 != null) {
                log.info("Consumer2 received " + new String(message2.getData()));
                messageCount2 += 1;
                consumer2.acknowledge(message2);
                ackCount2 += 1;
            }
            message1 = consumer1.receive(500, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
        } while (message1 != null || message2 != null);
        log.info(key + " messageCount1 = " + messageCount1);
        log.info(key + " messageCount2 = " + messageCount2);
        log.info(key + " ackCount1 = " + ackCount1);
        log.info(key + " ackCount2 = " + ackCount2);
        assertEquals(messageCount1 + messageCount2, totalMessages);

        // 5. Check if Messages redelivered again
        // Since receive is a blocking call hoping that timeout will kick in
        log.info(key + " Timeout should be triggered now");
        message1 = consumer1.receive();
        messageCount1 = 0;
        do {
            if (message1 != null) {
                log.info("Consumer1 received " + new String(message1.getData()));
                messageCount1 += 1;
                consumer1.acknowledge(message1);
                ackCount1 += 1;
            }
            if (message2 != null) {
                log.info("Consumer2 received " + new String(message2.getData()));
                messageCount2 += 1;
            }
            message1 = consumer1.receive(500, TimeUnit.MILLISECONDS);
            message2 = consumer2.receive(500, TimeUnit.MILLISECONDS);
        } while (message1 != null || message2 != null);
        log.info(key + " messageCount1 = " + messageCount1);
        log.info(key + " messageCount2 = " + messageCount2);
        log.info(key + " ackCount1 = " + ackCount1);
        log.info(key + " ackCount2 = " + ackCount2);
        assertEquals(ackCount1 + messageCount2, totalMessages);
    }

    @Test
    public void testAckTimeoutMinValue() throws PulsarClientException {
        ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
        consumerConfig.setReceiverQueueSize(7);
        consumerConfig.setSubscriptionType(SubscriptionType.Failover);
        try {
            consumerConfig.setAckTimeout(999, TimeUnit.MILLISECONDS);
            Assert.fail("Exception should have been thrown since the set timeout is less than min timeout.");
        } catch (Exception ex) {
            // Ok
        }
    }

    @Test(timeOut = testTimeout)
    public void testCheckUnAcknowledgedMessageTimer() throws PulsarClientException, InterruptedException {
        String key = "testCheckUnAcknowledgedMessageTimer";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 3;

        // 1. producer connect
        Producer producer = pulsarClient.createProducer(topicName);

        // 2. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(7);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        ConsumerBase consumer = (ConsumerBase) pulsarClient.subscribe(topicName, subscriptionName, conf);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Consumer receives the message
        assertEquals(consumer.getUnAckedMessageTracker().size(), 0);
        consumer.receive();
        assertEquals(consumer.getUnAckedMessageTracker().size(), 1);
        Thread.sleep(ackTimeOutMillis);
        consumer.receive();
        assertEquals(consumer.getUnAckedMessageTracker().size(), 2);
        Thread.sleep((long) (ackTimeOutMillis * 1.1));
        // Timeout should have been triggered here

        // 5. Trying to receive messages again
        assertEquals(consumer.getUnAckedMessageTracker().size(), 0);
        Message message1 = consumer.receive();
        assertEquals(consumer.getUnAckedMessageTracker().size(), 1);
        Thread.sleep(ackTimeOutMillis);
        Message message2 = consumer.receive();
        assertEquals(consumer.getUnAckedMessageTracker().size(), 2);
        consumer.acknowledge(message1);
        consumer.acknowledge(message2);
        assertEquals(consumer.getUnAckedMessageTracker().size(), 0);
    }

    @Test()
    public void testConfiguration() {
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setAckTimeout(10, TimeUnit.MINUTES);
        assertEquals(conf.getAckTimeoutMillis(), 10 * 60 * 1000);
        conf.setAckTimeout(11, TimeUnit.SECONDS);
        assertEquals(conf.getAckTimeoutMillis(), 11 * 1000);
        conf.setAckTimeout(15000000, TimeUnit.MICROSECONDS);
        assertEquals(conf.getAckTimeoutMillis(), 15000);
        conf.setAckTimeout(17000, TimeUnit.MILLISECONDS);
        assertEquals(conf.getAckTimeoutMillis(), 17000);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        assertEquals(conf.getAckTimeoutMillis(), ackTimeOutMillis);
    }
}
