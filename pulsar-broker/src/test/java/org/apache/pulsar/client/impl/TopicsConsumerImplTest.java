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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.netty.util.Timeout;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TopicsConsumerImplTest extends ProducerConsumerBase {
    private static final long testTimeout = 90000; // 1.5 min
    private static final Logger log = LoggerFactory.getLogger(TopicsConsumerImplTest.class);
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

    // Verify subscribe topics from different namespace should return error.
    @Test(timeOut = testTimeout)
    public void testDifferentTopicsNameSubscribe() throws Exception {
        String key = "TopicsFromDifferentNamespace";
        final String subscriptionName = "my-ex-subscription-" + key;

        final String topicName1 = "persistent://prop/use/ns-abc1/topic-1-" + key;
        final String topicName2 = "persistent://prop/use/ns-abc2/topic-2-" + key;
        final String topicName3 = "persistent://prop/use/ns-abc3/topic-3-" + key;
        List<String> topicNames = Lists.newArrayList(topicName1, topicName2, topicName3);

        admin.tenants().createTenant("prop", new TenantInfo());
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 2. Create consumer
        try {
            pulsarClient.newConsumer()
                .topics(topicNames)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscribe();
            fail("subscribe for topics from different namespace should fail.");
        } catch (IllegalArgumentException e) {
            // expected for have different namespace
        }
    }

    @Test(timeOut = testTimeout)
    public void testGetConsumersAndGetTopics() throws Exception {
        String key = "TopicsConsumerGet";
        final String subscriptionName = "my-ex-subscription-" + key;

        final String topicName1 = "persistent://prop/use/ns-abc/topic-1-" + key;
        final String topicName2 = "persistent://prop/use/ns-abc/topic-2-" + key;
        final String topicName3 = "persistent://prop/use/ns-abc/topic-3-" + key;
        List<String> topicNames = Lists.newArrayList(topicName1, topicName2);

        admin.tenants().createTenant("prop", new TenantInfo());
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topics(topicNames)
            .topic(topicName3)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();
        assertTrue(consumer instanceof MultiTopicsConsumerImpl);
        assertTrue(consumer.getTopic().startsWith(MultiTopicsConsumerImpl.DUMMY_TOPIC_NAME_PREFIX));

        List<String> topics = ((MultiTopicsConsumerImpl<byte[]>) consumer).getPartitionedTopics();
        List<ConsumerImpl<byte[]>> consumers = ((MultiTopicsConsumerImpl) consumer).getConsumers();

        topics.forEach(topic -> log.info("topic: {}", topic));
        consumers.forEach(c -> log.info("consumer: {}", c.getTopic()));

        IntStream.range(0, 6).forEach(index ->
            assertTrue(topics.get(index).equals(consumers.get(index).getTopic())));

        assertTrue(((MultiTopicsConsumerImpl<byte[]>) consumer).getTopics().size() == 3);

        consumer.unsubscribe();
        consumer.close();
    }

    @Test(timeOut = testTimeout)
    public void testSyncProducerAndConsumer() throws Exception {
        String key = "TopicsConsumerSyncTest";
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 30;

        final String topicName1 = "persistent://prop/use/ns-abc/topic-1-" + key;
        final String topicName2 = "persistent://prop/use/ns-abc/topic-2-" + key;
        final String topicName3 = "persistent://prop/use/ns-abc/topic-3-" + key;
        List<String> topicNames = Lists.newArrayList(topicName1, topicName2, topicName3);

        admin.tenants().createTenant("prop", new TenantInfo());
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 1. producer connect
        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topics(topicNames)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();
        assertTrue(consumer instanceof MultiTopicsConsumerImpl);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        int messageSet = 0;
        Message<byte[]> message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
    }

    @Test(timeOut = testTimeout)
    public void testAsyncConsumer() throws Exception {
        String key = "TopicsConsumerAsyncTest";
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 30;

        final String topicName1 = "persistent://prop/use/ns-abc/topic-1-" + key;
        final String topicName2 = "persistent://prop/use/ns-abc/topic-2-" + key;
        final String topicName3 = "persistent://prop/use/ns-abc/topic-3-" + key;
        List<String> topicNames = Lists.newArrayList(topicName1, topicName2, topicName3);

        admin.tenants().createTenant("prop", new TenantInfo());
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 1. producer connect
        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topics(topicNames)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();
        assertTrue(consumer instanceof MultiTopicsConsumerImpl);

        // Asynchronously produce messages
        List<Future<MessageId>> futures = Lists.newArrayList();
        for (int i = 0; i < totalMessages / 3; i++) {
            futures.add(producer1.sendAsync((messagePredicate + "producer1-" + i).getBytes()));
            futures.add(producer2.sendAsync((messagePredicate + "producer2-" + i).getBytes()));
            futures.add(producer3.sendAsync((messagePredicate + "producer3-" + i).getBytes()));
        }
        log.info("Waiting for async publish to complete : {}", futures.size());
        for (Future<MessageId> future : futures) {
            future.get();
        }

        log.info("start async consume");
        CountDownLatch latch = new CountDownLatch(totalMessages);
        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(() -> IntStream.range(0, totalMessages).forEach(index ->
            consumer.receiveAsync()
                .thenAccept(msg -> {
                    assertTrue(msg instanceof TopicMessageImpl);
                    try {
                        consumer.acknowledge(msg);
                    } catch (PulsarClientException e1) {
                        fail("message acknowledge failed", e1);
                    }
                    latch.countDown();
                    log.info("receive index: {}, latch countDown: {}", index, latch.getCount());
                })
                .exceptionally(ex -> {
                    log.warn("receive index: {}, failed receive message {}", index, ex.getMessage());
                    ex.printStackTrace();
                    return null;
                })));

        latch.await();
        log.info("success latch wait");

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
    }

    @Test(timeOut = testTimeout)
    public void testConsumerUnackedRedelivery() throws Exception {
        String key = "TopicsConsumerRedeliveryTest";
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 30;

        final String topicName1 = "persistent://prop/use/ns-abc/topic-1-" + key;
        final String topicName2 = "persistent://prop/use/ns-abc/topic-2-" + key;
        final String topicName3 = "persistent://prop/use/ns-abc/topic-3-" + key;
        List<String> topicNames = Lists.newArrayList(topicName1, topicName2, topicName3);

        admin.tenants().createTenant("prop", new TenantInfo());
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 1. producer connect
        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topics(topicNames)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();
        assertTrue(consumer instanceof MultiTopicsConsumerImpl);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        // 4. Receiver receives the message, not ack, Unacked Message Tracker size should be totalMessages.
        Message<byte[]> message = consumer.receive();
        while (message != null) {
            assertTrue(message instanceof TopicMessageImpl);
            log.debug("Consumer received : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        }
        long size = ((MultiTopicsConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
        log.debug(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, totalMessages);

        // 5. Blocking call, redeliver should kick in, after receive and ack, Unacked Message Tracker size should be 0.
        message = consumer.receive();
        HashSet<String> hSet = new HashSet<>();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            hSet.add(new String(message.getData()));
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);

        size = ((MultiTopicsConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
        log.debug(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 0);
        assertEquals(hSet.size(), totalMessages);

        // 6. producer publish more messages
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-round2" + i).getBytes());
            producer2.send((messagePredicate + "producer2-round2" + i).getBytes());
            producer3.send((messagePredicate + "producer3-round2" + i).getBytes());
        }

        // 7. Receiver receives the message, ack them
        message = consumer.receive();
        int received = 0;
        while (message != null) {
            assertTrue(message instanceof TopicMessageImpl);
            received++;
            String data = new String(message.getData());
            log.debug("Consumer received : " + data);
            consumer.acknowledge(message);
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        size = ((MultiTopicsConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
        log.debug(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 0);
        assertEquals(received, totalMessages);

        // 8. Simulate ackTimeout
        Thread.sleep(ackTimeOutMillis);

        // 9. producer publish more messages
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-round3" + i).getBytes());
            producer2.send((messagePredicate + "producer2-round3" + i).getBytes());
            producer3.send((messagePredicate + "producer3-round3" + i).getBytes());
        }

        // 10. Receiver receives the message, doesn't ack
        message = consumer.receive();
        while (message != null) {
            String data = new String(message.getData());
            log.debug("Consumer received : " + data);
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        size = ((MultiTopicsConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
        log.debug(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 30);

        Thread.sleep(ackTimeOutMillis);

        // 11. Receiver receives redelivered messages
        message = consumer.receive();
        int redelivered = 0;
        while (message != null) {
            assertTrue(message instanceof TopicMessageImpl);
            redelivered++;
            String data = new String(message.getData());
            log.debug("Consumer received : " + data);
            consumer.acknowledge(message);
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
        }
        assertEquals(redelivered, 30);
        size =  ((MultiTopicsConsumerImpl<byte[]>) consumer).getUnAckedMessageTracker().size();
        log.info(key + " Unacked Message Tracker size is " + size);
        assertEquals(size, 0);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
    }

    @Test
    public void testSubscribeUnsubscribeSingleTopic() throws Exception {
        String key = "TopicsConsumerSubscribeUnsubscribeSingleTopicTest";
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 30;

        final String topicName1 = "persistent://prop/use/ns-abc/topic-1-" + key;
        final String topicName2 = "persistent://prop/use/ns-abc/topic-2-" + key;
        final String topicName3 = "persistent://prop/use/ns-abc/topic-3-" + key;
        List<String> topicNames = Lists.newArrayList(topicName1, topicName2, topicName3);

        admin.tenants().createTenant("prop", new TenantInfo());
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 1. producer connect
        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();

        // 2. Create consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topics(topicNames)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();
        assertTrue(consumer instanceof MultiTopicsConsumerImpl);

        // 3. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        int messageSet = 0;
        Message<byte[]> message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        // 4, unsubscribe topic3
        CompletableFuture<Void> unsubFuture = ((MultiTopicsConsumerImpl<byte[]>) consumer).unsubscribeAsync(topicName3);
        unsubFuture.get();

        // 5. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-round2" + i).getBytes());
            producer2.send((messagePredicate + "producer2-round2" + i).getBytes());
            producer3.send((messagePredicate + "producer3-round2" + i).getBytes());
        }

        // 6. should not receive messages from topic3, verify get 2/3 of all messages
        messageSet = 0;
        message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages * 2 / 3);

        // 7. use getter to verify internal topics number after un-subscribe topic3
        List<String> topics = ((MultiTopicsConsumerImpl<byte[]>) consumer).getPartitionedTopics();
        List<ConsumerImpl<byte[]>> consumers = ((MultiTopicsConsumerImpl) consumer).getConsumers();

        assertEquals(topics.size(), 3);
        assertEquals(consumers.size(), 3);
        assertTrue(((MultiTopicsConsumerImpl<byte[]>) consumer).getTopics().size() == 2);

        // 8. re-subscribe topic3
        CompletableFuture<Void> subFuture = ((MultiTopicsConsumerImpl<byte[]>)consumer).subscribeAsync(topicName3);
        subFuture.get();

        // 9. producer publish messages
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-round3" + i).getBytes());
            producer2.send((messagePredicate + "producer2-round3" + i).getBytes());
            producer3.send((messagePredicate + "producer3-round3" + i).getBytes());
        }

        // 10. should receive messages from all 3 topics
        messageSet = 0;
        message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        // 11. use getter to verify internal topics number after subscribe topic3
        topics = ((MultiTopicsConsumerImpl<byte[]>) consumer).getPartitionedTopics();
        consumers = ((MultiTopicsConsumerImpl) consumer).getConsumers();

        assertEquals(topics.size(), 6);
        assertEquals(consumers.size(), 6);
        assertTrue(((MultiTopicsConsumerImpl<byte[]>) consumer).getTopics().size() == 3);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
    }


    @Test(timeOut = testTimeout)
    public void testTopicsNameSubscribeWithBuilderFail() throws Exception {
        String key = "TopicsNameSubscribeWithBuilder";
        final String subscriptionName = "my-ex-subscription-" + key;

        final String topicName2 = "persistent://prop/use/ns-abc/topic-2-" + key;
        final String topicName3 = "persistent://prop/use/ns-abc/topic-3-" + key;

        admin.tenants().createTenant("prop", new TenantInfo());
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // test failing builder with empty topics
        try {
            pulsarClient.newConsumer()
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscribe();
            fail("subscribe1 with no topicName should fail.");
        } catch (PulsarClientException e) {
            // expected
        }

        try {
            pulsarClient.newConsumer()
                .topic()
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscribe();
            fail("subscribe2 with no topicName should fail.");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            pulsarClient.newConsumer()
                .topics(null)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscribe();
            fail("subscribe3 with no topicName should fail.");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            pulsarClient.newConsumer()
                .topics(Lists.newArrayList())
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscribe();
            fail("subscribe4 with no topicName should fail.");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * Test Listener for github issue #2547
     */
    @Test(timeOut = 30000)
    public void testMultiTopicsMessageListener() throws Exception {
        String key = "MultiTopicsMessageListenerTest";
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 6;

        // set latch larger than totalMessages, so timeout message get resend
        CountDownLatch latch = new CountDownLatch(totalMessages * 3);

        final String topicName1 = "persistent://prop/use/ns-abc/topic-1-" + key;
        List<String> topicNames = Lists.newArrayList(topicName1);

        admin.tenants().createTenant("prop", new TenantInfo());
        admin.topics().createPartitionedTopic(topicName1, 2);

        // 1. producer connect
        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 2. Create consumer, set not ack in message listener, so time-out message will resend
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topics(topicNames)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(1000, TimeUnit.MILLISECONDS)
            .receiverQueueSize(100)
            .messageListener((c1, msg) -> {
                assertNotNull(msg, "Message cannot be null");
                String receivedMessage = new String(msg.getData());
                latch.countDown();

                log.info("Received message [{}] in the listener, latch: {}",
                    receivedMessage, latch.getCount());
                // since not acked, it should retry another time
                //c1.acknowledgeAsync(msg);
            })
            .subscribe();
        assertTrue(consumer instanceof MultiTopicsConsumerImpl);

        MultiTopicsConsumerImpl topicsConsumer = (MultiTopicsConsumerImpl) consumer;

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
        }

        // verify should not time out, because of message redelivered several times.
        latch.await();

        consumer.close();
    }


    /**
     * Test topic partitions auto subscribed.
     *
     * Steps:
     * 1. Create a consumer with 2 topics, and each topic has 2 partitions: xx-partition-0, xx-partition-1.
     * 2. produce message to xx-partition-2, and verify consumer could not receive message.
     * 3. update topics to have 3 partitions.
     * 4. trigger partitionsAutoUpdate. this should be done automatically, this is to save time to manually trigger.
     * 5. produce message to xx-partition-2 again,  and verify consumer could receive message.
     *
     */
    @Test(timeOut = 30000)
    public void testTopicAutoUpdatePartitions() throws Exception {
        String key = "TestTopicAutoUpdatePartitions";
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";
        final int totalMessages = 6;

        final String topicName1 = "persistent://prop/use/ns-abc/topic-1-" + key;
        final String topicName2 = "persistent://prop/use/ns-abc/topic-2-" + key;
        List<String> topicNames = Lists.newArrayList(topicName1, topicName2);

        admin.tenants().createTenant("prop", new TenantInfo());
        admin.topics().createPartitionedTopic(topicName1, 2);
        admin.topics().createPartitionedTopic(topicName2, 2);

        // 1. Create a  consumer
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topics(topicNames)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .autoUpdatePartitions(true)
            .subscribe();
        assertTrue(consumer instanceof MultiTopicsConsumerImpl);

        MultiTopicsConsumerImpl topicsConsumer = (MultiTopicsConsumerImpl) consumer;

        // 2. use partition-2 producer,
        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1 + "-partition-2")
            .enableBatching(false)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2 + "-partition-2")
            .enableBatching(false)
            .create();
        for (int i = 0; i < totalMessages; i++) {
            producer1.send((messagePredicate + "topic1-partition-2 index:" + i).getBytes());
            producer2.send((messagePredicate + "topic2-partition-2 index:" + i).getBytes());
            log.info("produce message to partition-2. message index: {}", i);
        }
        // since partition-2 not subscribed,  could not receive any message.
        Message<byte[]> message = consumer.receive(200, TimeUnit.MILLISECONDS);
        assertNull(message);

        // 3. update to 3 partitions
        admin.topics().updatePartitionedTopic(topicName1, 3);
        admin.topics().updatePartitionedTopic(topicName2, 3);

        // 4. trigger partitionsAutoUpdate. this should be done automatically in 1 minutes,
        // this is to save time to manually trigger.
        log.info("trigger partitionsAutoUpdateTimerTask");
        Timeout timeout = topicsConsumer.getPartitionsAutoUpdateTimeout();
        timeout.task().run(timeout);
        Thread.sleep(200);

        // 5. produce message to xx-partition-2 again,  and verify consumer could receive message.
        for (int i = 0; i < totalMessages; i++) {
            producer1.send((messagePredicate + "topic1-partition-2 index:" + i).getBytes());
            producer2.send((messagePredicate + "topic2-partition-2 index:" + i).getBytes());
            log.info("produce message to partition-2 again. messageindex: {}", i);
        }
        int messageSet = 0;
        message = consumer.receive();
        do {
            messageSet ++;
            consumer.acknowledge(message);
            log.info("4 Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(200, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, 2 * totalMessages);

        consumer.close();
    }
    
    @Test(timeOut = testTimeout)
    public void testDefaultBacklogTTL() throws Exception {

        int defaultTTLSec = 1;
        int totalMessages = 10;
        this.conf.setTtlDurationDefaultInSeconds(defaultTTLSec);

        final String namespace = "prop/use/expiry";
        final String topicName = "persistent://" + namespace + "/expiry";
        final String subName = "expiredSub";

        admin.clusters().createCluster("use", new ClusterData(brokerUrl.toString()));

        admin.tenants().createTenant("prop", new TenantInfo(null, Sets.newHashSet("use")));
        admin.namespaces().createNamespace(namespace);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared).ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create();
        for (int i = 0; i < totalMessages; i++) {
            producer.send(("" + i).getBytes());
        }

        Optional<Topic> topic = pulsar.getBrokerService().getTopic(topicName, false).get();
        assertTrue(topic.isPresent());
        PersistentSubscription subscription = (PersistentSubscription) topic.get().getSubscription(subName);

        Thread.sleep((defaultTTLSec + 5) * 1000);

        topic.get().checkMessageExpiry();

        retryStrategically((test) -> subscription.getNumberOfEntriesInBacklog() == 0, 5, 200);

        assertEquals(subscription.getNumberOfEntriesInBacklog(), 0);
    }
    
}
