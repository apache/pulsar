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
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;

import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ZeroQueueSizeTest extends BrokerTestBase {
    private static final Logger log = LoggerFactory.getLogger(ZeroQueueSizeTest.class);
    private final int totalMessages = 10;

    @BeforeClass
    @Override
    public void setup() throws Exception {
        baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void validQueueSizeConfig() {
        pulsarClient.newConsumer().receiverQueueSize(0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void InvalidQueueSizeConfig() {
        pulsarClient.newConsumer().receiverQueueSize(-1);
    }

    @Test(expectedExceptions = PulsarClientException.InvalidConfigurationException.class)
    public void zeroQueueSizeReceiveAsyncInCompatibility() throws PulsarClientException {
        String key = "zeroQueueSizeReceiveAsyncInCompatibility";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(0).subscribe();
        consumer.receive(10, TimeUnit.SECONDS);
    }

    @Test(expectedExceptions = PulsarClientException.class)
    public void zeroQueueSizePartitionedTopicInCompatibility() throws PulsarClientException, PulsarAdminException {
        String key = "zeroQueueSizePartitionedTopicInCompatibility";
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        int numberOfPartitions = 3;
        admin.topics().createPartitionedTopic(topicName, numberOfPartitions);
        pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).receiverQueueSize(0).subscribe();
    }

    @Test
    public void zeroQueueSizeNormalConsumer() throws PulsarClientException {
        String key = "nonZeroQueueSizeNormalConsumer";

        // 1. Config
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(0).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message
        Message<byte[]> message;
        for (int i = 0; i < totalMessages; i++) {
            assertEquals(consumer.numMessagesInQueue(), 0);
            message = consumer.receive();
            assertEquals(new String(message.getData()), messagePredicate + i);
            assertEquals(consumer.numMessagesInQueue(), 0);
            log.info("Consumer received : " + new String(message.getData()));
        }
    }

    @Test
    public void zeroQueueSizeConsumerListener() throws Exception {
        String key = "zeroQueueSizeConsumerListener";

        // 1. Config
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        List<Message<byte[]>> messages = Lists.newArrayList();
        CountDownLatch latch = new CountDownLatch(totalMessages);
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(0).messageListener((cons, msg) -> {
                    assertEquals(((ConsumerImpl) cons).numMessagesInQueue(), 0);
                    synchronized(messages) {
                        messages.add(msg);
                    }
                    log.info("Consumer received: " + new String(msg.getData()));
                    latch.countDown();
                }).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message
        latch.await();
        assertEquals(consumer.numMessagesInQueue(), 0);
        assertEquals(messages.size(), totalMessages);
        for (int i = 0; i < messages.size(); i++) {
            assertEquals(new String(messages.get(i).getData()), messagePredicate + i);
        }
    }

    @Test
    public void zeroQueueSizeSharedSubscription() throws PulsarClientException {
        String key = "zeroQueueSizeSharedSubscription";

        // 1. Config
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        int numOfSubscribers = 4;
        ConsumerImpl<?>[] consumers = new ConsumerImpl[numOfSubscribers];
        for (int i = 0; i < numOfSubscribers; i++) {
            consumers[i] = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriptionName).receiverQueueSize(0).subscriptionType(SubscriptionType.Shared)
                    .subscribe();
        }

        // 4. Produce Messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }

        // 5. Consume messages
        Message<?> message;
        for (int i = 0; i < totalMessages; i++) {
            assertEquals(consumers[i % numOfSubscribers].numMessagesInQueue(), 0);
            message = consumers[i % numOfSubscribers].receive();
            assertEquals(new String(message.getData()), messagePredicate + i);
            assertEquals(consumers[i % numOfSubscribers].numMessagesInQueue(), 0);
            log.info("Consumer received : " + new String(message.getData()));
        }
    }

    @Test
    public void zeroQueueSizeFailoverSubscription() throws PulsarClientException {
        String key = "zeroQueueSizeFailoverSubscription";

        // 1. Config
        final String topicName = "persistent://prop/use/ns-abc/topic-" + key;
        final String subscriptionName = "my-ex-subscription-" + key;
        final String messagePredicate = "my-message-" + key + "-";

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(0).subscriptionType(SubscriptionType.Failover)
                .consumerName("consumer-1").subscribe();
        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(0).subscriptionType(SubscriptionType.Failover)
                .consumerName("consumer-2").subscribe();

        // 4. Produce Messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }

        // 5. Consume messages
        Message<byte[]> message;
        for (int i = 0; i < totalMessages / 2; i++) {
            assertEquals(consumer1.numMessagesInQueue(), 0);
            message = consumer1.receive();
            assertEquals(new String(message.getData()), messagePredicate + i);
            assertEquals(consumer1.numMessagesInQueue(), 0);
            log.info("Consumer received : " + new String(message.getData()));
        }

        // 6. Trigger redelivery
        consumer1.redeliverUnacknowledgedMessages();

        // 7. Trigger Failover
        consumer1.close();

        // 8. Receive messages on failed over consumer
        for (int i = 0; i < totalMessages / 2; i++) {
            assertEquals(consumer2.numMessagesInQueue(), 0);
            message = consumer2.receive();
            assertEquals(new String(message.getData()), messagePredicate + i);
            assertEquals(consumer2.numMessagesInQueue(), 0);
            log.info("Consumer received : " + new String(message.getData()));
        }
    }

    @Test
    public void testFailedZeroQueueSizeBatchMessage() throws PulsarClientException {

        int batchMessageDelayMs = 100;
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns-abc/topic1")
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared).receiverQueueSize(0)
                .subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
            .topic("persistent://prop-xyz/use/ns-abc/topic1")
            .messageRoutingMode(MessageRoutingMode.SinglePartition);

        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        } else {
            producerBuilder.enableBatching(false);
        }

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        try {
            consumer.receiveAsync().handle((ok, e) -> {
                if (e == null) {
                    // as zero receiverQueueSize doesn't support batch message, must receive exception at callback.
                    Assert.fail();
                }
                return null;
            });
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testZeroQueueSizeMessageRedelivery() throws PulsarClientException {
        final String topic = "persistent://prop/ns-abc/testZeroQueueSizeMessageRedelivery";
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .receiverQueueSize(0)
            .subscriptionName("sub")
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(1, TimeUnit.SECONDS)
            .subscribe();

        final int messages = 10;
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < messages; i++) {
            producer.send(i);
        }

        Set<Integer> receivedMessages = new HashSet<>();
        for (int i = 0; i < messages * 2; i++) {
            receivedMessages.add(consumer.receive().getValue());
        }

        Assert.assertEquals(receivedMessages.size(), messages);

        consumer.close();
        producer.close();
    }

    @Test
    public void testZeroQueueSizeMessageRedeliveryForListener() throws Exception {
        final String topic = "persistent://prop/ns-abc/testZeroQueueSizeMessageRedeliveryForListener";
        final int messages = 10;
        final CountDownLatch latch = new CountDownLatch(messages * 2);
        Set<Integer> receivedMessages = new HashSet<>();
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .receiverQueueSize(0)
            .subscriptionName("sub")
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(1, TimeUnit.SECONDS)
            .messageListener((MessageListener<Integer>) (c, msg) -> {
                try {
                    receivedMessages.add(msg.getValue());
                } finally {
                    latch.countDown();
                }
            })
            .subscribe();

        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < messages; i++) {
            producer.send(i);
        }

        latch.await();
        Assert.assertEquals(receivedMessages.size(), messages);

        consumer.close();
        producer.close();
    }

    @Test
    public void testZeroQueueSizeMessageRedeliveryForAsyncReceive() throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "persistent://prop/ns-abc/testZeroQueueSizeMessageRedeliveryForAsyncReceive";
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .receiverQueueSize(0)
            .subscriptionName("sub")
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(1, TimeUnit.SECONDS)
            .subscribe();

        final int messages = 10;
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < messages; i++) {
            producer.send(i);
        }

        Set<Integer> receivedMessages = new HashSet<>();
        List<CompletableFuture<Message<Integer>>> futures = new ArrayList<>(20);
        for (int i = 0; i < messages * 2; i++) {
            futures.add(consumer.receiveAsync());
        }
        for (CompletableFuture<Message<Integer>> future : futures) {
            receivedMessages.add(future.get().getValue());
        }

        Assert.assertEquals(receivedMessages.size(), messages);

        consumer.close();
        producer.close();
    }

    @Test(timeOut = 30000)
    public void testPauseAndResume() throws Exception {
        final String topicName = "persistent://prop/ns-abc/zero-queue-pause-and-resume";
        final String subName = "sub";

        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
        AtomicInteger received = new AtomicInteger();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(0).messageListener((c1, msg) -> {
                    assertNotNull(msg, "Message cannot be null");
                    c1.acknowledgeAsync(msg);
                    received.incrementAndGet();
                    latch.get().countDown();
                }).subscribe();
        consumer.pause();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create();

        for (int i = 0; i < 2; i++) {
            producer.send(("my-message-" + i).getBytes());
        }

        // Paused consumer receives only one message
        assertTrue(latch.get().await(2, TimeUnit.SECONDS), "Timed out waiting for message listener acks");
        Thread.sleep(2000);
        assertEquals(received.intValue(), 1, "Consumer received messages while paused");

        latch.set(new CountDownLatch(1));
        consumer.resume();
        assertTrue(latch.get().await(2, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        consumer.unsubscribe();
        producer.close();
    }

    @Test(timeOut = 30000)
    public void testPauseAndResumeWithUnloading() throws Exception {
        final String topicName = "persistent://prop/ns-abc/zero-queue-pause-and-resume-with-unloading";
        final String subName = "sub";

        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
        AtomicInteger received = new AtomicInteger();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(0).messageListener((c1, msg) -> {
                    assertNotNull(msg, "Message cannot be null");
                    c1.acknowledgeAsync(msg);
                    received.incrementAndGet();
                    latch.get().countDown();
                }).subscribe();
        consumer.pause();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create();

        for (int i = 0; i < 2; i++) {
            producer.send(("my-message-" + i).getBytes());
        }

        // Paused consumer receives only one message
        assertTrue(latch.get().await(2, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        // Make sure no flow permits are sent when the consumer reconnects to the topic
        admin.topics().unload(topicName);
        Thread.sleep(2000);
        assertEquals(received.intValue(), 1, "Consumer received messages while paused");

        latch.set(new CountDownLatch(1));
        consumer.resume();
        assertTrue(latch.get().await(2, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        consumer.unsubscribe();
        producer.close();
    }

    @Test(timeOut = 30000)
    public void testPauseAndResumeNoReconnection() throws Exception {
        final String topicName = "persistent://prop/ns-abc/zero-queue-pause-and-resume-no-reconnection";
        final String subName = "sub";

        final Object object = new Object();
        AtomicBoolean running = new AtomicBoolean(false);

        final List<Integer> receivedMessages = Collections.synchronizedList(new ArrayList<>());
        final Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topicName)
                .subscriptionName(subName)
                .receiverQueueSize(0)
                .messageListener((MessageListener<Integer>) (consumer1, msg) -> {
                    assertNotNull(msg, "Message cannot be null");
                    receivedMessages.add(msg.getValue());
                    try {
                        consumer1.acknowledge(msg);
                    } catch (PulsarClientException ignored) {
                    }
                    synchronized (object) {
                        running.set(false);
                        object.notifyAll();
                    }
                }).subscribe();
        final Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .enableBatching(false)
                .create();
        final int numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            consumer.resume();
            producer.newMessage().value(i).sendAsync();
            synchronized (object) {
                running.set(true);
                while (running.get()) {
                    object.wait();
                }
            }
            consumer.pause();
        }

        log.info("Received messages: {}", receivedMessages);
        assertEquals(receivedMessages.size(), numMessages);
        for (int i = 0; i < numMessages; i++) {
            assertEquals(receivedMessages.get(i).intValue(), i);
        }
    }
}
