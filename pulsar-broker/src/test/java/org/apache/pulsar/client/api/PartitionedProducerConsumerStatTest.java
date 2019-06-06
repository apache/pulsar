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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static java.util.UUID.randomUUID;
import static org.apache.pulsar.client.api.SimpleProducerConsumerStatTest.validatingLogInfo;
import static org.testng.Assert.*;

public class PartitionedProducerConsumerStatTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(SimpleProducerConsumerStatTest.class);
    private String lookupUrl;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        init();
        lookupUrl = brokerUrl.toString();
        if (isTcpLookup) {
            lookupUrl = new URI("pulsar://localhost:" + BROKER_PORT).toString();
        }
        pulsarClient = newPulsarClient(lookupUrl, 1);
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "batch")
    public Object[][] batchMessageDelayMsProvider() {
        return new Object[][]{
                {0, MessageRoutingMode.CustomPartition}, {1000, MessageRoutingMode.RoundRobinPartition},
                {0, MessageRoutingMode.RoundRobinPartition}, {1000, MessageRoutingMode.RoundRobinPartition}
        };
    }

    @DataProvider(name = "batch_with_timeout")
    public Object[][] ackTimeoutSecProvider() {
        return new Object[][]{
                {0, 0, 3, MessageRoutingMode.RoundRobinPartition},
                {0, 2, 3, MessageRoutingMode.RoundRobinPartition},
                {1000, 0, 3, MessageRoutingMode.RoundRobinPartition},
                {1000, 2, 3, MessageRoutingMode.RoundRobinPartition},
                {1000, 2, 3, MessageRoutingMode.CustomPartition},
        };
    }

    @Test(dataProvider = "batch_with_timeout")
    public void testSyncPartitionedProducerAndPartitionConsumer(
            int batchMessageDelayMs,
            int ackTimeoutSec,
            int intervalInSecs,
            MessageRoutingMode messageRoutingMode
    ) throws Exception {
        log.info("-- Starting {} test --", methodName);
        String randomTopicName = "persistent://my-property/tp1/my-ns/my-topic1"+ randomUUID().toString();
        pulsarClient = newPulsarClient(lookupUrl, intervalInSecs);
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer()
                .topic(randomTopicName)
                .subscriptionName("my-subscriber-name")
                .receiverQueueSize(10);

        // Cumulative Ack-counter works if ackTimeOutTimer-task is enabled
        boolean isAckTimeoutTaskEnabledForCumulativeAck = ackTimeoutSec > 0;
        if (ackTimeoutSec > 0) {
            consumerBuilder.ackTimeout(ackTimeoutSec, TimeUnit.SECONDS);
        }

        Consumer<byte[]> consumer = consumerBuilder.subscribe();
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic(randomTopicName)
                .messageRoutingMode(messageRoutingMode);
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        }
        if (messageRoutingMode.equals(MessageRoutingMode.CustomPartition)) {
            producerBuilder.messageRouter(new AlwaysTwoMessageRouter());
        }
        Producer<byte[]> producer = producerBuilder.create();

        int numMessages = 11;
        for (int i = 0; i < numMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < numMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        Thread.sleep(3000);
        consumer.close();
        producer.close();
        validatingLogInfo(consumer, producer, isAckTimeoutTaskEnabledForCumulativeAck);
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "batch_with_timeout")
    public void testAsyncPartitionedProducerAndAsyncAck(
            int batchMessageDelayMs,
            int ackTimeoutSec,
            int intervalInSecs,
            MessageRoutingMode messageRoutingMode
    ) throws Exception {
        log.info("-- Starting {} test --", methodName);
        String randomTopicName = "persistent://my-property/tp1/my-ns/my-topic2"+ randomUUID().toString();
        pulsarClient = newPulsarClient(lookupUrl, intervalInSecs);
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer()
                .topic(randomTopicName)
                .subscriptionName("my-subscriber-name")
                .receiverQueueSize(10);
        if (ackTimeoutSec > 0) {
            consumerBuilder.ackTimeout(ackTimeoutSec, TimeUnit.SECONDS);
        }

        Consumer<byte[]> consumer = consumerBuilder.subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic(randomTopicName)
                .messageRoutingMode(messageRoutingMode);
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        } else {
            producerBuilder.enableBatching(false);
        }
        if (messageRoutingMode.equals(MessageRoutingMode.CustomPartition)) {
            producerBuilder.messageRouter(new AlwaysTwoMessageRouter());
        }
        Producer<byte[]> producer = producerBuilder.create();
        List<Future<MessageId>> futures = Lists.newArrayList();

        int numMessages = 50;
        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            final String message = "my-message-" + i;
            Future<MessageId> future = producer.sendAsync(message.getBytes());
            futures.add(future);
        }

        log.info("Waiting for async publish to complete");
        for (Future<MessageId> future : futures) {
            future.get();
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < numMessages; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Asynchronously acknowledge upto and including the last message
        Future<Void> ackFuture = consumer.acknowledgeCumulativeAsync(msg);
        log.info("Waiting for async ack to complete");
        ackFuture.get();
        Thread.sleep(3000);
        consumer.close();
        producer.close();
        validatingLogInfo(consumer, producer, batchMessageDelayMs == 0 && ackTimeoutSec > 0);
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "batch_with_timeout")
    public void testAsyncPartitionedProducerAndReceiveAsyncAndAsyncAck(
            int batchMessageDelayMs,
            int ackTimeoutSec,
            int intervalInSecs,
            MessageRoutingMode messageRoutingMode) throws Exception {
        log.info("-- Starting {} test --", methodName);
        String randomTopicName = "persistent://my-property/tp1/my-ns/my-topic2"+ randomUUID().toString();
        pulsarClient = newPulsarClient(lookupUrl, intervalInSecs);
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer()
                .topic(randomTopicName)
                .subscriptionName("my-subscriber-name")
                .receiverQueueSize(10);
        if (ackTimeoutSec > 0) {
            consumerBuilder.ackTimeout(ackTimeoutSec, TimeUnit.SECONDS);
        }

        Consumer<byte[]> consumer = consumerBuilder.subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic(randomTopicName)
                .messageRoutingMode(messageRoutingMode);
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        } else {
            producerBuilder.enableBatching(false);
        }
        if (messageRoutingMode.equals(MessageRoutingMode.CustomPartition)) {
            producerBuilder.messageRouter(new AlwaysTwoMessageRouter());
        }
        Producer<byte[]> producer = producerBuilder.create();
        List<Future<MessageId>> futures = Lists.newArrayList();

        int numMessages = 101;
        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            final String message = "my-message-" + i;
            Future<MessageId> future = producer.sendAsync(message.getBytes());
            futures.add(future);
        }

        log.info("Waiting for async publish to complete");
        for (Future<MessageId> future : futures) {
            future.get();
        }
        Message<byte[]> msg = null;
        CompletableFuture<Message<byte[]>> future_msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < numMessages; i++) {
            future_msg = consumer.receiveAsync();
            Thread.sleep(10);
            msg = future_msg.get();
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Asynchronously acknowledge upto and including the last message
        Future<Void> ackFuture = consumer.acknowledgeCumulativeAsync(msg);
        log.info("Waiting for async ack to complete");
        ackFuture.get();
        Thread.sleep(5000);
        consumer.close();
        producer.close();
        validatingLogInfo(consumer, producer, batchMessageDelayMs == 0 && ackTimeoutSec > 0);
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "batch", timeOut = 100000)
    public void testMessageListener(int batchMessageDelayMs, MessageRoutingMode messageRoutingMode) throws Exception {
        log.info("-- Starting {} test --", methodName);
        int numMessages = 100;
        final CountDownLatch latch = new CountDownLatch(numMessages);
        String randomTopicName = "persistent://my-property/tp1/my-ns/my-topic3"+ randomUUID().toString();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(randomTopicName)
                .subscriptionName("my-subscriber-name").ackTimeout(100, TimeUnit.SECONDS)
                .receiverQueueSize(10)
                .messageListener((consumer1, msg) -> {
                    assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    consumer1.acknowledgeAsync(msg);
                    latch.countDown();
                }).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic(randomTopicName)
                .messageRoutingMode(messageRoutingMode);
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true)
                    .batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        }
        if (messageRoutingMode.equals(MessageRoutingMode.CustomPartition)) {
            producerBuilder.messageRouter(new AlwaysTwoMessageRouter());
        }
        Producer<byte[]> producer = producerBuilder.create();
        List<Future<MessageId>> futures = Lists.newArrayList();

        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            final String message = "my-message-" + i;
            Future<MessageId> future = producer.sendAsync(message.getBytes());
            futures.add(future);
        }

        log.info("Waiting for async publish to complete");
        for (Future<MessageId> future : futures) {
            future.get();
        }
        Thread.sleep(5000);
        log.info("Waiting for message listener to ack all messages");
        assertEquals(latch.await(numMessages, TimeUnit.SECONDS), true, "Timed out waiting for message listener acks");
        consumer.close();
        producer.close();
        validatingLogInfo(consumer, producer, true);
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "batch")
    public void testSendTimeout(int batchMessageDelayMs, MessageRoutingMode messageRoutingMode) throws Exception {
        log.info("-- Starting {} test --", methodName);
        String randomTopicName = "persistent://my-property/tp1/my-ns/my-topic4"+ randomUUID().toString();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(randomTopicName)
                .subscriptionName("my-subscriber-name").receiverQueueSize(10).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic(randomTopicName)
                .sendTimeout(1, TimeUnit.SECONDS)
                .messageRoutingMode(messageRoutingMode);
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(2 * batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        }
        if (messageRoutingMode.equals(MessageRoutingMode.CustomPartition)) {
            producerBuilder.messageRouter(new AlwaysTwoMessageRouter());
        }
        Producer<byte[]> producer = producerBuilder.create();

        final String message = "my-message";

        // Trigger the send timeout
        stopBroker();

        Future<MessageId> future = producer.sendAsync(message.getBytes());

        try {
            future.get();
            fail("Send operation should have failed");
        } catch (ExecutionException e) {
            // Expected
        }

        startBroker();

        // We should not have received any message
        Message<byte[]> msg = consumer.receive(3, TimeUnit.SECONDS);
        assertNull(msg);
        consumer.close();
        producer.close();
        Thread.sleep(1000);
        ConsumerStats cStat = consumer.getStats();
        ProducerStats pStat = producer.getStats();
        assertEquals(pStat.getTotalMsgsSent(), 0);
        assertEquals(pStat.getTotalSendFailed(), 1);
        assertEquals(cStat.getTotalMsgsReceived(), 0);
        assertEquals(cStat.getTotalMsgsReceived(), cStat.getTotalAcksSent());
        log.info("-- Exiting {} test --", methodName);
    }

    private class AlwaysTwoMessageRouter implements MessageRouter {
        @Override
        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
            return 2;
        }
    }
}
