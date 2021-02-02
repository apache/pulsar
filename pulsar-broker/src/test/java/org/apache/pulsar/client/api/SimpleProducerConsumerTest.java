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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import lombok.Cleanup;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.mledger.impl.EntryCacheImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SimpleProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(SimpleProducerConsumerTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @DataProvider
    public static Object[][] variationsForExpectedPos() {
        return new Object[][] {
                // batching / start-inclusive / num-of-messages
                {true, true, 10 },
                {true, false, 10 },
                {false, true, 10 },
                {false, false, 10 },

                {true, true, 100 },
                {true, false, 100 },
                {false, true, 100 },
                {false, false, 100 },
        };
    }

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPublishTimestampBatchDisabled() throws Exception {

        log.info("-- Starting {} test --", methodName);

        AtomicLong ticker = new AtomicLong(0);

        Clock clock = new Clock() {
            @Override
            public ZoneId getZone() {
                return ZoneId.systemDefault();
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return Instant.ofEpochMilli(millis());
            }

            @Override
            public long millis() {
                return ticker.incrementAndGet();
            }
        };

        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder()
            .serviceUrl(lookupUrl.toString())
            .clock(clock)
            .build();

        final String topic = "persistent://my-property/my-ns/test-publish-timestamp";

        @Cleanup
        Consumer<byte[]> consumer = newPulsarClient.newConsumer()
            .topic(topic)
            .subscriptionName("my-sub")
            .subscribe();

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .create();

        final int numMessages = 5;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage()
                .value(("value-" + i).getBytes(UTF_8))
                .eventTime((i + 1) * 100L)
                .sendAsync();
        }
        producer.flush();

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive();
            log.info("Received message '{}'.", new String(msg.getValue(), UTF_8));
            assertEquals(1L + i, msg.getPublishTime());
            assertEquals(100L * (i + 1), msg.getEventTime());
        }
    }

    @Test
    public void testPublishTimestampBatchEnabled() throws Exception {

        log.info("-- Starting {} test --", methodName);

        AtomicLong ticker = new AtomicLong(0);

        Clock clock = new Clock() {
            @Override
            public ZoneId getZone() {
                return ZoneId.systemDefault();
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return Instant.ofEpochMilli(millis());
            }

            @Override
            public long millis() {
                return ticker.incrementAndGet();
            }
        };

        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder()
            .serviceUrl(lookupUrl.toString())
            .clock(clock)
            .build();

        final String topic = "persistent://my-property/my-ns/test-publish-timestamp";

        @Cleanup
        Consumer<byte[]> consumer = newPulsarClient.newConsumer()
            .topic(topic)
            .subscriptionName("my-sub")
            .subscribe();

        final int numMessages = 5;

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer()
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(10 * numMessages)
                .create();

        for (int i = 0; i < numMessages; i++) {
            producer.newMessage()
                .value(("value-" + i).getBytes(UTF_8))
                .eventTime((i + 1) * 100L)
                .sendAsync();
        }
        producer.flush();

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive();
            log.info("Received message '{}'.", new String(msg.getValue(), UTF_8));
            assertEquals(1L, msg.getPublishTime());
            assertEquals(100L * (i + 1), msg.getEventTime());
        }
    }

    @DataProvider(name = "batchAndAckReceipt")
    public Object[][] codecProviderWithAckReceipt() {
        return new Object[][] { { 0, true}, { 1000, false }, { 0, true }, { 1000, false }};
    }

    @DataProvider(name = "batch")
    public Object[][] codecProvider() {
        return new Object[][] { { 0 }, { 1000 } };
    }

    @Test(dataProvider = "batch")
    public void testSyncProducerAndConsumer(int batchMessageDelayMs) throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic1");

        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true);
            producerBuilder.batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerBuilder.batchingMaxMessages(5);
        }

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "batchAndAckReceipt")
    public void testAsyncProducerAndAsyncAck(int batchMessageDelayMs, boolean ackReceiptEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic2")
                .isAckReceiptEnabled(ackReceiptEnabled)
                .subscriptionName("my-subscriber-name").subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic2");

        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true);
            producerBuilder.batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerBuilder.batchingMaxMessages(5);
        }
        Producer<byte[]> producer = producerBuilder.create();
        List<Future<MessageId>> futures = Lists.newArrayList();

        // Asynchronously produce messages
        for (int i = 0; i < 10; i++) {
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
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Asynchronously acknowledge upto and including the last message
        Future<Void> ackFuture = consumer.acknowledgeCumulativeAsync(msg);
        log.info("Waiting for async ack to complete");
        ackFuture.get();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "batch", timeOut = 100000)
    public void testMessageListener(int batchMessageDelayMs) throws Exception {
        log.info("-- Starting {} test --", methodName);

        int numMessages = 100;
        final CountDownLatch latch = new CountDownLatch(numMessages);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic3")
                .subscriptionName("my-subscriber-name").messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    c1.acknowledgeAsync(msg);
                    latch.countDown();
                }).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic3");

        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true);
            producerBuilder.batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerBuilder.batchingMaxMessages(5);
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

        log.info("Waiting for message listener to ack all messages");
        assertTrue(latch.await(numMessages, TimeUnit.SECONDS), "Timed out waiting for message listener acks");
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 100000)
    public void testPauseAndResume() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int receiverQueueSize = 20;     // number of permits broker has when consumer initially subscribes

        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(receiverQueueSize));
        AtomicInteger received = new AtomicInteger();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().receiverQueueSize(receiverQueueSize)
                .topic("persistent://my-property/my-ns/my-topic-pr")
                .subscriptionName("my-subscriber-name").messageListener((c1, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    c1.acknowledgeAsync(msg);
                    received.incrementAndGet();
                    latch.get().countDown();
                }).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic-pr").create();

        consumer.pause();

        for (int i = 0; i < receiverQueueSize * 2; i++) producer.send(("my-message-" + i).getBytes());

        log.info("Waiting for message listener to ack " + receiverQueueSize + " messages");
        assertTrue(latch.get().await(receiverQueueSize, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        log.info("Giving message listener an opportunity to receive messages while paused");
        Thread.sleep(2000);     // hopefully this is long enough
        assertEquals(received.intValue(), receiverQueueSize, "Consumer received messages while paused");

        latch.set(new CountDownLatch(receiverQueueSize));

        consumer.resume();

        log.info("Waiting for message listener to ack all messages");
        assertTrue(latch.get().await(receiverQueueSize, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        consumer.close();
        producer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testPauseAndResumeWithUnloading() throws Exception {
        final String topicName = "persistent://my-property/my-ns/pause-and-resume-with-unloading";
        final String subName = "sub";
        final int receiverQueueSize = 20;

        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(receiverQueueSize));
        AtomicInteger received = new AtomicInteger();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(receiverQueueSize).messageListener((c1, msg) -> {
                    assertNotNull(msg, "Message cannot be null");
                    c1.acknowledgeAsync(msg);
                    received.incrementAndGet();
                    latch.get().countDown();
                }).subscribe();
        consumer.pause();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create();

        for (int i = 0; i < receiverQueueSize * 2; i++) {
            producer.send(("my-message-" + i).getBytes());
        }

        // Paused consumer receives only `receiverQueueSize` messages
        assertTrue(latch.get().await(receiverQueueSize, TimeUnit.SECONDS),
                "Timed out waiting for message listener acks");

        // Make sure no flow permits are sent when the consumer reconnects to the topic
        admin.topics().unload(topicName);
        Thread.sleep(2000);
        assertEquals(received.intValue(), receiverQueueSize, "Consumer received messages while paused");

        latch.set(new CountDownLatch(receiverQueueSize));
        consumer.resume();
        assertTrue(latch.get().await(receiverQueueSize, TimeUnit.SECONDS),
                "Timed out waiting for message listener acks");

        consumer.unsubscribe();
        producer.close();
    }

    @Test(dataProvider = "batch")
    public void testBackoffAndReconnect(int batchMessageDelayMs) throws Exception {
        log.info("-- Starting {} test --", methodName);
        // Create consumer and producer
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic4")
                .subscriptionName("my-subscriber-name")
                .startMessageIdInclusive()
                .subscribe();
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic4");

        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true);
            producerBuilder.batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerBuilder.batchingMaxMessages(5);
        } else {
            producerBuilder.enableBatching(false);
        }
        Producer<byte[]> producer = producerBuilder.create();

        // Produce messages
        for (int i = 0; i < 10; i++) {
            producer.sendAsync(("my-message-" + i).getBytes()).thenApply(msgId -> {
                log.info("Published message id: {}", msgId);
                return msgId;
            });
        }

        producer.flush();

        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            log.info("Received: [{}]", new String(msg.getData()));
        }

        // Restart the broker and wait for the backoff to kick in. The client library will try to reconnect, and once
        // the broker is up, the consumer should receive the duplicate messages.
        log.info("-- Restarting broker --");
        restartBroker();

        msg = null;
        log.info("Receiving duplicate messages..");
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            log.info("Received: [{}]", new String(msg.getData()));
            Assert.assertNotNull(msg, "Message cannot be null");
        }
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "batch")
    public void testSendTimeout(int batchMessageDelayMs) throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic5")
                .subscriptionName("my-subscriber-name").subscribe();
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic5").sendTimeout(1, TimeUnit.SECONDS);

        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true);
            producerBuilder.batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerBuilder.batchingMaxMessages(5);
        }
        Producer<byte[]> producer = producerBuilder.create();
        final String message = "my-message";

        // Trigger the send timeout
        stopBroker();

        Future<MessageId> future = producer.sendAsync(message.getBytes());

        try {
            future.get();
            Assert.fail("Send operation should have failed");
        } catch (ExecutionException e) {
            // Expected
        }

        startBroker();

        // We should not have received any message
        Message<byte[]> msg = consumer.receive(3, TimeUnit.SECONDS);
        Assert.assertNull(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testInvalidSequence() throws Exception {
        log.info("-- Starting {} test --", methodName);

        PulsarClient client1 = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        client1.close();

        try {
            client1.newConsumer().topic("persistent://my-property/my-ns/my-topic6")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("Should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.AlreadyClosedException);
        }

        try {
            client1.newProducer().topic("persistent://my-property/my-ns/my-topic6").create();
            Assert.fail("Should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.AlreadyClosedException);
        }

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic6")
                    .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic6")
                .subscriptionName("my-subscriber-name").subscribe();

        try {
            TypedMessageBuilder<byte[]> builder = producer.newMessage().value("InvalidMessage".getBytes());
            Message<byte[]> msg = ((TypedMessageBuilderImpl<byte[]>) builder).getMessage();
            consumer.acknowledge(msg);
        } catch (PulsarClientException.InvalidMessageException e) {
            // ok
        }

        consumer.close();

        try {
            consumer.receive();
            Assert.fail("Should fail");
        } catch (PulsarClientException.AlreadyClosedException e) {
            // ok
        }

        try {
            consumer.unsubscribe();
            Assert.fail("Should fail");
        } catch (PulsarClientException.AlreadyClosedException e) {
            // ok
        }

        producer.close();

        try {
            producer.send("message".getBytes());
            Assert.fail("Should fail");
        } catch (PulsarClientException.AlreadyClosedException e) {
            // ok
        }

    }

    @Test
    public void testSillyUser() {
        try {
            PulsarClient.builder().serviceUrl("invalid://url").build();
            Assert.fail("should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.InvalidServiceURL);
        }

        try {
            pulsarClient.newProducer().sendTimeout(-1, TimeUnit.SECONDS);
            Assert.fail("should fail");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            pulsarClient.newProducer().maxPendingMessages(0);
            Assert.fail("should fail");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            pulsarClient.newProducer().topic("invalid://topic").create();
            Assert.fail("should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.InvalidTopicNameException);
        }

        try {
            pulsarClient.newConsumer().messageListener(null);
            Assert.fail("should fail");
        } catch (NullPointerException e) {
            // ok
        }

        try {
            pulsarClient.newConsumer().subscriptionType(null);
            Assert.fail("should fail");
        } catch (NullPointerException e) {
            // ok
        }

        try {
            pulsarClient.newConsumer().receiverQueueSize(-1);
            Assert.fail("should fail");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic7").subscriptionName(null)
                    .subscribe();
            Assert.fail("Should fail");
        } catch (PulsarClientException | IllegalArgumentException e) {
            assertEquals(e.getClass(), IllegalArgumentException.class);
        }

        try {
            pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic7").subscriptionName("")
                    .subscribe();
            Assert.fail("Should fail");
        } catch (PulsarClientException | IllegalArgumentException e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        try {
            pulsarClient.newConsumer().topic("invalid://topic7").subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("Should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.InvalidTopicNameException);
        }

    }

    // This is to test that the flow control counter doesn't get corrupted while concurrent receives during
    // reconnections
    @Test(dataProvider = "batch")
    public void testConcurrentConsumerReceiveWhileReconnect(int batchMessageDelayMs) throws Exception {
        final int recvQueueSize = 100;
        final int numConsumersThreads = 10;

        String subName = UUID.randomUUID().toString();
        final Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic7").subscriptionName(subName)
                .startMessageIdInclusive()
                .receiverQueueSize(recvQueueSize).subscribe();
        ExecutorService executor = Executors.newCachedThreadPool();

        final CyclicBarrier barrier = new CyclicBarrier(numConsumersThreads + 1);
        for (int i = 0; i < numConsumersThreads; i++) {
            executor.submit((Callable<Void>) () -> {
                barrier.await();
                consumer.receive();
                return null;
            });
        }

        barrier.await();
        // there will be 10 threads calling receive() from the same consumer and will block
        Thread.sleep(100);

        // we restart the broker to reconnect
        restartBroker();
        Thread.sleep(2000);

        // publish 100 messages so that the consumers blocked on receive() will now get the messages
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic7");

        if (batchMessageDelayMs != 0) {
            producerBuilder.batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerBuilder.batchingMaxMessages(5);
            producerBuilder.enableBatching(true);
        }
        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < recvQueueSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(500);

        ConsumerImpl<byte[]> consumerImpl = (ConsumerImpl<byte[]>) consumer;
        // The available permits should be 10 and num messages in the queue should be 90
        Assert.assertEquals(consumerImpl.getAvailablePermits(), numConsumersThreads);
        Assert.assertEquals(consumerImpl.numMessagesInQueue(), recvQueueSize - numConsumersThreads);

        barrier.reset();
        for (int i = 0; i < numConsumersThreads; i++) {
            executor.submit((Callable<Void>) () -> {
                barrier.await();
                consumer.receive();
                return null;
            });
        }
        barrier.await();
        Thread.sleep(100);

        // The available permits should be 20 and num messages in the queue should be 80
        Assert.assertEquals(consumerImpl.getAvailablePermits(), numConsumersThreads * 2);
        Assert.assertEquals(consumerImpl.numMessagesInQueue(), recvQueueSize - (numConsumersThreads * 2));

        // clear the queue
        while (true) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
        }

        // The available permits should be 0 and num messages in the queue should be 0
        Assert.assertEquals(consumerImpl.getAvailablePermits(), 0);
        Assert.assertEquals(consumerImpl.numMessagesInQueue(), 0);

        barrier.reset();
        for (int i = 0; i < numConsumersThreads; i++) {
            executor.submit((Callable<Void>) () -> {
                barrier.await();
                consumer.receive();
                return null;
            });
        }
        barrier.await();
        // we again make 10 threads call receive() and get blocked
        Thread.sleep(100);

        restartBroker();
        Thread.sleep(2000);

        // The available permits should be 10 and num messages in the queue should be 90
        Assert.assertEquals(consumerImpl.getAvailablePermits(), numConsumersThreads);
        Assert.assertEquals(consumerImpl.numMessagesInQueue(), recvQueueSize - numConsumersThreads);
        consumer.close();
        executor.shutdown();
    }

    @Test
    public void testSendBigMessageSize() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topic = "persistent://my-property/my-ns/bigMsg";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();


        // Messages are allowed up to MaxMessageSize
        producer.newMessage().value(new byte[Commands.DEFAULT_MAX_MESSAGE_SIZE]);

        try {
            producer.send(new byte[Commands.DEFAULT_MAX_MESSAGE_SIZE + 1]);
            fail("Should have thrown exception");
        } catch (PulsarClientException.InvalidMessageException e) {
            // OK
        }
    }

    /**
     * Verifies non-batch message size being validated after performing compression while batch-messaging validates
     * before compression of message
     *
     * <pre>
     * send msg with size > MAX_SIZE (5 MB)
     * a. non-batch with compression: pass
     * b. batch-msg with compression: pass
     * c. non-batch w/o  compression: fail
     * d. non-batch with compression, consumer consume: pass
     * e. batch-msg w/o compression: fail
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testSendBigMessageSizeButCompressed() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topic = "persistent://my-property/my-ns/bigMsg";

        // (a) non-batch msg with compression
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .compressionType(CompressionType.LZ4)
            .create();
        producer.send(new byte[Commands.DEFAULT_MAX_MESSAGE_SIZE + 1]);
        producer.close();

        // (b) batch-msg with compression
        producer = pulsarClient.newProducer().topic(topic)
            .enableBatching(true)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .compressionType(CompressionType.LZ4)
            .create();
        producer.send(new byte[Commands.DEFAULT_MAX_MESSAGE_SIZE + 1]);
        producer.close();

        // (c) non-batch msg without compression
        producer = pulsarClient.newProducer().topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .compressionType(CompressionType.NONE)
            .create();
        try {
            producer.send(new byte[Commands.DEFAULT_MAX_MESSAGE_SIZE + 1]);
            fail("Should have thrown exception");
        } catch (PulsarClientException.InvalidMessageException e) {
            // OK
        }
        producer.close();

        // (d) non-batch msg with compression and try to consume message
        producer = pulsarClient.newProducer()
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .compressionType(CompressionType.LZ4).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").subscribe();
        byte[] content = new byte[Commands.DEFAULT_MAX_MESSAGE_SIZE + 10];
        producer.send(content);
        assertEquals(consumer.receive().getData(), content);
        producer.close();

        // (e) batch-msg w/o compression
        producer = pulsarClient.newProducer().topic(topic)
            .enableBatching(true)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .compressionType(CompressionType.NONE)
            .create();
        try {
            producer.send(new byte[Commands.DEFAULT_MAX_MESSAGE_SIZE + 1]);
            fail("Should have thrown exception");
        } catch (PulsarClientException.InvalidMessageException e) {
            // OK
        }
        producer.close();

        consumer.close();

    }

    /**
     * Usecase 1: Only 1 Active Subscription - 1 subscriber - Produce Messages - EntryCache should cache messages -
     * EntryCache should be cleaned : Once active subscription consumes messages
     *
     * Usecase 2: 2 Active Subscriptions (faster and slower) and slower gets closed - 2 subscribers - Produce Messages -
     * 1 faster-subscriber consumes all messages and another slower-subscriber none - EntryCache should have cached
     * messages as slower-subscriber has not consumed messages yet - close slower-subscriber - EntryCache should be
     * cleared
     *
     * @throws Exception
     */
    @Test
    public void testActiveAndInActiveConsumerEntryCacheBehavior() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final long batchMessageDelayMs = 100;
        final int receiverSize = 10;
        final String topicName = "cache-topic";
        final String sub1 = "faster-sub1";
        final String sub2 = "slower-sub2";

        /************ usecase-1: *************/
        // 1. Subscriber Faster subscriber
        Consumer<byte[]> subscriber1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/" + topicName).subscriptionName(sub1)
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(receiverSize).subscribe();
        final String topic = "persistent://my-property/my-ns/" + topicName;
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topic);

        if (batchMessageDelayMs != 0) {
            producerBuilder.batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerBuilder.batchingMaxMessages(5);
            producerBuilder.enableBatching(true);
        }
        Producer<byte[]> producer = producerBuilder.create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topicRef.getManagedLedger();
        Field cacheField = ManagedLedgerImpl.class.getDeclaredField("entryCache");
        cacheField.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(cacheField, cacheField.getModifiers() & ~Modifier.FINAL);
        EntryCacheImpl entryCache = spy((EntryCacheImpl) cacheField.get(ledger));
        cacheField.set(ledger, entryCache);

        Message<byte[]> msg = null;
        // 2. Produce messages
        for (int i = 0; i < 30; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        // 3. Consume messages
        for (int i = 0; i < 30; i++) {
            msg = subscriber1.receive(5, TimeUnit.SECONDS);
            subscriber1.acknowledge(msg);
        }

        // Verify: EntryCache has been invalidated
        verify(entryCache, atLeastOnce()).invalidateEntries(any());

        // sleep for a second: as ledger.updateCursorRateLimit RateLimiter will allow to invoke cursor-update after a
        // second
        Thread.sleep(1000);//
        // produce-consume one more message to trigger : ledger.internalReadFromLedger(..) which updates cursor and
        // EntryCache
        producer.send("message".getBytes());
        msg = subscriber1.receive(5, TimeUnit.SECONDS);

        /************ usecase-2: *************/
        // 1.b Subscriber slower-subscriber
        Consumer<byte[]> subscriber2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/" + topicName).subscriptionName(sub2).subscribe();
        // Produce messages
        final int moreMessages = 10;
        for (int i = 0; i < receiverSize + moreMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        // Consume messages
        for (int i = 0; i < receiverSize + moreMessages; i++) {
            msg = subscriber1.receive(5, TimeUnit.SECONDS);
            subscriber1.acknowledge(msg);
        }

        // sleep for a second: as ledger.updateCursorRateLimit RateLimiter will allow to invoke cursor-update after a
        // second
        Thread.sleep(1000);//
        // produce-consume one more message to trigger : ledger.internalReadFromLedger(..) which updates cursor and
        // EntryCache
        producer.send("message".getBytes());
        msg = subscriber1.receive(5, TimeUnit.SECONDS);

        // Verify: as active-subscriber2 has not consumed messages: EntryCache must have those entries in cache
        assertTrue(entryCache.getSize() != 0);

        // 3.b Close subscriber2: which will trigger cache to clear the cache
        subscriber2.close();

        // retry strategically until broker clean up closed subscribers and invalidate all cache entries
        retryStrategically((test) -> entryCache.getSize() == 0, 5, 100);

        // Verify: EntryCache should be cleared
        assertEquals(entryCache.getSize(), 0);
        subscriber1.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testDeactivatingBacklogConsumer(boolean ackReceiptEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final long batchMessageDelayMs = 100;
        final int receiverSize = 10;
        final String topicName = "cache-topic";
        final String topic = "persistent://my-property/my-ns/" + topicName;
        final String sub1 = "faster-sub1";
        final String sub2 = "slower-sub2";

        // 1. Subscriber Faster subscriber: let it consume all messages immediately
        Consumer<byte[]> subscriber1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/" + topicName).subscriptionName(sub1)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(receiverSize).subscribe();
        // 1.b. Subscriber Slow subscriber:
        Consumer<byte[]> subscriber2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/" + topicName).subscriptionName(sub2)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(receiverSize).subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topic);
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        }
        Producer<byte[]> producer = producerBuilder.create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topicRef.getManagedLedger();

        // reflection to set/get cache-backlog fields value:
        final long maxMessageCacheRetentionTimeMillis = conf.getManagedLedgerCacheEvictionTimeThresholdMillis();
        final long maxActiveCursorBacklogEntries = conf.getManagedLedgerCursorBackloggedThreshold();

        Message<byte[]> msg = null;
        final int totalMsgs = (int) maxActiveCursorBacklogEntries + receiverSize + 1;
        // 2. Produce messages
        for (int i = 0; i < totalMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        // 3. Consume messages: at Faster subscriber
        for (int i = 0; i < totalMsgs; i++) {
            msg = subscriber1.receive(100, TimeUnit.MILLISECONDS);
            subscriber1.acknowledgeAsync(msg);
        }

        // wait : so message can be eligible to to be evict from cache
        Thread.sleep(maxMessageCacheRetentionTimeMillis);

        // 4. deactivate subscriber which has built the backlog
        topicRef.checkBackloggedCursors();
        Thread.sleep(100);

        // 5. verify: active subscribers
        Set<String> activeSubscriber = Sets.newHashSet();
        ledger.getActiveCursors().forEach(c -> activeSubscriber.add(c.getName()));
        assertTrue(activeSubscriber.contains(sub1));
        assertFalse(activeSubscriber.contains(sub2));

        // 6. consume messages : at slower subscriber
        for (int i = 0; i < totalMsgs; i++) {
            msg = subscriber2.receive(100, TimeUnit.MILLISECONDS);
            subscriber2.acknowledgeAsync(msg);
        }

        topicRef.checkBackloggedCursors();

        activeSubscriber.clear();
        ledger.getActiveCursors().forEach(c -> activeSubscriber.add(c.getName()));

        assertTrue(activeSubscriber.contains(sub1));
        assertTrue(activeSubscriber.contains(sub2));
    }

    @Test(timeOut = 2000)
    public void testAsyncProducerAndConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;
        final Set<String> produceMsgs = Sets.newHashSet();
        final Set<String> consumeMsgs = Sets.newHashSet();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        // produce message
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic1")
                .create();
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            produceMsgs.add(message);
        }

        log.info(" start receiving messages :");
        CountDownLatch latch = new CountDownLatch(totalMsg);
        // receive messages
        ExecutorService executor = Executors.newFixedThreadPool(1);
        receiveAsync(consumer, totalMsg, 0, latch, consumeMsgs, executor);

        latch.await();

        // verify message produced correctly
        assertEquals(produceMsgs.size(), totalMsg);
        // verify produced and consumed messages must be exactly same
        produceMsgs.removeAll(consumeMsgs);
        assertTrue(produceMsgs.isEmpty());

        producer.close();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
        executor.shutdown();
    }

    @Test(timeOut = 2000)
    public void testAsyncProducerAndConsumerWithZeroQueueSize() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;
        final Set<String> produceMsgs = Sets.newHashSet();
        final Set<String> consumeMsgs = Sets.newHashSet();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();
        ;

        // produce message
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic1")
                .create();
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            produceMsgs.add(message);
        }

        log.info(" start receiving messages :");
        CountDownLatch latch = new CountDownLatch(totalMsg);
        // receive messages
        ExecutorService executor = Executors.newFixedThreadPool(1);
        receiveAsync(consumer, totalMsg, 0, latch, consumeMsgs, executor);

        latch.await();

        // verify message produced correctly
        assertEquals(produceMsgs.size(), totalMsg);
        // verify produced and consumed messages must be exactly same
        produceMsgs.removeAll(consumeMsgs);
        assertTrue(produceMsgs.isEmpty());

        producer.close();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
        executor.shutdown();
    }

    @Test
    public void testtSendCallBackReturnSequenceId() throws Exception {
        log.info("-- Starting {} test --", methodName);

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .enableBatching(false)
                .topic("persistent://my-property/my-ns/my-topic5")
                .sendTimeout(1, TimeUnit.SECONDS);

        Producer<byte[]> producer = producerBuilder.create();
        final String message = "my-message";

        // Trigger the send timeout
        stopBroker();
        List<CompletableFuture<MessageId>> futures = new ArrayList<CompletableFuture<MessageId>>();
        for(int i = 0 ; i < 3 ; i++) {
             CompletableFuture<MessageId> future = producer.newMessage().sequenceId(i).value(message.getBytes()).sendAsync();
             futures.add(future);
        }
        Thread.sleep(3000);
        futures.get(0).exceptionally(ex -> {
            long sequenceId = ((PulsarClientException) ex.getCause()).getSequenceId();
            Assert.assertEquals(sequenceId, 0L);
            return null;
        });
        futures.get(1).exceptionally(ex -> {
            long sequenceId = ((PulsarClientException) ex.getCause()).getSequenceId();
            Assert.assertEquals(sequenceId, 1L);
            return null;
        });
        futures.get(2).exceptionally(ex -> {
            long sequenceId = ((PulsarClientException) ex.getCause()).getSequenceId();
            Assert.assertEquals(sequenceId, 2L);
            return null;
        });
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testSendCallBack() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic1")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < totalMsg; i++) {
            final String message = "my-message-" + i;
            int len = message.getBytes().length;
            final AtomicInteger msgLength = new AtomicInteger();
            CompletableFuture<MessageId> future = producer.sendAsync(message.getBytes()).handle((r, ex) -> {
                if (ex != null) {
                    log.error("Message send failed:", ex);
                } else {
                    msgLength.set(len);
                }
                return null;
            });
            future.get();
            assertEquals(message.getBytes().length, msgLength.get());
        }
    }

    /**
     * consume message from consumer1 and send acknowledgement from different consumer subscribed under same
     * subscription-name
     *
     * @throws Exception
     */
    @Test(dataProvider = "ackReceiptEnabled", timeOut = 30000)
    public void testSharedConsumerAckDifferentConsumer(boolean ackReceiptEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic1").subscriptionName("my-subscriber-name")
                .receiverQueueSize(1).subscriptionType(SubscriptionType.Shared)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS);
        Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic1")
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<Message<byte[]>> consumerMsgSet1 = Sets.newHashSet();
        Set<Message<byte[]>> consumerMsgSet2 = Sets.newHashSet();
        for (int i = 0; i < 5; i++) {
            msg = consumer1.receive();
            consumerMsgSet1.add(msg);

            msg = consumer2.receive();
            consumerMsgSet2.add(msg);
        }

        consumerMsgSet1.stream().forEach(m -> {
            try {
                consumer2.acknowledge(m);
            } catch (PulsarClientException e) {
                fail();
            }
        });
        consumerMsgSet2.stream().forEach(m -> {
            try {
                consumer1.acknowledge(m);
            } catch (PulsarClientException e) {
                fail();
            }
        });

        consumer1.redeliverUnacknowledgedMessages();
        consumer2.redeliverUnacknowledgedMessages();

        try {
            if (consumer1.receive(100, TimeUnit.MILLISECONDS) != null
                    || consumer2.receive(100, TimeUnit.MILLISECONDS) != null) {
                fail();
            }
        } finally {
            consumer1.close();
            consumer2.close();
        }

        log.info("-- Exiting {} test --", methodName);
    }

    private void receiveAsync(Consumer<byte[]> consumer, int totalMessage, int currentMessage, CountDownLatch latch,
            final Set<String> consumeMsg, ExecutorService executor) throws PulsarClientException {
        if (currentMessage < totalMessage) {
            CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
            future.handle((msg, exception) -> {
                if (exception == null) {
                    // add message to consumer-queue to verify with produced messages
                    consumeMsg.add(new String(msg.getData()));
                    try {
                        consumer.acknowledge(msg);
                    } catch (PulsarClientException e1) {
                        fail("message acknowledge failed", e1);
                    }
                    // consume next message
                    executor.execute(() -> {
                        try {
                            receiveAsync(consumer, totalMessage, currentMessage + 1, latch, consumeMsg, executor);
                        } catch (PulsarClientException e) {
                            fail("message receive failed", e);
                        }
                    });
                    latch.countDown();
                }
                return null;
            });
        }
    }

    /**
     * Verify: Consumer stops receiving msg when reach unack-msg limit and starts receiving once acks messages 1.
     * Produce X (600) messages 2. Consumer has receive size (10) and receive message without acknowledging 3. Consumer
     * will stop receiving message after unAckThreshold = 500 4. Consumer acks messages and starts consuming remaining
     * messages This test case enables checksum sending while producing message and broker verifies the checksum for the
     * message.
     *
     * @throws Exception
     */
    @Test(dataProvider = "ackReceiptEnabled")
    public void testConsumerBlockingWithUnAckedMessages(boolean ackReceiptEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        try {
            final int unAckedMessagesBufferSize = 500;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 600;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessagesBufferSize);
            Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .isAckReceiptEnabled(ackReceiptEnabled)
                    .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Shared).subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic").create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message<byte[]> msg = null;
            List<Message<byte[]>> messages = Lists.newArrayList();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages.add(msg);
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }
            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertEquals(messages.size(), unAckedMessagesBufferSize);

            // start acknowledging messages
            messages.forEach(consumer::acknowledgeAsync);

            // try to consume remaining messages
            int remainingMessages = totalProducedMsgs - messages.size();
            for (int i = 0; i < remainingMessages; i++) {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages.add(msg);
                    log.info("Received message: " + new String(msg.getData()));
                }
            }

            // total received-messages should match to produced messages
            assertEquals(totalProducedMsgs, messages.size());
            producer.close();
            consumer.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    /**
     * Verify: iteration of a. message receive w/o acking b. stop receiving msg c. ack msgs d. started receiving msgs
     *
     * 1. Produce total X (1500) messages 2. Consumer consumes messages without acking until stop receiving from broker
     * due to reaching ack-threshold (500) 3. Consumer acks messages after stop getting messages 4. Consumer again tries
     * to consume messages 5. Consumer should be able to complete consuming all 1500 messages in 3 iteration (1500/500)
     *
     * @throws Exception
     */
    @Test(dataProvider = "ackReceiptEnabled")
    public void testConsumerBlockingWithUnAckedMessagesMultipleIteration(boolean ackReceiptEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        try {
            final int unAckedMessagesBufferSize = 500;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 1500;

            // receiver consumes messages in iteration after acknowledging broker
            final int totalReceiveIteration = totalProducedMsgs / unAckedMessagesBufferSize;
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessagesBufferSize);
            Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Shared)
                    .isAckReceiptEnabled(ackReceiptEnabled)
                    .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic").create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            int totalReceivedMessages = 0;
            // (2) Receive Messages
            for (int j = 0; j < totalReceiveIteration; j++) {

                Message<byte[]> msg = null;
                List<Message<byte[]>> messages = Lists.newArrayList();
                for (int i = 0; i < totalProducedMsgs; i++) {
                    msg = consumer.receive(1, TimeUnit.SECONDS);
                    if (msg != null) {
                        messages.add(msg);
                        log.info("Received message: " + new String(msg.getData()));
                    } else {
                        break;
                    }
                }
                // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
                assertEquals(messages.size(), unAckedMessagesBufferSize);

                // start acknowledging messages
                messages.forEach(m -> {
                    try {
                        consumer.acknowledge(m);
                    } catch (PulsarClientException e) {
                        fail("ack failed", e);
                    }
                });
                totalReceivedMessages += messages.size();
            }

            // total received-messages should match to produced messages
            assertEquals(totalReceivedMessages, totalProducedMsgs);
            producer.close();
            consumer.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    /**
     * Verify: Consumer1 which doesn't send ack will not impact Consumer2 which sends ack for consumed message.
     *
     *
     * @throws Exception
     */
    @Test(dataProvider = "ackReceiptEnabled")
    public void testMutlipleSharedConsumerBlockingWithUnAckedMessages(boolean ackReceiptEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        try {
            final int maxUnackedMessages = 20;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 100;
            int totalReceiveMessages = 0;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(maxUnackedMessages);
            Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .isAckReceiptEnabled(ackReceiptEnabled)
                    .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Shared).subscribe();

            PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
            Consumer<byte[]> consumer2 = newPulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .isAckReceiptEnabled(ackReceiptEnabled)
                    .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Shared).subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic").create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) Consumer1: consume without ack:
            // try to consume messages: but will be able to consume number of messages = maxUnackedMessages
            Message<byte[]> msg = null;
            List<Message<byte[]>> messages = Lists.newArrayList();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer1.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages.add(msg);
                    totalReceiveMessages++;
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }
            // client must receive number of messages = unAckedMessagesBufferSize rather all produced messages
            assertEquals(messages.size(), maxUnackedMessages);

            // (3.1) Consumer2 will start consuming messages without ack: it should stop after maxUnackedMessages
            messages.clear();
            for (int i = 0; i < totalProducedMsgs - maxUnackedMessages; i++) {
                msg = consumer2.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages.add(msg);
                    totalReceiveMessages++;
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }
            assertEquals(messages.size(), maxUnackedMessages);
            // (3.2) ack for all maxUnackedMessages
            messages.forEach(m -> {
                try {
                    consumer2.acknowledge(m);
                } catch (PulsarClientException e) {
                    fail("shouldn't have failed ", e);
                }
            });

            // (4) Consumer2 consumer and ack: so it should consume all remaining messages
            messages.clear();
            for (int i = 0; i < totalProducedMsgs - (2 * maxUnackedMessages); i++) {
                msg = consumer2.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages.add(msg);
                    totalReceiveMessages++;
                    consumer2.acknowledge(msg);
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            // verify total-consumer messages = total-produce messages
            assertEquals(totalProducedMsgs, totalReceiveMessages);
            producer.close();
            consumer1.close();
            consumer2.close();
            newPulsarClient.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    @Test
    public void testShouldNotBlockConsumerIfRedeliverBeforeReceive() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        int totalReceiveMsg = 0;
        try {
            final int receiverQueueSize = 20;
            final int totalProducedMsgs = 100;

            ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize).ackTimeout(1, TimeUnit.SECONDS)
                    .subscriptionType(SubscriptionType.Shared).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/unacked-topic")
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) wait for consumer to receive messages
            Thread.sleep(1000);
            assertEquals(consumer.numMessagesInQueue(), receiverQueueSize);

            // (3) wait for messages to expire, we should've received more
            Thread.sleep(2000);
            assertEquals(consumer.numMessagesInQueue(), receiverQueueSize);

            for (int i = 0; i < totalProducedMsgs; i++) {
                Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    consumer.acknowledge(msg);
                    totalReceiveMsg++;
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            // total received-messages should match to produced messages
            assertEquals(totalProducedMsgs, totalReceiveMsg);
            producer.close();
            consumer.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testUnackBlockRedeliverMessages(boolean ackReceiptEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        int totalReceiveMsg = 0;
        try {
            final int unAckedMessagesBufferSize = 20;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 100;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessagesBufferSize);
            ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .isAckReceiptEnabled(ackReceiptEnabled)
                    .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Shared).subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic").create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message<byte[]> msg = null;
            List<Message<byte[]>> messages = Lists.newArrayList();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages.add(msg);
                    totalReceiveMsg++;
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            consumer.redeliverUnacknowledgedMessages();

            Thread.sleep(1000);
            int alreadyConsumedMessages = messages.size();
            messages.clear();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    consumer.acknowledge(msg);
                    totalReceiveMsg++;
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            // total received-messages should match to produced messages
            assertEquals(totalProducedMsgs + alreadyConsumedMessages, totalReceiveMsg);
            producer.close();
            consumer.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    @Test(dataProvider = "batchAndAckReceipt")
    public void testUnackedBlockAtBatch(int batchMessageDelayMs, boolean ackReceiptEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        try {
            final int maxUnackedMessages = 20;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 100;
            int totalReceiveMessages = 0;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(maxUnackedMessages);
            Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .isAckReceiptEnabled(ackReceiptEnabled)
                    .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Shared).subscribe();

            ProducerBuilder<byte[]> producerBuidler = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic");

            if (batchMessageDelayMs != 0) {
                producerBuidler.enableBatching(true);
                producerBuidler.batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
                producerBuidler.batchingMaxMessages(5);
            } else {
                producerBuidler.enableBatching(false);
            }

            Producer<byte[]> producer = producerBuidler.create();

            List<CompletableFuture<MessageId>> futures = Lists.newArrayList();
            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                futures.add(producer.sendAsync(message.getBytes()));
            }

            FutureUtil.waitForAll(futures).get();

            // (2) Consumer1: consume without ack:
            // try to consume messages: but will be able to consume number of messages = maxUnackedMessages
            Message<byte[]> msg = null;
            List<Message<byte[]>> messages = Lists.newArrayList();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer1.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages.add(msg);
                    totalReceiveMessages++;
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }
            // should be blocked due to unack-msgs and should not consume all msgs
            assertNotEquals(messages.size(), totalProducedMsgs);
            // ack for all maxUnackedMessages
            messages.forEach(m -> {
                try {
                    consumer1.acknowledge(m);
                } catch (PulsarClientException e) {
                    fail("shouldn't have failed ", e);
                }
            });

            // (3) Consumer consumes and ack: so it should consume all remaining messages
            messages.clear();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer1.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages.add(msg);
                    totalReceiveMessages++;
                    consumer1.acknowledgeAsync(msg);
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }
            // verify total-consumer messages = total-produce messages
            assertEquals(totalProducedMsgs, totalReceiveMessages);
            producer.close();
            consumer1.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    /**
     * Verify: Consumer2 sends ack of Consumer1 and consumer1 should be unblock if it is blocked due to unack-messages
     *
     *
     * @throws Exception
     */
    @Test
    public void testBlockUnackConsumerAckByDifferentConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        try {
            final int maxUnackedMessages = 20;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 100;
            int totalReceiveMessages = 0;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(maxUnackedMessages);
            ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Shared);
            Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
            Consumer<byte[]> consumer2 = consumerBuilder.subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic").create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) Consumer1: consume without ack:
            // try to consume messages: but will be able to consume number of messages = maxUnackedMessages
            Message<byte[]> msg = null;
            List<Message<byte[]>> messages = Lists.newArrayList();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer1.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages.add(msg);
                    totalReceiveMessages++;
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            assertEquals(messages.size(), maxUnackedMessages); // consumer1

            // (3) ack for all UnackedMessages from consumer2
            messages.forEach(m -> {
                try {
                    consumer2.acknowledge(m);
                } catch (PulsarClientException e) {
                    fail("shouldn't have failed ", e);
                }
            });

            // (4) consumer1 will consumer remaining msgs and consumer2 will ack those messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer1.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    totalReceiveMessages++;
                    consumer2.acknowledge(msg);
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer2.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    totalReceiveMessages++;
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            // verify total-consumer messages = total-produce messages
            assertEquals(totalProducedMsgs, totalReceiveMessages);
            producer.close();
            consumer1.close();
            consumer2.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    @Test
    public void testEnabledChecksumClient() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 10;
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic1");
        final int batchMessageDelayMs = 300;
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        }

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < totalMsg; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies that redelivery-of-specific messages: that redelivers all those messages even when consumer gets
     * blocked due to unacked messsages
     *
     * Usecase: produce message with 10ms interval: so, consumer can consume only 10 messages without acking
     *
     * @throws Exception
     */
    @Test
    public void testBlockUnackedConsumerRedeliverySpecificMessagesProduceWithPause() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        try {
            final int unAckedMessagesBufferSize = 10;
            final int receiverQueueSize = 20;
            final int totalProducedMsgs = 20;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessagesBufferSize);
            ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Shared).subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic").create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
                Thread.sleep(10);
            }

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message<byte[]> msg = null;
            List<Message<byte[]>> messages1 = Lists.newArrayList();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages1.add(msg);
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            // client should not receive all produced messages and should be blocked due to unack-messages
            assertEquals(messages1.size(), unAckedMessagesBufferSize);
            Set<MessageIdImpl> redeliveryMessages = messages1.stream().map(m -> {
                return (MessageIdImpl) m.getMessageId();
            }).collect(Collectors.toSet());

            // (3) redeliver all consumed messages
            consumer.redeliverUnacknowledgedMessages(Sets.newHashSet(redeliveryMessages));
            Thread.sleep(1000);

            Set<MessageIdImpl> messages2 = Sets.newHashSet();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages2.add((MessageIdImpl) msg.getMessageId());
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            assertEquals(messages1.size(), messages2.size());
            // (4) Verify: redelivered all previous unacked-consumed messages
            messages2.removeAll(redeliveryMessages);
            assertEquals(messages2.size(), 0);
            producer.close();
            consumer.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    /**
     * It verifies that redelivery-of-specific messages: that redelivers all those messages even when consumer gets
     * blocked due to unacked messsages
     *
     * Usecase: Consumer starts consuming only after all messages have been produced. So, consumer consumes total
     * receiver-queue-size number messages => ask for redelivery and receives all messages again.
     *
     * @throws Exception
     */
    @Test
    public void testBlockUnackedConsumerRedeliverySpecificMessagesCloseConsumerWhileProduce() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        try {
            final int unAckedMessagesBufferSize = 10;
            final int receiverQueueSize = 20;
            final int totalProducedMsgs = 50;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessagesBufferSize);
            // Only subscribe consumer
            ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Shared).subscribe();
            consumer.close();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/my-ns/unacked-topic").create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
                Thread.sleep(10);
            }

            // (1.a) start consumer again
            consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                    .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Shared).subscribe();

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message<byte[]> msg = null;
            List<Message<byte[]>> messages1 = Lists.newArrayList();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages1.add(msg);
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            // client should not receive all produced messages and should be blocked due to unack-messages
            Set<MessageIdImpl> redeliveryMessages = messages1.stream().map(m -> {
                return (MessageIdImpl) m.getMessageId();
            }).collect(Collectors.toSet());

            // (3) redeliver all consumed messages
            consumer.redeliverUnacknowledgedMessages(Sets.newHashSet(redeliveryMessages));
            Thread.sleep(1000);

            Set<MessageIdImpl> messages2 = Sets.newHashSet();
            for (int i = 0; i < totalProducedMsgs; i++) {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    messages2.add((MessageIdImpl) msg.getMessageId());
                    log.info("Received message: " + new String(msg.getData()));
                } else {
                    break;
                }
            }

            assertEquals(messages1.size(), messages2.size());
            // (4) Verify: redelivered all previous unacked-consumed messages
            messages2.removeAll(redeliveryMessages);
            assertEquals(messages2.size(), 0);
            producer.close();
            consumer.close();
            log.info("-- Exiting {} test --", methodName);
        } catch (Exception e) {
            fail();
        } finally {
            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessages);
        }
    }

    @Test
    public void testPriorityConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic2").subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(5).priorityLevel(1).subscribe();

        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer2 = newPulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic2").subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(5).priorityLevel(1).subscribe();

        PulsarClient newPulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer3 = newPulsarClient1.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic2").subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(5).priorityLevel(1).subscribe();

        PulsarClient newPulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer4 = newPulsarClient2.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic2").subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(5).priorityLevel(2).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic2")
                .create();
        List<Future<MessageId>> futures = Lists.newArrayList();

        // Asynchronously produce messages
        for (int i = 0; i < 15; i++) {
            final String message = "my-message-" + i;
            Future<MessageId> future = producer.sendAsync(message.getBytes());
            futures.add(future);
        }

        log.info("Waiting for async publish to complete");
        for (Future<MessageId> future : futures) {
            future.get();
        }

        for (int i = 0; i < 20; i++) {
            consumer1.receive(100, TimeUnit.MILLISECONDS);
            consumer2.receive(100, TimeUnit.MILLISECONDS);
        }

        /**
         * a. consumer1 and consumer2 now has more permits (as received and sent more permits) b. try to produce more
         * messages: which will again distribute among consumer1 and consumer2 and should not dispatch to consumer4
         *
         */
        for (int i = 0; i < 5; i++) {
            final String message = "my-message-" + i;
            Future<MessageId> future = producer.sendAsync(message.getBytes());
            futures.add(future);
        }

        Assert.assertNull(consumer4.receive(100, TimeUnit.MILLISECONDS));

        // Asynchronously acknowledge upto and including the last message
        producer.close();
        consumer1.close();
        consumer2.close();
        consumer3.close();
        consumer4.close();
        newPulsarClient.close();
        newPulsarClient1.close();
        newPulsarClient2.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * <pre>
     * Verifies Dispatcher dispatches messages properly with shared-subscription consumers with combination of blocked
     * and unblocked consumers.
     *
     * 1. Dispatcher will have 5 consumers : c1, c2, c3, c4, c5.
     *      Out of which : c1,c2,c4,c5 will be blocked due to MaxUnackedMessages limit.
     * 2. So, dispatcher should moves round-robin and make sure it delivers unblocked consumer : c3
     * </pre>
     *
     * @throws Exception
     */
    @Test(timeOut = 5000)
    public void testSharedSamePriorityConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);
        final int queueSize = 5;
        int maxUnAckMsgs = pulsar.getConfiguration().getMaxConcurrentLookupRequest();
        pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(queueSize);

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic2")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> c1 = newPulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic2").subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(queueSize)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        PulsarClient newPulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> c2 = newPulsarClient1.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic2").subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(queueSize)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
        List<Future<MessageId>> futures = Lists.newArrayList();

        // Asynchronously produce messages
        final int totalPublishMessages = 500;
        for (int i = 0; i < totalPublishMessages; i++) {
            final String message = "my-message-" + i;
            Future<MessageId> future = producer.sendAsync(message.getBytes());
            futures.add(future);
        }

        log.info("Waiting for async publish to complete");
        for (Future<MessageId> future : futures) {
            future.get();
        }

        List<Message<byte[]>> messages = Lists.newArrayList();

        // let consumer1 and consumer2 cosume messages up to the queue will be full
        for (int i = 0; i < totalPublishMessages; i++) {
            Message<byte[]> msg = c1.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messages.add(msg);
            } else {
                break;
            }
        }
        for (int i = 0; i < totalPublishMessages; i++) {
            Message<byte[]> msg = c2.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messages.add(msg);
            } else {
                break;
            }
        }

        Assert.assertEquals(queueSize * 2, messages.size());

        // create new consumers with the same priority
        PulsarClient newPulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> c3 = newPulsarClient2.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic2").subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(queueSize)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        PulsarClient newPulsarClient3 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> c4 = newPulsarClient3.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic2").subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(queueSize)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        PulsarClient newPulsarClient4 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> c5 = newPulsarClient4.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic2").subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared).receiverQueueSize(queueSize)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        // c1 and c2 are blocked: so, let c3, c4 and c5 consume rest of the messages

        for (int i = 0; i < totalPublishMessages; i++) {
            Message<byte[]> msg = c4.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messages.add(msg);
            } else {
                break;
            }
        }

        for (int i = 0; i < totalPublishMessages; i++) {
            Message<byte[]> msg = c5.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messages.add(msg);
            } else {
                break;
            }
        }

        for (int i = 0; i < totalPublishMessages; i++) {
            Message<byte[]> msg = c3.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messages.add(msg);
                c3.acknowledge(msg);
            } else {
                break;
            }
        }

        // total messages must be consumed by all consumers
        Assert.assertEquals(messages.size(), totalPublishMessages);

        // Asynchronously acknowledge upto and including the last message
        producer.close();
        c1.close();
        c2.close();
        c3.close();
        c4.close();
        c5.close();
        newPulsarClient.close();
        newPulsarClient1.close();
        newPulsarClient2.close();
        newPulsarClient3.close();
        newPulsarClient4.close();
        pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(maxUnAckMsgs);
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testRedeliveryFailOverConsumer(boolean ackReceiptEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int receiverQueueSize = 10;

        // Only subscribe consumer
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/unacked-topic").subscriptionName("subscriber-1")
                .receiverQueueSize(receiverQueueSize).subscriptionType(SubscriptionType.Failover)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/unacked-topic")
                .create();

        // (1) First round to produce-consume messages
        int consumeMsgInParts = 4;
        for (int i = 0; i < receiverQueueSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            Thread.sleep(10);
        }
        // (1.a) consume first consumeMsgInParts msgs and trigger redeliver
        Message<byte[]> msg = null;
        List<Message<byte[]>> messages1 = Lists.newArrayList();
        for (int i = 0; i < consumeMsgInParts; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                messages1.add(msg);
                consumer.acknowledge(msg);
                log.info("Received message: " + new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messages1.size(), consumeMsgInParts);
        consumer.redeliverUnacknowledgedMessages();

        // (1.b) consume second consumeMsgInParts msgs and trigger redeliver
        messages1.clear();
        for (int i = 0; i < consumeMsgInParts; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                messages1.add(msg);
                consumer.acknowledge(msg);
                log.info("Received message: " + new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messages1.size(), consumeMsgInParts);
        consumer.redeliverUnacknowledgedMessages();

        // (2) Second round to produce-consume messages
        for (int i = 0; i < receiverQueueSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            Thread.sleep(100);
        }

        int remainingMsgs = (2 * receiverQueueSize) - (2 * consumeMsgInParts);
        messages1.clear();
        for (int i = 0; i < remainingMsgs; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                messages1.add(msg);
                consumer.acknowledge(msg);
                log.info("Received message: " + new String(msg.getData()));
            } else {
                break;
            }
        }
        assertEquals(messages1.size(), remainingMsgs);

        producer.close();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);

    }

    @Test(timeOut = 5000)
    public void testFailReceiveAsyncOnConsumerClose() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // (1) simple consumers
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/failAsyncReceive-1").subscriptionName("my-subscriber-name")
                .subscribe();
        consumer.close();
        // receive messages
        try {
            consumer.receiveAsync().get(1, TimeUnit.SECONDS);
            fail("it should have failed because consumer is already closed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PulsarClientException.AlreadyClosedException);
        }

        // (2) Partitioned-consumer
        int numPartitions = 4;
        TopicName topicName = TopicName.get("persistent://my-property/my-ns/failAsyncReceive-2");
        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);
        Consumer<byte[]> partitionedConsumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber").subscribe();
        partitionedConsumer.close();
        // receive messages
        try {
            partitionedConsumer.receiveAsync().get(1, TimeUnit.SECONDS);
            fail("it should have failed because consumer is already closed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PulsarClientException.AlreadyClosedException);
        }

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(groups = "encryption")
    public void testECDSAEncryption() throws Exception {
        log.info("-- Starting {} test --", methodName);
        String topicName = "persistent://my-property/my-ns/myecdsa-topic1-" + System.currentTimeMillis();

        class EncKeyReader implements CryptoKeyReader {

            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        return keyInfo;
                    } catch (IOException e) {
                        Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                    }
                } else {
                    Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
                }
                return null;
            }

            @Override
            public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/private-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        return keyInfo;
                    } catch (IOException e) {
                        Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                    }
                } else {
                    Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
                }
                return null;
            }
        }

        final int totalMsg = 10;

        Set<String> messageSet = Sets.newHashSet();

        Consumer<byte[]> cryptoConsumer = pulsarClient.newConsumer()
                .topic(topicName).subscriptionName("my-subscriber-name")
                .cryptoKeyReader(new EncKeyReader()).subscribe();

        Consumer<byte[]> normalConsumer = pulsarClient.newConsumer()
                .topic(topicName).subscriptionName("my-subscriber-name-normal")
                .subscribe();

        Producer<byte[]> cryptoProducer = pulsarClient.newProducer()
                .topic(topicName).addEncryptionKey("client-ecdsa.pem")
                .cryptoKeyReader(new EncKeyReader()).create();
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            cryptoProducer.send(message.getBytes());
        }

        Message<byte[]> msg = null;

        msg = normalConsumer.receive(500, TimeUnit.MILLISECONDS);
        // should not able to read message using normal message.
        assertNull(msg);

        for (int i = 0; i < totalMsg; i++) {
            msg = cryptoConsumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Acknowledge the consumption of all messages at once
        cryptoConsumer.acknowledgeCumulative(msg);
        cryptoConsumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(groups = "encryption")
    public void testRSAEncryption() throws Exception {
        log.info("-- Starting {} test --", methodName);
        String topicName = "persistent://my-property/my-ns/myrsa-topic1-"+ System.currentTimeMillis();

        class EncKeyReader implements CryptoKeyReader {

            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        return keyInfo;
                    } catch (IOException e) {
                        Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                    }
                } else {
                    Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
                }
                return null;
            }

            @Override
            public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/private-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        return keyInfo;
                    } catch (IOException e) {
                        Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                    }
                } else {
                    Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
                }
                return null;
            }
        }

        final int totalMsg = 10;

        Set<String> messageSet = Sets.newHashSet();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/myrsa-topic1")
                .subscriptionName("my-subscriber-name").cryptoKeyReader(new EncKeyReader()).subscribe();
        Consumer<byte[]> normalConsumer = pulsarClient.newConsumer()
                .topic(topicName).subscriptionName("my-subscriber-name-normal")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/myrsa-topic1")
                .addEncryptionKey("client-rsa.pem").cryptoKeyReader(new EncKeyReader()).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic("persistent://my-property/my-ns/myrsa-topic1")
                .addEncryptionKey("client-rsa.pem").cryptoKeyReader(new EncKeyReader()).create();

        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        for (int i = totalMsg; i < totalMsg * 2; i++) {
            String message = "my-message-" + i;
            producer2.send(message.getBytes());
        }

        MessageImpl<byte[]> msg = null;

        msg = (MessageImpl<byte[]>) normalConsumer.receive(500, TimeUnit.MILLISECONDS);
        // should not able to read message using normal message.
        assertNull(msg);

        for (int i = 0; i < totalMsg * 2; i++) {
            msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
            // verify that encrypted message contains encryption-context
            msg.getEncryptionCtx()
                    .orElseThrow(() -> new IllegalStateException("encryption-ctx not present for encrypted message"));
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(groups = "encryption")
    public void testDefaultCryptoKeyReader() throws Exception {
        final String topic = "persistent://my-property/my-ns/default-crypto-key-reader" + System.currentTimeMillis();
        final String ecdsaPublicKeyFile = "file:./src/test/resources/certificate/public-key.client-ecdsa.pem";
        final String ecdsaPrivateKeyFile = "file:./src/test/resources/certificate/private-key.client-ecdsa.pem";
        final String ecdsaPublicKeyData = "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUlIS01JR2pCZ2NxaGtqT1BRSUJNSUdYQWdFQk1Cd0dCeXFHU000OUFRRUNFUUQvLy8vOS8vLy8vLy8vLy8vLwovLy8vTURzRUVQLy8vLzMvLy8vLy8vLy8vLy8vLy93RUVPaDFlY0VRZWZROTJDU1pQQ3p1WHRNREZRQUFEZzFOCmFXNW5hSFZoVVhVTXdEcEVjOUEyZVFRaEJCWWY5MUtMaVpzdERDaGdmS1VzVzRiUFdzZzVXNi9yRThBdG9wTGQKN1hxREFoRUEvLy8vL2dBQUFBQjFvdzBia0RpaEZRSUJBUU1pQUFUcktqNlJQSEdQTktjWktJT2NjTjR0Z0VOTQpuMWR6S2pMck1aVGtKNG9BYVE9PQotLS0tLUVORCBQVUJMSUMgS0VZLS0tLS0K";
        final String ecdsaPrivateKeyData = "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBFQyBQQVJBTUVURVJTLS0tLS0KTUlHWEFnRUJNQndHQnlxR1NNNDlBUUVDRVFELy8vLzkvLy8vLy8vLy8vLy8vLy8vTURzRUVQLy8vLzMvLy8vLwovLy8vLy8vLy8vd0VFT2gxZWNFUWVmUTkyQ1NaUEN6dVh0TURGUUFBRGcxTmFXNW5hSFZoVVhVTXdEcEVjOUEyCmVRUWhCQllmOTFLTGlac3REQ2hnZktVc1c0YlBXc2c1VzYvckU4QXRvcExkN1hxREFoRUEvLy8vL2dBQUFBQjEKb3cwYmtEaWhGUUlCQVE9PQotLS0tLUVORCBFQyBQQVJBTUVURVJTLS0tLS0KLS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1JSFlBZ0VCQkJEZXU5aGM4a092TDNwbCtMWVNqTHE5b0lHYU1JR1hBZ0VCTUJ3R0J5cUdTTTQ5QVFFQ0VRRC8KLy8vOS8vLy8vLy8vLy8vLy8vLy9NRHNFRVAvLy8vMy8vLy8vLy8vLy8vLy8vL3dFRU9oMWVjRVFlZlE5MkNTWgpQQ3p1WHRNREZRQUFEZzFOYVc1bmFIVmhVWFVNd0RwRWM5QTJlUVFoQkJZZjkxS0xpWnN0RENoZ2ZLVXNXNGJQCldzZzVXNi9yRThBdG9wTGQ3WHFEQWhFQS8vLy8vZ0FBQUFCMW93MGJrRGloRlFJQkFhRWtBeUlBQk9zcVBwRTgKY1k4MHB4a29nNXh3M2kyQVEweWZWM01xTXVzeGxPUW5pZ0JwCi0tLS0tRU5EIEVDIFBSSVZBVEUgS0VZLS0tLS0K";
        final String rsaPublicKeyFile = "file:./src/test/resources/certificate/public-key.client-rsa.pem";
        final String rsaPrivateKeyFile = "file:./src/test/resources/certificate/private-key.client-rsa.pem";
        final String rsaPublicKeyData = "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF0S1d3Z3FkblRZck9DditqMU1rVApXZlNIMHdDc0haWmNhOXdBVzNxUDR1dWhsQnZuYjEwSmNGZjVaanpQOUJTWEsrdEhtSTh1b04zNjh2RXY2eWhVClJITTR5dVhxekN4enVBd2tRU28zOXJ6WDhQR0M3cWRqQ043TERKM01ucWlCSXJVc1NhRVAxd3JOc0Ixa0krbzkKRVIxZTVPL3VFUEFvdFA5MzNoSFEwSjJoTUVla0hxTDdzQmxKOThoNk5tc2ljRWFVa2FyZGswVE9YcmxrakMrYwpNZDhaYkdTY1BxSTlNMzhibW4zT0x4RlRuMXZ0aHB2blhMdkNtRzRNKzZ4dFl0RCtucGNWUFp3MWkxUjkwZk1zCjdwcFpuUmJ2OEhjL0RGZE9LVlFJZ2FtNkNEZG5OS2dXN2M3SUJNclAwQUVtMzdIVHUwTFNPalAyT0hYbHZ2bFEKR1FJREFRQUIKLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg==";
        final String rsaPrivateKeyData = "data:application/x-pem-file;base64,LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFb3dJQkFBS0NBUUVBdEtXd2dxZG5UWXJPQ3YrajFNa1RXZlNIMHdDc0haWmNhOXdBVzNxUDR1dWhsQnZuCmIxMEpjRmY1Wmp6UDlCU1hLK3RIbUk4dW9OMzY4dkV2NnloVVJITTR5dVhxekN4enVBd2tRU28zOXJ6WDhQR0MKN3FkakNON0xESjNNbnFpQklyVXNTYUVQMXdyTnNCMWtJK285RVIxZTVPL3VFUEFvdFA5MzNoSFEwSjJoTUVlawpIcUw3c0JsSjk4aDZObXNpY0VhVWthcmRrMFRPWHJsa2pDK2NNZDhaYkdTY1BxSTlNMzhibW4zT0x4RlRuMXZ0Cmhwdm5YTHZDbUc0TSs2eHRZdEQrbnBjVlBadzFpMVI5MGZNczdwcFpuUmJ2OEhjL0RGZE9LVlFJZ2FtNkNEZG4KTktnVzdjN0lCTXJQMEFFbTM3SFR1MExTT2pQMk9IWGx2dmxRR1FJREFRQUJBb0lCQUFhSkZBaTJDN3UzY05yZgpBc3RZOXZWRExvTEl2SEZabGtCa3RqS1pEWW1WSXNSYitoU0NWaXdWVXJXTEw2N1I2K0l2NGVnNERlVE9BeDAwCjhwbmNYS2daVHcyd0liMS9RalIvWS9SamxhQzhsa2RtUldsaTd1ZE1RQ1pWc3lodVNqVzZQajd2cjhZRTR3b2oKRmhOaWp4RUdjZjl3V3JtTUpyemRuVFdRaVhCeW8rZVR2VVE5QlBnUEdyUmpzTVptVGtMeUFWSmZmMkRmeE81YgpJV0ZEWURKY3lZQU1DSU1RdTd2eXMvSTUwb3U2aWxiMUNPNlFNNlo3S3BQZU9vVkZQd3R6Ymg4Y2Y5eE04VU5TCmo2Si9KbWRXaGdJMzRHUzNOQTY4eFRRNlBWN3pqbmhDYytpY2NtM0pLeXpHWHdhQXBBWitFb2NlLzlqNFdLbXUKNUI0emlSMENnWUVBM2wvOU9IYmwxem15VityUnhXT0lqL2kyclR2SHp3Qm5iblBKeXVlbUw1Vk1GZHBHb2RRMwp2d0h2eVFtY0VDUlZSeG1Yb2pRNFF1UFBIczNxcDZ3RUVGUENXeENoTFNUeGxVYzg1U09GSFdVMk85OWpWN3pJCjcrSk9wREsvTXN0c3g5bkhnWGR1SkYrZ2xURnRBM0xIOE9xeWx6dTJhRlBzcHJ3S3VaZjk0UThDZ1lFQXovWngKYWtFRytQRU10UDVZUzI4Y1g1WGZqc0lYL1YyNkZzNi9zSDE2UWpVSUVkZEU1VDRmQ3Vva3hDalNpd1VjV2htbApwSEVKNVM1eHAzVllSZklTVzNqUlczcXN0SUgxdHBaaXBCNitTMHpUdUptTEpiQTNJaVdFZzJydE10N1gxdUp2CkEvYllPcWUwaE9QVHVYdVpkdFZaMG5NVEtrN0dHOE82VmtCSTdGY0NnWUVBa0RmQ21zY0pnczdKYWhsQldIbVgKekg5cHdlbStTUEtqSWMvNE5CNk4rZGdpa3gyUHAwNWhwUC9WaWhVd1lJdWZ2cy9MTm9nVllOUXJ0SGVwVW5yTgoyK1RtYkhiWmdOU3YxTGR4dDgyVWZCN3kwRnV0S3U2bGhtWEh5TmVjaG8zRmk4c2loMFYwYWlTV21ZdUhmckFICkdhaXNrRVpLbzFpaVp2UVhKSXg5TzJNQ2dZQVRCZjByOWhUWU10eXh0YzZIMy9zZGQwMUM5dGhROGdEeTB5alAKMFRxYzBkTVNKcm9EcW1JV2tvS1lldzkvYmhGQTRMVzVUQ25Xa0NBUGJIbU50RzRmZGZiWXdta0gvaGRuQTJ5MApqS2RscGZwOEdYZVVGQUdIR3gxN0ZBM3NxRnZnS1VoMGVXRWdSSFVMN3ZkUU1WRkJnSlM5M283elFNOTRmTGdQCjZjT0I4d0tCZ0ZjR1Y0R2pJMld3OWNpbGxhQzU1NE12b1NqZjhCLyswNGtYekRPaDhpWUlJek85RVVpbDFqaksKSnZ4cDRobkx6VEtXYnV4M01FV3F1ckxrWWFzNkdwS0JqdytpTk9DYXI2WWRxV0dWcU0zUlV4N1BUVWFad2tLeApVZFA2M0lmWTdpWkNJVC9RYnlIUXZJVWUyTWFpVm5IK3VseGRrSzZZNWU3Z3hjYmNrSUg0Ci0tLS0tRU5EIFJTQSBQUklWQVRFIEtFWS0tLS0tCg==";
        final int numMsg = 10;

        Map<String, String> privateKeyFileMap = Maps.newHashMap();
        privateKeyFileMap.put("client-ecdsa.pem", ecdsaPrivateKeyFile);
        privateKeyFileMap.put("client-rsa.pem", rsaPrivateKeyFile);
        Map<String, String> privateKeyDataMap = Maps.newHashMap();
        privateKeyDataMap.put("client-ecdsa.pem", ecdsaPrivateKeyData);
        privateKeyDataMap.put("client-rsa.pem", rsaPrivateKeyData);

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1")
                .defaultCryptoKeyReader(ecdsaPrivateKeyFile).subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic(topic).subscriptionName("sub2")
                .defaultCryptoKeyReader(ecdsaPrivateKeyData).subscribe();
        Consumer<byte[]> consumer3 = pulsarClient.newConsumer().topic(topic).subscriptionName("sub3")
                .defaultCryptoKeyReader(privateKeyFileMap).subscribe();
        Consumer<byte[]> consumer4 = pulsarClient.newConsumer().topic(topic).subscriptionName("sub4")
                .defaultCryptoKeyReader(privateKeyDataMap).subscribe();

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topic).addEncryptionKey("client-ecdsa.pem")
                .defaultCryptoKeyReader(ecdsaPublicKeyFile).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topic).addEncryptionKey("client-ecdsa.pem")
                .defaultCryptoKeyReader(ecdsaPublicKeyData).create();

        for (int i = 0; i < numMsg; i++) {
            producer1.send(("my-message-" + i).getBytes());
        }
        for (int i = numMsg; i < numMsg * 2; i++) {
            producer2.send(("my-message-" + i).getBytes());
        }

        producer1.close();
        producer2.close();

        for (Consumer<byte[]> consumer : (List<Consumer<byte[]>>) Lists.newArrayList(consumer1, consumer2)) {
            MessageImpl<byte[]> msg = null;

            for (int i = 0; i < numMsg * 2; i++) {
                msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
                // verify that encrypted message contains encryption-context
                msg.getEncryptionCtx().orElseThrow(
                        () -> new IllegalStateException("encryption-ctx not present for encrypted message"));
                assertEquals(new String(msg.getData()), "my-message-" + i);
            }

            // Acknowledge the consumption of all messages at once
            consumer.acknowledgeCumulative(msg);
        }

        consumer1.unsubscribe();
        consumer2.unsubscribe();

        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topic).addEncryptionKey("client-rsa.pem")
                .defaultCryptoKeyReader(rsaPublicKeyFile).create();
        Producer<byte[]> producer4 = pulsarClient.newProducer().topic(topic).addEncryptionKey("client-rsa.pem")
                .defaultCryptoKeyReader(rsaPublicKeyData).create();

        for (int i = numMsg * 2; i < numMsg * 3; i++) {
            producer3.send(("my-message-" + i).getBytes());
        }
        for (int i = numMsg * 3; i < numMsg * 4; i++) {
            producer4.send(("my-message-" + i).getBytes());
        }

        producer3.close();
        producer4.close();

        for (Consumer<byte[]> consumer : (List<Consumer<byte[]>>) Lists.newArrayList(consumer3, consumer4)) {
            MessageImpl<byte[]> msg = null;

            for (int i = 0; i < numMsg * 4; i++) {
                msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
                // verify that encrypted message contains encryption-context
                msg.getEncryptionCtx().orElseThrow(
                        () -> new IllegalStateException("encryption-ctx not present for encrypted message"));
                assertEquals(new String(msg.getData()), "my-message-" + i);
            }

            // Acknowledge the consumption of all messages at once
            consumer.acknowledgeCumulative(msg);
        }

        consumer3.unsubscribe();
        consumer4.unsubscribe();
    }

    @Test(groups = "encryption")
    public void testRedeliveryOfFailedMessages() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String encryptionKeyName = "client-rsa.pem";
        final String encryptionKeyVersion = "1.0";
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put("version", encryptionKeyVersion);
        class EncKeyReader implements CryptoKeyReader {
            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        keyInfo.setMetadata(metadata);
                        return keyInfo;
                    } catch (IOException e) {
                        Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                    }
                } else {
                    Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
                }
                return null;
            }

            @Override
            public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/private-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        keyInfo.setMetadata(metadata);
                        return keyInfo;
                    } catch (IOException e) {
                        Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                    }
                } else {
                    Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
                }
                return null;
            }
        }

        class InvalidKeyReader implements CryptoKeyReader {
            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                return null;
            }

            @Override
            public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
                return null;
            }
        }

        /*
         * Redelivery functionality guarantees that customer will get a chance to process the message again.
         * In case of shared subscription eventually every client will get a chance to process the message, till one of them acks it.
         *
         * For client with Encryption enabled where in cases like a new production rollout or a buggy client configuration, we might have a mismatch of consumers
         * - few which can decrypt, few which can't (due to errors or cryptoReader not configured).
         *
         * In that case eventually all messages should be acked as long as there is a single consumer who can decrypt the message.
         *
         * Consumer 1 - Can decrypt message
         * Consumer 2 - Has invalid Reader configured.
         * Consumer 3 - Has no reader configured.
         *
         */

        String topicName = "persistent://my-property/my-ns/myrsa-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .addEncryptionKey(encryptionKeyName).compressionType(CompressionType.LZ4)
                .cryptoKeyReader(new EncKeyReader()).create();

        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer1 = newPulsarClient.newConsumer().topicsPattern(topicName)
                .subscriptionName("my-subscriber-name").cryptoKeyReader(new EncKeyReader())
                .subscriptionType(SubscriptionType.Shared).ackTimeout(1, TimeUnit.SECONDS).subscribe();

        PulsarClient newPulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer2 = newPulsarClient1.newConsumer().topicsPattern(topicName)
                .subscriptionName("my-subscriber-name").cryptoKeyReader(new InvalidKeyReader())
                .subscriptionType(SubscriptionType.Shared).ackTimeout(1, TimeUnit.SECONDS).subscribe();

        PulsarClient newPulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer3 = newPulsarClient2.newConsumer().topicsPattern(topicName)
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared).ackTimeout(1, TimeUnit.SECONDS).subscribe();

        int numberOfMessages = 100;
        String message = "my-message";
        Set<String> messages = new HashSet(); // Since messages are in random order
        for (int i = 0; i<numberOfMessages; i++) {
            producer.send((message + i).getBytes());
        }

        // Consuming from consumer 2 and 3
        // no message should be returned since they can't decrypt the message
        Message m = consumer2.receive(3, TimeUnit.SECONDS);
        assertNull(m);
        m = consumer3.receive(3, TimeUnit.SECONDS);
        assertNull(m);

        for (int i = 0; i<numberOfMessages; i++) {
            // All messages would be received by consumer 1
            m = consumer1.receive();
            messages.add(new String(m.getData()));
            consumer1.acknowledge(m);
        }

        // Consuming from consumer 2 and 3 again just to be sure
        // no message should be returned since they can't decrypt the message
        m = consumer2.receive(3, TimeUnit.SECONDS);
        assertNull(m);
        m = consumer3.receive(3, TimeUnit.SECONDS);
        assertNull(m);

        // checking if all messages were received
        for (int i = 0; i<numberOfMessages; i++) {
            assertTrue(messages.contains((message + i)));
        }

        consumer1.close();
        consumer2.close();
        consumer3.close();
        newPulsarClient.close();
        newPulsarClient1.close();
        newPulsarClient2.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(groups = "encryption")
    public void testEncryptionFailure() throws Exception {
        log.info("-- Starting {} test --", methodName);

        class EncKeyReader implements CryptoKeyReader {

            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        return keyInfo;
                    } catch (IOException e) {
                        log.error("Failed to read certificate from {}", CERT_FILE_PATH);
                    }
                }
                return null;
            }

            @Override
            public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/private-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        return keyInfo;
                    } catch (IOException e) {
                        log.error("Failed to read certificate from {}", CERT_FILE_PATH);
                    }
                }
                return null;
            }
        }

        final int totalMsg = 10;

        MessageImpl<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/myenc-ns/myenc-topic1").subscriptionName("my-subscriber-name")
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        // 1. Invalid key name
        try {
            pulsarClient.newProducer().topic("persistent://my-property/use/myenc-ns/myenc-topic1")
                    .addEncryptionKey("client-non-existant-rsa.pem").cryptoKeyReader(new EncKeyReader()).create();
            Assert.fail("Producer creation should not suceed if failing to read key");
        } catch (Exception e) {
            // ok
        }

        // 2. Producer with valid key name
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic("persistent://my-property/use/myenc-ns/myenc-topic1")
            .addEncryptionKey("client-rsa.pem")
            .cryptoKeyReader(new EncKeyReader())
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // 3. KeyReder is not set by consumer
        // Receive should fail since key reader is not setup
        msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(msg, "Receive should have failed with no keyreader");

        // 4. Set consumer config to consume even if decryption fails
        consumer.close();
        consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/myenc-ns/myenc-topic1")
                .subscriptionName("my-subscriber-name").cryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        int msgNum = 0;
        try {
            // Receive should proceed and deliver encrypted message
            msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + msgNum++;
            Assert.assertNotEquals(receivedMessage, expectedMessage, "Received encrypted message " + receivedMessage
                    + " should not match the expected message " + expectedMessage);
            consumer.acknowledgeCumulative(msg);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to receive message even aftet ConsumerCryptoFailureAction.CONSUME is set.");
        }

        // 5. Set keyreader and failure action
        consumer.close();
        // Set keyreader
        consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/myenc-ns/myenc-topic1")
                .subscriptionName("my-subscriber-name").cryptoFailureAction(ConsumerCryptoFailureAction.FAIL)
                .cryptoKeyReader(new EncKeyReader()).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        for (int i = msgNum; i < totalMsg - 1; i++) {
            msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
            // verify that encrypted message contains encryption-context
            msg.getEncryptionCtx()
                    .orElseThrow(() -> new IllegalStateException("encryption-ctx not present for encrypted message"));
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

        // 6. Set consumer config to discard if decryption fails
        consumer.close();
        consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/myenc-ns/myenc-topic1")
                .subscriptionName("my-subscriber-name").cryptoFailureAction(ConsumerCryptoFailureAction.DISCARD)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        // Receive should proceed and discard encrypted messages
        msg = (MessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(msg, "Message received even aftet ConsumerCryptoFailureAction.DISCARD is set.");

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(groups = "encryption")
    public void testEncryptionConsumerWithoutCryptoReader() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String encryptionKeyName = "client-rsa.pem";
        final String encryptionKeyVersion = "1.0";
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put("version", encryptionKeyVersion);
        class EncKeyReader implements CryptoKeyReader {
            EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        keyInfo.setMetadata(metadata);
                        return keyInfo;
                    } catch (IOException e) {
                        Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                    }
                } else {
                    Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
                }
                return null;
            }

            @Override
            public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
                String CERT_FILE_PATH = "./src/test/resources/certificate/private-key." + keyName;
                if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                    try {
                        keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                        keyInfo.setMetadata(metadata);
                        return keyInfo;
                    } catch (IOException e) {
                        Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                    }
                } else {
                    Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
                }
                return null;
            }
        }

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/myrsa-topic1")
                .addEncryptionKey(encryptionKeyName).compressionType(CompressionType.LZ4)
                .cryptoKeyReader(new EncKeyReader()).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topicsPattern("persistent://my-property/my-ns/myrsa-topic1")
                .subscriptionName("my-subscriber-name").cryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
                .subscribe();

        String message = "my-message";
        producer.send(message.getBytes());

        TopicMessageImpl<byte[]> msg = (TopicMessageImpl<byte[]>) consumer.receive(5, TimeUnit.SECONDS);

        String receivedMessage = decryptMessage(msg, encryptionKeyName, new EncKeyReader());
        assertEquals(message, receivedMessage);

        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    private String decryptMessage(TopicMessageImpl<byte[]> msg, String encryptionKeyName, CryptoKeyReader reader)
            throws Exception {
        Optional<EncryptionContext> ctx = msg.getEncryptionCtx();
        Assert.assertTrue(ctx.isPresent());
        EncryptionContext encryptionCtx = ctx
                .orElseThrow(() -> new IllegalStateException("encryption-ctx not present for encrypted message"));

        Map<String, EncryptionKey> keys = encryptionCtx.getKeys();
        assertEquals(keys.size(), 1);
        EncryptionKey encryptionKey = keys.get(encryptionKeyName);
        byte[] dataKey = encryptionKey.getKeyValue();
        Map<String, String> metadata = encryptionKey.getMetadata();
        String version = metadata.get("version");
        assertEquals(version, "1.0");

        CompressionType compressionType = encryptionCtx.getCompressionType();
        int uncompressedSize = encryptionCtx.getUncompressedMessageSize();
        byte[] encrParam = encryptionCtx.getParam();
        String encAlgo = encryptionCtx.getAlgorithm();
        int batchSize = encryptionCtx.getBatchSize().orElse(0);

        ByteBuf payloadBuf = Unpooled.wrappedBuffer(msg.getData());
        // try to decrypt use default MessageCryptoBc
        MessageCrypto crypto = new MessageCryptoBc("test", false);

        MessageMetadata messageMetadata = new MessageMetadata()
                .setEncryptionParam(encrParam)
                .setProducerName("test")
                .setSequenceId(123)
                .setPublishTime(12333453454L)
                .setCompression(CompressionCodecProvider.convertToWireProtocol(compressionType))
                .setUncompressedSize(uncompressedSize);
        messageMetadata.addEncryptionKey()
                .setKey(encryptionKeyName)
                .setValue(dataKey);
        if (encAlgo != null) {
            messageMetadata.setEncryptionAlgo(encAlgo);
        }

        ByteBuf decryptedPayload = crypto.decrypt(() -> messageMetadata, payloadBuf, reader);

        // try to uncompress
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        ByteBuf uncompressedPayload = codec.decode(decryptedPayload, uncompressedSize);

        if (batchSize > 0) {
            SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
            uncompressedPayload = Commands.deSerializeSingleMessageInBatch(uncompressedPayload,
                    singleMessageMetadata, 0, batchSize);
        }

        byte[] data = new byte[uncompressedPayload.readableBytes()];
        uncompressedPayload.readBytes(data);
        uncompressedPayload.release();
        return new String(data);
    }

    @Test
    public void testConsumerSubscriptionInitialize() throws Exception {
        log.info("-- Starting {} test --", methodName);
        String topicName = "persistent://my-property/my-ns/test-subscription-initialize-topic";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .create();

        // 1, produce 5 messages
        for (int i = 0; i < 5; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // 2, create consumer
        Consumer<byte[]> defaultConsumer = pulsarClient.newConsumer().topic(topicName)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscriptionName("test-subscription-default").subscribe();
        Consumer<byte[]> latestConsumer = pulsarClient.newConsumer().topic(topicName)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscriptionName("test-subscription-latest")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest).subscribe();
        Consumer<byte[]> earliestConsumer = pulsarClient.newConsumer().topic(topicName)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscriptionName("test-subscription-earliest")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        // 3, produce 5 messages more
        for (int i = 5; i < 10; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // 4, verify consumer get right message.
        assertEquals(defaultConsumer.receive().getData(), "my-message-5".getBytes());
        assertEquals(latestConsumer.receive().getData(), "my-message-5".getBytes());
        assertEquals(earliestConsumer.receive().getData(), "my-message-0".getBytes());

        defaultConsumer.close();
        latestConsumer.close();
        earliestConsumer.close();

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testMultiTopicsConsumerImplPause() throws Exception {
        log.info("-- Starting {} test --", methodName);
        String topicName = "persistent://my-property/my-ns/partition-topic";

        admin.topics().createPartitionedTopic(topicName, 1);


        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .autoUpdatePartitionsInterval(2 ,TimeUnit.SECONDS)
                .create();

        // 1. produce 5 messages
        for (int i = 0; i < 5; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes(UTF_8));
        }

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1)
                .autoUpdatePartitionsInterval(2 ,TimeUnit.SECONDS)
                .subscriptionName("test-multi-topic-consumer").subscribe();

        int counter = 0;
        for (; counter < 5; counter ++) {
            assertEquals(consumer.receive().getData(), ("my-message-" + counter).getBytes());
        }

        // 2. pause multi-topic consumer
        consumer.pause();

        // 3. update partition
        admin.topics().updatePartitionedTopic(topicName, 3);

        // 4. wait for client to update partitions
        while(((MultiTopicsConsumerImpl)consumer).getConsumers().size() <= 1) {
            Thread.sleep(1);
        }

        // 5. produce 5 messages more
        for (int i = 5; i < 10; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        while(consumer.receive(3, TimeUnit.SECONDS) != null) {
            counter++;
        }

        assertTrue(counter < 10);
        // 6. resume multi-topic consumer
        consumer.resume();

        // 7. continue consume
        while(consumer.receive(3, TimeUnit.SECONDS) != null) {
           counter++;
        }
        assertEquals(counter, 10);

        producer.close();;
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testFlushBatchEnabled() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic("persistent://my-property/my-ns/test-flush-enabled")
            .subscriptionName("my-subscriber-name").subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/test-flush-enabled")
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .batchingMaxMessages(10000);

        try (Producer<byte[]> producer = producerBuilder.create()) {
            for (int i = 0; i < 10; i++) {
                String message = "my-message-" + i;
                producer.sendAsync(message.getBytes());
            }
            producer.flush();
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testFlushBatchDisabled() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic("persistent://my-property/my-ns/test-flush-disabled")
                .startMessageIdInclusive()
            .subscriptionName("my-subscriber-name").subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/test-flush-disabled")
                .enableBatching(false);

        try (Producer<byte[]> producer = producerBuilder.create()) {
            for (int i = 0; i < 10; i++) {
                String message = "my-message-" + i;
                producer.sendAsync(message.getBytes());
            }
            producer.flush();
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    // Issue 1452: https://github.com/apache/pulsar/issues/1452
    // reachedEndOfTopic should be called only once if a topic has been terminated before subscription
    @Test
    public void testReachedEndOfTopic() throws Exception
    {
        String topicName = "persistent://my-property/my-ns/testReachedEndOfTopic";
        Producer producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false).create();
        producer.close();

        admin.topics().terminateTopicAsync(topicName).get();

        CountDownLatch latch = new CountDownLatch(2);
        Consumer consumer = pulsarClient.newConsumer()
            .topic(topicName)
            .subscriptionName("my-subscriber-name")
            .messageListener(new MessageListener()
            {
                @Override
                public void reachedEndOfTopic(Consumer consumer)
                {
                    log.info("called reachedEndOfTopic  {}", methodName);
                    latch.countDown();
                }

                @Override
                public void received(Consumer consumer, Message message)
                {
                    // do nothing
                }
            })
            .subscribe();

        assertFalse(latch.await(1, TimeUnit.SECONDS));
        assertEquals(latch.getCount(), 1);
        consumer.close();
    }

    /**
     * This test verifies that broker activates fail-over consumer by considering priority-level as well.
     *
     * <pre>
     * 1. Start two failover consumer with same priority level, broker selects consumer based on name-sorting (consumer1).
     * 2. Switch non-active consumer to active (consumer2): by giving it higher priority
     * Partitioned-topic with 9 partitions:
     * 1. C1 (priority=1)
     * 2. C2,C3,C4 (priority=0)
     * So, broker should evenly distribute C2,C3,C4 active consumers among 9 partitions.
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testFailOverConsumerPriority() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topicName = "persistent://my-property/my-ns/priority-topic";
        final String subscriptionName = "my-sub";
        final int noOfPartitions = 9;

        // create partitioned topic
        admin.topics().createPartitionedTopic(topicName, noOfPartitions);

        // Only subscribe consumer
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .consumerName("aaa").subscriptionType(SubscriptionType.Failover)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).priorityLevel(1).subscribe();

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).consumerName("bbb1").subscriptionType(SubscriptionType.Failover)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).priorityLevel(1);

        Consumer<byte[]> consumer2 = consumerBuilder.subscribe();

        AtomicInteger consumer1Count = new AtomicInteger(0);
        admin.topics().getPartitionedStats(topicName, true).partitions.forEach((p, stats) -> {
            String activeConsumerName = stats.subscriptions.entrySet().iterator().next().getValue().activeConsumerName;
            if (activeConsumerName.equals("aaa")) {
                consumer1Count.incrementAndGet();
            }
        });

        // validate even distribution among two consumers
        assertNotEquals(consumer1Count, noOfPartitions);

        consumer2.close();
        consumer2 = consumerBuilder.priorityLevel(0).subscribe();
        Consumer<byte[]> consumer3 = consumerBuilder.consumerName("bbb2").priorityLevel(0).subscribe();
        Consumer<byte[]> consumer4 = consumerBuilder.consumerName("bbb3").priorityLevel(0).subscribe();
        Consumer<byte[]> consumer5 = consumerBuilder.consumerName("bbb4").priorityLevel(1).subscribe();

        Integer evenDistributionCount = noOfPartitions / 3;
        retryStrategically((test) -> {
            try {
                Map<String, Integer> subsCount = Maps.newHashMap();
                admin.topics().getPartitionedStats(topicName, true).partitions.forEach((p, stats) -> {
                    String activeConsumerName = stats.subscriptions.entrySet().iterator().next()
                            .getValue().activeConsumerName;
                    subsCount.compute(activeConsumerName, (k, v) -> v != null ? v + 1 : 1);
                });
                return subsCount.size() == 3 && subsCount.get("bbb1") == evenDistributionCount
                        && subsCount.get("bbb2") == evenDistributionCount
                        && subsCount.get("bbb3") == evenDistributionCount;

            } catch (PulsarAdminException e) {
                // Ok
            }
            return false;
        }, 5, 100);

        Map<String, Integer> subsCount = Maps.newHashMap();
        admin.topics().getPartitionedStats(topicName, true).partitions.forEach((p, stats) -> {
            String activeConsumerName = stats.subscriptions.entrySet().iterator().next().getValue().activeConsumerName;
            subsCount.compute(activeConsumerName, (k, v) -> v != null ? v + 1 : 1);
        });
        assertEquals(subsCount.size(), 3);
        assertEquals(subsCount.get("bbb1"), evenDistributionCount);
        assertEquals(subsCount.get("bbb2"), evenDistributionCount);
        assertEquals(subsCount.get("bbb3"), evenDistributionCount);

        consumer1.close();
        consumer2.close();
        consumer3.close();
        consumer4.close();
        consumer5.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * This test verifies Producer and Consumer of PartitionedTopic with 1 partition works well.
     *
     * <pre>
     * 1. create producer/consumer with both original name and PARTITIONED_TOPIC_SUFFIX.
     * 2. verify producer/consumer could produce/consume messages from same underline persistent topic.
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testPartitionedTopicWithOnePartition() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topicName = "persistent://my-property/my-ns/one-partitioned-topic";
        final String subscriptionName = "my-sub-";

        // create partitioned topic
        admin.topics().createPartitionedTopic(topicName, 1);
        assertEquals(admin.topics().getPartitionedTopicMetadata(topicName).partitions, 1);

        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
            .topic(topicName)
            .subscriptionName(subscriptionName + 1)
            .consumerName("aaa")
            .subscribe();
        log.info("Consumer1 created. topic: {}", consumer1.getTopic());

        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
            .topic(topicName + PARTITIONED_TOPIC_SUFFIX + 0)
            .subscriptionName(subscriptionName + 2)
            .consumerName("bbb")
            .subscribe();
        log.info("Consumer2 created. topic: {}", consumer2.getTopic());

        @Cleanup
        Producer<byte[]> producer1 = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .create();
        log.info("Producer1 created. topic: {}", producer1.getTopic());

        @Cleanup
        Producer<byte[]> producer2 = pulsarClient.newProducer()
            .topic(topicName + PARTITIONED_TOPIC_SUFFIX + 0)
            .enableBatching(false)
            .create();
        log.info("Producer2 created. topic: {}", producer2.getTopic());

        final int numMessages = 10;
        for (int i = 0; i < numMessages; i++) {
            producer1.newMessage()
                .value(("one-partitioned-topic-value-producer1-" + i).getBytes(UTF_8))
                .send();

            producer2.newMessage()
                .value(("one-partitioned-topic-value-producer2-" + i).getBytes(UTF_8))
                .send();
        }

        for (int i = 0; i < numMessages * 2; i++) {
            Message<byte[]> msg = consumer1.receive(200, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            log.info("Consumer1 Received message '{}'.", new String(msg.getValue(), UTF_8));

            msg = consumer2.receive(200, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            log.info("Consumer2 Received message '{}'.", new String(msg.getValue(), UTF_8));
        }

        assertNull(consumer1.receive(200, TimeUnit.MILLISECONDS));
        assertNull(consumer2.receive(200, TimeUnit.MILLISECONDS));

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "variationsForExpectedPos")
    public void testConsumerStartMessageIdAtExpectedPos(boolean batching, boolean startInclusive, int numOfMessages)
            throws Exception {
        final String topicName = "persistent://my-property/my-ns/ConsumerStartMessageIdAtExpectedPos";
        final int resetIndex = new Random().nextInt(numOfMessages); // Choose some random index to reset
        final int firstMessage = startInclusive ? resetIndex : resetIndex + 1; // First message of reset

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(batching)
                .create();

        CountDownLatch latch = new CountDownLatch(numOfMessages);

        final AtomicReference<MessageId> resetPos = new AtomicReference<>();

        for (int i = 0; i < numOfMessages; i++) {

            final int j = i;

            producer.sendAsync(String.format("msg num %d", i).getBytes())
                    .thenCompose(messageId -> FutureUtils.value(Pair.of(j, messageId)))
                    .whenComplete((p, e) -> {
                        if (e != null) {
                            fail("send msg failed due to " + e.getMessage());
                        } else {
                            log.info("send msg with id {}", p.getRight());
                            if (p.getLeft() == resetIndex) {
                                resetPos.set(p.getRight());
                            }
                        }
                        latch.countDown();
                    });
        }

        latch.await();

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer()
                .topic(topicName);

        if (startInclusive) {
            consumerBuilder.startMessageIdInclusive();
        }

        Consumer<byte[]> consumer = consumerBuilder.subscriptionName("my-subscriber-name").subscribe();
        consumer.seek(resetPos.get());
        log.info("reset cursor to {}", resetPos.get());
        Set<String> messageSet = Sets.newHashSet();
        for (int i = firstMessage; i < numOfMessages; i++) {
            Message<byte[]> message = consumer.receive();
            String receivedMessage = new String(message.getData());
            String expectedMessage = String.format("msg num %d", i);
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        assertEquals(((ConsumerImpl) consumer).numMessagesInQueue(), 0);

        // Processed messages should be the number of messages in the range: [FirstResetMessage..TotalNumOfMessages]
        assertEquals(messageSet.size(), numOfMessages - firstMessage);

        consumer.close();
        producer.close();
    }

    /**
     * It verifies that message failure successfully releases semaphore and client successfully receives
     * InvalidMessageException.
     *
     * @throws Exception
     */
    @Test
    public void testReleaseSemaphoreOnFailMessages() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int maxPendingMessages = 10;
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().enableBatching(false)
                .blockIfQueueFull(true).maxPendingMessages(maxPendingMessages)
                .topic("persistent://my-property/my-ns/my-topic2");

        Producer<byte[]> producer = producerBuilder.create();
        List<Future<MessageId>> futures = Lists.newArrayList();

        // Asynchronously produce messages
        byte[] message = new byte[ClientCnx.getMaxMessageSize() + 1];
        for (int i = 0; i < maxPendingMessages + 10; i++) {
            Future<MessageId> future = producer.sendAsync(message);
            try {
                future.get();
                fail("should fail with InvalidMessageException");
            } catch (Exception e) {
                assertTrue(e.getCause() instanceof PulsarClientException.InvalidMessageException);
            }
        }
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 5000)
    public void testReceiveAsyncCompletedWhenClosing() throws Exception {
        final String topic = "persistent://my-property/my-ns/testCompletedWhenClosing";
        final String partitionedTopic = "persistent://my-property/my-ns/testCompletedWhenClosing-partitioned";
        final String errorMsg = "cleaning and closing the consumers";
        BatchReceivePolicy batchReceivePolicy
                = BatchReceivePolicy.builder().maxNumBytes(10 * 1024).maxNumMessages(10).timeout(-1, TimeUnit.SECONDS).build();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic).subscriptionName("my-subscriber-name")
                .batchReceivePolicy(batchReceivePolicy).subscribe();
        // 1) Test receiveAsync is interrupted
        CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                consumer.receiveAsync().get();
                Assert.fail("should be interrupted");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains(errorMsg));
                countDownLatch.countDown();
            }
        }).start();
        new Thread(() -> {
            try {
                consumer.close();
            } catch (PulsarClientException ignore) {
            }
        }).start();
        countDownLatch.await();

        // 2) Test batchReceiveAsync is interrupted
        CountDownLatch countDownLatch2 = new CountDownLatch(1);
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic).subscriptionName("my-subscriber-name")
                .batchReceivePolicy(batchReceivePolicy).subscribe();
        new Thread(() -> {
            try {
                consumer2.batchReceiveAsync().get();
                Assert.fail("should be interrupted");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains(errorMsg));
                countDownLatch2.countDown();
            }
        }).start();
        new Thread(() -> {
            try {
                consumer2.close();
            } catch (PulsarClientException ignore) {
            }
        }).start();
        countDownLatch2.await();
        // 3) Test partitioned topic batchReceiveAsync is interrupted
        CountDownLatch countDownLatch3 = new CountDownLatch(1);
        admin.topics().createPartitionedTopic(partitionedTopic, 3);
        Consumer<String> partitionedTopicConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(partitionedTopic).subscriptionName("my-subscriber-name-partitionedTopic")
                .batchReceivePolicy(batchReceivePolicy).subscribe();
        new Thread(() -> {
            try {
                partitionedTopicConsumer.batchReceiveAsync().get();
                Assert.fail("should be interrupted");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains(errorMsg));
                countDownLatch3.countDown();
            }
        }).start();
        new Thread(() -> {
            try {
                partitionedTopicConsumer.close();
            } catch (PulsarClientException ignore) {
            }
        }).start();
        countDownLatch3.await();
    }

    @Test(timeOut = 20000)
    public void testResetPosition() throws Exception {
        final String topicName = "persistent://my-property/my-ns/testResetPosition";
        final String subName = "my-sub";
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName).subscriptionName(subName).subscribe();
        for (int i = 0; i < 50; i++) {
            producer.send("msg" + i);
        }
        Message<String> lastMsg = null;
        for (int i = 0; i < 10; i++) {
            lastMsg = consumer.receive();
            assertNotNull(lastMsg);
            consumer.acknowledge(lastMsg);
        }
        MessageIdImpl lastMessageId = (MessageIdImpl)lastMsg.getMessageId();
        consumer.close();
        producer.close();

        admin.topics().resetCursor(topicName, subName, lastMsg.getMessageId());
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subName).subscribe();
        Message<String> message = consumer2.receive(1, TimeUnit.SECONDS);
        assertEquals(message.getMessageId(), lastMsg.getMessageId());
        consumer2.close();

        admin.topics().resetCursor(topicName, subName, lastMsg.getMessageId(), true);
        Consumer<String> consumer3 = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subName).subscribe();
        message = consumer3.receive(1, TimeUnit.SECONDS);
        assertNotEquals(message.getMessageId(), lastMsg.getMessageId());
        MessageIdImpl messageId = (MessageIdImpl)message.getMessageId();
        assertEquals(messageId.getEntryId() - 1, lastMessageId.getEntryId());
        consumer3.close();

        admin.topics().resetCursorAsync(topicName, subName, lastMsg.getMessageId(), true).get(3, TimeUnit.SECONDS);
        Consumer<String> consumer4 = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subName).subscribe();
        message = consumer4.receive(1, TimeUnit.SECONDS);
        assertNotEquals(message.getMessageId(), lastMsg.getMessageId());
        messageId = (MessageIdImpl)message.getMessageId();
        assertEquals(messageId.getEntryId() - 1, lastMessageId.getEntryId());
        consumer4.close();

        admin.topics().resetCursorAsync(topicName, subName, lastMsg.getMessageId()).get(3, TimeUnit.SECONDS);
        Consumer<String> consumer5 = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subName).subscribe();
        message = consumer5.receive(1, TimeUnit.SECONDS);
        assertEquals(message.getMessageId(), lastMsg.getMessageId());
        consumer5.close();
    }

    @Test
    public void testGetLastDisconnectedTimestamp() throws Exception {
        final String topicName = "persistent://my-property/my-ns/testGetLastDisconnectedTimestamp";
        final String subName = "my-sub";
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName).subscriptionName(subName).subscribe();
        Assert.assertEquals(producer.getLastDisconnectedTimestamp(), 0L);
        Assert.assertEquals(consumer.getLastDisconnectedTimestamp(), 0L);

        pulsar.close();

        Assert.assertTrue(producer.getLastDisconnectedTimestamp() > 0);
        Assert.assertTrue(consumer.getLastDisconnectedTimestamp() > 0);
    }

    @Test
    public void testGetLastDisconnectedTimestampForPartitionedTopic() throws Exception {
        final String topicName = "persistent://my-property/my-ns/testGetLastDisconnectedTimestampForPartitionedTopic";
        final String subName = "my-sub";

        admin.topics().createPartitionedTopic(topicName, 3);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName).subscriptionName(subName).subscribe();
        Assert.assertEquals(producer.getLastDisconnectedTimestamp(), 0L);
        Assert.assertEquals(consumer.getLastDisconnectedTimestamp(), 0L);

        pulsar.close();

        Assert.assertTrue(producer.getLastDisconnectedTimestamp() > 0);
        Assert.assertTrue(consumer.getLastDisconnectedTimestamp() > 0);
    }

    @Test
    public void testGetStats() throws Exception {
        final String topicName = "persistent://my-property/my-ns/testGetStats" + UUID.randomUUID();
        final String subName = "my-sub";
        final int receiveQueueSize = 100;
        PulsarClient client = newPulsarClient(lookupUrl.toString(), 100);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING)
                .topic(topicName).receiverQueueSize(receiveQueueSize).subscriptionName(subName).subscribe();
        Assert.assertNull(consumer.getStats().getMsgNumInSubReceiverQueue());
        Assert.assertEquals(consumer.getStats().getMsgNumInReceiverQueue().intValue(), 0);

        for (int i = 0; i < receiveQueueSize; i++) {
            producer.sendAsync("msg" + i);
        }
        //Give some time to consume
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(consumer.getStats().getMsgNumInReceiverQueue().intValue(), receiveQueueSize));
        consumer.close();
        producer.close();
    }

    @Test
    public void testGetStatsForPartitionedTopic() throws Exception {
        final String topicName = "persistent://my-property/my-ns/testGetStatsForPartitionedTopic";
        final String subName = "my-sub";
        final int receiveQueueSize = 100;

        admin.topics().createPartitionedTopic(topicName, 3);
        PulsarClient client = newPulsarClient(lookupUrl.toString(), 100);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topicName).create();
        MultiTopicsConsumerImpl<String> consumer = (MultiTopicsConsumerImpl<String>) client.newConsumer(Schema.STRING)
                .topic(topicName).receiverQueueSize(receiveQueueSize).subscriptionName(subName).subscribe();
        Assert.assertEquals(consumer.getStats().getMsgNumInSubReceiverQueue().size(), 3);
        Assert.assertEquals(consumer.getStats().getMsgNumInReceiverQueue().intValue(), 0);

        consumer.getStats().getMsgNumInSubReceiverQueue()
                .forEach((key, value) -> Assert.assertEquals((int) value, 0));

        for (int i = 0; i < receiveQueueSize; i++) {
            producer.sendAsync("msg" + i);
        }
        //Give some time to consume
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(consumer.getStats().getMsgNumInReceiverQueue().intValue(), receiveQueueSize));
        consumer.close();
        producer.close();
    }

    @DataProvider(name = "partitioned")
    public static Object[] isPartitioned() {
        return new Object[] {false, true};
    }

    @Test(dataProvider = "partitioned")
    public void testIncomingMessageSize(boolean isPartitioned) throws Exception {
        final String topicName = "persistent://my-property/my-ns/testIncomingMessageSize-" +
                UUID.randomUUID().toString();
        final String subName = "my-sub";

        if (isPartitioned) {
            admin.topics().createPartitionedTopic(topicName, 3);
        }

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        final int messages = 100;
        List<CompletableFuture<MessageId>> messageIds = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            messageIds.add(producer.newMessage().key(i + "").value(("Message-" + i).getBytes()).sendAsync());
        }
        FutureUtil.waitForAll(messageIds).get();

        Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            long size = ((ConsumerBase<byte[]>) consumer).getIncomingMessageSize();
            log.info("Check the incoming message size should greater that 0, current size is {}", size);
            Assert.assertTrue(size > 0);
        });

        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
        }

        Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            long size = ((ConsumerBase<byte[]>) consumer).getIncomingMessageSize();
            log.info("Check the incoming message size should be 0, current size is {}", size);
            Assert.assertEquals(size, 0);
        });
    }
}
