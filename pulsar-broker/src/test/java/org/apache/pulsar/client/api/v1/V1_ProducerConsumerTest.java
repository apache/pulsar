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
package org.apache.pulsar.client.api.v1;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
import java.util.stream.Collectors;

import lombok.Cleanup;
import org.apache.bookkeeper.mledger.impl.EntryCacheImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.protocol.Commands;
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

/**
 * Basic tests using the deprecated client APIs from Pulsar-1.x
 */
@Test(groups = "flaky")
public class V1_ProducerConsumerTest extends V1_ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(V1_ProducerConsumerTest.class);
    private static final long BATCHING_MAX_PUBLISH_DELAY_THRESHOLD = 1;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "batch")
    public Object[][] codecProvider() {
        return new Object[][] { { 0 }, { 1000 } };
    }

    @Test(dataProvider = "batch")
    public void testSyncProducerAndConsumer(int batchMessageDelayMs) throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe();

        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/use/my-ns/my-topic1");

        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true)
                .batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(5);
        } else {
            producerBuilder.enableBatching(false);
        }

        Producer<String> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            producer.send("my-message-" + i);
        }

        Message<String> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = msg.getValue();
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "batch")
    public void testAsyncProducerAndAsyncAck(int batchMessageDelayMs) throws Exception {
        log.info("-- Starting {} test --", methodName);
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .batchingMaxMessages(5)
                .batchingMaxPublishDelay(BATCHING_MAX_PUBLISH_DELAY_THRESHOLD, TimeUnit.MILLISECONDS)
                .enableBatching(batchMessageDelayMs != 0)
                .create();

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

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic3")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .messageListener((c, msg) -> {
                    Assert.assertNotNull(msg, "Message cannot be null");
                    String receivedMessage = new String(msg.getData());
                    log.debug("Received message [{}] in the listener", receivedMessage);
                    c.acknowledgeAsync(msg);
                    latch.countDown();
                })
                .subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic3")
                .batchingMaxMessages(5)
                .batchingMaxPublishDelay(BATCHING_MAX_PUBLISH_DELAY_THRESHOLD, TimeUnit.MILLISECONDS)
                .enableBatching(batchMessageDelayMs != 0)
                .create();

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

    @Test(dataProvider = "batch")
    public void testBackoffAndReconnect(int batchMessageDelayMs) throws Exception {
        log.info("-- Starting {} test --", methodName);
        // Create consumer and producer
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic4")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .startMessageIdInclusive()
                .subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic4")
                .batchingMaxMessages(5)
                .batchingMaxPublishDelay(BATCHING_MAX_PUBLISH_DELAY_THRESHOLD, TimeUnit.MILLISECONDS)
                .enableBatching(batchMessageDelayMs != 0)
                .create();

        // Produce messages
        CompletableFuture<MessageId> lastFuture = null;
        for (int i = 0; i < 10; i++) {
            lastFuture = producer.sendAsync(("my-message-" + i).getBytes()).thenApply(msgId -> {
                log.info("Published message id: {}", msgId);
                return msgId;
            });
        }

        lastFuture.get();

        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            log.info("Received: [{}]", new String(msg.getData()));
        }

        // Restart the broker and wait for the backoff to kick in. The client library will try to reconnect, and once
        // the broker is up, the consumer should receive the duplicate messages.
        log.info("-- Restarting broker --");
        restartBroker();

        msg = null;
        log.info("Receiving duplicate messages..");
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
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

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic5")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic5")
                .batchingMaxMessages(5)
                .batchingMaxPublishDelay(2 * BATCHING_MAX_PUBLISH_DELAY_THRESHOLD, TimeUnit.MILLISECONDS)
                .enableBatching(batchMessageDelayMs != 0)
                .sendTimeout(1, TimeUnit.SECONDS)
                .create();

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
            client1.newConsumer()
                    .topic("persistent://my-property/use/my-ns/my-topic6")
                    .subscriptionName("my-subscriber-name")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe();
            Assert.fail("Should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.AlreadyClosedException);
        }

        try {
            client1.newProducer().topic("persistent://my-property/use/my-ns/my-topic6").create();
            Assert.fail("Should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.AlreadyClosedException);
        }

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic6")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic6")
                .create();

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
    public void testSillyUser() throws Exception {
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
            pulsarClient.newConsumer()
                    .topic("persistent://my-property/use/my-ns/my-topic7")
                    .subscriptionName(null)
                    .subscribe();
            Assert.fail("Should fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            pulsarClient.newConsumer()
                    .topic("persistent://my-property/use/my-ns/my-topic7")
                    .subscriptionName("")
                    .subscribe();
            Assert.fail("Should fail");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            pulsarClient.newConsumer()
                    .topic("invalid://topic7")
                    .subscriptionName(null)
                    .subscribe();
            Assert.fail("Should fail");
        } catch (IllegalArgumentException e) {
            // Expected
        }

    }

    // This is to test that the flow control counter doesn't get corrupted while concurrent receives during
    // reconnections
    @Test(dataProvider = "batch")
    public void testConcurrentConsumerReceiveWhileReconnect(int batchMessageDelayMs) throws Exception {
        final int recvQueueSize = 100;
        final int numConsumersThreads = 10;
        String topic = "persistent://my-property/use/my-ns/my-topic-" + UUID.randomUUID().toString();

        String subName = UUID.randomUUID().toString();
        final Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .startMessageIdInclusive()
                .receiverQueueSize(recvQueueSize).subscribe();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        final CyclicBarrier barrier = new CyclicBarrier(numConsumersThreads + 1);
        for (int i = 0; i < numConsumersThreads; i++) {
            executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    barrier.await();
                    consumer.receive();
                    return null;
                }
            });
        }

        barrier.await();
        // there will be 10 threads calling receive() from the same consumer and will block
        Thread.sleep(100);

        // we restart the broker to reconnect
        restartBroker();
        Thread.sleep(2000);

        // publish 100 messages so that the consumers blocked on receive() will now get the messages
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .batchingMaxPublishDelay(BATCHING_MAX_PUBLISH_DELAY_THRESHOLD, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(5)
                .enableBatching(batchMessageDelayMs != 0)
                .create();
        for (int i = 0; i < recvQueueSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        ConsumerImpl<byte[]> consumerImpl = (ConsumerImpl<byte[]>) consumer;
        // The available permits should be 10 and num messages in the queue should be 90
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(consumerImpl.getAvailablePermits(), numConsumersThreads));
        Assert.assertEquals(consumerImpl.numMessagesInQueue(), recvQueueSize - numConsumersThreads);

        barrier.reset();
        for (int i = 0; i < numConsumersThreads; i++) {
            executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    barrier.await();
                    consumer.receive();
                    return null;
                }
            });
        }
        barrier.await();

        // The available permits should be 20 and num messages in the queue should be 80
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(consumerImpl.getAvailablePermits(), numConsumersThreads * 2));
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
            executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    barrier.await();
                    consumer.receive();
                    return null;
                }
            });
        }
        barrier.await();
        // we again make 10 threads call receive() and get blocked
        Thread.sleep(100);

        restartBroker();

        // The available permits should be 10 and num messages in the queue should be 90
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(consumerImpl.getAvailablePermits(), numConsumersThreads));
        Assert.assertEquals(consumerImpl.numMessagesInQueue(), recvQueueSize - numConsumersThreads);
        consumer.close();
    }

    @Test
    public void testSendBigMessageSize() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topic = "persistent://my-property/use/my-ns/bigMsg";
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
    @Test(groups = "quarantine")
    public void testActiveAndInActiveConsumerEntryCacheBehavior() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final long batchMessageDelayMs = 100;
        final int receiverSize = 10;
        final String topicName = "cache-topic-" + UUID.randomUUID().toString();
        final String sub1 = "faster-sub1";
        final String sub2 = "slower-sub2";

        /************ usecase-1: *************/
        // 1. Subscriber Faster subscriber
        Consumer<byte[]> subscriber1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/" + topicName)
                .subscriptionName(sub1)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(receiverSize)
                .subscribe();
        final String topic = "persistent://my-property/use/my-ns/" + topicName;
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(batchMessageDelayMs != 0)
                .batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(5)
                .create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topicRef.getManagedLedger();
        Field cacheField = ManagedLedgerImpl.class.getDeclaredField("entryCache");
        cacheField.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(cacheField, cacheField.getModifiers() & ~Modifier.FINAL);
        EntryCacheImpl entryCache = spy((EntryCacheImpl) cacheField.get(ledger));
        cacheField.set(ledger, entryCache);

        Message<byte[]>msg = null;
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
                .topic("persistent://my-property/use/my-ns/" + topicName)
                .subscriptionName(sub2)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
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
        retryStrategically((test) -> entryCache.getSize() > 0, 10, 100);
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


    @Test(timeOut = 2000)
    public void testAsyncProducerAndConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;
        final Set<String> produceMsgs = Sets.newHashSet();
        final Set<String> consumeMsgs = Sets.newHashSet();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        // produce message
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .create();
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            produceMsgs.add(message);
        }

        log.info(" start receiving messages :");
        CountDownLatch latch = new CountDownLatch(totalMsg);
        // receive messages
        @Cleanup("shutdownNow")
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
    }

    @Test(timeOut = 2000)
    public void testAsyncProducerAndConsumerWithZeroQueueSize() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;
        final Set<String> produceMsgs = Sets.newHashSet();
        final Set<String> consumeMsgs = Sets.newHashSet();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        // produce message
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .create();
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            produceMsgs.add(message);
        }

        log.info(" start receiving messages :");
        CountDownLatch latch = new CountDownLatch(totalMsg);
        // receive messages
        @Cleanup("shutdownNow")
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
    }

    @Test
    public void testSendCallBack() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .enableBatching(false)
                .create();
        for (int i = 0; i < totalMsg; i++) {
            final String message = "my-message-" + i;
            final AtomicInteger msgLength = new AtomicInteger();
            CompletableFuture<MessageId> future = producer.sendAsync(message.getBytes()).handle((r, ex) -> {
                if (ex != null) {
                    log.error("Message send failed:", ex);
                } else {
                    msgLength.set(message.length());
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
    @Test(timeOut = 30000)
    public void testSharedConsumerAckDifferentConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        ConsumerBuilder<byte[]> cb = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Shared)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .receiverQueueSize(1);
        Consumer<byte[]> consumer1 = cb.subscribe();
        Consumer<byte[]> consumer2 = cb.subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]>msg = null;
        Set<Message<byte[]>> consumerMsgSet1 = Sets.newHashSet();
        Set<Message<byte[]>> consumerMsgSet2 = Sets.newHashSet();
        for (int i = 0; i < 5; i++) {
            msg = consumer1.receive();
            consumerMsgSet1.add(msg);

            msg = consumer2.receive();
            consumerMsgSet2.add(msg);
        }

        consumerMsgSet1.forEach(m -> {
            try {
                consumer2.acknowledge(m);
            } catch (PulsarClientException e) {
                fail();
            }
        });
        consumerMsgSet2.forEach(m -> {
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
     * will stop receiving message after unAckThreshold = 500 4. Consumer acks messages and starts consuming remanining
     * messages This testcase enables checksum sending while producing message and broker verifies the checksum for the
     * message.
     *
     * @throws Exception
     */
    @Test
    public void testConsumerBlockingWithUnAckedMessages() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        try {
            final int unAckedMessagesBufferSize = 500;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 600;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessagesBufferSize);
            Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .subscriptionName( "subscriber-1")
                    .receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared)
                   .subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message<byte[]>msg = null;
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
    @Test
    public void testConsumerBlockingWithUnAckedMessagesMultipleIteration() throws Exception {
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
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            int totalReceivedMessages = 0;
            // (2) Receive Messages
            for (int j = 0; j < totalReceiveIteration; j++) {

                Message<byte[]>msg = null;
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
     * @throws Exception
     */
    @Test
    public void testMutlipleSharedConsumerBlockingWithUnAckedMessages() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        try {
            final int maxUnackedMessages = 20;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 100;
            int totalReceiveMessages = 0;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(maxUnackedMessages);
            Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();

            @Cleanup
            PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
            Consumer<byte[]> consumer2 = newPulsarClient.newConsumer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) Consumer1: consume without ack:
            // try to consume messages: but will be able to consume number of messages = maxUnackedMessages
            Message<byte[]>msg = null;
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
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize)
                    .ackTimeout(1, TimeUnit.SECONDS)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .enableBatching(false)
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
                Message<byte[]>msg = consumer.receive(1, TimeUnit.SECONDS);
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

    @Test
    public void testUnackBlockRedeliverMessages() throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        int totalReceiveMsg = 0;
        try {
            final int unAckedMessagesBufferSize = 20;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 100;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(unAckedMessagesBufferSize);
            ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message<byte[]>msg = null;
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

    @Test(dataProvider = "batch")
    public void testUnackedBlockAtBatch(int batchMessageDelayMs) throws Exception {
        log.info("-- Starting {} test --", methodName);

        int unAckedMessages = pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer();
        try {
            final int maxUnackedMessages = 20;
            final int receiverQueueSize = 10;
            final int totalProducedMsgs = 100;
            int totalReceiveMessages = 0;

            pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(maxUnackedMessages);
            Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .subscriptionName("subscriber-1")
                    .subscriptionType(SubscriptionType.Shared)
                    .receiverQueueSize(receiverQueueSize)
                    .subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .enableBatching(batchMessageDelayMs != 0)
                    .batchingMaxPublishDelay(BATCHING_MAX_PUBLISH_DELAY_THRESHOLD, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5)
                    .create();

            List<CompletableFuture<MessageId>> futures = Lists.newArrayList();
            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                futures.add(producer.sendAsync(message.getBytes()));
            }

            FutureUtil.waitForAll(futures).get();

            // (2) Consumer1: consume without ack:
            // try to consume messages: but will be able to consume number of messages = maxUnackedMessages
            Message<byte[]>msg = null;
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
                    consumer1.acknowledge(msg);
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
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared);

            Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
            Consumer<byte[]> consumer2 = consumerBuilder.subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) Consumer1: consume without ack:
            // try to consume messages: but will be able to consume number of messages = maxUnackedMessages
            Message<byte[]>msg = null;
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

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name")
                .subscribe();

        final int batchMessageDelayMs = 300;
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .enableBatching(true)
                .batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(5)
                .create();

        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]>msg = null;
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
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .subscriptionName("subscriber-1")
                    .subscriptionType(SubscriptionType.Shared)
                    .receiverQueueSize(receiverQueueSize)
                    .subscribe();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
                Thread.sleep(10);
            }

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message<byte[]>msg = null;
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
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .subscriptionName("subscriber-1")
                    .receiverQueueSize(receiverQueueSize)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();
            consumer.close();

            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .create();

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
                Thread.sleep(10);
            }

            // (1.a) start consumer again
            consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                    .topic("persistent://my-property/use/my-ns/unacked-topic")
                    .receiverQueueSize(receiverQueueSize)
                    .subscriptionName("subscriber-1")
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message<byte[]>msg = null;
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
            assertEquals(messages1.size(), receiverQueueSize);
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

        @Cleanup
        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer1 = newPulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared)
                .priorityLevel(1).receiverQueueSize(5).subscribe();

        @Cleanup
        PulsarClient newPulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer2 = newPulsarClient1.newConsumer().topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared)
                .priorityLevel(1).receiverQueueSize(5).subscribe();

        @Cleanup
        PulsarClient newPulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer3 = newPulsarClient2.newConsumer().topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared)
                .priorityLevel(1).receiverQueueSize(5).subscribe();

        @Cleanup
        PulsarClient newPulsarClient3 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> consumer4 = newPulsarClient3.newConsumer().topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared)
                .priorityLevel(2).receiverQueueSize(5).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
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
    @Test
    public void testSharedSamePriorityConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int queueSize = 5;
        int maxUnAckMsgs = pulsar.getConfiguration().getMaxConcurrentLookupRequest();
        pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(queueSize);

        Consumer<byte[]> c1 = pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(queueSize).subscribe();

        @Cleanup
        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> c2 = newPulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(queueSize).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .enableBatching(false)
                .create();
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
            Message<byte[]>msg = c1.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messages.add(msg);
            } else {
                break;
            }
        }
        for (int i = 0; i < totalPublishMessages; i++) {
            Message<byte[]>msg = c2.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messages.add(msg);
            } else {
                break;
            }
        }

        Assert.assertEquals(queueSize * 2, messages.size());

        // create new consumers with the same priority
        @Cleanup
        PulsarClient newPulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> c3 = newPulsarClient1.newConsumer().topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(queueSize).subscribe();

        @Cleanup
        PulsarClient newPulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> c4 = newPulsarClient2.newConsumer().topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(queueSize).subscribe();

        @Cleanup
        PulsarClient newPulsarClient3 = newPulsarClient(lookupUrl.toString(), 0);// Creates new client connection
        Consumer<byte[]> c5 = newPulsarClient3.newConsumer().topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(queueSize).subscribe();

        // c1 and c2 are blocked: so, let c3, c4 and c5 consume rest of the messages

        for (int i = 0; i < totalPublishMessages; i++) {
            Message<byte[]>msg = c4.receive(500, TimeUnit.MILLISECONDS);
            if (msg != null) {
                messages.add(msg);
            } else {
                break;
            }
        }

        for (int i = 0; i < totalPublishMessages; i++) {
            Message<byte[]>msg = c5.receive(500, TimeUnit.MILLISECONDS);
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
        pulsar.getConfiguration().setMaxUnackedMessagesPerConsumer(maxUnAckMsgs);
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testRedeliveryFailOverConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int receiverQueueSize = 10;

        // Only subscribe consumer
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/unacked-topic")
                .subscriptionName("subscriber-1")
                .receiverQueueSize(receiverQueueSize)
                .subscriptionType(SubscriptionType.Failover)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/unacked-topic")
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
                .topic("persistent://my-property/use/my-ns/failAsyncReceive-1")
                .subscriptionName("my-subscriber-name")
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
        TopicName topicName = TopicName.get("persistent://my-property/use/my-ns/failAsyncReceive-2");
        admin.topics().createPartitionedTopic(topicName.toString(), numPartitions);
        Consumer<byte[]> partitionedConsumer = pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionName("my-partitioned-subscriber")
                .subscribe();
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

    @Test
    public void testECDSAEncryption() throws Exception {
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
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/myecdsa-topic1")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .cryptoKeyReader(new EncKeyReader())
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/myecdsa-topic1")
                .addEncryptionKey("client-ecdsa.pem")
                .cryptoKeyReader(new EncKeyReader())
                .create();
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;

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

    @Test
    public void testRSAEncryption() throws Exception {
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
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/myrsa-topic1")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .cryptoKeyReader(new EncKeyReader())
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/myrsa-topic1")
                .addEncryptionKey("client-rsa.pem")
                .cryptoKeyReader(new EncKeyReader())
                .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/myrsa-topic1")
                .addEncryptionKey("client-rsa.pem")
                .cryptoKeyReader(new EncKeyReader())
                .create();
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        for (int i = totalMsg; i < totalMsg * 2; i++) {
            String message = "my-message-" + i;
            producer2.send(message.getBytes());
        }

        Message<byte[]> msg = null;

        for (int i = 0; i < totalMsg * 2; i++) {
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

        Message<String> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/use/my-ns/myenc-topic1")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        try {
            // 1. Invalid key name
            pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/use/myenc-ns/myenc-topic1")
                .enableBatching(false)
                .addEncryptionKey("client-non-existant-rsa.pem")
                .cryptoKeyReader(new EncKeyReader())
                .create();
            Assert.fail("Producer creation should not suceed if failing to read key");
        } catch (Exception e) {
            // ok
        }

        // 2. Producer with valid key name
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/use/my-ns/myenc-topic1")
                .enableBatching(false)
                .addEncryptionKey("client-rsa.pem")
                .cryptoKeyReader(new EncKeyReader())
                .create();

        for (int i = 0; i < totalMsg; i++) {
            producer.send("my-message-" + i);
        }

        // 3. KeyReder is not set by consumer
        // Receive should fail since key reader is not setup
        msg = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(msg, "Receive should have failed with no keyreader");

        // 4. Set consumer config to consume even if decryption fails
        consumer.close();
        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/use/my-ns/myenc-topic1")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .cryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
                .subscribe();

        int msgNum = 0;
        try {
            // Receive should proceed and deliver encrypted message
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = msg.getValue();
            String expectedMessage = "my-message-" + (msgNum++);
            Assert.assertNotEquals(receivedMessage, expectedMessage, "Received encrypted message " + receivedMessage
                    + " should not match the expected message " + expectedMessage);
            consumer.acknowledgeCumulative(msg);
        } catch (Exception e) {
            Assert.fail("Failed to receive message even aftet ConsumerCryptoFailureAction.CONSUME is set.");
        }

        // 5. Set keyreader and failure action
        consumer.close();
        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/use/my-ns/myenc-topic1")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .cryptoKeyReader(new EncKeyReader())
                .cryptoFailureAction(ConsumerCryptoFailureAction.FAIL)
                .subscribe();

        for (int i = msgNum; i < totalMsg - 1; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = msg.getValue();
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

        // 6. Set consumer config to discard if decryption fails
        consumer.close();
        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/use/my-ns/myenc-topic1")
                .subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive)
                .cryptoFailureAction(ConsumerCryptoFailureAction.DISCARD)
                .subscribe();

        // Receive should proceed and discard encrypted messages
        msg = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(msg, "Message received even after ConsumerCryptoFailureAction.DISCARD is set.");

        log.info("-- Exiting {} test --", methodName);
    }

}
