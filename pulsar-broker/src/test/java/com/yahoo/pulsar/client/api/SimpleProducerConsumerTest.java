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
package com.yahoo.pulsar.client.api;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
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

import org.apache.bookkeeper.mledger.impl.EntryCacheImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.pulsar.broker.service.persistent.PersistentTopic;
import com.yahoo.pulsar.client.impl.ConsumerImpl;
import com.yahoo.pulsar.client.impl.MessageIdImpl;
import com.yahoo.pulsar.client.util.FutureUtil;
import com.yahoo.pulsar.common.api.PulsarDecoder;

public class SimpleProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(SimpleProducerConsumerTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
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

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1", "my-subscriber-name",
                conf);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        if (batchMessageDelayMs != 0) {
            producerConf.setBatchingEnabled(true);
            producerConf.setBatchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerConf.setBatchingMaxMessages(5);
        }

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message msg = null;
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

    @Test(dataProvider = "batch")
    public void testAsyncProducerAndAsyncAck(int batchMessageDelayMs) throws Exception {
        log.info("-- Starting {} test --", methodName);
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic2", "my-subscriber-name",
                conf);
        ProducerConfiguration producerConf = new ProducerConfiguration();
        if (batchMessageDelayMs != 0) {
            producerConf.setBatchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerConf.setBatchingMaxMessages(5);
            producerConf.setBatchingEnabled(true);
        }
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic2", producerConf);
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

        Message msg = null;
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
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);

        int numMessages = 100;
        final CountDownLatch latch = new CountDownLatch(numMessages);

        conf.setMessageListener((consumer, msg) -> {
            Assert.assertNotNull(msg, "Message cannot be null");
            String receivedMessage = new String(msg.getData());
            log.debug("Received message [{}] in the listener", receivedMessage);
            consumer.acknowledgeAsync(msg);
            latch.countDown();
        });
        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic3", "my-subscriber-name",
                conf);
        ProducerConfiguration producerConf = new ProducerConfiguration();
        if (batchMessageDelayMs != 0) {
            producerConf.setBatchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerConf.setBatchingMaxMessages(5);
            producerConf.setBatchingEnabled(true);
        }
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic3", producerConf);
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
        assertEquals(latch.await(numMessages, TimeUnit.SECONDS), true, "Timed out waiting for message listener acks");
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "batch")
    public void testBackoffAndReconnect(int batchMessageDelayMs) throws Exception {
        log.info("-- Starting {} test --", methodName);
        // Create consumer and producer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic4", "my-subscriber-name",
                conf);
        ProducerConfiguration producerConf = new ProducerConfiguration();
        if (batchMessageDelayMs != 0) {
            producerConf.setBatchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerConf.setBatchingMaxMessages(5);
            producerConf.setBatchingEnabled(true);
        }
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic4", producerConf);

        // Produce messages
        for (int i = 0; i < 10; i++) {
            producer.send("my-message".getBytes());
        }

        Message msg = null;
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

        ConsumerConfiguration consumerConf = new ConsumerConfiguration();
        consumerConf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic5", "my-subscriber-name",
                consumerConf);
        ProducerConfiguration producerConf = new ProducerConfiguration();
        if (batchMessageDelayMs != 0) {
            producerConf.setBatchingMaxPublishDelay(2 * batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerConf.setBatchingMaxMessages(5);
            producerConf.setBatchingEnabled(true);
        }
        producerConf.setSendTimeout(1, TimeUnit.SECONDS);

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic5", producerConf);
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
        Message msg = consumer.receive(3, TimeUnit.SECONDS);
        Assert.assertNull(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testInvalidSequence() throws Exception {
        log.info("-- Starting {} test --", methodName);

        PulsarClient client1 = PulsarClient.create("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT);
        client1.close();

        ConsumerConfiguration consumerConf = new ConsumerConfiguration();
        consumerConf.setSubscriptionType(SubscriptionType.Exclusive);

        try {
            Consumer consumer = client1.subscribe("persistent://my-property/use/my-ns/my-topic6", "my-subscriber-name",
                    consumerConf);
            Assert.fail("Should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.AlreadyClosedException);
        }

        try {
            Producer producer = client1.createProducer("persistent://my-property/use/my-ns/my-topic6");
            Assert.fail("Should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.AlreadyClosedException);
        }

        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic6", "my-subscriber-name",
                consumerConf);

        try {
            Message msg = MessageBuilder.create().setContent("InvalidMessage".getBytes()).build();
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

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic6");
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
            PulsarClient client1 = PulsarClient.create("invalid://url");
            Assert.fail("should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.InvalidServiceURL);
        }

        ProducerConfiguration producerConf = new ProducerConfiguration();

        try {
            producerConf.setSendTimeout(-1, TimeUnit.SECONDS);
            Assert.fail("should fail");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            producerConf.setMaxPendingMessages(0);
            Assert.fail("should fail");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic7", null);
            Assert.fail("should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.InvalidConfigurationException);
        }

        try {
            Producer producer = pulsarClient.createProducer("invalid://topic", producerConf);
            Assert.fail("should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.InvalidTopicNameException);
        }

        ConsumerConfiguration consumerConf = new ConsumerConfiguration();

        try {
            consumerConf.setMessageListener(null);
            Assert.fail("should fail");
        } catch (NullPointerException e) {
            // ok
        }

        try {
            consumerConf.setSubscriptionType(null);
            Assert.fail("should fail");
        } catch (NullPointerException e) {
            // ok
        }

        try {
            consumerConf.setReceiverQueueSize(-1);
            Assert.fail("should fail");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic7",
                    "my-subscriber-name", null);
            Assert.fail("Should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.InvalidConfigurationException);
        }

        try {
            Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic7", null,
                    consumerConf);
            Assert.fail("Should fail");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof PulsarClientException.InvalidConfigurationException);
        }

        try {
            Consumer consumer = pulsarClient.subscribe("invalid://topic7", "my-subscriber-name", consumerConf);
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

        final ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(recvQueueSize);
        String subName = UUID.randomUUID().toString();
        final Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic7", subName, conf);
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
        ProducerConfiguration producerConf = new ProducerConfiguration();
        if (batchMessageDelayMs != 0) {
            producerConf.setBatchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerConf.setBatchingMaxMessages(5);
            producerConf.setBatchingEnabled(true);
        }
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic7", producerConf);
        for (int i = 0; i < recvQueueSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Thread.sleep(500);

        ConsumerImpl consumerImpl = (ConsumerImpl) consumer;
        // The available permits should be 10 and num messages in the queue should be 90
        Assert.assertEquals(consumerImpl.getAvailablePermits(), numConsumersThreads);
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
        Thread.sleep(100);

        // The available permits should be 20 and num messages in the queue should be 80
        Assert.assertEquals(consumerImpl.getAvailablePermits(), numConsumersThreads * 2);
        Assert.assertEquals(consumerImpl.numMessagesInQueue(), recvQueueSize - (numConsumersThreads * 2));

        // clear the queue
        while (true) {
            Message msg = consumer.receive(1, TimeUnit.SECONDS);
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
        Thread.sleep(2000);

        // The available permits should be 10 and num messages in the queue should be 90
        Assert.assertEquals(consumerImpl.getAvailablePermits(), numConsumersThreads);
        Assert.assertEquals(consumerImpl.numMessagesInQueue(), recvQueueSize - numConsumersThreads);
        consumer.close();
    }

    @Test
    public void testSendBigMessageSize() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Messages are allowed up to MaxMessageSize
        MessageBuilder.create().setContent(new byte[PulsarDecoder.MaxMessageSize]).build();

        try {
            MessageBuilder.create().setContent(new byte[PulsarDecoder.MaxMessageSize + 1]).build();
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
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
    @Test
    public void testActiveAndInActiveConsumerEntryCacheBehavior() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final long batchMessageDelayMs = 100;
        final int receiverSize = 10;
        final String topicName = "cache-topic";
        final String sub1 = "faster-sub1";
        final String sub2 = "slower-sub2";
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        conf.setReceiverQueueSize(receiverSize);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        if (batchMessageDelayMs != 0) {
            producerConf.setBatchingEnabled(true);
            producerConf.setBatchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerConf.setBatchingMaxMessages(5);
        }

        /************ usecase-1: *************/
        // 1. Subscriber Faster subscriber
        Consumer subscriber1 = pulsarClient.subscribe("persistent://my-property/use/my-ns/" + topicName, sub1, conf);
        final String topic = "persistent://my-property/use/my-ns/" + topicName;
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topicRef.getManagedLedger();
        Field cacheField = ManagedLedgerImpl.class.getDeclaredField("entryCache");
        cacheField.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(cacheField, cacheField.getModifiers() & ~Modifier.FINAL);
        EntryCacheImpl entryCache = spy((EntryCacheImpl) cacheField.get(ledger));
        cacheField.set(ledger, entryCache);

        Message msg = null;
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

        // Verify: cache has to be cleared as there is no message needs to be consumed by active subscriber
        assertTrue(entryCache.getSize() == 0);

        /************ usecase-2: *************/
        // 1.b Subscriber slower-subscriber
        Consumer subscriber2 = pulsarClient.subscribe("persistent://my-property/use/my-ns/" + topicName, sub2, conf);
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

        // Verify: EntryCache should be cleared
        assertTrue(entryCache.getSize() == 0);
        subscriber1.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testDeactivatingBacklogConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final long batchMessageDelayMs = 100;
        final int receiverSize = 10;
        final String topicName = "cache-topic";
        final String topic = "persistent://my-property/use/my-ns/" + topicName;
        final String sub1 = "faster-sub1";
        final String sub2 = "slower-sub2";
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        conf.setReceiverQueueSize(receiverSize);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        if (batchMessageDelayMs != 0) {
            producerConf.setBatchingEnabled(true);
            producerConf.setBatchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerConf.setBatchingMaxMessages(5);
        }

        // 1. Subscriber Faster subscriber: let it consume all messages immediately
        Consumer subscriber1 = pulsarClient.subscribe("persistent://my-property/use/my-ns/" + topicName, sub1, conf);
        // 1.b. Subscriber Slow subscriber:
        conf.setReceiverQueueSize(receiverSize);
        Consumer subscriber2 = pulsarClient.subscribe("persistent://my-property/use/my-ns/" + topicName, sub2, conf);
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topicRef.getManagedLedger();

        // reflection to set/get cache-backlog fields value:
        final long maxMessageCacheRetentionTimeMillis = 100;
        Field backlogThresholdField = ManagedLedgerImpl.class.getDeclaredField("maxActiveCursorBacklogEntries");
        backlogThresholdField.setAccessible(true);
        Field field = ManagedLedgerImpl.class.getDeclaredField("maxMessageCacheRetentionTimeMillis");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(ledger, maxMessageCacheRetentionTimeMillis);
        final long maxActiveCursorBacklogEntries = (long) backlogThresholdField.get(ledger);

        Message msg = null;
        final int totalMsgs = (int) maxActiveCursorBacklogEntries + receiverSize + 1;
        // 2. Produce messages
        for (int i = 0; i < totalMsgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        // 3. Consume messages: at Faster subscriber
        for (int i = 0; i < totalMsgs; i++) {
            msg = subscriber1.receive(100, TimeUnit.MILLISECONDS);
            subscriber1.acknowledge(msg);
        }

        // wait : so message can be eligible to to be evict from cache
        Thread.sleep(maxMessageCacheRetentionTimeMillis);

        // 4. deactivate subscriber which has built the backlog
        ledger.checkBackloggedCursors();
        Thread.sleep(100);

        // 5. verify: active subscribers
        Set<String> activeSubscriber = Sets.newHashSet();
        ledger.getActiveCursors().forEach(c -> activeSubscriber.add(c.getName()));
        assertTrue(activeSubscriber.contains(sub1));
        assertFalse(activeSubscriber.contains(sub2));

        // 6. consume messages : at slower subscriber
        for (int i = 0; i < totalMsgs; i++) {
            msg = subscriber2.receive(100, TimeUnit.MILLISECONDS);
            subscriber2.acknowledge(msg);
        }

        ledger.checkBackloggedCursors();

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
        final ProducerConfiguration producerConf = new ProducerConfiguration();
        final ConsumerConfiguration conf = new ConsumerConfiguration();

        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1", "my-subscriber-name",
                conf);

        // produce message
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
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
    }

    @Test(timeOut = 2000)
    public void testAsyncProducerAndConsumerWithZeroQueueSize() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;
        final Set<String> produceMsgs = Sets.newHashSet();
        final Set<String> consumeMsgs = Sets.newHashSet();
        final ProducerConfiguration producerConf = new ProducerConfiguration();
        final ConsumerConfiguration conf = new ConsumerConfiguration();

        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1", "my-subscriber-name",
                conf);

        // produce message
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
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
    }

    @Test
    public void testSendCallBack() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int totalMsg = 100;
        final ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
        for (int i = 0; i < totalMsg; i++) {
            final String message = "my-message-" + i;
            Message msg = MessageBuilder.create().setContent(message.getBytes()).build();
            final AtomicInteger msgLength = new AtomicInteger();
            CompletableFuture<MessageId> future = producer.sendAsync(msg).handle((r, ex) -> {
                if (ex != null) {
                    log.error("Message send failed:", ex);
                } else {
                    msgLength.set(msg.getData().length);
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

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(1);
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer1 = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1", "my-subscriber-name",
                conf);
        Consumer consumer2 = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1", "my-subscriber-name",
                conf);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message msg = null;
        Set<Message> consumerMsgSet1 = Sets.newHashSet();
        Set<Message> consumerMsgSet2 = Sets.newHashSet();
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

    private void receiveAsync(Consumer consumer, int totalMessage, int currentMessage, CountDownLatch latch,
            final Set<String> consumeMsg, ExecutorService executor) throws PulsarClientException {
        if (currentMessage < totalMessage) {
            CompletableFuture<Message> future = consumer.receiveAsync();
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
     * Verify: Consumer stops receiving msg when reach unack-msg limit and
     * starts receiving once acks messages
     * 1. Produce X (600) messages
     * 2. Consumer has receive size (10) and receive message without acknowledging
     * 3. Consumer will stop receiving message after unAckThreshold = 500
     * 4. Consumer acks messages and starts consuming remanining messages
     * This testcase enables checksum sending while producing message and broker verifies the checksum for the message.
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
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/unacked-topic",
                    "subscriber-1", conf);

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message msg = null;
            List<Message> messages = Lists.newArrayList();
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
     * Verify: iteration of
     * a. message receive w/o acking
     * b. stop receiving msg
     * c. ack msgs
     * d. started receiving msgs
     *
     * 1. Produce total X (1500) messages
     * 2. Consumer consumes messages without acking until stop receiving
     * from broker due to reaching ack-threshold (500)
     * 3. Consumer acks messages after stop getting messages
     * 4. Consumer again tries to consume messages
     * 5. Consumer should be able to complete consuming all 1500 messages in 3 iteration (1500/500)
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
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/unacked-topic",
                    "subscriber-1", conf);

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            int totalReceivedMessages = 0;
            // (2) Receive Messages
            for (int j = 0; j < totalReceiveIteration; j++) {

                Message msg = null;
                List<Message> messages = Lists.newArrayList();
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
     * @param batchMessageDelayMs
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
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            Consumer consumer1 = pulsarClient.subscribe("persistent://my-property/use/my-ns/unacked-topic",
                    "subscriber-1", conf);
            Consumer consumer2 = pulsarClient.subscribe("persistent://my-property/use/my-ns/unacked-topic",
                    "subscriber-1", conf);

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }


            // (2) Consumer1: consume without ack:
            // try to consume messages: but will be able to consume number of messages = maxUnackedMessages
            Message msg = null;
            List<Message> messages = Lists.newArrayList();
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

            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setAckTimeout(1, TimeUnit.SECONDS);
            conf.setSubscriptionType(SubscriptionType.Shared);
            ConsumerImpl consumer = (ConsumerImpl) pulsarClient
                    .subscribe("persistent://my-property/use/my-ns/unacked-topic", "subscriber-1", conf);

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) wait for consumer to receive messages
            Thread.sleep(200);
            assertEquals(consumer.numMessagesInQueue(), receiverQueueSize);

            // (3) wait for messages to expire, we should've received more
            Thread.sleep(2000);
            assertEquals(consumer.numMessagesInQueue(), receiverQueueSize);

            for (int i = 0; i < totalProducedMsgs; i++) {
                Message msg = consumer.receive(1, TimeUnit.SECONDS);
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
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            ConsumerImpl consumer = (ConsumerImpl) pulsarClient
                    .subscribe("persistent://my-property/use/my-ns/unacked-topic", "subscriber-1", conf);

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message msg = null;
            List<Message> messages = Lists.newArrayList();
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
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            Consumer consumer1 = pulsarClient.subscribe("persistent://my-property/use/my-ns/unacked-topic",
                    "subscriber-1", conf);

            ProducerConfiguration producerConf = new ProducerConfiguration();

            if (batchMessageDelayMs != 0) {
                producerConf.setBatchingEnabled(true);
                producerConf.setBatchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
                producerConf.setBatchingMaxMessages(5);
            }

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            List<CompletableFuture<MessageId>> futures = Lists.newArrayList();
            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                futures.add(producer.sendAsync(message.getBytes()));
            }

            FutureUtil.waitForAll(futures).get();

            // (2) Consumer1: consume without ack:
            // try to consume messages: but will be able to consume number of messages = maxUnackedMessages
            Message msg = null;
            List<Message> messages = Lists.newArrayList();
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
     *
     * @param batchMessageDelayMs
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
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            Consumer consumer1 = pulsarClient.subscribe("persistent://my-property/use/my-ns/unacked-topic",
                    "subscriber-1", conf);
            Consumer consumer2 = pulsarClient.subscribe("persistent://my-property/use/my-ns/unacked-topic",
                    "subscriber-1", conf);

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }


            // (2) Consumer1: consume without ack:
            // try to consume messages: but will be able to consume number of messages = maxUnackedMessages
            Message msg = null;
            List<Message> messages = Lists.newArrayList();
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

            assertEquals(messages.size(), maxUnackedMessages); //consumer1

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
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1", "my-subscriber-name",
                conf);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        final int batchMessageDelayMs = 300;
        if (batchMessageDelayMs != 0) {
            producerConf.setBatchingEnabled(true);
            producerConf.setBatchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS);
            producerConf.setBatchingMaxMessages(5);
        }

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
        for (int i = 0; i < totalMsg; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message msg = null;
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
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            ConsumerImpl consumer = (ConsumerImpl) pulsarClient
                    .subscribe("persistent://my-property/use/my-ns/unacked-topic", "subscriber-1", conf);

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
                Thread.sleep(10);
            }

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message msg = null;
            List<Message> messages1 = Lists.newArrayList();
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
     * Usecase: Consumer starts consuming only after all messages have been produced.
     * So, consumer consumes total receiver-queue-size number messages => ask for redelivery and receives all messages again.
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
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setReceiverQueueSize(receiverQueueSize);
            conf.setSubscriptionType(SubscriptionType.Shared);
            // Only subscribe consumer
            ConsumerImpl consumer = (ConsumerImpl) pulsarClient
                    .subscribe("persistent://my-property/use/my-ns/unacked-topic", "subscriber-1", conf);
            consumer.close();

            ProducerConfiguration producerConf = new ProducerConfiguration();

            Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                    producerConf);

            // (1) Produced Messages
            for (int i = 0; i < totalProducedMsgs; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
                Thread.sleep(10);
            }

            // (1.a) start consumer again
            consumer = (ConsumerImpl) pulsarClient.subscribe("persistent://my-property/use/my-ns/unacked-topic",
                    "subscriber-1", conf);

            // (2) try to consume messages: but will be able to consume number of messages = unAckedMessagesBufferSize
            Message msg = null;
            List<Message> messages1 = Lists.newArrayList();
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
        ConsumerConfiguration conf1 = new ConsumerConfiguration();
        conf1.setSubscriptionType(SubscriptionType.Shared);
        conf1.setPriorityLevel(1);
        conf1.setReceiverQueueSize(5);
        ConsumerConfiguration conf4 = new ConsumerConfiguration();
        conf4.setSubscriptionType(SubscriptionType.Shared);
        conf4.setPriorityLevel(2);
        conf4.setReceiverQueueSize(5);
        Consumer consumer1 = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic2", "my-subscriber-name",
                conf1);
        Consumer consumer2 = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic2", "my-subscriber-name",
                conf1);
        Consumer consumer3 = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic2", "my-subscriber-name",
                conf1);
        Consumer consumer4 = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic2", "my-subscriber-name",
                conf4);
        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic2", producerConf);
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
         * a. consumer1 and consumer2 now has more permits (as received and sent more permits) 
         * b. try to produce more messages: which will again distribute among consumer1 and consumer2 
         * and should not dispatch to consumer4
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

    @Test
    public void testRedeliveryFailOverConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int receiverQueueSize = 10;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(receiverQueueSize);
        conf.setSubscriptionType(SubscriptionType.Failover);
        // Only subscribe consumer
        ConsumerImpl consumer = (ConsumerImpl) pulsarClient
                .subscribe("persistent://my-property/use/my-ns/unacked-topic", "subscriber-1", conf);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/unacked-topic",
                producerConf);

        // (1) First round to produce-consume messages
        int consumeMsgInParts = 4;
        for (int i = 0; i < receiverQueueSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            Thread.sleep(10);
        }
        // (1.a) consume first consumeMsgInParts msgs and trigger redeliver
        Message msg = null;
        List<Message> messages1 = Lists.newArrayList();
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
    
}