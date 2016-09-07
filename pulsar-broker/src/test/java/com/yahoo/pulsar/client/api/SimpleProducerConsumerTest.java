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

        System.out.println(" start receiving messages :");
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

        System.out.println(" start receiving messages :");
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
}
