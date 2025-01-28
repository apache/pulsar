/*
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
import static org.apache.pulsar.broker.BrokerTestUtil.newUniqueName;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class KeySharedSubscriptionBrokerCacheTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(KeySharedSubscriptionBrokerCacheTest.class);
    private static final String SUBSCRIPTION_NAME = "key_shared";

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setUnblockStuckSubscriptionEnabled(false);
        conf.setSubscriptionKeySharedUseConsistentHashing(true);
        conf.setManagedLedgerCacheSizeMB(100);

        // configure to evict entries after 30 seconds so that we can test retrieval from cache
        conf.setManagedLedgerCacheEvictionTimeThresholdMillis(30000);
        conf.setManagedLedgerCacheEvictionIntervalMs(30000);

        // Important: this is currently necessary to make use of cache for replay queue reads
        conf.setCacheEvictionByMarkDeletedPosition(true);

        conf.setManagedLedgerMaxReadsInFlightSizeInMB(100);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    public void resetAfterMethod() throws Exception {
        List<String> list = admin.namespaces().getTopics("public/default");
        for (String topicName : list){
            if (!pulsar.getBrokerService().isSystemTopic(topicName)) {
                admin.topics().delete(topicName, false);
            }
        }
        pulsarTestContext.getMockBookKeeper().setReadHandleInterceptor(null);
    }

    // Use a fixed seed to make the tests using random values deterministic
    // When a test fails, it's possible to re-run it to reproduce the issue
    private static final Random random = new Random(1);

    private Producer<Integer> createProducer(String topic, boolean enableBatch) throws PulsarClientException {
        Producer<Integer> producer = null;
        if (enableBatch) {
            producer = pulsarClient.newProducer(Schema.INT32)
                    .topic(topic)
                    .enableBatching(true)
                    .maxPendingMessages(2001)
                    .batcherBuilder(BatcherBuilder.KEY_BASED)
                    .create();
        } else {
            producer = pulsarClient.newProducer(Schema.INT32)
                    .topic(topic)
                    .maxPendingMessages(2001)
                    .enableBatching(false)
                    .create();
        }
        return producer;
    }

    private StickyKeyConsumerSelector getSelector(String topic, String subscription) {
        return getStickyKeyDispatcher(topic, subscription).getSelector();
    }

    @SneakyThrows
    private PersistentStickyKeyDispatcherMultipleConsumers getStickyKeyDispatcher(String topic, String subscription) {
        Topic t = pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        PersistentSubscription sub = (PersistentSubscription) t.getSubscription(subscription);
        PersistentStickyKeyDispatcherMultipleConsumers dispatcher =
                (PersistentStickyKeyDispatcherMultipleConsumers) sub.getDispatcher();
        return dispatcher;
    }

    @Test(invocationCount = 1)
    public void testReplayQueueReadsGettingCached() throws Exception {
        String topic = newUniqueName("testReplayQueueReadsGettingCached");
        int numberOfKeys = 100;
        long pauseTime = 100L;
        long testStartNanos = System.nanoTime();

        @Cleanup
        Producer<Integer> producer = createProducer(topic, false);

        // create a consumer and close it to create a subscription
        pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe()
                .close();

        Set<Integer> remainingMessageValues = Collections.synchronizedSet(new HashSet<>());
        BlockingQueue<Pair<Consumer<Integer>, Message<Integer>>> unackedMessages = new LinkedBlockingQueue<>();
        AtomicBoolean c2MessagesShouldBeUnacked = new AtomicBoolean(true);
        Set<String> keysForC2 = new HashSet<>();
        AtomicLong lastMessageTimestamp = new AtomicLong(System.currentTimeMillis());

        MessageListener<Integer> messageHandler = (consumer, msg) -> {
            lastMessageTimestamp.set(System.currentTimeMillis());
            synchronized (this) {
                String key = msg.getKey();
                if (c2MessagesShouldBeUnacked.get() && keysForC2.contains(key)) {
                    unackedMessages.add(Pair.of(consumer, msg));
                    return;
                }
                remainingMessageValues.remove(msg.getValue());
                consumer.acknowledgeAsync(msg);
            }
        };

        pulsarTestContext.getMockBookKeeper().setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, entries) -> {
            log.error("Attempting to read from BK when cache should be used. {}:{} to {}:{}", ledgerId, firstEntry,
                    ledgerId, lastEntry);
            return CompletableFuture.failedFuture(
                    new ManagedLedgerException.NonRecoverableLedgerException(
                            "Should not read from BK since cache should be used."));
        });

        // Adding a new consumer.
        @Cleanup
        Consumer<Integer> c1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c1")
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(messageHandler)
                .subscribe();

        @Cleanup
        Consumer<Integer> c2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c2")
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(messageHandler)
                .subscribe();

        @Cleanup
        Consumer<Integer> c3 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c3")
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(messageHandler)
                .subscribe();

        PersistentStickyKeyDispatcherMultipleConsumers dispatcher = getStickyKeyDispatcher(topic, SUBSCRIPTION_NAME);
        StickyKeyConsumerSelector selector = dispatcher.getSelector();

        // find keys that will be assigned to c2
        for (int i = 0; i < numberOfKeys; i++) {
            String key = String.valueOf(i);
            byte[] keyBytes = key.getBytes(UTF_8);
            int hash = StickyKeyConsumerSelector.makeStickyKeyHash(keyBytes);
            if (selector.select(hash).consumerName().equals("c2")) {
                keysForC2.add(key);
            }
        }

        // close c2
        c2.close();

        // produce messages with random keys
        for (int i = 0; i < 1000; i++) {
            String key = String.valueOf(random.nextInt(numberOfKeys));
            //log.info("Producing message with key: {} value: {}", key, i);
            remainingMessageValues.add(i);
            producer.newMessage()
                    .key(key)
                    .value(i)
                    .send();
        }

        // reconnect c2
        c2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c2")
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(messageHandler)
                .startPaused(true)
                .subscribe();

        // ack the unacked messages to unblock c2 keys
        c2MessagesShouldBeUnacked.set(false);
        Pair<Consumer<Integer>, Message<Integer>> consumerMessagePair;
        while ((consumerMessagePair = unackedMessages.poll()) != null) {
            messageHandler.received(consumerMessagePair.getLeft(), consumerMessagePair.getRight());
        }

        // produce more messages with random keys
        for (int i = 0; i < 1000; i++) {
            String key = String.valueOf(random.nextInt(numberOfKeys));
            //log.info("Producing message with key: {} value: {}", key, i);
            remainingMessageValues.add(i);
            producer.newMessage()
                    .key(key)
                    .value(i)
                    .send();
        }

        c2.resume();

        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
            return remainingMessageValues.isEmpty()
                    || System.currentTimeMillis() - lastMessageTimestamp.get() > 50 * pauseTime;
        });

        try {
            assertSoftly(softly -> {
                softly.assertThat(remainingMessageValues).as("remainingMessageValues").isEmpty();
                ManagedLedgerFactoryMBeanImpl cacheStats =
                        ((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory()).getMbean();
                cacheStats.refreshStats(System.nanoTime() - testStartNanos, TimeUnit.NANOSECONDS);
                softly.assertThat(cacheStats.getCacheHitsRate()).as("cache hits").isGreaterThan(0.0);
                softly.assertThat(cacheStats.getCacheMissesRate()).as("cache misses").isEqualTo(0.0);
                softly.assertThat(cacheStats.getNumberOfCacheEvictions()).as("cache evictions").isEqualTo(0);
            });
        } finally {
            logTopicStats(topic);
        }
    }
}
