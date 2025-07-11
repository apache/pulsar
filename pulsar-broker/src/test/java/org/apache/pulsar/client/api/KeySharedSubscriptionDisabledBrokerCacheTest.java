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
import static org.testng.Assert.fail;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.StickyKeyDispatcher;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.tests.KeySharedImplementationType;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class KeySharedSubscriptionDisabledBrokerCacheTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(KeySharedSubscriptionDisabledBrokerCacheTest.class);
    private static final List<String> keys = Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    private static final String SUBSCRIPTION_NAME = "key_shared";
    private final KeySharedImplementationType implementationType;

    // Comment out the next line (Factory annotation) to run tests manually in IntelliJ, one-by-one
    @Factory
    public static Object[] createTestInstances() {
        return KeySharedImplementationType.generateTestInstances(KeySharedSubscriptionDisabledBrokerCacheTest::new);
    }

    public KeySharedSubscriptionDisabledBrokerCacheTest() {
        // set the default implementation type for manual running in IntelliJ
        this(KeySharedImplementationType.PIP379);
    }

    public KeySharedSubscriptionDisabledBrokerCacheTest(KeySharedImplementationType implementationType) {
        this.implementationType = implementationType;
    }

    @DataProvider(name = "currentImplementationType")
    public Object[] currentImplementationType() {
        return new Object[]{ implementationType };
    }

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setSubscriptionKeySharedUseClassicPersistentImplementation(implementationType.classic);
        conf.setSubscriptionSharedUseClassicPersistentImplementation(implementationType.classic);
        this.conf.setUnblockStuckSubscriptionEnabled(false);
        this.conf.setSubscriptionKeySharedUseConsistentHashing(true);
        conf.setManagedLedgerCacheSizeMB(0);
        conf.setManagedLedgerMaxReadsInFlightSizeInMB(0);
        conf.setDispatcherRetryBackoffInitialTimeInMs(0);
        conf.setDispatcherRetryBackoffMaxTimeInMs(0);
        conf.setKeySharedUnblockingIntervalMs(0);
        conf.setBrokerDeduplicationEnabled(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    public void resetDefaultNamespace() throws Exception {
        List<String> list = admin.namespaces().getTopics("public/default");
        for (String topicName : list){
            if (!pulsar.getBrokerService().isSystemTopic(topicName)) {
                admin.topics().delete(topicName, false);
            }
        }
        // reset read ahead limits to defaults
        ServiceConfiguration defaultConf = new ServiceConfiguration();
        conf.setKeySharedLookAheadMsgInReplayThresholdPerSubscription(
                defaultConf.getKeySharedLookAheadMsgInReplayThresholdPerSubscription());
        conf.setKeySharedLookAheadMsgInReplayThresholdPerConsumer(
                defaultConf.getKeySharedLookAheadMsgInReplayThresholdPerConsumer());
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

    @SneakyThrows
    private StickyKeyConsumerSelector getSelector(String topic, String subscription) {
        Topic t = pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        PersistentSubscription sub = (PersistentSubscription) t.getSubscription(subscription);
        StickyKeyDispatcher dispatcher = (StickyKeyDispatcher) sub.getDispatcher();
        return dispatcher.getSelector();
    }

    @Test(dataProvider = "currentImplementationType", invocationCount = 1)
    public void testMessageOrderInSingleConsumerReconnect(KeySharedImplementationType impl) throws Exception {
        String topic = newUniqueName("testMessageOrderInSingleConsumerReconnect");
        int numberOfKeys = 100;
        long pauseTime = 100L;
        // don't fail if duplicates are out-of-order
        // it's possible to change this setting while experimenting
        boolean failOnDuplicatesOutOfOrder = false;

        @Cleanup
        PulsarClient pulsarClient2 = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .build();

        @Cleanup
        PulsarClient pulsarClient3 = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .build();

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
        List<Throwable> exceptionsInHandler = Collections.synchronizedList(new ArrayList<>());

        Map<String, Pair<Position, String>> keyPositions = new HashMap<>();
        MessageListener<Integer> messageHandler = (consumer, msg) -> {
            lastMessageTimestamp.set(System.currentTimeMillis());
            synchronized (this) {
                try {
                    String key = msg.getKey();
                    if (c2MessagesShouldBeUnacked.get() && keysForC2.contains(key)) {
                        unackedMessages.add(Pair.of(consumer, msg));
                        return;
                    }
                    long delayMillis = ThreadLocalRandom.current().nextLong(25, 50);
                    CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS).execute(() ->
                            consumer.acknowledgeAsync(msg));
                    MessageIdAdv msgId = (MessageIdAdv) msg.getMessageId();
                    Position currentPosition = PositionFactory.create(msgId.getLedgerId(), msgId.getEntryId());
                    Pair<Position, String> prevPair = keyPositions.get(key);
                    if (prevPair != null && prevPair.getLeft().compareTo(currentPosition) > 0) {
                        boolean isDuplicate = !remainingMessageValues.contains(msg.getValue());
                        String errorMessage = String.format(
                                        "out of order: key: %s value: %s prev: %s/%s current: %s/%s duplicate: %s",
                                        key, msg.getValue(),
                                        prevPair.getLeft(), prevPair.getRight(),
                                        currentPosition, consumer.getConsumerName(), isDuplicate);
                        log.error(errorMessage);
                        if (!isDuplicate || failOnDuplicatesOutOfOrder) {
                            fail(errorMessage);
                        }
                    }
                    keyPositions.put(key, Pair.of(currentPosition, consumer.getConsumerName()));
                    boolean removed = remainingMessageValues.remove(msg.getValue());
                    if (!removed) {
                        // duplicates are possible during reconnects, this is not an error
                        log.warn("Duplicate message: {} value: {}", msg.getMessageId(), msg.getValue());
                    }
                } catch (Throwable t) {
                    exceptionsInHandler.add(t);
                    if (!(t instanceof AssertionError)) {
                        log.error("Error in message handler", t);
                    }
                }
            }
        };

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
        Consumer<Integer> c2 = pulsarClient2.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c2")
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(messageHandler)
                .subscribe();

        @Cleanup
        Consumer<Integer> c3 = pulsarClient3.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c3")
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(messageHandler)
                .subscribe();

        StickyKeyConsumerSelector selector = getSelector(topic, SUBSCRIPTION_NAME);

        // find keys that will be assigned to c2
        for (int i = 0; i < numberOfKeys; i++) {
            String key = String.valueOf(i);
            byte[] keyBytes = key.getBytes(UTF_8);
            int hash = selector.makeStickyKeyHash(keyBytes);
            if (selector.select(hash).consumerName().equals("c2")) {
                keysForC2.add(key);
            }
        }

        // close c2
        c2.close();
        Thread.sleep(pauseTime);

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
        c2 = pulsarClient2.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c2")
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(messageHandler)
                .startPaused(true)
                .subscribe();

        Thread.sleep(2 * pauseTime);

        // produce messages with c2 keys so that possible race conditions would be more likely to happen
        List<String> keysForC2List=new ArrayList<>(keysForC2);
        for (int i = 1000; i < 1100; i++) {
            String key = keysForC2List.get(random.nextInt(keysForC2List.size()));
            log.info("Producing message with key: {} value: {}", key, i);
            remainingMessageValues.add(i);
            producer.newMessage()
                    .key(key)
                    .value(i)
                    .send();
        }

        Thread.sleep(2 * pauseTime);

        log.info("Acking unacked messages to unblock c2 keys");
        // ack the unacked messages to unblock c2 keys
        c2MessagesShouldBeUnacked.set(false);
        Pair<Consumer<Integer>, Message<Integer>> consumerMessagePair;
        while ((consumerMessagePair = unackedMessages.poll()) != null) {
            messageHandler.received(consumerMessagePair.getLeft(), consumerMessagePair.getRight());
        }

        // resume c2 so that permits are while hashes are unblocked so that possible race conditions would
        // be more likely to happen
        log.info("Resuming c2");
        c2.resume();

        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
            return remainingMessageValues.isEmpty()
                    || System.currentTimeMillis() - lastMessageTimestamp.get() > 50 * pauseTime;
        });

        try {
            assertSoftly(softly -> {
                softly.assertThat(remainingMessageValues).as("remainingMessageValues").isEmpty();
                softly.assertThat(exceptionsInHandler).as("exceptionsInHandler").isEmpty();
            });
        } finally {
            logTopicStats(topic);
        }
    }
}
