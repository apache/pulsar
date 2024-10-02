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
import static org.apache.pulsar.broker.BrokerTestUtil.receiveMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.DrainingHashesTracker;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.MessageRedeliveryController;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class KeySharedSubscriptionTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(KeySharedSubscriptionTest.class);
    private static final List<String> keys = Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

    @DataProvider(name = "batch")
    public Object[] batchProvider() {
        return new Object[] {
                false,
                true
        };
    }

    @DataProvider(name = "partitioned")
    public Object[][] partitionedProvider() {
        return new Object[][] {
                { false },
                { true }
        };
    }

    @DataProvider(name = "data")
    public Object[][] dataProvider() {
        return new Object[][] {
                // Topic-Type and "Batching"
                { "persistent", false  },
                { "persistent", true  },
                { "non-persistent", false },
                { "non-persistent", true },
        };
    }

    @DataProvider(name = "topicDomain")
    public Object[][] topicDomainProvider() {
        return new Object[][] {
                { "persistent" },
                { "non-persistent" }
        };
    }

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.conf.setUnblockStuckSubscriptionEnabled(true);
        super.internalSetup();
        super.producerBaseSetup();
        this.conf.setSubscriptionKeySharedUseConsistentHashing(true);
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

    private static final Random random = new Random(System.nanoTime());
    private static final int NUMBER_OF_KEYS = 300;

    @Test(dataProvider = "data")
    public void testSendAndReceiveWithHashRangeAutoSplitStickyKeyConsumerSelector(String topicType, boolean enableBatch)
            throws PulsarClientException {
        String topic = topicType + "://public/default/key_shared-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic);

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        for (int i = 0; i < 1000; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        receiveAndCheckDistribution(Lists.newArrayList(consumer1, consumer2, consumer3), 1000);
    }

    @Test(dataProvider = "data")
    public void testSendAndReceiveWithBatching(String topicType, boolean enableBatch) throws Exception {
        String topic = topicType + "://public/default/key_shared-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic);

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        CompletableFuture<MessageId> future;

        for (int i = 0; i < 1000; i++) {
            // Send the same key twice so that we'll have a batch message
            String key = String.valueOf(random.nextInt(NUMBER_OF_KEYS));
            future = producer.newMessage()
                    .key(key)
                    .value(i)
                    .sendAsync();

            // If not batching, need to wait for message to be persisted
            if (!enableBatch) {
                future.get();
            }

            future = producer.newMessage()
                    .key(key)
                    .value(i)
                    .sendAsync();
            if (!enableBatch) {
                future.get();
            }
        }
        // If batching, flush buffered messages
        producer.flush();

        receiveAndCheckDistribution(Lists.newArrayList(consumer1, consumer2, consumer3), 1000 * 2);
    }

    @Test(dataProvider = "batch")
    public void testSendAndReceiveWithHashRangeExclusiveStickyKeyConsumerSelector(boolean enableBatch) throws PulsarClientException {
        String topic = "persistent://public/default/key_shared_exclusive-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic, KeySharedPolicy.stickyHashRange()
                .ranges(Range.of(0, 20000)));

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic, KeySharedPolicy.stickyHashRange()
                .ranges(Range.of(20001, 40000)));

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic, KeySharedPolicy.stickyHashRange()
                .ranges(Range.of(40001, KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE-1)));

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        int consumer1ExpectMessages = 0;
        int consumer2ExpectMessages = 0;
        int consumer3ExpectMessages = 0;

        for (int i = 0; i < 10; i++) {
            for (String key : keys) {
                int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes())
                        % KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE;
                if (slot <= 20000) {
                    consumer1ExpectMessages++;
                } else if (slot <= 40000) {
                    consumer2ExpectMessages++;
                } else {
                    consumer3ExpectMessages++;
                }
                producer.newMessage()
                        .key(key)
                        .value(i)
                        .send();
            }
        }

        List<KeyValue<Consumer<Integer>, Integer>> checkList = new ArrayList<>();
        checkList.add(new KeyValue<>(consumer1, consumer1ExpectMessages));
        checkList.add(new KeyValue<>(consumer2, consumer2ExpectMessages));
        checkList.add(new KeyValue<>(consumer3, consumer3ExpectMessages));

        receiveAndCheck(checkList);

    }

    @Test(dataProvider = "data")
    public void testConsumerCrashSendAndReceiveWithHashRangeAutoSplitStickyKeyConsumerSelector(
        String topicType,
        boolean enableBatch
    ) throws PulsarClientException, InterruptedException {
        String topic = topicType + "://public/default/key_shared_consumer_crash-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic);

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        for (int i = 0; i < 1000; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        receiveAndCheckDistribution(Lists.newArrayList(consumer1, consumer2, consumer3), 1000);

        // wait for consumer grouping acking send.
        Thread.sleep(1000);

        consumer1.close();
        consumer2.close();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        receiveAndCheckDistribution(Lists.newArrayList(consumer3), 10);
    }

    @Test(dataProvider = "data")
    public void testNoKeySendAndReceiveWithHashRangeAutoSplitStickyKeyConsumerSelector(
        String topicType,
        boolean enableBatch
    ) throws PulsarClientException {
        String topic = topicType + "://public/default/key_shared_no_key-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic);

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        for (int i = 0; i < 100; i++) {
            producer.newMessage()
                    .value(i)
                    .send();
        }

        receiveAndCheckDistribution(Lists.newArrayList(consumer1, consumer2, consumer3), 100);
    }

    @Test(dataProvider = "batch")
    public void testNoKeySendAndReceiveWithHashRangeExclusiveStickyKeyConsumerSelector(boolean enableBatch)
            throws PulsarClientException {
        String topic = "persistent://public/default/key_shared_no_key_exclusive-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic, KeySharedPolicy.stickyHashRange()
                .ranges(Range.of(0, 20000)));

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic, KeySharedPolicy.stickyHashRange()
                .ranges(Range.of(20001, 40000)));

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic, KeySharedPolicy.stickyHashRange()
                .ranges(Range.of(40001, KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE-1)));

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        int consumer1ExpectMessages = 0;
        int consumer2ExpectMessages = 0;
        int consumer3ExpectMessages = 0;

        for (int i = 0; i < 100; i++) {
            producer.newMessage()
                    .value(i)
                    .send();

            String fallbackKey = producer.getProducerName() + "-" + producer.getLastSequenceId();
            int slot = Murmur3_32Hash.getInstance().makeHash(fallbackKey.getBytes())
                    % KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE;
            if (slot <= 20000) {
                consumer1ExpectMessages++;
            } else if (slot <= 40000) {
                consumer2ExpectMessages++;
            } else {
                consumer3ExpectMessages++;
            }
        }

        List<KeyValue<Consumer<Integer>, Integer>> checkList = new ArrayList<>();
        checkList.add(new KeyValue<>(consumer1, consumer1ExpectMessages));
        checkList.add(new KeyValue<>(consumer2, consumer2ExpectMessages));
        checkList.add(new KeyValue<>(consumer3, consumer3ExpectMessages));

        receiveAndCheck(checkList);
    }

    @Test(dataProvider = "batch")
    public void testOrderingKeyWithHashRangeAutoSplitStickyKeyConsumerSelector(boolean enableBatch)
            throws PulsarClientException {
        String topic = "persistent://public/default/key_shared_ordering_key-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic);

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        for (int i = 0; i < 1000; i++) {
            producer.newMessage()
                    .key("any key")
                    .orderingKey(String.valueOf(random.nextInt(NUMBER_OF_KEYS)).getBytes())
                    .value(i)
                    .send();
        }

        receiveAndCheckDistribution(Lists.newArrayList(consumer1, consumer2, consumer3), 1000);
    }

    @Test(dataProvider = "batch")
    public void testOrderingKeyWithHashRangeExclusiveStickyKeyConsumerSelector(boolean enableBatch)
            throws PulsarClientException {
        String topic = "persistent://public/default/key_shared_exclusive_ordering_key-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic, KeySharedPolicy.stickyHashRange()
                .ranges(Range.of(0, 20000)));

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic, KeySharedPolicy.stickyHashRange()
                .ranges(Range.of(20001, 40000)));

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic, KeySharedPolicy.stickyHashRange()
                .ranges(Range.of(40001, KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE-1)));

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        int consumer1ExpectMessages = 0;
        int consumer2ExpectMessages = 0;
        int consumer3ExpectMessages = 0;

        for (int i = 0; i < 10; i++) {
            for (String key : keys) {
                int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes())
                        % KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE;
                if (slot <= 20000) {
                    consumer1ExpectMessages++;
                } else if (slot <= 40000) {
                    consumer2ExpectMessages++;
                } else {
                    consumer3ExpectMessages++;
                }
                producer.newMessage()
                        .key("any key")
                        .orderingKey(key.getBytes())
                        .value(i)
                        .send();
            }
        }

        List<KeyValue<Consumer<Integer>, Integer>> checkList = new ArrayList<>();
        checkList.add(new KeyValue<>(consumer1, consumer1ExpectMessages));
        checkList.add(new KeyValue<>(consumer2, consumer2ExpectMessages));
        checkList.add(new KeyValue<>(consumer3, consumer3ExpectMessages));

        receiveAndCheck(checkList);
    }

    @Test(expectedExceptions = PulsarClientException.NotAllowedException.class)
    public void testDisableKeySharedSubscription() throws PulsarClientException {
        this.conf.getSubscriptionTypesEnabled().remove("Key_Shared");
        String topic = "persistent://public/default/key_shared_disabled";
        try {
            @Cleanup
            Consumer c = pulsarClient.newConsumer()
                    .topic(topic)
                    .subscriptionName("key_shared")
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .ackTimeout(10, TimeUnit.SECONDS)
                    .subscribe();
        } finally {
            // reset subscription types.
            this.conf.getSubscriptionTypesEnabled().add("Key_Shared");
        }
    }

    @Test
    public void testCannotUseAcknowledgeCumulative() throws PulsarClientException {
        String topic = "persistent://public/default/key_shared_ack_cumulative-" + UUID.randomUUID();

        @Cleanup
        Producer<Integer> producer = createProducer(topic, false);

        @Cleanup
        Consumer<Integer> consumer = createConsumer(topic);

        for (int i = 0; i < 10; i++) {
            producer.send(i);
        }

        for (int i = 0; i < 10; i++) {
            Message<Integer> message = consumer.receive();
            if (i == 9) {
                try {
                    consumer.acknowledgeCumulative(message);
                    fail("should have failed");
                } catch (PulsarClientException.InvalidConfigurationException ignore) {}
            }
        }
    }

    @Test(dataProvider = "batch")
    public void testMakingProgressWithSlowerConsumer(boolean enableBatch) throws Exception {
        String topic = "testMakingProgressWithSlowerConsumer-" + UUID.randomUUID();

        String slowKey = "slowKey";

        List<PulsarClient> clients = new ArrayList<>();
        List<Consumer> consumers = new ArrayList<>();
        try {
            AtomicInteger receivedMessages = new AtomicInteger();

            for (int i = 0; i < 10; i++) {
                PulsarClient client = PulsarClient.builder()
                        .serviceUrl(brokerUrl.toString())
                        .build();
                clients.add(client);

                Consumer c = client.newConsumer(Schema.INT32)
                        .topic(topic)
                        .subscriptionName("key_shared")
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .receiverQueueSize(1)
                        .messageListener((consumer, msg) -> {
                            try {
                                if (slowKey.equals(msg.getKey())) {
                                    // Block the thread to simulate a slow consumer
                                    Thread.sleep(10000);
                                }

                                receivedMessages.incrementAndGet();
                                consumer.acknowledge(msg);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        })
                        .subscribe();
                consumers.add(c);
            }

            @Cleanup
            Producer<Integer> producer = createProducer(topic, enableBatch);

            // First send the "slow key" so that 1 consumer will get stuck
            producer.newMessage()
                    .key(slowKey)
                    .value(-1)
                    .send();

            int N = 1000;

            // Then send all the other keys
            for (int i = 0; i < N; i++) {
                producer.newMessage()
                        .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                        .value(i)
                        .send();
            }

            // Since only 1 out of 10 consumers is stuck, we should be able to receive ~90% messages,
            // plus or minus for some skew in the key distribution.
            Awaitility.await().untilAsserted(() -> {
                assertEquals((double) receivedMessages.get(), N * 0.9, N * 0.3);
            });

            for (Consumer c : consumers) {
                c.close();
            }
        } finally {
            for (PulsarClient c : clients) {
                c.close();
            }
        }
    }

    @Test
    public void testOrderingWhenAddingConsumers() throws Exception {
        String topic = "testOrderingWhenAddingConsumers-" + UUID.randomUUID();

        @Cleanup
        Producer<Integer> producer = createProducer(topic, false);

        @Cleanup
        Consumer<Integer> c1 = createConsumer(topic);

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        // All the already published messages will be pre-fetched by C1.

        // Adding a new consumer.
        @Cleanup
        Consumer<Integer> c2 = createConsumer(topic);

        for (int i = 10; i < 20; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        // Closing c1, would trigger all messages to go to c2
        c1.close();

        for (int i = 0; i < 20; i++) {
            Message<Integer> msg = c2.receive();
            assertEquals(msg.getValue().intValue(), i);

            c2.acknowledge(msg);
        }
    }

    @Test
    public void testReadAheadWithConfiguredLookAheadLimit() throws Exception {
        String topic = "testReadAheadWithConfiguredLookAheadLimit-" + UUID.randomUUID();

        // Set the look ahead limit to 50 for subscriptions
        conf.setKeySharedLookAheadMsgInReplayThresholdPerSubscription(50);

        @Cleanup
        Producer<Integer> producer = createProducer(topic, false);

        @Cleanup
        Consumer<Integer> c1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("key_shared")
                .subscriptionType(SubscriptionType.Key_Shared)
                .receiverQueueSize(10)
                .subscribe();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        // All the already published messages will be pre-fetched by C1.

        // Adding a new consumer.
        @Cleanup
        Consumer<Integer> c2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("key_shared")
                .subscriptionType(SubscriptionType.Key_Shared)
                .receiverQueueSize(10)
                .subscribe();

        // C2 will not be able to receive any messages until C1 is done processing whatever he got prefetched

        for (int i = 10; i < 1000; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .sendAsync();
        }

        producer.flush();
        Thread.sleep(1000);

        Topic t = pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        PersistentSubscription sub = (PersistentSubscription) t.getSubscription("key_shared");

        // We need to ensure that dispatcher does not keep to look ahead in the topic,
        Position readPosition = sub.getCursor().getReadPosition();
        long entryId = readPosition.getEntryId();
        assertTrue(entryId < 100);
    }

    @Test
    public void testRemoveFirstConsumer() throws Exception {
        String topic = "testReadAheadWhenAddingConsumers-" + UUID.randomUUID();
        int numberOfKeys = 10;

        @Cleanup
        Producer<Integer> producer = createProducer(topic, false);

        @Cleanup
        Consumer<Integer> c1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("key_shared")
                .subscriptionType(SubscriptionType.Key_Shared)
                .receiverQueueSize(10)
                .consumerName("c1")
                .subscribe();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .key(String.valueOf(i % numberOfKeys))
                    .value(i)
                    .send();
        }

        // All the already published messages will be pre-fetched by C1.
        Awaitility.await().untilAsserted(() ->
                assertEquals(((ConsumerImpl<Integer>) c1).getTotalIncomingMessages(), 10));

        // Adding a new consumer.
        @Cleanup
        Consumer<Integer> c2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("key_shared")
                .subscriptionType(SubscriptionType.Key_Shared)
                .receiverQueueSize(10)
                .consumerName("c2")
                .subscribe();

        for (int i = 10; i < 20; i++) {
            producer.newMessage()
                    .key(String.valueOf(i % numberOfKeys))
                    .value(i)
                    .send();
        }

        // C2 will not be able to receive any messages until C1 is done processing whatever he got prefetched
        assertNull(c2.receive(1, TimeUnit.SECONDS));

        c1.close();

        // Now C2 will get all messages
        for (int i = 0; i < 20; i++) {
            Message<Integer> msg = c2.receive();
            assertEquals(msg.getValue().intValue(), i);
            c2.acknowledge(msg);
        }
    }

    @Test
    public void testHashRangeConflict() throws PulsarClientException {
        final String topic = "persistent://public/default/testHashRangeConflict-" + UUID.randomUUID().toString();
        final String sub = "test";

        Consumer<String> consumer1 = createFixedHashRangesConsumer(topic, sub, Range.of(0,99), Range.of(400, 65535));
        Assert.assertTrue(consumer1.isConnected());

        Consumer<String> consumer2 = createFixedHashRangesConsumer(topic, sub, Range.of(100,399));
        Assert.assertTrue(consumer2.isConnected());

        PersistentStickyKeyDispatcherMultipleConsumers dispatcher = (PersistentStickyKeyDispatcherMultipleConsumers) pulsar
                .getBrokerService().getTopicReference(topic).get().getSubscription(sub).getDispatcher();
        Assert.assertEquals(dispatcher.getConsumers().size(), 2);

        try {
            createFixedHashRangesConsumer(topic, sub, Range.of(0, 65535));
            Assert.fail("Should failed with conflict range.");
        } catch (PulsarClientException.ConsumerAssignException ignore) {
        }

        try {
            createFixedHashRangesConsumer(topic, sub, Range.of(1,1));
            Assert.fail("Should failed with conflict range.");
        } catch (PulsarClientException.ConsumerAssignException ignore) {
        }

        Assert.assertEquals(dispatcher.getConsumers().size(), 2);
        consumer1.close();
        Assert.assertEquals(dispatcher.getConsumers().size(), 1);

        try {
            createFixedHashRangesConsumer(topic, sub, Range.of(0, 65535));
            Assert.fail("Should failed with conflict range.");
        } catch (PulsarClientException.ConsumerAssignException ignore) {
        }

        try {
            createFixedHashRangesConsumer(topic, sub, Range.of(50,100));
            Assert.fail("Should failed with conflict range.");
        } catch (PulsarClientException.ConsumerAssignException ignore) {
        }

        try {
            createFixedHashRangesConsumer(topic, sub, Range.of(399,500));
            Assert.fail("Should failed with conflict range.");
        } catch (PulsarClientException.ConsumerAssignException ignore) {
        }

        Consumer<String> consumer3 = createFixedHashRangesConsumer(topic, sub, Range.of(400,600));
        Assert.assertTrue(consumer3.isConnected());

        Consumer<String> consumer4 = createFixedHashRangesConsumer(topic, sub, Range.of(50,99));
        Assert.assertTrue(consumer4.isConnected());

        Assert.assertEquals(dispatcher.getConsumers().size(), 3);
        consumer2.close();
        consumer3.close();
        consumer4.close();
        Assert.assertFalse(dispatcher.isConsumerConnected());
    }

    @Test
    public void testWithMessageCompression() throws Exception {
        final String topic = "testWithMessageCompression" + UUID.randomUUID().toString();
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .compressionType(CompressionType.LZ4)
                .create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer.send(("Hello Pulsar > " + i).getBytes());
        }
        List<Message<byte[]>> receives = new ArrayList<>();
        for (int i = 0; i < messages; i++) {
            Message<byte[]> received = consumer.receive();
            receives.add(received);
            consumer.acknowledge(received);
        }
        Assert.assertEquals(receives.size(), messages);
        producer.close();
        consumer.close();
    }

    @Test
    public void testAttachKeyToMessageMetadata() throws PulsarClientException {
        String topic = "persistent://public/default/key_shared-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic);

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .create();

        for (int i = 0; i < 1000; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        receiveAndCheckDistribution(Lists.newArrayList(consumer1, consumer2, consumer3), 1000);
    }

    @Test
    public void testContinueDispatchMessagesWhenMessageTTL() throws Exception {
        int defaultTTLSec = 3;
        int totalMessages = 1000;
        int numberOfKeys = 50;
        this.conf.setTtlDurationDefaultInSeconds(defaultTTLSec);
        final String topic = "persistent://public/default/key_shared-" + UUID.randomUUID();
        final String subName = "my-sub";

        @Cleanup
        Consumer<Integer> consumer1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("consumer1")
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        Topic t = pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        PersistentSubscription sub = (PersistentSubscription) t.getSubscription(subName);
        // get the dispatcher reference
        PersistentStickyKeyDispatcherMultipleConsumers dispatcher =
                (PersistentStickyKeyDispatcherMultipleConsumers) sub.getDispatcher();
        StickyKeyConsumerSelector selector = dispatcher.getSelector();

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .create();

        for (int i = 0; i < totalMessages; i++) {
            producer.newMessage()
                    .key(String.valueOf(i % numberOfKeys))
                    .value(i)
                    .send();
        }

        Set<Integer> blockedHashes = new HashSet<>();
        // pull up to numberOfKeys messages and don't ack them
        for (int i = 0; i < numberOfKeys + 1; i++) {
            Message<Integer> received = consumer1.receive();
            int stickyKeyHash = selector.makeStickyKeyHash(received.getKeyBytes());
            log.info("Received message {} with sticky key hash: {}", received.getMessageId(), stickyKeyHash);
            blockedHashes.add(stickyKeyHash);
        }

        // The consumer1 and consumer2 should be stuck since all hashes are blocked

        @Cleanup
        Consumer<Integer> consumer2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("consumer2")
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        Message<Integer> received = null;
        try {
            received = consumer2.receive(1, TimeUnit.SECONDS);
        } catch (PulsarClientException ignore) {
        }
        if (received != null) {
            int stickyKeyHash = selector.makeStickyKeyHash(received.getKeyBytes());
            DrainingHashesTracker.DrainingHashEntry entry =
                    dispatcher.getDrainingHashesTracker().getEntry(stickyKeyHash);
            Assertions.fail("Received message %s with sticky key hash that should have been blocked: %d. entry=%s, "
                            + "included in blockedHashes=%s",
                    received.getMessageId(), stickyKeyHash, entry, blockedHashes.contains(stickyKeyHash));
        }

        @Cleanup
        Consumer<Integer> consumer3 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("consumer3")
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        try {
            received = consumer3.receive(1, TimeUnit.SECONDS);
        } catch (PulsarClientException ignore) {
        }
        if (received != null) {
            int stickyKeyHash = selector.makeStickyKeyHash(received.getKeyBytes());
            DrainingHashesTracker.DrainingHashEntry entry =
                    dispatcher.getDrainingHashesTracker().getEntry(stickyKeyHash);
            Assertions.fail("Received message %s with sticky key hash that should have been blocked: %d. entry=%s, "
                            + "included in blockedHashes=%s",
                    received.getMessageId(), stickyKeyHash, entry, blockedHashes.contains(stickyKeyHash));
        }

        Optional<Topic> topicRef = pulsar.getBrokerService().getTopic(topic, false).get();
        assertTrue(topicRef.isPresent());
        Thread.sleep((defaultTTLSec - 1) * 1000);
        topicRef.get().checkMessageExpiry();

        // The mark delete position is move forward, so the consumers should receive new messages now.
        for (int i = 0; i < totalMessages; i++) {
            producer.newMessage()
                    .key(String.valueOf(i % numberOfKeys))
                    .value(i)
                    .send();
        }

        Map<String, AtomicInteger> receivedMessagesCountByConsumer = new ConcurrentHashMap<>();
        receiveMessages((consumer, message) -> {
            consumer.acknowledgeAsync(message);
            receivedMessagesCountByConsumer.computeIfAbsent(consumer.getConsumerName(), id -> new AtomicInteger(0))
                    .incrementAndGet();
            return true;
        }, Duration.ofSeconds(2), consumer1, consumer2, consumer3);

        assertThat(receivedMessagesCountByConsumer.values().stream().mapToInt(AtomicInteger::intValue)
                .sum()).isGreaterThanOrEqualTo(totalMessages);
        assertThat(receivedMessagesCountByConsumer.values()).allSatisfy(
                count -> assertThat(count.get()).isGreaterThan(0));
    }

    @Test(dataProvider = "partitioned")
    public void testOrderingWithConsumerListener(boolean partitioned) throws Exception {
        final String topic = "persistent://public/default/key_shared-" + UUID.randomUUID();
        if (partitioned) {
            admin.topics().createPartitionedTopic(topic, 3);
        }
        final String subName = "my-sub";
        final int messages = 1000;
        List<Message<Integer>> received = Collections.synchronizedList(new ArrayList<>(1000));
        Random random = new Random();
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .listenerThreads(8)
                .build();

        Consumer<Integer> consumer = client.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener((MessageListener<Integer>) (consumer1, msg) -> {
                    try {
                        Thread.sleep(random.nextInt(5));
                        received.add(msg);
                    } catch (InterruptedException ignore) {
                    }
                })
                .subscribe();


        Producer<Integer> producer = client.newProducer(Schema.INT32)
                .topic(topic)
                .create();

        String[] keys = new String[]{"key-1", "key-2", "key-3"};
        for (int i = 0; i < messages; i++) {
            producer.newMessage().key(keys[i % 3]).value(i).send();
        }

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(received.size(), messages));

        Map<String, Integer> maxValueOfKeys = new HashMap<>();
        for (Message<Integer> msg : received) {
            String key = msg.getKey();
            Integer value = msg.getValue();
            if (maxValueOfKeys.containsKey(key)) {
                Assert.assertTrue(value > maxValueOfKeys.get(key));
            }
            maxValueOfKeys.put(key, value);
            consumer.acknowledge(msg);
        }

        producer.close();
        consumer.close();
    }

    @Test
    public void testKeySharedConsumerWithEncrypted() throws Exception {
        final String topic = "persistent://public/default/key_shared-" + UUID.randomUUID();
        final int totalMessages = 100;

        @Cleanup
        Consumer<Integer> consumer1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Key_Shared)
                .cryptoKeyReader(new EncKeyReader())
                .subscribe();

        @Cleanup
        Consumer<Integer> consumer2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Key_Shared)
                .cryptoKeyReader(new EncKeyReader())
                .subscribe();

        List<Consumer<Integer>> consumers = Lists.newArrayList(consumer1, consumer2);

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .cryptoKeyReader(new EncKeyReader())
                .create();

        for (int i = 0; i < totalMessages; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        List<Message<Integer>> receives = new ArrayList<>(totalMessages);
        int[] consumerReceivesCount = new int[] {0, 0};

        for (int i = 0; i < consumers.size(); i++) {
            while (true) {
                Message<Integer> received = consumers.get(i).receive(3, TimeUnit.SECONDS);
                if (received != null) {
                    receives.add(received);
                    int current = consumerReceivesCount[i];
                    consumerReceivesCount[i] = current + 1;
                } else {
                    break;
                }
            }
        }

        Assert.assertEquals(receives.size(), totalMessages);
        Assert.assertEquals(consumerReceivesCount[0] + consumerReceivesCount[1], totalMessages);
        Assert.assertTrue(consumerReceivesCount[0] > 0);
        Assert.assertTrue(consumerReceivesCount[1] > 0);

        Map<String, Integer> maxValueOfKey = new HashMap<>();
        receives.forEach(msg -> {
            if (maxValueOfKey.containsKey(msg.getKey())) {
                Assert.assertTrue(msg.getValue() > maxValueOfKey.get(msg.getKey()));
            }
            maxValueOfKey.put(msg.getKey(), msg.getValue());
        });
    }

    @Test(dataProvider = "topicDomain")
    public void testSelectorChangedAfterAllConsumerDisconnected(String topicDomain) throws PulsarClientException,
            ExecutionException, InterruptedException {
        final String topicName = TopicName.get(topicDomain, "public", "default",
                "testSelectorChangedAfterAllConsumerDisconnected" + UUID.randomUUID()).toString();

        final String subName = "my-sub";

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .consumerName("first-consumer")
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
                .cryptoKeyReader(new EncKeyReader())
                .subscribe();

        CompletableFuture<Optional<Topic>> future = pulsar.getBrokerService().getTopicIfExists(topicName);
        assertTrue(future.isDone());
        assertTrue(future.get().isPresent());
        Topic topic = future.get().get();
        KeySharedMode keySharedMode = getKeySharedModeOfSubscription(topic, subName);
        assertNotNull(keySharedMode);
        assertEquals(keySharedMode, KeySharedMode.AUTO_SPLIT);

        consumer1.close();

        consumer1 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .consumerName("second-consumer")
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(Range.of(0, 65535)))
                .cryptoKeyReader(new EncKeyReader())
                .subscribe();

        future = pulsar.getBrokerService().getTopicIfExists(topicName);
        assertTrue(future.isDone());
        assertTrue(future.get().isPresent());
        topic = future.get().get();
        keySharedMode = getKeySharedModeOfSubscription(topic, subName);
        assertNotNull(keySharedMode);
        assertEquals(keySharedMode, KeySharedMode.STICKY);
        consumer1.close();
    }

    @Test
    public void testAllowOutOfOrderDeliveryChangedAfterAllConsumerDisconnected() throws Exception {
        final String topicName = "persistent://public/default/change-allow-ooo-delivery-" + UUID.randomUUID();
        final String subName = "my-sub";

        final Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.autoSplitHashRange().setAllowOutOfOrderDelivery(true))
                .subscribe();

        @Cleanup
        final Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .create();
        producer.send("message".getBytes());
        Awaitility.await().untilAsserted(() -> assertNotNull(consumer1.receive(100, TimeUnit.MILLISECONDS)));

        CompletableFuture<Optional<Topic>> future = pulsar.getBrokerService().getTopicIfExists(topicName);
        assertTrue(future.isDone());
        assertTrue(future.get().isPresent());
        Topic topic = future.get().get();
        PersistentStickyKeyDispatcherMultipleConsumers dispatcher =
                (PersistentStickyKeyDispatcherMultipleConsumers) topic.getSubscription(subName).getDispatcher();
        assertTrue(dispatcher.isAllowOutOfOrderDelivery());
        consumer1.close();

        final Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.autoSplitHashRange().setAllowOutOfOrderDelivery(false))
                .subscribe();
        producer.send("message".getBytes());
        Awaitility.await().untilAsserted(() -> assertNotNull(consumer2.receive(100, TimeUnit.MILLISECONDS)));

        future = pulsar.getBrokerService().getTopicIfExists(topicName);
        assertTrue(future.isDone());
        assertTrue(future.get().isPresent());
        topic = future.get().get();
        dispatcher = (PersistentStickyKeyDispatcherMultipleConsumers) topic.getSubscription(subName).getDispatcher();
        assertFalse(dispatcher.isAllowOutOfOrderDelivery());
        consumer2.close();
    }

    @Test(timeOut = 30_000)
    public void testCheckConsumersWithSameName() throws Exception {
        final String topicName = "persistent://public/default/same-name-" + UUID.randomUUID();
        final String subName = "my-sub";
        final String consumerName = "name";

        ConsumerBuilder<String> cb = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .consumerName(consumerName)
                .subscriptionType(SubscriptionType.Key_Shared);

        // Create 3 consumers with same name
        @Cleanup
        Consumer<String> c1 = cb.subscribe();

        @Cleanup
        Consumer<String> c2 = cb.subscribe();
        @Cleanup
        Consumer<String> c3 = cb.subscribe();

        @Cleanup
        Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();
        for (int i = 0; i < 100; i++) {
            p.newMessage()
                    .key(Integer.toString(i))
                    .value("msg-" + i)
                    .send();
        }

        // C1 receives some messages and won't ack
        for (int i = 0; i < 5; i++) {
            c1.receive();
        }

        // Close C1, now all messages should go to c2 & c3
        c1.close();

        CountDownLatch l = new CountDownLatch(100);

        @Cleanup("shutdownNow")
        ExecutorService e = Executors.newCachedThreadPool();
        e.submit(() -> {
            while (l.getCount() > 0) {
                try {
                    Message<String> msg = c2.receive(1, TimeUnit.SECONDS);
                    c2.acknowledge(msg);
                    l.countDown();
                } catch (PulsarClientException ex) {
                    ex.printStackTrace();
                }
            }
        });

        e.submit(() -> {
            while (l.getCount() > 0) {
                try {
                    Message<String> msg = c3.receive(1, TimeUnit.SECONDS);
                    c3.acknowledge(msg);
                    l.countDown();
                } catch (PulsarClientException ex) {
                    ex.printStackTrace();
                }
            }
        });

        l.await();
    }

    @DataProvider(name = "preSend")
    private Object[][] preSendProvider() {
        return new Object[][] { { false }, { true } };
    }

    @Test(timeOut = 30_000, dataProvider = "preSend", enabled = false)
    public void testCheckBetweenSkippingAndRecentlyJoinedConsumers(boolean preSend) throws Exception {
        conf.setSubscriptionKeySharedUseConsistentHashing(true);

        final String topicName = "persistent://public/default/recently-joined-consumers-" + UUID.randomUUID();
        final String subName = "my-sub";

        @Cleanup
        final Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();
        if (preSend) {
            // verify that the test succeeds even if the topic has a message
            p.send("msg");
        }

        final Supplier<ConsumerBuilder<String>> cb = () -> pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.autoSplitHashRange()
                        .setAllowOutOfOrderDelivery(false));

        // create 2 consumers
        final String c1ConsumerName = "c1";
        @Cleanup
        final Consumer<String> c1 = cb.get().consumerName(c1ConsumerName).receiverQueueSize(1).subscribe();
        @Cleanup
        final Consumer<String> c2 = cb.get().consumerName("c2").receiverQueueSize(1000).subscribe();

        final PersistentStickyKeyDispatcherMultipleConsumers dispatcher =
                (PersistentStickyKeyDispatcherMultipleConsumers) pulsar.getBrokerService().getTopicIfExists(topicName).get().get().getSubscription(subName).getDispatcher();
        final Field recentlyJoinedConsumersField = PersistentStickyKeyDispatcherMultipleConsumers.class.getDeclaredField("recentlyJoinedConsumers");
        recentlyJoinedConsumersField.setAccessible(true);
        final LinkedHashMap<org.apache.pulsar.broker.service.Consumer, Position> recentlyJoinedConsumers = (LinkedHashMap<org.apache.pulsar.broker.service.Consumer, Position>) recentlyJoinedConsumersField.get(dispatcher);
        final String keyA = "key-a";
        final int hashA = Murmur3_32Hash.getInstance().makeHash(keyA.getBytes());
        final Map<Integer, String> hashConsumerMap = new HashMap<>();
        hashConsumerMap.put(hashA, c1.getConsumerName());

        // enforce the selector will return c1 if keyA
        final Field selectorField = PersistentStickyKeyDispatcherMultipleConsumers.class.getDeclaredField("selector");
        selectorField.setAccessible(true);
        final StickyKeyConsumerSelector selector = spy((StickyKeyConsumerSelector) selectorField.get(dispatcher));
        selectorField.set(dispatcher, selector);
        doAnswer((invocationOnMock -> {
            final int hash = invocationOnMock.getArgument(0);
            final String consumerName = hashConsumerMap.getOrDefault(hash, c2.getConsumerName());
            return dispatcher.getConsumers().stream().filter(consumer -> consumer.consumerName().equals(consumerName)).findFirst().get();
        })).when(selector).select(anyInt());

        // send and receive
        Awaitility.await().untilAsserted(() -> assertEquals(admin.topics().getStats(topicName).getSubscriptions().get(subName).getConsumers().stream().filter(c -> c.getConsumerName().equals(c1ConsumerName)).findFirst().get().getAvailablePermits(), 1));
        final MessageIdImpl msg0Id = (MessageIdImpl) p.newMessage().key(keyA).value("msg-0").send();
        Awaitility.await().untilAsserted(() -> assertEquals(admin.topics().getStats(topicName).getSubscriptions().get(subName).getConsumers().stream().filter(c -> c.getConsumerName().equals(c1ConsumerName)).findFirst().get().getAvailablePermits(), 0));

        final MessageIdImpl msg1Id = (MessageIdImpl) p.newMessage().key(keyA).value("msg-1").send();
        Awaitility.await().untilAsserted(() -> assertEquals(admin.topics().getStats(topicName).getSubscriptions().get(subName).getMsgBacklog(), 2));

        final Field redeliveryMessagesField = PersistentDispatcherMultipleConsumers.class
                .getDeclaredField("redeliveryMessages");
        redeliveryMessagesField.setAccessible(true);
        final MessageRedeliveryController redeliveryMessages = (MessageRedeliveryController) redeliveryMessagesField.get(dispatcher);

        final Set<Position> replayMsgSet = redeliveryMessages.getMessagesToReplayNow(3, item -> true);
        assertEquals(replayMsgSet.size(), 1);
        final Position replayMsg = replayMsgSet.stream().findAny().get();
        assertEquals(replayMsg, PositionFactory.create(msg1Id.getLedgerId(), msg1Id.getEntryId()));

        // add c3
        final String c3ConsumerName = "c3";
        hashConsumerMap.put(hashA, c3ConsumerName);
        @Cleanup
        final Consumer<String> c3 = cb.get().consumerName(c3ConsumerName).subscribe();
        final List<Message<String>> c3Msgs = new ArrayList<>();
        final org.apache.pulsar.broker.service.Consumer c3Broker = dispatcher.getConsumers().stream().filter(consumer -> consumer.consumerName().equals(c3ConsumerName)).findFirst().get();
        assertEquals(recentlyJoinedConsumers.get(c3Broker), PositionFactory.create(msg0Id.getLedgerId(), msg0Id.getEntryId()));

        // None of messages are sent to c3.
        Message<String> c3Msg = c3.receive(100, TimeUnit.MILLISECONDS);
        assertNull(c3Msg);

        // Disconnect c1
        c1.close();

        c3Msg = c3.receive(100, TimeUnit.MILLISECONDS);
        assertNotNull(c3Msg);
        c3Msgs.add(c3Msg);
        // The mark delete position will move forward. Then remove c3 from recentlyJoinedConsumers.
        c3.acknowledge(c3Msg);
        Awaitility.await().untilAsserted(() -> assertNull(recentlyJoinedConsumers.get(c3Broker)));
        c3Msg = c3.receive(100, TimeUnit.MILLISECONDS);
        assertNotNull(c3Msg);
        c3Msgs.add(c3Msg);
        c3.acknowledge(c3Msg);

        // check ordering
        assertTrue(c3Msgs.get(0).getMessageId().compareTo(c3Msgs.get(1).getMessageId()) < 0);
    }

    private KeySharedMode getKeySharedModeOfSubscription(Topic topic, String subscription) {
        if (TopicName.get(topic.getName()).getDomain().equals(TopicDomain.persistent)) {
            return ((PersistentStickyKeyDispatcherMultipleConsumers) topic.getSubscription(subscription)
                    .getDispatcher()).getKeySharedMode();
        } else if (TopicName.get(topic.getName()).getDomain().equals(TopicDomain.non_persistent)) {
            return ((NonPersistentStickyKeyDispatcherMultipleConsumers) topic.getSubscription(subscription)
                    .getDispatcher()).getKeySharedMode();
        }
        return null;
    }

    private Consumer<String> createFixedHashRangesConsumer(String topic, String subscription, Range... ranges) throws PulsarClientException {
        return pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionName(subscription)
                .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(ranges))
                .subscribe();
    }

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

    private Consumer<Integer> createConsumer(String topic) throws PulsarClientException {
        return createConsumer(topic, null);
    }

    private Consumer<Integer> createConsumer(String topic, KeySharedPolicy keySharedPolicy)
            throws PulsarClientException {
        ConsumerBuilder<Integer> builder = pulsarClient.newConsumer(Schema.INT32);
        builder.topic(topic)
                .subscriptionName("key_shared")
                .subscriptionType(SubscriptionType.Key_Shared)
                .ackTimeout(3, TimeUnit.SECONDS);
        if (keySharedPolicy != null) {
            builder.keySharedPolicy(keySharedPolicy);
        }
        return builder.subscribe();
    }

    private void receive(List<Consumer<?>> consumers) throws PulsarClientException {
        // Add a key so that we know this key was already assigned to one consumer
        Map<String, Consumer<?>> keyToConsumer = new HashMap<>();

        for (Consumer<?> c : consumers) {
            while (true) {
                Message<?> msg = c.receive(100, TimeUnit.MILLISECONDS);
                if (msg == null) {
                    // Go to next consumer
                    break;
                }

                c.acknowledge(msg);

                if (msg.hasKey()) {
                    Consumer<?> assignedConsumer = keyToConsumer.get(msg.getKey());
                    if (assignedConsumer == null) {
                        // This is a new key
                        keyToConsumer.put(msg.getKey(), c);
                    } else {
                        // The consumer should be the same
                        assertEquals(c, assignedConsumer);
                    }
                }
            }
        }
    }

    /**
     * Check that every consumer receives a fair number of messages and that same key is delivered to only 1 consumer
     */
    private void receiveAndCheckDistribution(List<Consumer<?>> consumers, int expectedTotalMessage) throws PulsarClientException {
        // Add a key so that we know this key was already assigned to one consumer
        Map<String, Consumer<?>> keyToConsumer = new HashMap<>();
        Map<Consumer<?>, Integer> messagesPerConsumer = new HashMap<>();

        int totalMessages = 0;

        for (Consumer<?> c : consumers) {
            int messagesForThisConsumer = 0;
            while (true) {
                Message<?> msg = c.receive(100, TimeUnit.MILLISECONDS);
                if (msg == null) {
                    // Go to next consumer
                    messagesPerConsumer.put(c, messagesForThisConsumer);
                    break;
                }

                ++totalMessages;
                ++messagesForThisConsumer;
                c.acknowledge(msg);

                if (msg.hasKey() || msg.hasOrderingKey()) {
                    String key = msg.hasOrderingKey() ? new String(msg.getOrderingKey()) : msg.getKey();
                    Consumer<?> assignedConsumer = keyToConsumer.get(key);
                    if (assignedConsumer == null) {
                        // This is a new key
                        keyToConsumer.put(key, c);
                    } else {
                        // The consumer should be the same
                        assertEquals(c, assignedConsumer);
                    }
                }
            }
        }

        final double PERCENT_ERROR = 0.40; // 40 %

        double expectedMessagesPerConsumer = totalMessages / consumers.size();
        Assert.assertEquals(expectedTotalMessage, totalMessages);
        for (int count : messagesPerConsumer.values()) {
            Assert.assertEquals(count, expectedMessagesPerConsumer, expectedMessagesPerConsumer * PERCENT_ERROR);
        }
    }

    private void receiveAndCheck(List<KeyValue<Consumer<Integer>, Integer>> checkList) throws PulsarClientException {
        Map<Consumer, Set<String>> consumerKeys = new HashMap<>();
        for (KeyValue<Consumer<Integer>, Integer> check : checkList) {
            Consumer<Integer> consumer = check.getKey();
            int received = 0;
            Map<String, Message<Integer>> lastMessageForKey = new HashMap<>();
            for (Integer i = 0; i < check.getValue(); i++) {
                Message<Integer> message = consumer.receive();
                if (i % 2 == 0) {
                    consumer.acknowledge(message);
                }
                String key = message.hasOrderingKey() ? new String(message.getOrderingKey()) : message.getKey();
                log.info("[{}] Receive message key: {} value: {} messageId: {}",
                        consumer.getConsumerName(), key, message.getValue(), message.getMessageId());
                // check messages is order by key
                if (lastMessageForKey.get(key) == null) {
                    Assert.assertNotNull(message);
                } else {
                    Assert.assertTrue(message.getValue()
                        .compareTo(lastMessageForKey.get(key).getValue()) > 0);
                }
                lastMessageForKey.put(key, message);
                consumerKeys.putIfAbsent(consumer, new HashSet<>());
                consumerKeys.get(consumer).add(key);
                received++;
            }
            Assert.assertEquals(check.getValue().intValue(), received);
            int redeliveryCount = check.getValue() / 2;
            log.info("[{}] Consumer wait for {} messages redelivery ...", check, redeliveryCount);
            // messages not acked, test redelivery
            lastMessageForKey = new HashMap<>();
            for (int i = 0; i < redeliveryCount; i++) {
                Message<Integer> message = consumer.receive();
                received++;
                consumer.acknowledge(message);
                String key = message.hasOrderingKey() ? new String(message.getOrderingKey()) : message.getKey();
                log.info("[{}] Receive redeliver message key: {} value: {} messageId: {}",
                        consumer.getConsumerName(), key, message.getValue(), message.getMessageId());
                // check redelivery messages is order by key
                if (lastMessageForKey.get(key) == null) {
                    Assert.assertNotNull(message);
                } else {
                    Assert.assertTrue(message.getValue()
                            .compareTo(lastMessageForKey.get(key).getValue()) > 0);
                }
                lastMessageForKey.put(key, message);
            }
            Message noMessages = null;
            try {
                noMessages = consumer.receive(100, TimeUnit.MILLISECONDS);
            } catch (PulsarClientException ignore) {
            }
            Assert.assertNull(noMessages, "redeliver too many messages.");
            Assert.assertEquals((check.getValue() + redeliveryCount), received);
        }
        Set<String> allKeys = new HashSet<>();
        consumerKeys.forEach((k, v) -> v.stream().filter(Objects::nonNull).forEach(key -> {
            assertTrue(allKeys.add(key),
                "Key " + key + " is distributed to multiple consumers." );
        }));
    }

    private static class EncKeyReader implements CryptoKeyReader {

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

    @Test
    public void testStickyKeyRangesRestartConsumers() throws Exception {
        final String topic = TopicName.get("persistent", "public", "default",
                "testStickyKeyRangesRestartConsumers" + UUID.randomUUID()).toString();

        final String subscriptionName = "my-sub";

        final int numMessages = 100;
        // start 2 consumers
        Set<String> sentMessages = new ConcurrentSkipListSet<>();

        CountDownLatch count1 = new CountDownLatch(2);
        CountDownLatch count2 = new CountDownLatch(13); // consumer 2 usually receive the fix messages
        CountDownLatch count3 = new CountDownLatch(numMessages);
        @Cleanup
        Consumer<String> consumer1 = pulsarClient.newConsumer(
                        Schema.STRING)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(Range.of(0, 65536 / 2)))
                .messageListener((consumer, msg) -> {
                    consumer.acknowledgeAsync(msg).whenComplete((m, e) -> {
                        if (e != null) {
                            log.error("error", e);
                        } else {
                            sentMessages.remove(msg.getKey());
                            count1.countDown();
                            count3.countDown();
                        }
                    });
                })
                .subscribe();

        @Cleanup
        Consumer<String> consumer2 = pulsarClient.newConsumer(
                        Schema.STRING)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(Range.of(65536 / 2 + 1, 65535)))
                .messageListener((consumer, msg) -> {
                    consumer.acknowledgeAsync(msg).whenComplete((m, e) -> {
                        if (e != null) {
                            log.error("error", e);
                        } else {
                            sentMessages.remove(msg.getKey());
                            count2.countDown();
                            count3.countDown();
                        }
                    });
                })
                .subscribe();

        Future producerFuture = pulsar.getExecutor().submit(() -> {
            try
            {
                try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                        .topic(topic)
                        .enableBatching(false)
                        .create()) {
                    for (int i = 0; i < numMessages; i++)
                    {
                        String key = "test" + i;
                        sentMessages.add(key);
                        producer.newMessage()
                                .key(key)
                                .value("test" + i).
                                send();
                        Thread.sleep(100);
                    }
                }
            } catch (Throwable t) {
                log.error("error", t);
            }});

        // wait for some messages to be received by both of the consumers
        count1.await();
        count2.await();
        consumer1.close();
        consumer2.close();

        // this sleep is to trigger a race condition that happens
        // when there are some messages that cannot be dispatched while consuming
        Thread.sleep(3000);

        // start consuming again...
        @Cleanup
        Consumer consumer3 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(Range.of(0, 65536 / 2)))
                .messageListener((consumer, msg) -> {
                    consumer.acknowledgeAsync(msg).whenComplete((m, e) -> {
                        if (e != null) {
                            log.error("error", e);
                        } else {
                            sentMessages.remove(msg.getKey());
                            count3.countDown();
                        }
                    });
                })
                .subscribe();
        @Cleanup
        Consumer consumer4 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(Range.of(65536 / 2 + 1, 65535)))
                .messageListener((consumer, msg) -> {
                    consumer.acknowledgeAsync(msg).whenComplete((m, e) -> {
                        if (e != null) {
                            log.error("error", e);
                        } else {
                            sentMessages.remove(msg.getKey());
                            count3.countDown();
                        }
                    });
                })
                .subscribe();
        // wait for all the messages to be delivered
        count3.await();
        assertTrue(sentMessages.isEmpty(), "didn't receive " + sentMessages);

        producerFuture.get();
    }

    @Test
    public void testContinueDispatchMessagesWhenMessageDelayed() throws Exception {
        int delayedMessages = 40;
        int messages = 40;
        int sum = 0;
        final String topic = "persistent://public/default/key_shared-" + UUID.randomUUID();
        final String subName = "my-sub";

        @Cleanup
        Consumer<Integer> consumer1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .create();

        for (int i = 0; i < delayedMessages; i++) {
            MessageId messageId = producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(100 + i)
                    .deliverAfter(10, TimeUnit.SECONDS)
                    .send();
            log.info("Published delayed message :{}", messageId);
        }

        for (int i = 0; i < messages; i++) {
            MessageId messageId = producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
            log.info("Published message :{}", messageId);
        }

        @Cleanup
        Consumer<Integer> consumer2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(30)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        for (int i = 0; i < delayedMessages + messages; i++) {
            Message<Integer> msg = consumer1.receive(30, TimeUnit.SECONDS);
            if (msg != null) {
                log.info("c1 message: {}, {}", msg.getValue(), msg.getMessageId());
                consumer1.acknowledge(msg);
            } else {
                break;
            }
            sum++;
        }

        log.info("Got {} messages...", sum);

        int remaining = delayedMessages + messages - sum;
        for (int i = 0; i < remaining; i++) {
            Message<Integer> msg = consumer2.receive(30, TimeUnit.SECONDS);
            if (msg != null) {
                log.info("c2 message: {}, {}", msg.getValue(), msg.getMessageId());
                consumer2.acknowledge(msg);
            } else {
                break;
            }
            sum++;
        }

        log.info("Got {} other messages...", sum);
        Assert.assertEquals(sum, delayedMessages + messages);
    }

    private AtomicInteger injectReplayReadCounter(String topicName, String cursorName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) managedLedger.openCursor(cursorName);
        managedLedger.getCursors().removeCursor(cursor.getName());
        managedLedger.getActiveCursors().removeCursor(cursor.getName());
        ManagedCursorImpl spyCursor = Mockito.spy(cursor);
        managedLedger.getCursors().add(spyCursor, PositionFactory.EARLIEST);
        managedLedger.getActiveCursors().add(spyCursor, PositionFactory.EARLIEST);
        AtomicInteger replyReadCounter = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            if (!String.valueOf(invocation.getArguments()[2]).equals("Normal")) {
                replyReadCounter.incrementAndGet();
            }
            return invocation.callRealMethod();
        }).when(spyCursor).asyncReplayEntries(Mockito.anySet(), Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            if (!String.valueOf(invocation.getArguments()[2]).equals("Normal")) {
                replyReadCounter.incrementAndGet();
            }
            return invocation.callRealMethod();
        }).when(spyCursor).asyncReplayEntries(Mockito.anySet(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
        admin.topics().createSubscription(topicName, cursorName, MessageId.earliest);
        return replyReadCounter;
    }

    @Test
    public void testNoRepeatedReadAndDiscard() throws Exception {
        int delayedMessages = 100;
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subName = "my-sub";
        admin.topics().createNonPartitionedTopic(topic);
        AtomicInteger replyReadCounter = injectReplayReadCounter(topic, subName);

        // Send messages.
        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32).topic(topic).enableBatching(false).create();
        for (int i = 0; i < delayedMessages; i++) {
            MessageId messageId = producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(100 + i)
                    .send();
            log.info("Published message :{}", messageId);
        }
        producer.close();

        // Make ack holes.
        @Cleanup
        Consumer<Integer> consumer1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        @Cleanup
        Consumer<Integer> consumer2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        List<Message> msgList1 = new ArrayList<>();
        List<Message> msgList2 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message msg1 = consumer1.receive(1, TimeUnit.SECONDS);
            if (msg1 != null) {
                msgList1.add(msg1);
            }
            Message msg2 = consumer2.receive(1, TimeUnit.SECONDS);
            if (msg2 != null) {
                msgList2.add(msg2);
            }
        }
        Consumer<Integer> redeliverConsumer = null;
        if (!msgList1.isEmpty()) {
            msgList1.forEach(msg -> consumer1.acknowledgeAsync(msg));
            redeliverConsumer = consumer2;
        } else {
            msgList2.forEach(msg -> consumer2.acknowledgeAsync(msg));
            redeliverConsumer = consumer1;
        }

        @Cleanup
        Consumer<Integer> consumer3 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(1000)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        redeliverConsumer.close();

        Thread.sleep(5000);
        // Verify: no repeated Read-and-discard.
        int maxReplayCount = delayedMessages * 2;
        assertThat(replyReadCounter.get()).isLessThanOrEqualTo(maxReplayCount);
    }

    @DataProvider(name = "allowKeySharedOutOfOrder")
    public Object[][] allowKeySharedOutOfOrder() {
        return new Object[][]{
                {true},
                {false}
        };
    }

    /**
     * This test is in order to guarantee the feature added by https://github.com/apache/pulsar/pull/7105.
     * 1. Start 3 consumers:
     *   - consumer1 will be closed and trigger a messages redeliver.
     *   - consumer2 will not ack any messages to make the new consumer joined late will be stuck due
     *     to the mechanism "recentlyJoinedConsumers".
     *   - consumer3 will always receive and ack messages.
     * 2. Add consumer4 after consumer1 was close, and consumer4 will be stuck due to the mechanism
     *    "recentlyJoinedConsumers".
     * 3. Verify:
     *   - (Main purpose) consumer3 can still receive messages util the cursor.readerPosition is larger than LAC.
     *   - no repeated Read-and-discard.
     *   - at last, all messages will be received.
     */
    @Test(timeOut = 180 * 1000, dataProvider = "allowKeySharedOutOfOrder") // the test will be finished in 60s.
    public void testRecentJoinedPosWillNotStuckOtherConsumer(boolean allowKeySharedOutOfOrder) throws Exception {
        final int messagesSentPerTime = 100;
        final Set<Integer> totalReceivedMessages = new TreeSet<>();
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subName = "my-sub";
        admin.topics().createNonPartitionedTopic(topic);
        AtomicInteger replyReadCounter = injectReplayReadCounter(topic, subName);

        // Send messages.
        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32).topic(topic).enableBatching(false).create();
        for (int i = 0; i < messagesSentPerTime; i++) {
            MessageId messageId = producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(100 + i)
                    .send();
            log.info("Published message :{}", messageId);
        }

        KeySharedPolicy keySharedPolicy = KeySharedPolicy.autoSplitHashRange()
                .setAllowOutOfOrderDelivery(allowKeySharedOutOfOrder);
        // 1. Start 3 consumers and make ack holes.
        //   - one consumer will be closed and trigger a messages redeliver.
        //   - one consumer will not ack any messages to make the new consumer joined late will be stuck due to the
        //     mechanism "recentlyJoinedConsumers".
        //   - one consumer will always receive and ack messages.
        Consumer<Integer> consumer1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(keySharedPolicy)
                .subscribe();
        Consumer<Integer> consumer2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(keySharedPolicy)
                .subscribe();
        Consumer<Integer> consumer3 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(keySharedPolicy)
                .subscribe();
        List<Message> msgList1 = new ArrayList<>();
        List<Message> msgList2 = new ArrayList<>();
        List<Message> msgList3 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message<Integer> msg1 = consumer1.receive(1, TimeUnit.SECONDS);
            if (msg1 != null) {
                totalReceivedMessages.add(msg1.getValue());
                msgList1.add(msg1);
            }
            Message<Integer> msg2 = consumer2.receive(1, TimeUnit.SECONDS);
            if (msg2 != null) {
                totalReceivedMessages.add(msg2.getValue());
                msgList2.add(msg2);
            }
            Message<Integer> msg3 = consumer3.receive(1, TimeUnit.SECONDS);
            if (msg3 != null) {
                totalReceivedMessages.add(msg3.getValue());
                msgList3.add(msg3);
            }
        }
        Consumer<Integer> consumerWillBeClose = null;
        Consumer<Integer> consumerAlwaysAck = null;
        Consumer<Integer> consumerStuck = null;
        Runnable consumerStuckAckHandler;

        if (!msgList1.isEmpty()) {
            msgList1.forEach(msg -> consumer1.acknowledgeAsync(msg));
            consumerAlwaysAck = consumer1;
            consumerWillBeClose = consumer2;
            consumerStuck = consumer3;
            consumerStuckAckHandler = () -> {
                msgList3.forEach(msg -> consumer3.acknowledgeAsync(msg));
            };
        } else if (!msgList2.isEmpty()){
            msgList2.forEach(msg -> consumer2.acknowledgeAsync(msg));
            consumerAlwaysAck = consumer2;
            consumerWillBeClose = consumer3;
            consumerStuck = consumer1;
            consumerStuckAckHandler = () -> {
                msgList1.forEach(msg -> consumer1.acknowledgeAsync(msg));
            };
        } else {
            msgList3.forEach(msg -> consumer3.acknowledgeAsync(msg));
            consumerAlwaysAck = consumer3;
            consumerWillBeClose = consumer1;
            consumerStuck = consumer2;
            consumerStuckAckHandler = () -> {
                msgList2.forEach(msg -> consumer2.acknowledgeAsync(msg));
            };
        }


        // 2. Add consumer4 after "consumerWillBeClose" was close, and consumer4 will be stuck due to the mechanism
        //    "recentlyJoinedConsumers".
        Consumer<Integer> consumer4 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(1000)
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(keySharedPolicy)
                .subscribe();
        consumerWillBeClose.close();

        Thread.sleep(2000);

        for (int i = messagesSentPerTime; i < messagesSentPerTime * 2; i++) {
            MessageId messageId = producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(100 + i)
                    .send();
            log.info("Published message :{}", messageId);
        }

        // Send messages again.
        // Verify: "consumerAlwaysAck" can receive messages util the cursor.readerPosition is larger than LAC.
        while (true) {
            Message<Integer> msg = consumerAlwaysAck.receive(2, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            totalReceivedMessages.add(msg.getValue());
            consumerAlwaysAck.acknowledge(msg);
        }
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) managedLedger.openCursor(subName);
        log.info("cursor_readPosition {}, LAC {}", cursor.getReadPosition(), managedLedger.getLastConfirmedEntry());
        assertTrue((cursor.getReadPosition())
                .compareTo(managedLedger.getLastConfirmedEntry())  > 0);

        // Make all consumers to start to read and acknowledge messages.
        // Verify: no repeated Read-and-discard.
        Thread.sleep(5 * 1000);
        int maxReplayCount = messagesSentPerTime * 2;
        log.info("Reply read count: {}", replyReadCounter.get());
        assertTrue(replyReadCounter.get() < maxReplayCount);
        // Verify: at last, all messages will be received.
        consumerStuckAckHandler.run();
        ReceivedMessages<Integer> receivedMessages = ackAllMessages(consumerAlwaysAck, consumerStuck, consumer4);
        totalReceivedMessages.addAll(receivedMessages.messagesReceived.stream().map(p -> p.getRight()).collect(
                Collectors.toList()));
        assertEquals(totalReceivedMessages.size(), messagesSentPerTime * 2);

        // cleanup.
        consumer1.close();
        consumer2.close();
        consumer3.close();
        consumer4.close();
        producer.close();
        admin.topics().delete(topic, false);
    }

    @Test
    public void testReadAheadLimit() throws Exception {
        String topic = "testReadAheadLimit-" + UUID.randomUUID();
        int numberOfKeys = 1000;
        long pauseTime = 100L;
        int readAheadLimit = 20;
        pulsar.getConfig().setKeySharedLookAheadMsgInReplayThresholdPerSubscription(readAheadLimit);

        @Cleanup
        Producer<Integer> producer = createProducer(topic, false);

        // create a consumer and close it to create a subscription
        String subscriptionName = "key_shared";
        pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe()
                .close();

        Topic t = pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        PersistentSubscription sub = (PersistentSubscription) t.getSubscription(subscriptionName);
        // get the dispatcher reference
        PersistentStickyKeyDispatcherMultipleConsumers dispatcher =
                (PersistentStickyKeyDispatcherMultipleConsumers) sub.getDispatcher();

        // create a function to use for checking the number of messages in replay
        Runnable checkLimit = () -> {
            assertThat(dispatcher.getNumberOfMessagesInReplay()).isLessThanOrEqualTo(readAheadLimit);
        };

        // Adding a new consumer.
        @Cleanup
        Consumer<Integer> c1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c1")
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .receiverQueueSize(10)
                .startPaused(true) // start paused
                .subscribe();

        @Cleanup
        Consumer<Integer> c2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c2")
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .receiverQueueSize(500) // use large receiver queue size
                .subscribe();

        @Cleanup
        Consumer<Integer> c3 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c3")
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .receiverQueueSize(10)
                .startPaused(true) // start paused
                .subscribe();

        // find keys that will be assigned to c2
        List<String> keysForC2 = new ArrayList<>();
        for (int i = 0; i < numberOfKeys; i++) {
            String key = String.valueOf(i);
            byte[] keyBytes = key.getBytes(UTF_8);
            if (dispatcher.getSelector().select(keyBytes).consumerName().equals("c2")) {
                keysForC2.add(key);
            }
        }

        Set<Integer> remainingMessageValues = new HashSet<>();
        // produce messages with keys that all get assigned to c2
        for (int i = 0; i < 1000; i++) {
            String key = keysForC2.get(random.nextInt(keysForC2.size()));
            //log.info("Producing message with key: {} value: {}", key, i);
            producer.newMessage()
                    .key(key)
                    .value(i)
                    .send();
            remainingMessageValues.add(i);
        }

        checkLimit.run();

        Thread.sleep(pauseTime);
        checkLimit.run();

        Thread.sleep(pauseTime);
        checkLimit.run();

        // resume c1 and c3
        c1.resume();
        c3.resume();

        Thread.sleep(pauseTime);
        checkLimit.run();

        // produce more messages
        for (int i = 1000; i < 2000; i++) {
            String key = String.valueOf(random.nextInt(numberOfKeys));
            producer.newMessage()
                    .key(key)
                    .value(i)
                    .send();
            remainingMessageValues.add(i);
            checkLimit.run();
        }

        // consume the messages
        receiveMessages((consumer, msg) -> {
            synchronized (this) {
                try {
                    consumer.acknowledge(msg);
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
                remainingMessageValues.remove(msg.getValue());
                checkLimit.run();
                return true;
            }
        }, Duration.ofSeconds(2), c1, c2, c3);
        assertEquals(remainingMessageValues, Collections.emptySet());
    }
}
