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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Cleanup;

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "flaky")
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

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        this.conf.setSubscriptionKeySharedUseConsistentHashing(true);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private static final Random random = new Random(System.nanoTime());
    private static final int NUMBER_OF_KEYS = 300;

    @Test(dataProvider = "data")
    public void testSendAndReceiveWithHashRangeAutoSplitStickyKeyConsumerSelector(String topicType, boolean enableBatch)
            throws PulsarClientException {
        this.conf.setSubscriptionKeySharedEnable(true);
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
    public void testSendAndReceiveWithBatching(String topicType, boolean enableBatch)
            throws Exception {
        this.conf.setSubscriptionKeySharedEnable(true);
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
        this.conf.setSubscriptionKeySharedEnable(true);
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
    public void testConsumerCrashSendAndReceiveWithHashRangeAutoSplitStickyKeyConsumerSelector(String topicType,
            boolean enableBatch) throws PulsarClientException, InterruptedException {

        this.conf.setSubscriptionKeySharedEnable(true);
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
    public void testNonKeySendAndReceiveWithHashRangeAutoSplitStickyKeyConsumerSelector(String topicType,
                                                                                        boolean enableBatch)
            throws PulsarClientException {
        this.conf.setSubscriptionKeySharedEnable(true);
        String topic = topicType + "://public/default/key_shared_none_key-" + UUID.randomUUID();

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

        receive(Lists.newArrayList(consumer1, consumer2, consumer3));
    }

    @Test(dataProvider = "batch")
    public void testNonKeySendAndReceiveWithHashRangeExclusiveStickyKeyConsumerSelector(boolean enableBatch)
            throws PulsarClientException {
        this.conf.setSubscriptionKeySharedEnable(true);
        String topic = "persistent://public/default/key_shared_none_key_exclusive-" + UUID.randomUUID();

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

        for (int i = 0; i < 100; i++) {
            producer.newMessage()
                    .value(i)
                    .send();
        }
        int slot = Murmur3_32Hash.getInstance().makeHash("NONE_KEY".getBytes())
                % KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE;
        List<KeyValue<Consumer<Integer>, Integer>> checkList = new ArrayList<>();
        if (slot <= 20000) {
            checkList.add(new KeyValue<>(consumer1, 100));
        } else if (slot <= 40000) {
            checkList.add(new KeyValue<>(consumer2, 100));
        } else {
            checkList.add(new KeyValue<>(consumer3, 100));
        }
        receiveAndCheck(checkList);
    }

    @Test(dataProvider = "batch")
    public void testOrderingKeyWithHashRangeAutoSplitStickyKeyConsumerSelector(boolean enableBatch)
            throws PulsarClientException {
        this.conf.setSubscriptionKeySharedEnable(true);
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
        this.conf.setSubscriptionKeySharedEnable(true);
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
        this.conf.setSubscriptionKeySharedEnable(false);
        String topic = "persistent://public/default/key_shared_disabled";
        pulsarClient.newConsumer()
            .topic(topic)
            .subscriptionName("key_shared")
            .subscriptionType(SubscriptionType.Key_Shared)
            .ackTimeout(10, TimeUnit.SECONDS)
            .subscribe();
    }

    @Test
    public void testCannotUseAcknowledgeCumulative() throws PulsarClientException {
        this.conf.setSubscriptionKeySharedEnable(true);
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
        this.conf.setSubscriptionKeySharedEnable(true);
        String topic = "testMakingProgressWithSlowerConsumer-" + UUID.randomUUID();

        String slowKey = "slowKey";

        List<PulsarClient> clients = new ArrayList<>();
        try {
            AtomicInteger receivedMessages = new AtomicInteger();

            for (int i = 0; i < 10; i++) {
                PulsarClient client = PulsarClient.builder()
                        .serviceUrl(brokerUrl.toString())
                        .build();
                clients.add(client);

                client.newConsumer(Schema.INT32)
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
        } finally {
            for (PulsarClient c : clients) {
                c.close();
            }
        }
    }

    @Test
    public void testOrderingWhenAddingConsumers() throws Exception {
        this.conf.setSubscriptionKeySharedEnable(true);
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
    public void testReadAheadWhenAddingConsumers() throws Exception {
        this.conf.setSubscriptionKeySharedEnable(true);
        String topic = "testReadAheadWhenAddingConsumers-" + UUID.randomUUID();

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
        PositionImpl readPosition = (PositionImpl) sub.getCursor().getReadPosition();
        assertTrue(readPosition.getEntryId() < 1000);
    }

    @Test
    public void testRemoveFirstConsumer() throws Exception {
        this.conf.setSubscriptionKeySharedEnable(true);
        String topic = "testReadAheadWhenAddingConsumers-" + UUID.randomUUID();

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
                .consumerName("c2")
                .subscribe();

        for (int i = 10; i < 20; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        // C2 will not be able to receive any messages until C1 is done processing whatever he got prefetched
        assertNull(c2.receive(100, TimeUnit.MILLISECONDS));

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
        this.conf.setSubscriptionKeySharedEnable(true);
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
    public void testAttachKeyToMessageMetadata()
            throws PulsarClientException {
        this.conf.setSubscriptionKeySharedEnable(true);
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
        this.conf.setTtlDurationDefaultInSeconds(defaultTTLSec);
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

        for (int i = 0; i < totalMessages; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        // don't ack the first message
        consumer1.receive();
        consumer1.acknowledge(consumer1.receive());

        // The consumer1 and consumer2 should be stuck because of the mark delete position did not move forward.

        @Cleanup
        Consumer<Integer> consumer2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        Message<Integer> received = null;
        try {
            received = consumer2.receive(1, TimeUnit.SECONDS);
        } catch (PulsarClientException ignore) {
        }
        Assert.assertNull(received);

        @Cleanup
        Consumer<Integer> consumer3 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        try {
            received = consumer3.receive(1, TimeUnit.SECONDS);
        } catch (PulsarClientException ignore) {
        }
        Assert.assertNull(received);

        Optional<Topic> topicRef = pulsar.getBrokerService().getTopic(topic, false).get();
        assertTrue(topicRef.isPresent());
        Thread.sleep((defaultTTLSec - 1) * 1000);
        topicRef.get().checkMessageExpiry();

        // The mark delete position is move forward, so the consumers should receive new messages now.
        for (int i = 0; i < totalMessages; i++) {
            producer.newMessage()
                    .key(String.valueOf(random.nextInt(NUMBER_OF_KEYS)))
                    .value(i)
                    .send();
        }

        // Wait broker dispatch messages.
        Assert.assertNotNull(consumer2.receive(1, TimeUnit.SECONDS));
        Assert.assertNotNull(consumer3.receive(1, TimeUnit.SECONDS));
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
            if (check.getValue() % 2 != 0) {
                throw new IllegalArgumentException();
            }
            int received = 0;
            Map<String, Message<Integer>> lastMessageForKey = new HashMap<>();
            for (Integer i = 0; i < check.getValue(); i++) {
                Message<Integer> message = check.getKey().receive();
                if (i % 2 == 0) {
                    check.getKey().acknowledge(message);
                }
                String key = message.hasOrderingKey() ? new String(message.getOrderingKey()) : message.getKey();
                log.info("[{}] Receive message key: {} value: {} messageId: {}",
                    check.getKey().getConsumerName(), key, message.getValue(), message.getMessageId());
                // check messages is order by key
                if (lastMessageForKey.get(key) == null) {
                    Assert.assertNotNull(message);
                } else {
                    Assert.assertTrue(message.getValue()
                        .compareTo(lastMessageForKey.get(key).getValue()) > 0);
                }
                lastMessageForKey.put(key, message);
                consumerKeys.putIfAbsent(check.getKey(), Sets.newHashSet());
                consumerKeys.get(check.getKey()).add(key);
                received++;
            }
            Assert.assertEquals(check.getValue().intValue(), received);
            int redeliveryCount = check.getValue() / 2;
            log.info("[{}] Consumer wait for {} messages redelivery ...", check, redeliveryCount);
            // messages not acked, test redelivery
            lastMessageForKey = new HashMap<>();
            for (int i = 0; i < redeliveryCount; i++) {
                Message<Integer> message = check.getKey().receive();
                received++;
                check.getKey().acknowledge(message);
                String key = message.hasOrderingKey() ? new String(message.getOrderingKey()) : message.getKey();
                log.info("[{}] Receive redeliver message key: {} value: {} messageId: {}",
                        check.getKey().getConsumerName(), key, message.getValue(), message.getMessageId());
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
                noMessages = check.getKey().receive(100, TimeUnit.MILLISECONDS);
            } catch (PulsarClientException ignore) {
            }
            Assert.assertNull(noMessages, "redeliver too many messages.");
            Assert.assertEquals((check.getValue() + redeliveryCount), received);
        }
        Set<String> allKeys = Sets.newHashSet();
        consumerKeys.forEach((k, v) -> v.forEach(key -> {
            assertTrue(allKeys.add(key),
                "Key "+ key +  "is distributed to multiple consumers." );
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
}
