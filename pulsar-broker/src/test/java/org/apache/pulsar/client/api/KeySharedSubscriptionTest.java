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

import com.google.common.collect.Sets;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.HashRangeAutoSplitStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class KeySharedSubscriptionTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(KeySharedSubscriptionTest.class);
    private static final List<String> keys = Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

    @DataProvider(name = "batch")
    public Object[][] batchProvider() {
        return new Object[][] {
                { false },
                { true }
        };
    }

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

    @Test(dataProvider = "batch")
    public void testSendAndReceiveWithHashRangeAutoSplitStickyKeyConsumerSelector(boolean enableBatch) throws PulsarClientException {
        this.conf.setSubscriptionKeySharedEnable(true);
        String topic = "persistent://public/default/key_shared-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic);

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        int consumer1Slot = HashRangeAutoSplitStickyKeyConsumerSelector.DEFAULT_RANGE_SIZE;
        int consumer2Slot = consumer1Slot >> 1;
        int consumer3Slot = consumer2Slot >> 1;

        int consumer1ExpectMessages = 0;
        int consumer2ExpectMessages = 0;
        int consumer3ExpectMessages = 0;

        for (int i = 0; i < 10; i++) {
            for (String key : keys) {
                int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes())
                    % HashRangeAutoSplitStickyKeyConsumerSelector.DEFAULT_RANGE_SIZE;
                if (slot < consumer3Slot) {
                    consumer3ExpectMessages++;
                } else if (slot < consumer2Slot) {
                    consumer2ExpectMessages++;
                } else {
                    consumer1ExpectMessages++;
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
                .ranges(Range.of(40001, KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE)));

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

    @Test(dataProvider = "batch")
    public void testConsumerCrashSendAndReceiveWithHashRangeAutoSplitStickyKeyConsumerSelector(boolean enableBatch) throws PulsarClientException, InterruptedException {

        this.conf.setSubscriptionKeySharedEnable(true);
        String topic = "persistent://public/default/key_shared_consumer_crash-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic);

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        int consumer1Slot = HashRangeAutoSplitStickyKeyConsumerSelector.DEFAULT_RANGE_SIZE;
        int consumer2Slot = consumer1Slot >> 1;
        int consumer3Slot = consumer2Slot >> 1;

        int consumer1ExpectMessages = 0;
        int consumer2ExpectMessages = 0;
        int consumer3ExpectMessages = 0;

        for (int i = 0; i < 10; i++) {
            for (String key : keys) {
                int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes())
                    % HashRangeAutoSplitStickyKeyConsumerSelector.DEFAULT_RANGE_SIZE;
                if (slot < consumer3Slot) {
                    consumer3ExpectMessages++;
                } else if (slot < consumer2Slot) {
                    consumer2ExpectMessages++;
                } else {
                    consumer1ExpectMessages++;
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

        // wait for consumer grouping acking send.
        Thread.sleep(1000);

        consumer1.close();
        consumer2.close();

        for (int i = 0; i < 10; i++) {
            for (String key : keys) {
                producer.newMessage()
                    .key(key)
                    .value(i)
                    .send();
            }
        }

        checkList = new ArrayList<>();
        checkList.add(new KeyValue<>(consumer3, 100));
        receiveAndCheck(checkList);
    }


    @Test(dataProvider = "batch")
    public void testNonKeySendAndReceiveWithHashRangeAutoSplitStickyKeyConsumerSelector(boolean enableBatch) throws PulsarClientException {
        this.conf.setSubscriptionKeySharedEnable(true);
        String topic = "persistent://public/default/key_shared_none_key-" + UUID.randomUUID();

        @Cleanup
        Consumer<Integer> consumer1 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer2 = createConsumer(topic);

        @Cleanup
        Consumer<Integer> consumer3 = createConsumer(topic);

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        int consumer1Slot = HashRangeAutoSplitStickyKeyConsumerSelector.DEFAULT_RANGE_SIZE;
        int consumer2Slot = consumer1Slot >> 1;
        int consumer3Slot = consumer2Slot >> 1;

        for (int i = 0; i < 100; i++) {
            producer.newMessage()
                    .value(i)
                    .send();
        }
        int slot = Murmur3_32Hash.getInstance().makeHash(PersistentStickyKeyDispatcherMultipleConsumers.NONE_KEY.getBytes())
            % HashRangeAutoSplitStickyKeyConsumerSelector.DEFAULT_RANGE_SIZE;
        List<KeyValue<Consumer<Integer>, Integer>> checkList = new ArrayList<>();
        if (slot < consumer3Slot) {
            checkList.add(new KeyValue<>(consumer3, 100));
        } else if (slot < consumer2Slot) {
            checkList.add(new KeyValue<>(consumer2, 100));
        } else {
            checkList.add(new KeyValue<>(consumer1, 100));
        }
        receiveAndCheck(checkList);
    }

    @Test(dataProvider = "batch")
    public void testNonKeySendAndReceiveWithHashRangeExclusiveStickyKeyConsumerSelector(boolean enableBatch) throws PulsarClientException {
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
                .ranges(Range.of(40001, KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE)));

        @Cleanup
        Producer<Integer> producer = createProducer(topic, enableBatch);

        for (int i = 0; i < 100; i++) {
            producer.newMessage()
                    .value(i)
                    .send();
        }
        int slot = Murmur3_32Hash.getInstance().makeHash(PersistentStickyKeyDispatcherMultipleConsumers.NONE_KEY.getBytes())
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
    public void testOrderingKeyWithHashRangeAutoSplitStickyKeyConsumerSelector(boolean enableBatch) throws PulsarClientException {
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

        int consumer1Slot = HashRangeAutoSplitStickyKeyConsumerSelector.DEFAULT_RANGE_SIZE;
        int consumer2Slot = consumer1Slot >> 1;
        int consumer3Slot = consumer2Slot >> 1;

        int consumer1ExpectMessages = 0;
        int consumer2ExpectMessages = 0;
        int consumer3ExpectMessages = 0;

        for (int i = 0; i < 10; i++) {
            for (String key : keys) {
                int slot = Murmur3_32Hash.getInstance().makeHash(key.getBytes())
                    % HashRangeAutoSplitStickyKeyConsumerSelector.DEFAULT_RANGE_SIZE;
                if (slot < consumer3Slot) {
                    consumer3ExpectMessages++;
                } else if (slot < consumer2Slot) {
                    consumer2ExpectMessages++;
                } else {
                    consumer1ExpectMessages++;
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

    @Test(dataProvider = "batch")
    public void testOrderingKeyWithHashRangeExclusiveStickyKeyConsumerSelector(boolean enableBatch) throws PulsarClientException {
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
                .ranges(Range.of(40001, KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE)));

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

    @Test(expectedExceptions = PulsarClientException.class)
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

    @Test()
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

    private Producer<Integer> createProducer(String topic, boolean enableBatch) throws PulsarClientException {
        Producer<Integer> producer = null;
        if (enableBatch) {
            producer = pulsarClient.newProducer(Schema.INT32)
                    .topic(topic)
                    .enableBatching(true)
                    .batcherBuilder(BatcherBuilder.KEY_BASED)
                    .create();
        } else {
            producer = pulsarClient.newProducer(Schema.INT32)
                    .topic(topic)
                    .enableBatching(false)
                    .create();
        }
        return producer;
    }

    private Consumer<Integer> createConsumer(String topic) throws PulsarClientException {
        return createConsumer(topic, null);
    }

    private Consumer<Integer> createConsumer(String topic, KeySharedPolicy keySharedPolicy) throws PulsarClientException {
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
            log.info("[{}] Consumer wait for {} messages redelivery ...", redeliveryCount);
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
}
