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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PatternMultiTopicsConsumerImpl;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class TopicGCTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @EqualsAndHashCode.Include
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setBrokerDeleteInactiveTopicsEnabled(true);
        conf.setBrokerDeleteInactiveTopicsMode(
                InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(10);
    }

    private enum SubscribeTopicType {
        MULTI_PARTITIONED_TOPIC,
        REGEX_TOPIC;
    }

    @DataProvider(name = "subscribeTopicTypes")
    public Object[][] subTopicTypes() {
        return new Object[][]{
                {SubscribeTopicType.MULTI_PARTITIONED_TOPIC},
                {SubscribeTopicType.REGEX_TOPIC}
        };
    }

    private void setSubscribeTopic(ConsumerBuilder consumerBuilder, SubscribeTopicType subscribeTopicType,
                                   String topicName, String topicPattern) {
        if (subscribeTopicType.equals(SubscribeTopicType.MULTI_PARTITIONED_TOPIC)) {
            consumerBuilder.topic(topicName);
        } else {
            consumerBuilder.topicsPattern(Pattern.compile(topicPattern));
        }
    }

    @Test(dataProvider = "subscribeTopicTypes", timeOut = 300 * 1000)
    public void testRecreateConsumerAfterOnePartGc(SubscribeTopicType subscribeTopicType) throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String topicPattern = "persistent://public/default/tp.*";
        final String partition0 = topic + "-partition-0";
        final String partition1 = topic + "-partition-1";
        final String subscription = "s1";
        admin.topics().createPartitionedTopic(topic, 2);
        admin.topics().createSubscription(topic, subscription, MessageId.earliest);

        // create consumers and producers.
        Producer<String> producer0 = pulsarClient.newProducer(Schema.STRING).topic(partition0)
                .enableBatching(false).create();
        Producer<String> producer1 = pulsarClient.newProducer(Schema.STRING).topic(partition1)
                .enableBatching(false).create();
        ConsumerBuilder<String> consumerBuilder1 = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscription)
                .isAckReceiptEnabled(true)
                .subscriptionType(SubscriptionType.Shared);
        setSubscribeTopic(consumerBuilder1, subscribeTopicType, topic, topicPattern);
        Consumer<String> consumer1 = consumerBuilder1.subscribe();

        // Make consume all messages for one topic, do not consume any messages for another one.
        producer0.send("1");
        producer1.send("2");
        admin.topics().skipAllMessages(partition0, subscription);

        // Wait for topic GC.
        // Partition 0 will be deleted about 20s later, left 2min to avoid flaky.
        producer0.close();
        consumer1.close();
        Awaitility.await().atMost(2, TimeUnit.MINUTES).untilAsserted(() -> {
            CompletableFuture<Optional<Topic>> tp1 = pulsar.getBrokerService().getTopic(partition0, false);
            CompletableFuture<Optional<Topic>> tp2 = pulsar.getBrokerService().getTopic(partition1, false);
            assertTrue(tp1 == null || !tp1.get().isPresent());
            assertTrue(tp2 != null && tp2.get().isPresent());
        });

        // Verify that the consumer subscribed with partitioned topic can be created successful.
        ConsumerBuilder<String> consumerBuilder2 = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscription)
                .isAckReceiptEnabled(true)
                .subscriptionType(SubscriptionType.Shared);
        setSubscribeTopic(consumerBuilder2, subscribeTopicType, topic, topicPattern);
        Consumer<String> consumer2 = consumerBuilder2.subscribe();
        Message<String> msg = consumer2.receive(2, TimeUnit.SECONDS);
        String receivedMsgValue = msg.getValue();
        log.info("received msg: {}", receivedMsgValue);
        consumer2.acknowledge(msg);

        // cleanup.
        consumer2.close();
        producer0.close();
        producer1.close();
        admin.topics().deletePartitionedTopic(topic);
    }

    @Test(dataProvider = "subscribeTopicTypes", timeOut = 300 * 1000)
    public void testAppendCreateConsumerAfterOnePartGc(SubscribeTopicType subscribeTopicType) throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String topicPattern = "persistent://public/default/tp.*";
        final String partition0 = topic + "-partition-0";
        final String partition1 = topic + "-partition-1";
        final String subscription = "s1";
        admin.topics().createPartitionedTopic(topic, 2);
        admin.topics().createSubscription(topic, subscription, MessageId.earliest);

        // create consumers and producers.
        Producer<String> producer0 = pulsarClient.newProducer(Schema.STRING).topic(partition0)
                .enableBatching(false).create();
        Producer<String> producer1 = pulsarClient.newProducer(Schema.STRING).topic(partition1)
                .enableBatching(false).create();
        ConsumerBuilder<String> consumerBuilder1 = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscription)
                .isAckReceiptEnabled(true)
                .subscriptionType(SubscriptionType.Shared);
        setSubscribeTopic(consumerBuilder1, subscribeTopicType, topic, topicPattern);
        Consumer<String> consumer1 = consumerBuilder1.subscribe();

        // Make consume all messages for one topic, do not consume any messages for another one.
        producer0.send("partition-0-1");
        producer1.send("partition-1-1");
        producer1.send("partition-1-2");
        producer1.send("partition-1-4");
        admin.topics().skipAllMessages(partition0, subscription);

        // Wait for topic GC.
        // Partition 0 will be deleted about 20s later, left 2min to avoid flaky.
        producer0.close();
        Awaitility.await().atMost(2, TimeUnit.MINUTES).untilAsserted(() -> {
            CompletableFuture<Optional<Topic>> tp1 = pulsar.getBrokerService().getTopic(partition0, false);
            CompletableFuture<Optional<Topic>> tp2 = pulsar.getBrokerService().getTopic(partition1, false);
            assertTrue(tp1 == null || !tp1.get().isPresent());
            assertTrue(tp2 != null && tp2.get().isPresent());
        });

        // Verify that the messages under "partition-1" still can be ack.
        for (int i = 0; i < 2; i++) {
            Message<String> msg = consumer1.receive(2, TimeUnit.SECONDS);
            assertNotNull(msg, "Expected at least received 2 messages.");
            log.info("received msg[{}]: {}", i, msg.getValue());
            TopicMessageId messageId = (TopicMessageId) msg.getMessageId();
            if (messageId.getOwnerTopic().equals(partition1)) {
                consumer1.acknowledgeAsync(msg);
            }
        }
        consumer1.close();

        // Verify that the consumer subscribed with partitioned topic can be created successful.
        ConsumerBuilder<String> consumerBuilder2 = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscription)
                .isAckReceiptEnabled(true)
                .subscriptionType(SubscriptionType.Shared);
        setSubscribeTopic(consumerBuilder2, subscribeTopicType, topic, topicPattern);
        Consumer<String> consumer2 = consumerBuilder2.subscribe();
        producer1.send("partition-1-5");
        Message<String> msg = consumer2.receive(2, TimeUnit.SECONDS);
        assertNotNull(msg);
        String receivedMsgValue = msg.getValue();
        log.info("received msg: {}", receivedMsgValue);
        consumer2.acknowledge(msg);

        // cleanup.
        consumer2.close();
        producer0.close();
        producer1.close();
        admin.topics().deletePartitionedTopic(topic);
    }

    @Test(timeOut = 180 * 1000)
    public void testPhasePartDeletion() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String topicPattern = "persistent://public/default/tp.*";
        final String partition0 = topic + "-partition-0";
        final String partition1 = topic + "-partition-1";
        final String partition2 = topic + "-partition-2";
        final String subscription = "s1";
        admin.topics().createPartitionedTopic(topic, 3);
        // Create consumer.
        PatternMultiTopicsConsumerImpl<String> c1 = (PatternMultiTopicsConsumerImpl<String>) pulsarClient
                .newConsumer(Schema.STRING)
                .subscriptionName(subscription)
                .isAckReceiptEnabled(true)
                .subscriptionType(SubscriptionType.Shared)
                .topicsPattern(Pattern.compile(topicPattern)).subscribe();
        // Check subscriptions.
        Awaitility.await().untilAsserted(() -> {
            ConcurrentHashMap<String, ConsumerImpl<?>> consumers
                    = WhiteboxImpl.getInternalState(c1, "consumers");
            ConcurrentHashMap<String, Integer> partitionedTopics
                    = WhiteboxImpl.getInternalState(c1, "partitionedTopics");
            assertEquals(partitionedTopics.size(), 1);
            assertEquals(partitionedTopics.get(topic), 3);
            assertEquals(consumers.size(), 3);
            assertTrue(consumers.containsKey(partition0));
            assertTrue(consumers.containsKey(partition1));
            assertTrue(consumers.containsKey(partition2));
        });
        // Delete partitions the first time.
        admin.topics().delete(partition0, true);
        // Check subscriptions.
        Awaitility.await().untilAsserted(() -> {
            ConcurrentHashMap<String, ConsumerImpl<?>> consumers
                    = WhiteboxImpl.getInternalState(c1, "consumers");
            ConcurrentHashMap<String, Integer> partitionedTopics
                    = WhiteboxImpl.getInternalState(c1, "partitionedTopics");
            assertEquals(partitionedTopics.size(), 1);
            assertEquals(partitionedTopics.get(topic), 3);
            assertEquals(consumers.size(), 2);
            assertTrue(consumers.containsKey(partition1));
            assertTrue(consumers.containsKey(partition2));
        });
        // Delete partitions the second time.
        admin.topics().delete(partition1, true);
        // Check subscriptions.
        Awaitility.await().untilAsserted(() -> {
            ConcurrentHashMap<String, ConsumerImpl<?>> consumers
                    = WhiteboxImpl.getInternalState(c1, "consumers");
            ConcurrentHashMap<String, Integer> partitionedTopics
                    = WhiteboxImpl.getInternalState(c1, "partitionedTopics");
            assertEquals(partitionedTopics.size(), 1);
            assertEquals(partitionedTopics.get(topic), 3);
            assertEquals(consumers.size(), 1);
            assertTrue(consumers.containsKey(partition2));
        });
        // Delete partitions the third time.
        admin.topics().delete(partition2, true);
        // Check subscriptions.
        Awaitility.await().untilAsserted(() -> {
            ConcurrentHashMap<String, ConsumerImpl<?>> consumers
                    = WhiteboxImpl.getInternalState(c1, "consumers");
            ConcurrentHashMap<String, Integer> partitionedTopics
                    = WhiteboxImpl.getInternalState(c1, "partitionedTopics");
            assertEquals(partitionedTopics.size(), 0);
            assertEquals(consumers.size(), 0);
        });

        // cleanup.
        c1.close();
        admin.topics().deletePartitionedTopic(topic);
    }

    @Test(timeOut = 180 * 1000)
    public void testExpandPartitions() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String topicPattern = "persistent://public/default/tp.*";
        final String partition0 = topic + "-partition-0";
        final String partition1 = topic + "-partition-1";
        final String subscription = "s1";
        admin.topics().createPartitionedTopic(topic, 2);
        // Delete partitions.
        admin.topics().delete(partition0, true);
        admin.topics().delete(partition1, true);
        // Create consumer.
        PatternMultiTopicsConsumerImpl<String> c1 = (PatternMultiTopicsConsumerImpl<String>) pulsarClient
                .newConsumer(Schema.STRING)
                .subscriptionName(subscription)
                .isAckReceiptEnabled(true)
                .subscriptionType(SubscriptionType.Shared)
                .topicsPattern(Pattern.compile(topicPattern)).subscribe();
        // Check subscriptions.
        Awaitility.await().untilAsserted(() -> {
            ConcurrentHashMap<String, ConsumerImpl<?>> consumers
                    = WhiteboxImpl.getInternalState(c1, "consumers");
            ConcurrentHashMap<String, Integer> partitionedTopics
                    = WhiteboxImpl.getInternalState(c1, "partitionedTopics");
            assertEquals(partitionedTopics.size(), 0);
            assertEquals(consumers.size(), 0);
        });
        // Trigger partitions creation.
        pulsarClient.newConsumer(Schema.STRING).subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared).topic(topic).subscribe().close();
        // Check subscriptions.
        Awaitility.await().untilAsserted(() -> {
            ConcurrentHashMap<String, ConsumerImpl<?>> consumers
                    = WhiteboxImpl.getInternalState(c1, "consumers");
            ConcurrentHashMap<String, Integer> partitionedTopics
                    = WhiteboxImpl.getInternalState(c1, "partitionedTopics");
            assertEquals(partitionedTopics.size(), 1);
            assertEquals(partitionedTopics.get(topic), 2);
            assertEquals(consumers.size(), 2);
            assertTrue(consumers.containsKey(partition0));
            assertTrue(consumers.containsKey(partition1));
        });
        // Expand partitions the first time.
        admin.topics().updatePartitionedTopic(topic, 3);
        final String partition2 = topic + "-partition-2";
        // Check subscriptions.
        Awaitility.await().untilAsserted(() -> {
            ConcurrentHashMap<String, ConsumerImpl<?>> consumers
                    = WhiteboxImpl.getInternalState(c1, "consumers");
            ConcurrentHashMap<String, Integer> partitionedTopics
                    = WhiteboxImpl.getInternalState(c1, "partitionedTopics");
            assertEquals(partitionedTopics.size(), 1);
            assertEquals(partitionedTopics.get(topic), 3);
            assertEquals(consumers.size(), 3);
            assertTrue(consumers.containsKey(partition0));
            assertTrue(consumers.containsKey(partition1));
            assertTrue(consumers.containsKey(partition2));
        });
        // Expand partitions the second time.
        admin.topics().updatePartitionedTopic(topic, 4);
        final String partition3 = topic + "-partition-3";
        // Check subscriptions.
        Awaitility.await().untilAsserted(() -> {
            ConcurrentHashMap<String, ConsumerImpl<?>> consumers
                    = WhiteboxImpl.getInternalState(c1, "consumers");
            ConcurrentHashMap<String, Integer> partitionedTopics
                    = WhiteboxImpl.getInternalState(c1, "partitionedTopics");
            assertEquals(partitionedTopics.size(), 1);
            assertEquals(partitionedTopics.get(topic), 4);
            assertEquals(consumers.size(), 4);
            assertTrue(consumers.containsKey(partition0));
            assertTrue(consumers.containsKey(partition1));
            assertTrue(consumers.containsKey(partition2));
            assertTrue(consumers.containsKey(partition3));
        });

        // cleanup.
        c1.close();
        admin.topics().deletePartitionedTopic(topic);
    }
}
