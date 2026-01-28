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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class PatternTopicsConsumerImplTest extends ProducerConsumerBase {
    private static final long testTimeout = 90000; // 1.5 min
    private static final Logger log = LoggerFactory.getLogger(PatternTopicsConsumerImplTest.class);
    private final long ackTimeOutMillis = TimeUnit.SECONDS.toMillis(2);

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        // set isTcpLookup = true, to use BinaryProtoLookupService to get topics for a pattern.
        isTcpLookup = true;
        // enabled transaction, to test pattern consumers not subscribe to transaction system topic.
        conf.setTransactionCoordinatorEnabled(true);
        conf.setSubscriptionPatternMaxLength(10000);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = testTimeout)
    public void testPatternTopicsSubscribeWithBuilderFail() throws Exception {
        String key = "PatternTopicsSubscribeWithBuilderFail";
        final String subscriptionName = "my-ex-subscription-" + key;

        final String topicName1 = "persistent://my-property/my-ns/topic-1-" + key;
        final String topicName2 = "persistent://my-property/my-ns/topic-2-" + key;
        final String topicName3 = "persistent://my-property/my-ns/topic-3-" + key;
        final String topicName4 = "non-persistent://my-property/my-ns/topic-4-" + key;
        List<String> topicNames = Lists.newArrayList(topicName1, topicName2, topicName3, topicName4);
        final String patternString = "persistent://my-property/my-ns/pattern-topic.*";
        Pattern pattern = Pattern.compile(patternString);

        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // test failing builder with pattern and topic should fail
        try {
            pulsarClient.newConsumer()
                .topicsPattern(pattern)
                .topic(topicName1)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscribe();
            fail("subscribe1 with pattern and topic should fail.");
        } catch (PulsarClientException e) {
            // expected
        }

        // test failing builder with pattern and topics should fail
        try {
            pulsarClient.newConsumer()
                .topicsPattern(pattern)
                .topics(topicNames)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscribe();
            fail("subscribe2 with pattern and topics should fail.");
        } catch (PulsarClientException e) {
            // expected
        }

        // test failing builder with pattern and patternString should fail
        try {
            pulsarClient.newConsumer()
                .topicsPattern(pattern)
                .topicsPattern(patternString)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .subscribe();
            fail("subscribe3 with pattern and patternString should fail.");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // test failing builder with empty pattern should fail
        try {
            pattern = Pattern.compile("");
            pulsarClient.newConsumer()
                    .topicsPattern(pattern)
                    .subscriptionName(subscriptionName)
                    .subscriptionType(SubscriptionType.Shared)
                    .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                    .subscribe();
            fail("subscribe4 with empty pattern should fail.");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Pattern has already been set or is empty.");
        }
        try {
            pulsarClient.newConsumer()
                    .topicsPattern("")
                    .subscriptionName(subscriptionName)
                    .subscriptionType(SubscriptionType.Shared)
                    .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                    .subscribe();
            fail("subscribe5 with empty pattern should fail.");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "topicsPattern should not be null or empty");
        }
    }

    // verify consumer create success, and works well.
    @Test(timeOut = testTimeout)
    public void testBinaryProtoToGetTopicsOfNamespacePersistent() throws Exception {
        String key = "BinaryProtoToGetTopics";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1-" + key;
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2-" + key;
        String topicName3 = "persistent://my-property/my-ns/pattern-topic-3-" + key;
        String topicName4 = "non-persistent://my-property/my-ns/pattern-topic-4-" + key;
        Pattern pattern = Pattern.compile("my-property/my-ns/pattern-topic.*");

        // 1. create partition
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 2. create producer
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 30;

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer4 = pulsarClient.newProducer().topic(topicName4)
            .enableBatching(false)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topicsPattern(pattern)
            .patternAutoDiscoveryPeriod(2)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();
        assertTrue(consumer.getTopic().startsWith(PatternMultiTopicsConsumerImpl.DUMMY_TOPIC_NAME_PREFIX));

        // Wait topic list watcher creation.
        waitTopicListWatcherCreation(consumer);

        // 4. verify consumer get methods, to get right number of partitions and topics.
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        List<String> topics = ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions();
        List<ConsumerImpl<byte[]>> consumers = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getConsumers();

        assertEquals(topics.size(), 6);
        assertEquals(consumers.size(), 6);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);

        topics.forEach(topic -> log.debug("topic: {}", topic));
        consumers.forEach(c -> log.debug("consumer: {}", c.getTopic()));

        IntStream.range(0, topics.size()).forEach(index ->
            assertEquals(consumers.get(index).getTopic(), topics.get(index)));

        ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics()
                .forEach(topic -> log.debug("getTopics topic: {}", topic));

        // 5. produce data
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
            producer4.send((messagePredicate + "producer4-" + i).getBytes());
        }

        // 6. should receive all the message
        int messageSet = 0;
        Message<byte[]> message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
        producer4.close();
    }

    private static void waitTopicListWatcherCreation(Consumer<?> consumer) {
        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<TopicListWatcher> completableFuture =
                    WhiteboxImpl.getInternalState(consumer, "watcherFuture");
            assertTrue(completableFuture.isDone() && !completableFuture.isCompletedExceptionally());
        });
    }

    @Test(timeOut = testTimeout)
    public void testBinaryProtoSubscribeAllTopicOfNamespace() throws Exception {
        String key = "testBinaryProtoSubscribeAllTopicOfNamespace";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1-" + key;
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2-" + key;
        String topicName3 = "persistent://my-property/my-ns/pattern-topic-3-" + key;
        Pattern pattern = Pattern.compile("my-property/my-ns/.*");

        // 1. create partition
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName1, 1);
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 2. create producer to trigger create system topic.
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName1)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topicsPattern(pattern)
                .patternAutoDiscoveryPeriod(2)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .receiverQueueSize(4)
                .subscribe();
        assertTrue(consumer.getTopic().startsWith(PatternMultiTopicsConsumerImpl.DUMMY_TOPIC_NAME_PREFIX));

        // Wait topic list watcher creation.
        waitTopicListWatcherCreation(consumer);

        // 4. verify consumer get methods, to get right number of partitions and topics.
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        List<String> topics = ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions();
        List<ConsumerImpl<byte[]>> consumers = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getConsumers();

        List<String> expectedTopics =
                List.of(topicName1 + "-partition-0",
                        topicName2 + "-partition-0", topicName2 + "-partition-1",
                        topicName3 + "-partition-0", topicName3 + "-partition-1", topicName3 + "-partition-2");
        assertThat(topics).containsExactlyInAnyOrderElementsOf(expectedTopics);
        assertEquals(consumers.size(), 6);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 3);

        topics.forEach(topic -> log.info("topic: {}", topic));
        consumers.forEach(c -> log.info("consumer: {}", c.getTopic()));

        IntStream.range(0, topics.size()).forEach(index ->
                assertEquals(consumers.get(index).getTopic(), topics.get(index)));

        consumer.unsubscribe();
        producer.close();
        consumer.close();
    }

    @Test(timeOut = testTimeout)
    public void testPubRateOnNonPersistent() throws Exception {
        cleanup();
        conf.setMaxPublishRatePerTopicInBytes(10000L);
        conf.setMaxPublishRatePerTopicInMessages(100);
        Thread.sleep(500);
        isTcpLookup = true;
        setup();
        testBinaryProtoToGetTopicsOfNamespaceNonPersistent();
    }

    // verify consumer create success, and works well.
    @Test(timeOut = testTimeout)
    public void testBinaryProtoToGetTopicsOfNamespaceNonPersistent() throws Exception {
        String key = "BinaryProtoToGetTopics";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://my-property/my-ns/np-pattern-topic-1-" + key;
        String topicName2 = "persistent://my-property/my-ns/np-pattern-topic-2-" + key;
        String topicName3 = "persistent://my-property/my-ns/np-pattern-topic-3-" + key;
        String topicName4 = "non-persistent://my-property/my-ns/np-pattern-topic-4-" + key;
        Pattern pattern = Pattern.compile("my-property/my-ns/np-pattern-topic.*");

        // 1. create partition
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 2. create producer
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 40;

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer4 = pulsarClient.newProducer().topic(topicName4)
            .enableBatching(false)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topicsPattern(pattern)
            .patternAutoDiscoveryPeriod(2)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .subscriptionTopicsMode(RegexSubscriptionMode.NonPersistentOnly)
            .subscribe();

        // Wait topic list watcher creation.
        waitTopicListWatcherCreation(consumer);

        // 4. verify consumer get methods, to get right number of partitions and topics.
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        List<String> topics = ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions();
        List<ConsumerImpl<byte[]>> consumers = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getConsumers();

        assertEquals(topics.size(), 1);
        assertEquals(consumers.size(), 1);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 0);

        topics.forEach(topic -> log.debug("topic: {}", topic));
        consumers.forEach(c -> log.debug("consumer: {}", c.getTopic()));

        IntStream.range(0, topics.size()).forEach(index ->
            assertEquals(consumers.get(index).getTopic(), topics.get(index)));

        ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics()
                .forEach(topic -> log.debug("getTopics topic: {}", topic));

        // 5. produce data
        for (int i = 0; i < totalMessages / 4; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
            producer4.send((messagePredicate + "producer4-" + i).getBytes());
        }

        // 6. should receive all the message
        int messageSet = 0;
        Message<byte[]> message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages / 4);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
        producer4.close();
    }

    // verify consumer create success, and works well.
    @Test(timeOut = testTimeout)
    public void testBinaryProtoToGetTopicsOfNamespaceAll() throws Exception {
        String key = "BinaryProtoToGetTopics";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1-" + key;
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2-" + key;
        String topicName3 = "persistent://my-property/my-ns/pattern-topic-3-" + key;
        String topicName4 = "non-persistent://my-property/my-ns/pattern-topic-4-" + key;
        Pattern pattern = Pattern.compile("my-property/my-ns/pattern-topic.*");

        // 1. create partition
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 2. create producer
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 40;

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer4 = pulsarClient.newProducer().topic(topicName4)
            .enableBatching(false)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topicsPattern(pattern)
            .patternAutoDiscoveryPeriod(2)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .subscriptionTopicsMode(RegexSubscriptionMode.AllTopics)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .subscribe();

        // Wait topic list watcher creation.
        waitTopicListWatcherCreation(consumer);

        // 4. verify consumer get methods, to get right number of partitions and topics.
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        List<String> topics = ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions();
        List<ConsumerImpl<byte[]>> consumers = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getConsumers();

        assertEquals(topics.size(), 7);
        assertEquals(consumers.size(), 7);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);

        topics.forEach(topic -> log.debug("topic: {}", topic));
        consumers.forEach(c -> log.debug("consumer: {}", c.getTopic()));

        IntStream.range(0, topics.size()).forEach(index ->
            assertEquals(consumers.get(index).getTopic(), topics.get(index)));

        ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics()
                .forEach(topic -> log.debug("getTopics topic: {}", topic));

        // 5. produce data
        for (int i = 0; i < totalMessages / 4; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
            producer4.send((messagePredicate + "producer4-" + i).getBytes());
        }

        // 6. should receive all the message
        int messageSet = 0;
        Message<byte[]> message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
        producer4.close();
    }

    // simulate subscribe a pattern which has no topics, but then matched topics added in.
    @Test(timeOut = testTimeout)
    public void testStartEmptyPatternConsumer() throws Exception {
        String key = "StartEmptyPatternConsumerTest";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1-" + key;
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2-" + key;
        String topicName3 = "persistent://my-property/my-ns/pattern-topic-3-" + key;
        Pattern pattern = Pattern.compile("persistent://my-property/my-ns/pattern-topic.*");

        // 1. create partition
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 2. Create consumer, this should success, but with empty sub-consumer internal
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topicsPattern(pattern)
            .patternAutoDiscoveryPeriod(2)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();
        // Wait topic list watcher creation.
        waitTopicListWatcherCreation(consumer);

        // 3. verify consumer get methods, to get 5 number of partitions and topics.
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 5);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 5);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);

        // 4. create producer
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 30;

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();

        // 5. verify consumer get methods, to get number of partitions and topics, value 6=1+2+3.
        Awaitility.await().untilAsserted(() -> {
            assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 6);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 6);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);
        });


        // 7. produce data
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        // 8. should receive all the message
        int messageSet = 0;
        Message<byte[]> message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
    }

    @DataProvider(name = "delayTypesOfWatchingTopics")
    public Object[][] delayTypesOfWatchingTopics(){
        return new Object[][]{
            {true},
            {false}
        };
    }

    @Test(timeOut = testTimeout, dataProvider = "delayTypesOfWatchingTopics")
    public void testAutoSubscribePatterConsumerFromBrokerWatcher(boolean delayWatchingTopics) throws Exception {
        final String key = "AutoSubscribePatternConsumer";
        final String subscriptionName = "my-ex-subscription-" + key;
        final Pattern pattern = Pattern.compile("persistent://my-property/my-ns/pattern-topic.*");

        PulsarClient client = null;
        if (delayWatchingTopics) {
            client = createDelayWatchTopicsClient();
        } else {
            client = pulsarClient;
        }

        Consumer<byte[]> consumer = client.newConsumer()
                .topicsPattern(pattern)
                // Disable automatic discovery.
                .patternAutoDiscoveryPeriod(1000)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .receiverQueueSize(4)
                .subscribe();

        // 1. create partition
        String topicName = "persistent://my-property/my-ns/pattern-topic-1-" + key;
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName, 4);

        // 2. verify consumer get methods. There is no need to trigger discovery, because the broker will push the
        // changes to update(CommandWatchTopicUpdate).
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        Awaitility.await().untilAsserted(() -> {
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 4);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 4);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 1);
        });

        // cleanup.
        consumer.close();
        admin.topics().deletePartitionedTopic(topicName);
        if (delayWatchingTopics) {
            client.close();
        }
    }

    @Test(timeOut = testTimeout)
    public void testSubscribePatterWithOutTopicDomain() throws Exception {
        final String key = "testSubscribePatterWithOutTopicDomain";
        final String subscriptionName = "my-ex-subscription-" + key;
        final Pattern pattern = Pattern.compile("my-property/my-ns/test-pattern.*");

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topicsPattern(pattern)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(4)
                .subscribe();

        // 0. Need make sure topic watcher started
        waitForTopicListWatcherStarted(consumer);

        // 1. create partition topic
        String topicName = "persistent://my-property/my-ns/test-pattern" + key;
        admin.topics().createPartitionedTopic(topicName, 4);

        // 2. verify broker will push the changes to update(CommandWatchTopicUpdate).
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 4);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 4);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 1);
        });

        // cleanup.
        consumer.close();
        admin.topics().deletePartitionedTopic(topicName);
        pulsarClient.close();
    }

    @Test(timeOut = testTimeout)
    public void testPeriodicReconciliationWithActiveWatcher() throws Exception {
        String key = "PeriodicReconciliationWithActiveWatcher";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName = "persistent://my-property/my-ns/pattern-topic-1-" + key;
        Pattern pattern = Pattern.compile("persistent://my-property/my-ns/pattern-topic.*");

        // Create a topic so consumer has something to subscribe to
        admin.topics().createPartitionedTopic(topicName, 2);

        // Create consumer with short auto-discovery period (1 second)
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topicsPattern(pattern)
                .patternAutoDiscoveryPeriod(1, TimeUnit.SECONDS)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        try {
            // Wait for topic list watcher to be connected
            waitForTopicListWatcherStarted(consumer);

            PatternMultiTopicsConsumerImpl<?> patternConsumer = (PatternMultiTopicsConsumerImpl<?>) consumer;

            // Verify that recheckPatternTimeout is NOT null even with active watcher
            // This confirms the periodic timer is always scheduled
            assertNotNull(patternConsumer.getRecheckPatternTimeout(),
                    "recheckPatternTimeout should not be null even when TopicListWatcher is connected");

            // Record the initial recheck epoch
            int initialEpoch = patternConsumer.getRecheckPatternEpoch();

            // Wait for at least 2 reconciliation cycles (with 1 second period)
            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> {
                        int currentEpoch = patternConsumer.getRecheckPatternEpoch();
                        assertTrue(currentEpoch > initialEpoch + 1,
                                "recheckPatternEpoch should increase over time, indicating periodic reconciliation. "
                                        + "Initial: " + initialEpoch + ", Current: " + currentEpoch);
                    });

            // Verify timeout is still scheduled after reconciliation cycles
            assertNotNull(patternConsumer.getRecheckPatternTimeout(),
                    "recheckPatternTimeout should remain scheduled after reconciliation cycles");

        } finally {
            consumer.close();
            admin.topics().deletePartitionedTopic(topicName);
        }
    }

    @DataProvider(name = "topicDomain")
    public Object[][] topicDomain(){
        return new Object[][]{
                {"persistent"},
                {"non-persistent"},
        };
    }

    @Test(timeOut = testTimeout, dataProvider = "topicDomain")
    public void testSubscribePatterBrokerDisable(String topicDomain) throws Exception {
        if (topicDomain.equals("persistent")) {
            conf.setEnableBrokerSideSubscriptionPatternEvaluation(false);
        }
        final String key = "testSubscribePatterWithOutTopicDomain";
        final String subscriptionName = "my-ex-subscription-" + key;
        final Pattern pattern = Pattern.compile("my-property/my-ns/test-pattern.*");

        // 0. Create topic1 with 4 partition
        String topicName1 = topicDomain + "://my-property/my-ns/test-pattern-0" + key;
        admin.topics().createPartitionedTopic(topicName1, 4);
        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1).create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topicsPattern(pattern)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionTopicsMode(topicDomain.equals("persistent") ? RegexSubscriptionMode.PersistentOnly
                        : RegexSubscriptionMode.NonPersistentOnly)
                .patternAutoDiscoveryPeriod(1, TimeUnit.SECONDS)
                .receiverQueueSize(4)
                .subscribe();

        // 1. Need make sure first check complete
        Awaitility.await().untilAsserted(() -> {
            int recheckEpoch = ((PatternMultiTopicsConsumerImpl<?>) consumer).getRecheckPatternEpoch();
            assertTrue(recheckEpoch > 0);
        });

        // 2. Create topic2 with 4 partition
        String topicName2 = topicDomain + "://my-property/my-ns/test-pattern" + key;
        admin.topics().createPartitionedTopic(topicName2, 4);
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2).create();

        // 3. verify will update the partitions and consumers
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 8);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 8);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);
        });

        // cleanup.
        consumer.close();
        producer1.close();
        producer2.close();
        admin.topics().deletePartitionedTopic(topicName1);
        admin.topics().deletePartitionedTopic(topicName2);
        pulsarClient.close();
        conf.setEnableBrokerSideSubscriptionPatternEvaluation(true);
    }

    @DataProvider(name = "regexpConsumerArgs")
    public Object[][] regexpConsumerArgs(){
        return new Object[][]{
                {true, true},
                {true, false},
                {false, true},
                {false, false}
        };
    }

    private void waitForTopicListWatcherStarted(Consumer<?> consumer) {
        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<TopicListWatcher> completableFuture =
                    WhiteboxImpl.getInternalState(consumer, "watcherFuture");
            log.info("isDone: {}, isCompletedExceptionally: {}", completableFuture.isDone(),
                    completableFuture.isCompletedExceptionally());
            assertTrue(completableFuture.isDone() && !completableFuture.isCompletedExceptionally());
        });
    }

    @Test(timeOut = testTimeout, dataProvider = "regexpConsumerArgs")
    public void testPreciseRegexpSubscribe(boolean partitioned, boolean createTopicAfterWatcherStarted)
            throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscriptionName = "s1";
        final Pattern pattern = Pattern.compile(String.format("%s$", topicName));

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topicsPattern(pattern)
                // Disable automatic discovery.
                .patternAutoDiscoveryPeriod(1000)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .receiverQueueSize(4)
                .subscribe();
        if (createTopicAfterWatcherStarted) {
            waitForTopicListWatcherStarted(consumer);
        }

        // 1. create topic.
        if (partitioned) {
            admin.topics().createPartitionedTopic(topicName, 1);
        } else {
            admin.topics().createNonPartitionedTopic(topicName);
        }

        // 2. verify consumer can subscribe the topic.
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        Awaitility.await().untilAsserted(() -> {
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 1);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 1);
            if (partitioned) {
                assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 1);
            } else {
                assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 0);
            }
        });

        // cleanup.
        consumer.close();
        if (partitioned) {
            admin.topics().deletePartitionedTopic(topicName);
        } else {
            admin.topics().delete(topicName);
        }
    }

    @DataProvider(name = "partitioned")
    public Object[][] partitioned(){
        return new Object[][]{
                {true},
                {true}
        };
    }

    @Test(timeOut = 240 * 1000, dataProvider = "partitioned")
    public void testPreciseRegexpSubscribeDisabledTopicWatcher(boolean partitioned) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscriptionName = "s1";
        final Pattern pattern = Pattern.compile(String.format("%s$", topicName));

        // Close all ServerCnx by close client-side sockets to make the config changes effect.
        pulsar.getConfig().setEnableBrokerSideSubscriptionPatternEvaluation(false);
        reconnectAllConnections();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topicsPattern(pattern)
                // Disable brokerSideSubscriptionPatternEvaluation will leading disable topic list watcher.
                // So set patternAutoDiscoveryPeriod to a little value.
                .patternAutoDiscoveryPeriod(5, TimeUnit.SECONDS)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .receiverQueueSize(4)
                .subscribe();

        // 1. create topic.
        if (partitioned) {
            admin.topics().createPartitionedTopic(topicName, 1);
        } else {
            admin.topics().createNonPartitionedTopic(topicName);
        }

        // 2. verify consumer can subscribe the topic.
        // Since the minimum value of `patternAutoDiscoveryPeriod` is 5s, we set the test timeout to a triple value.
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 1);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 1);
            if (partitioned) {
                assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 1);
            } else {
                assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 0);
            }
        });

        // cleanup.
        consumer.close();
        if (partitioned) {
            admin.topics().deletePartitionedTopic(topicName);
        } else {
            admin.topics().delete(topicName);
        }
        // Close all ServerCnx by close client-side sockets to make the config changes effect.
        pulsar.getConfig().setEnableBrokerSideSubscriptionPatternEvaluation(true);
        reconnectAllConnections();
    }

    private PulsarClient createDelayWatchTopicsClient() throws Exception {
        ClientBuilderImpl clientBuilder = (ClientBuilderImpl) PulsarClient.builder().serviceUrl(lookupUrl.toString());
        return InjectedClientCnxClientBuilder.create(clientBuilder,
            (conf, eventLoopGroup) -> new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup) {
                public CompletableFuture<CommandWatchTopicListSuccess> newWatchTopicList(
                        BaseCommand command, long requestId) {
                    // Inject 2 seconds delay when sending command New Watch Topics.
                    CompletableFuture<CommandWatchTopicListSuccess> res = new CompletableFuture<>();
                    new Thread(() -> {
                        sleepSeconds(2);
                        super.newWatchTopicList(command, requestId).whenComplete((v, ex) -> {
                            if (ex != null) {
                                res.completeExceptionally(ex);
                            } else {
                                res.complete(v);
                            }
                        });
                    }).start();
                    return res;
                }
            });
    }

    // simulate subscribe a pattern which has 3 topics, but then matched topic added in.
    @Test(timeOut = testTimeout)
    public void testAutoSubscribePatternConsumer() throws Exception {
        String key = "AutoSubscribePatternConsumer";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1-" + key;
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2-" + key;
        String topicName3 = "persistent://my-property/my-ns/pattern-topic-3-" + key;
        Pattern pattern = Pattern.compile("persistent://my-property/my-ns/pattern-topic.*");

        // 1. create partition
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 2. create producer
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 30;

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topicsPattern(pattern)
            .patternAutoDiscoveryPeriod(2)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();

        // Wait topic list watcher creation.
        waitTopicListWatcherCreation(consumer);

        assertTrue(consumer instanceof PatternMultiTopicsConsumerImpl);

        // 4. verify consumer get methods, to get 6 number of partitions and topics: 6=1+2+3
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 6);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 6);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);

        // 5. produce data to topic 1,2,3; verify should receive all the message
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        int messageSet = 0;
        Message<byte[]> message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        // 6. create another producer with 4 partitions
        String topicName4 = "persistent://my-property/my-ns/pattern-topic-4-" + key;
        admin.topics().createPartitionedTopic(topicName4, 4);
        Producer<byte[]> producer4 = pulsarClient.newProducer().topic(topicName4)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();

        // 7. call recheckTopics to subscribe each added topics above, verify topics number: 10=1+2+3+4
        log.debug("recheck topics change");
        PatternMultiTopicsConsumerImpl<byte[]> consumer1 = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 10);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 10);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 3);
        });

        // 8. produce data to topic3 and topic4, verify should receive all the message
        for (int i = 0; i < totalMessages / 2; i++) {
            producer3.send((messagePredicate + "round2-producer4-" + i).getBytes());
            producer4.send((messagePredicate + "round2-producer4-" + i).getBytes());
        }

        messageSet = 0;
        message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
        producer4.close();
    }

    @Test(timeOut = testTimeout)
    public void testAutoUnsubscribePatternConsumer() throws Exception {
        String key = "AutoUnsubscribePatternConsumer";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1-" + key;
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2-" + key;
        String topicName3 = "persistent://my-property/my-ns/pattern-topic-3-" + key;
        Pattern pattern = Pattern.compile("persistent://my-property/my-ns/pattern-topic.*");

        // 1. create partition
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName2, 2);
        admin.topics().createPartitionedTopic(topicName3, 3);

        // 2. create producer
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 30;

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName1)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName2)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topicsPattern(pattern)
                .patternAutoDiscoveryPeriod(3, TimeUnit.SECONDS)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .receiverQueueSize(4)
                .subscribe();

        // Wait topic list watcher creation.
        waitTopicListWatcherCreation(consumer);

        assertTrue(consumer instanceof PatternMultiTopicsConsumerImpl);

        // 4. verify consumer get methods, to get 0 number of partitions and topics: 6=1+2+3
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 6);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 6);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);

        // 5. produce data to topic 1,2,3; verify should receive all the message
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        int messageSet = 0;
        Message<byte[]> message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        // 6. remove producer 1,3 and delete topics; verify only consumer 2 left
        producer3.close();
        producer1.close();

        // 7. call recheckTopics to unsubscribe topic 1,3, verify topics number: 2=6-1-3
        log.debug("recheck topics change");
        Awaitility.await().untilAsserted(() -> {
            try {
                admin.topics().deletePartitionedTopic(topicName3, true);
            } catch (Exception e) {
                // ignore
            }
            try {
                admin.topics().delete(topicName1, true);
            } catch (Exception e) {
                // ignore
            }
            assertEquals(((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getPartitions().size(), 2);
            assertEquals(((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getConsumers().size(), 2);
            assertEquals(((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getPartitionedTopics().size(), 1);
        });

        // 8. produce data to topic2, verify should receive all the message
        for (int i = 0; i < totalMessages; i++) {
            producer2.send((messagePredicate + "round2-producer2-" + i).getBytes());
        }

        messageSet = 0;
        message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        consumer.unsubscribe();
        consumer.close();
        producer1.close();
        producer2.close();
        producer3.close();
    }

    @Test
    public void testTopicDeletion() throws Exception {
        String baseTopicName = "persistent://my-property/my-ns/pattern-topic-" + System.currentTimeMillis();
        Pattern pattern = Pattern.compile(baseTopicName + ".*");

        // Create 2 topics
        Producer<String> producer1 = pulsarClient.newProducer(Schema.STRING)
                .topic(baseTopicName + "-1")
                .create();
        Producer<String> producer2 = pulsarClient.newProducer(Schema.STRING)
                .topic(baseTopicName + "-2")
                .create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
            .topicsPattern(pattern)
            .patternAutoDiscoveryPeriod(1)
            .subscriptionName("sub")
            .subscribe();

        // Wait topic list watcher creation.
        waitTopicListWatcherCreation(consumer);

        assertTrue(consumer instanceof PatternMultiTopicsConsumerImpl);
        PatternMultiTopicsConsumerImpl<String> consumerImpl = (PatternMultiTopicsConsumerImpl<String>) consumer;

        // 4. verify consumer get methods
        assertSame(consumerImpl.getPattern().inputPattern(), pattern.pattern());
        assertEquals(consumerImpl.getPartitionedTopics().size(), 0);

        producer1.send("msg-1");

        producer1.close();

        Message<String> message = consumer.receive();
        assertEquals(message.getValue(), "msg-1");
        consumer.acknowledge(message);

        // Force delete the topic while the regex consumer is connected
        admin.topics().delete(baseTopicName + "-1", true);

        producer2.send("msg-2");

        message = consumer.receive();
        assertEquals(message.getValue(), "msg-2");
        consumer.acknowledge(message);

        assertEquals(pulsar.getBrokerService().getTopicIfExists(baseTopicName + "-1").join(), Optional.empty());
        assertTrue(pulsar.getBrokerService().getTopicIfExists(baseTopicName + "-2").join().isPresent());
    }

    @Test(dataProvider = "partitioned")
    public void testPatternQuote(boolean partitioned) throws Exception {
        final NamespaceName namespace = NamespaceName.get("public/default");
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final PulsarClientImpl client = (PulsarClientImpl) pulsarClient;
        final LookupService lookup = client.getLookup();
        List<String> expectedRes = new ArrayList<>();
        if (partitioned) {
            admin.topics().createPartitionedTopic(topicName, 2);
            expectedRes.add(TopicName.get(topicName).getPartition(0).toString());
            expectedRes.add(TopicName.get(topicName).getPartition(1).toString());
            Collections.sort(expectedRes);
        } else {
            admin.topics().createNonPartitionedTopic(topicName);
            expectedRes.add(topicName);
        }

        // Verify 1: "java.util.regex.Pattern.quote".
        String pattern1 = java.util.regex.Pattern.quote(topicName);
        List<String> res1 = lookup.getTopicsUnderNamespace(namespace, CommandGetTopicsOfNamespace.Mode.PERSISTENT,
                        pattern1, null).join().getNonPartitionedOrPartitionTopics();
        Collections.sort(res1);
        assertEquals(res1, expectedRes);

        // Verify 2: "com.google.re2j.Pattern.quote"
        String pattern2 = com.google.re2j.Pattern.quote(topicName);
        List<String> res2 = lookup.getTopicsUnderNamespace(namespace, CommandGetTopicsOfNamespace.Mode.PERSISTENT,
                pattern2, null).join().getNonPartitionedOrPartitionTopics();
        Collections.sort(res2);
        assertEquals(res2, expectedRes);

        // Verify 3: "java.util.regex.Pattern.quote" & "^$"
        String pattern3 = "^" + java.util.regex.Pattern.quote(topicName) + "$";
        List<String> res3 = lookup.getTopicsUnderNamespace(namespace, CommandGetTopicsOfNamespace.Mode.PERSISTENT,
                pattern3, null).join().getNonPartitionedOrPartitionTopics();
        Collections.sort(res3);
        assertEquals(res3, expectedRes);

        // Verify 4: "com.google.re2j.Pattern.quote" & "^$"
        String pattern4 = "^" + com.google.re2j.Pattern.quote(topicName) + "$";
        List<String> res4 = lookup.getTopicsUnderNamespace(namespace, CommandGetTopicsOfNamespace.Mode.PERSISTENT,
                pattern4, null).join().getNonPartitionedOrPartitionTopics();
        Collections.sort(res4);
        assertEquals(res4, expectedRes);

        // cleanup.
        if (partitioned) {
            admin.topics().deletePartitionedTopic(topicName, false);
        } else {
            admin.topics().delete(topicName, false);
        }
    }
}
