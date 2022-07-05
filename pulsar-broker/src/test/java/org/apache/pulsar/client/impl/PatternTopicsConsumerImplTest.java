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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
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

        // 4. verify consumer get methods, to get right number of partitions and topics.
        assertSame(pattern, ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern());
        List<String> topics = ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions();
        List<ConsumerImpl<byte[]>> consumers = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getConsumers();

        assertEquals(topics.size(), 6);
        assertEquals(consumers.size(), 6);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);

        topics.forEach(topic -> log.debug("topic: {}", topic));
        consumers.forEach(c -> log.debug("consumer: {}", c.getTopic()));

        IntStream.range(0, topics.size()).forEach(index ->
            assertEquals(consumers.get(index).getTopic(), topics.get(index)));

        ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().forEach(topic -> log.debug("getTopics topic: {}", topic));

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
            messageSet ++;
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
    public void testPubRateOnNonPersistent() throws Exception {
        internalCleanup();
        conf.setMaxPublishRatePerTopicInBytes(10000L);
        conf.setMaxPublishRatePerTopicInMessages(100);
        Thread.sleep(500);
        isTcpLookup = true;
        super.internalSetup();
        super.producerBaseSetup();
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
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
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

        // 4. verify consumer get methods, to get right number of partitions and topics.
        assertSame(pattern, ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern());
        List<String> topics = ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions();
        List<ConsumerImpl<byte[]>> consumers = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getConsumers();

        assertEquals(topics.size(), 1);
        assertEquals(consumers.size(), 1);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 0);

        topics.forEach(topic -> log.debug("topic: {}", topic));
        consumers.forEach(c -> log.debug("consumer: {}", c.getTopic()));

        IntStream.range(0, topics.size()).forEach(index ->
            assertEquals(consumers.get(index).getTopic(), topics.get(index)));

        ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().forEach(topic -> log.debug("getTopics topic: {}", topic));

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
            messageSet ++;
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
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
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

        // 4. verify consumer get methods, to get right number of partitions and topics.
        assertSame(pattern, ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern());
        List<String> topics = ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions();
        List<ConsumerImpl<byte[]>> consumers = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getConsumers();

        assertEquals(topics.size(), 7);
        assertEquals(consumers.size(), 7);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);

        topics.forEach(topic -> log.debug("topic: {}", topic));
        consumers.forEach(c -> log.debug("consumer: {}", c.getTopic()));

        IntStream.range(0, topics.size()).forEach(index ->
            assertEquals(consumers.get(index).getTopic(), topics.get(index)));

        ((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().forEach(topic -> log.debug("getTopics topic: {}", topic));

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

    @Test(timeOut = testTimeout)
    public void testTopicsPatternFilter() {
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1";
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2";
        String topicName3 = "persistent://my-property/my-ns/hello-3";
        String topicName4 = "non-persistent://my-property/my-ns/hello-4";

        List<String> topicsNames = Lists.newArrayList(topicName1, topicName2, topicName3, topicName4);

        Pattern pattern1 = Pattern.compile("persistent://my-property/my-ns/pattern-topic.*");
        List<String> result1 = PulsarClientImpl.topicsPatternFilter(topicsNames, pattern1);
        assertTrue(result1.size() == 2 && result1.contains(topicName1) && result1.contains(topicName2));

        Pattern pattern2 = Pattern.compile("persistent://my-property/my-ns/.*");
        List<String> result2 = PulsarClientImpl.topicsPatternFilter(topicsNames, pattern2);
        assertTrue(result2.size() == 4
            && Stream.of(topicName1, topicName2, topicName3, topicName4).allMatch(result2::contains));
    }

    @Test(timeOut = testTimeout)
    public void testTopicsListMinus() {
        String topicName1 = "persistent://my-property/my-ns/pattern-topic-1";
        String topicName2 = "persistent://my-property/my-ns/pattern-topic-2";
        String topicName3 = "persistent://my-property/my-ns/pattern-topic-3";
        String topicName4 = "persistent://my-property/my-ns/pattern-topic-4";
        String topicName5 = "persistent://my-property/my-ns/pattern-topic-5";
        String topicName6 = "persistent://my-property/my-ns/pattern-topic-6";

        List<String> oldNames = Lists.newArrayList(topicName1, topicName2, topicName3, topicName4);
        List<String> newNames = Lists.newArrayList(topicName3, topicName4, topicName5, topicName6);

        List<String> addedNames = PatternMultiTopicsConsumerImpl.topicsListsMinus(newNames, oldNames);
        List<String> removedNames = PatternMultiTopicsConsumerImpl.topicsListsMinus(oldNames, newNames);

        assertTrue(addedNames.size() == 2 &&
            addedNames.contains(topicName5) &&
            addedNames.contains(topicName6));
        assertTrue(removedNames.size() == 2 &&
            removedNames.contains(topicName1) &&
            removedNames.contains(topicName2));

        // totally 2 different list, should return content of first lists.
        List<String> addedNames2 = PatternMultiTopicsConsumerImpl.topicsListsMinus(addedNames, removedNames);
        assertTrue(addedNames2.size() == 2 &&
            addedNames2.contains(topicName5) &&
            addedNames2.contains(topicName6));

        // 2 same list, should return empty list.
        List<String> addedNames3 = PatternMultiTopicsConsumerImpl.topicsListsMinus(addedNames, addedNames);
        assertEquals(addedNames3.size(), 0);

        // empty list minus: addedNames2.size = 2, addedNames3.size = 0
        List<String> addedNames4 = PatternMultiTopicsConsumerImpl.topicsListsMinus(addedNames2, addedNames3);
        assertEquals(addedNames2.size(), addedNames4.size());
        addedNames4.forEach(name -> assertTrue(addedNames2.contains(name)));

        List<String> addedNames5 = PatternMultiTopicsConsumerImpl.topicsListsMinus(addedNames3, addedNames2);
        assertEquals(addedNames5.size(), 0);
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

        // 2. Create consumer, this should success, but with empty sub-consumser internal
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topicsPattern(pattern)
            .patternAutoDiscoveryPeriod(2)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();

        // 3. verify consumer get methods, to get 5 number of partitions and topics.
        assertSame(pattern, ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern());
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
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();

        // 5. call recheckTopics to subscribe each added topics above
        log.debug("recheck topics change");
        PatternMultiTopicsConsumerImpl<byte[]> consumer1 = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer);
        consumer1.run(consumer1.getRecheckPatternTimeout());
        Thread.sleep(100);

        // 6. verify consumer get methods, to get number of partitions and topics, value 6=1+2+3.
        assertSame(pattern, ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern());
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 6);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 6);
        assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 2);


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
            messageSet ++;
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
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topicsPattern(pattern)
            .patternAutoDiscoveryPeriod(2)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();

        assertTrue(consumer instanceof PatternMultiTopicsConsumerImpl);

        // 4. verify consumer get methods, to get 6 number of partitions and topics: 6=1+2+3
        assertSame(pattern, ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern());
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
            messageSet ++;
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
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();

        // 7. call recheckTopics to subscribe each added topics above, verify topics number: 10=1+2+3+4
        log.debug("recheck topics change");
        PatternMultiTopicsConsumerImpl<byte[]> consumer1 = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer);
        consumer1.run(consumer1.getRecheckPatternTimeout());
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
            messageSet ++;
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
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer<byte[]> producer3 = pulsarClient.newProducer().topic(topicName3)
            .enableBatching(false)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topicsPattern(pattern)
            .patternAutoDiscoveryPeriod(10, TimeUnit.SECONDS)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();

        assertTrue(consumer instanceof PatternMultiTopicsConsumerImpl);

        // 4. verify consumer get methods, to get 0 number of partitions and topics: 6=1+2+3
        assertSame(pattern, ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern());
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
            messageSet ++;
            consumer.acknowledge(message);
            log.debug("Consumer acknowledged : " + new String(message.getData()));
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
        } while (message != null);
        assertEquals(messageSet, totalMessages);

        // 6. remove producer 1,3; verify only consumer 2 left
        // seems no direct way to verify auto-unsubscribe, because this patternConsumer also referenced the topic.
        List<String> topicNames = Lists.newArrayList(topicName2);
        NamespaceService nss = pulsar.getNamespaceService();
        doReturn(CompletableFuture.completedFuture(topicNames)).when(nss)
                .getListOfPersistentTopics(NamespaceName.get("my-property/my-ns"));

        // 7. call recheckTopics to unsubscribe topic 1,3, verify topics number: 2=6-1-3
        log.debug("recheck topics change");
        PatternMultiTopicsConsumerImpl<byte[]> consumer1 = ((PatternMultiTopicsConsumerImpl<byte[]>) consumer);
        consumer1.run(consumer1.getRecheckPatternTimeout());
        Thread.sleep(100);
        assertEquals(((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getPartitions().size(), 2);
        assertEquals(((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getConsumers().size(), 2);
        assertEquals(((PatternMultiTopicsConsumerImpl<byte[]>) consumer).getPartitionedTopics().size(), 1);

        // 8. produce data to topic2, verify should receive all the message
        for (int i = 0; i < totalMessages; i++) {
            producer2.send((messagePredicate + "round2-producer2-" + i).getBytes());
        }

        messageSet = 0;
        message = consumer.receive();
        do {
            assertTrue(message instanceof TopicMessageImpl);
            messageSet ++;
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

        assertTrue(consumer instanceof PatternMultiTopicsConsumerImpl);
        PatternMultiTopicsConsumerImpl<String> consumerImpl = (PatternMultiTopicsConsumerImpl<String>) consumer;

        // 4. verify consumer get methods
        assertSame(consumerImpl.getPattern(), pattern);
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
}
