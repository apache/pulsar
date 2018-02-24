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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
    }

    @Override
    @AfterMethod
    public void cleanup() throws Exception {
        super.internalCleanup();
    }
    @Test(timeOut = testTimeout)
    public void testPatternTopicsSubscribeWithBuilderFail() throws Exception {
        String key = "PatternTopicsSubscribeWithBuilderFail";
        final String subscriptionName = "my-ex-subscription-" + key;

        final String topicName1 = "persistent://prop/use/ns-abc/topic-1-" + key;
        final String topicName2 = "persistent://prop/use/ns-abc/topic-2-" + key;
        final String topicName3 = "persistent://prop/use/ns-abc/topic-3-" + key;
        List<String> topicNames = Lists.newArrayList(topicName1, topicName2, topicName3);
        Pattern pattern = Pattern.compile("persistent://prop/use/ns-abc/pattern-topic.*");


        admin.properties().createProperty("prop", new PropertyAdmin());
        admin.persistentTopics().createPartitionedTopic(topicName2, 2);
        admin.persistentTopics().createPartitionedTopic(topicName3, 3);

        // test failing builder with empty topics
        try {
            Consumer consumer1 = pulsarClient.newConsumer()
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

        try {
            Consumer consumer2 = pulsarClient.newConsumer()
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
    }

    // verify consumer create success, and works well.
    @Test(timeOut = testTimeout)
    public void testBinaryProtoToGetTopicsOfNamespace() throws Exception {
        String key = "BinaryProtoToGetTopics";
        String subscriptionName = "my-ex-subscription-" + key;
        String topicName1 = "persistent://prop/use/ns-abc/pattern-topic-1-" + key;
        String topicName2 = "persistent://prop/use/ns-abc/pattern-topic-2-" + key;
        String topicName3 = "persistent://prop/use/ns-abc/pattern-topic-3-" + key;
        Pattern pattern = Pattern.compile("persistent://prop/use/ns-abc/pattern-topic.*");

        // 1. create partition
        admin.properties().createProperty("prop", new PropertyAdmin());
        admin.persistentTopics().createPartitionedTopic(topicName2, 2);
        admin.persistentTopics().createPartitionedTopic(topicName3, 3);

        // 2. create producer
        ProducerConfiguration producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        String messagePredicate = "my-message-" + key + "-";
        int totalMessages = 30;

        Producer producer1 = pulsarClient.newProducer().topic(topicName1)
            .create();
        Producer producer2 = pulsarClient.newProducer().topic(topicName2)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();
        Producer producer3 = pulsarClient.newProducer().topic(topicName3)
            .messageRoutingMode(org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition)
            .create();

        Consumer consumer = pulsarClient.newConsumer()
            .topicsPattern(pattern)
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
            .receiverQueueSize(4)
            .subscribe();

        // 4. verify consumer get methods, to get right number of partitions and topics.
        assertSame(pattern, ((PatternTopicsConsumerImpl) consumer).getPattern());
        List<String> topics = ((PatternTopicsConsumerImpl) consumer).getPartitionedTopics();
        List<ConsumerImpl> consumers = ((PatternTopicsConsumerImpl) consumer).getConsumers();

        assertEquals(topics.size(), 6);
        assertEquals(consumers.size(), 6);
        assertEquals(((PatternTopicsConsumerImpl) consumer).getTopics().size(), 3);

        topics.forEach(topic -> log.debug("topic: {}", topic));
        consumers.forEach(c -> log.debug("consumer: {}", c.getTopic()));

        IntStream.range(0, topics.size()).forEach(index ->
            assertTrue(topics.get(index).equals(consumers.get(index).getTopic())));

        ((PatternTopicsConsumerImpl) consumer).getTopics().forEach(topic -> log.debug("getTopics topic: {}", topic));

        // 5. produce data
        for (int i = 0; i < totalMessages / 3; i++) {
            producer1.send((messagePredicate + "producer1-" + i).getBytes());
            producer2.send((messagePredicate + "producer2-" + i).getBytes());
            producer3.send((messagePredicate + "producer3-" + i).getBytes());
        }

        // 6. should receive all the message
        int messageSet = 0;
        Message message = consumer.receive();
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

}
