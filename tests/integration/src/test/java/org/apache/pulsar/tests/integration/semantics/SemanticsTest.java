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
package org.apache.pulsar.tests.integration.semantics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

/**
 * Test pulsar produce/consume semantics
 */
@Slf4j
public class SemanticsTest extends PulsarTestSuite {

    //
    // Test Basic Publish & Consume Operations
    //

    @Test(dataProvider = "ServiceUrlAndTopics")
    public void testPublishAndConsume(Supplier<String> serviceUrl, boolean isPersistent) throws Exception {
        super.testPublishAndConsume(serviceUrl.get(), isPersistent);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testEffectivelyOnceDisabled(Supplier<String> serviceUrl) throws Exception {
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);

        String topicName = generateTopicName(nsName, "testeffectivelyonce", true);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl.get())
            .build();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic(topicName)
            .subscriptionName("test-sub")
            .ackTimeout(10, TimeUnit.SECONDS)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscribe();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
            .topic(topicName)
            .enableBatching(false)
            .producerName("effectively-once-producer")
            .initialSequenceId(1L)
            .create();

        // send messages
        sendMessagesIdempotency(producer);

        // checkout the result
        checkMessagesIdempotencyDisabled(consumer);
    }

    private static void sendMessagesIdempotency(Producer<String> producer) throws Exception {
        // sending message
        producer.newMessage()
            .sequenceId(1L)
            .value("message-1")
            .send();

        // sending a duplicated message
        producer.newMessage()
            .sequenceId(1L)
            .value("duplicated-message-1")
            .send();

        // sending a second message
        producer.newMessage()
            .sequenceId(2L)
            .value("message-2")
            .send();
    }

    private static void checkMessagesIdempotencyDisabled(Consumer<String> consumer) throws Exception {
        receiveAndAssertMessage(consumer, 1L, "message-1");
        receiveAndAssertMessage(consumer, 1L, "duplicated-message-1");
        receiveAndAssertMessage(consumer, 2L, "message-2");
    }

    private static void receiveAndAssertMessage(Consumer<String> consumer,
                                                long expectedSequenceId,
                                                String expectedContent) throws Exception {
        Message<String> msg = consumer.receive();
        log.info("Received message {}", msg);
        assertEquals(expectedSequenceId, msg.getSequenceId());
        assertEquals(expectedContent, msg.getValue());
    }

    @Test(dataProvider = "ServiceUrls")
    public void testEffectivelyOnceEnabled(Supplier<String> serviceUrl) throws Exception {
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);
        pulsarCluster.enableDeduplication(nsName, true);

        String topicName = generateTopicName(nsName, "testeffectivelyonce", true);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl.get())
            .build();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic(topicName)
            .subscriptionName("test-sub")
            .ackTimeout(10, TimeUnit.SECONDS)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscribe();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
            .topic(topicName)
            .enableBatching(false)
            .producerName("effectively-once-producer")
            .initialSequenceId(1L)
            .create();

        // send messages
        sendMessagesIdempotency(producer);

        // checkout the result
        checkMessagesIdempotencyEnabled(consumer);
    }

    private static void checkMessagesIdempotencyEnabled(Consumer<String> consumer) throws Exception {
        receiveAndAssertMessage(consumer, 1L, "message-1");
        receiveAndAssertMessage(consumer, 2L, "message-2");
    }

    @Test
    public void testSubscriptionInitialPositionOneTopic() throws Exception {
        testSubscriptionInitialPosition(1);
    }

    @Test
    public void testSubscriptionInitialPositionTwoTopics() throws Exception {
        testSubscriptionInitialPosition(2);
    }

    private void testSubscriptionInitialPosition(int numTopics) throws Exception {
        String topicName = generateTopicName("test-subscription-initial-pos", true);

        int numMessages = 10;

        try (PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build()) {

            for (int t = 0; t < numTopics; t++) {
                try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topicName + "-" + t)
                    .create()) {

                    for (int i = 0; i < numMessages; i++) {
                        producer.send("sip-topic-" + t + "-message-" + i);
                    }
                }
            }

            String[] topics = new String[numTopics];
            Map<Integer, AtomicInteger> topicCounters = new HashMap<>(numTopics);
            for (int i = 0; i < numTopics; i++) {
                topics[i] = topicName + "-" + i;
                topicCounters.put(i, new AtomicInteger(0));
            }

            try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topics)
                .subscriptionName("my-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {

                for (int i = 0; i < numTopics * numMessages; i++) {
                    Message<String> m = consumer.receive();
                    int topicIdx;
                    if (numTopics > 1) {
                        String topic = ((TopicMessageIdImpl) m.getMessageId()).getTopicPartitionName();

                        String[] topicParts = StringUtils.split(topic, '-');
                        topicIdx = Integer.parseInt(topicParts[topicParts.length - 1]);
                    } else {
                        topicIdx = 0;
                    }
                    int topicSeq = topicCounters.get(topicIdx).getAndIncrement();

                    assertEquals("sip-topic-" + topicIdx + "-message-" + topicSeq, m.getValue());
                }
            }
        }
    }

    @Test(dataProvider = "ServiceUrls")
    public void testBatchProducing(Supplier<String> serviceUrl) throws Exception {
        String topicName = generateTopicName("testbatchproducing", true);

        int numMessages = 10;

        List<MessageId> producedMsgIds;

        try (PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl.get())
            .build()) {

            try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscribe()) {

                try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topicName)
                    .enableBatching(true)
                    .batchingMaxMessages(5)
                    .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                    .create()) {

                    List<CompletableFuture<MessageId>> sendFutures = Lists.newArrayList();
                    for (int i = 0; i < numMessages; i++) {
                        sendFutures.add(producer.sendAsync("batch-message-" + i));
                    }
                    CompletableFuture.allOf(sendFutures.toArray(new CompletableFuture[numMessages])).get();
                    producedMsgIds = sendFutures.stream().map(future -> future.join()).collect(Collectors.toList());
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<String> m = consumer.receive();
                    assertEquals(producedMsgIds.get(i), m.getMessageId());
                    assertEquals("batch-message-" + i, m.getValue());
                }
            }
        }

        // inspect the message ids
        for (int i = 0; i < 5; i++) {
            assertTrue(producedMsgIds.get(i) instanceof BatchMessageIdImpl);
            BatchMessageIdImpl mid = (BatchMessageIdImpl) producedMsgIds.get(i);
            log.info("Message {} id : {}", i, mid);

            assertEquals(i, mid.getBatchIndex());
        }
        for (int i = 5; i < 10; i++) {
            assertTrue(producedMsgIds.get(i) instanceof BatchMessageIdImpl);
            BatchMessageIdImpl mid = (BatchMessageIdImpl) producedMsgIds.get(i);
            log.info("Message {} id : {}", i, mid);

            assertEquals(i - 5, mid.getBatchIndex());
        }
    }
}
