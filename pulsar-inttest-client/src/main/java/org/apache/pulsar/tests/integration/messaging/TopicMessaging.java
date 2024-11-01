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
package org.apache.pulsar.tests.integration.messaging;

import static org.apache.pulsar.tests.integration.utils.IntegTestUtils.getNonPartitionedTopic;
import static org.apache.pulsar.tests.integration.utils.IntegTestUtils.getPartitionedTopic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.integration.IntegTest;

@Slf4j
public class TopicMessaging extends IntegTest {

    public TopicMessaging(PulsarClient client, PulsarAdmin admin) {
        super(client, admin);
    }

    public void nonPartitionedTopicSendAndReceiveWithExclusive(boolean isPersistent) throws Exception {
        log.info("-- Starting nonPartitionedTopicSendAndReceiveWithExclusive test --");
        final String topicName = getNonPartitionedTopic(admin, "test-non-partitioned-consume-exclusive", isPersistent);
        @Cleanup final Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
        try {
            client.newConsumer(Schema.STRING)
                    .topic(topicName)
                    .subscriptionName("test-sub")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe();
            fail("should be failed");
        } catch (PulsarClientException ignore) {
        }
        final int messagesToSend = 10;
        final String producerName = "producerForExclusive";
        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .producerName(producerName)
                .create();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        log.info("public messages complete.");
        receiveMessagesCheckOrderAndDuplicate(Collections.singletonList(consumer), messagesToSend);
        log.info("-- Exiting nonPartitionedTopicSendAndReceiveWithExclusive test --");
    }

    public void partitionedTopicSendAndReceiveWithExclusive(boolean isPersistent) throws Exception {
        log.info("-- Starting partitionedTopicSendAndReceiveWithExclusive test --");
        final int partitions = 3;
        String topicName = getPartitionedTopic(admin, "test-partitioned-consume-exclusive", isPersistent, partitions);
        List<Consumer<String>> consumerList = new ArrayList<>(3);
        for (int i = 0; i < partitions; i++) {
            Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(topicName + "-partition-" + i)
                    .subscriptionName("test-sub")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe();
            consumerList.add(consumer);
        }
        assertEquals(partitions, consumerList.size());
        try {
            client.newConsumer(Schema.STRING)
                    .topic(topicName + "-partition-" + 0)
                    .subscriptionName("test-sub")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe();
            fail("should be failed");
        } catch (PulsarClientException ignore) {
        }
        final int messagesToSend = 9;
        final String producerName = "producerForExclusive";
        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .producerName(producerName)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        log.info("public messages complete.");
        receiveMessagesCheckOrderAndDuplicate(consumerList, messagesToSend);
        // To simulate a consumer crashed
        Consumer<String> crashedConsumer = consumerList.remove(0);
        crashedConsumer.close();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        receiveMessagesCheckOrderAndDuplicate(consumerList, messagesToSend - 3);
        closeConsumers(consumerList);
        log.info("-- Exiting partitionedTopicSendAndReceiveWithExclusive test --");
    }

    public void nonPartitionedTopicSendAndReceiveWithFailover(boolean isPersistent) throws Exception {
        log.info("-- Starting nonPartitionedTopicSendAndReceiveWithFailover test --");
        final String topicName = getNonPartitionedTopic(admin, "test-non-partitioned-consume-failover", isPersistent);
        List<Consumer<String>> consumerList = new ArrayList<>(2);
        final Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        consumerList.add(consumer);
        final Consumer<String> standbyConsumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        assertNotNull(standbyConsumer);
        assertTrue(standbyConsumer.isConnected());
        consumerList.add(standbyConsumer);
        final int messagesToSend = 10;
        final String producerName = "producerForFailover";
        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .producerName(producerName)
                .create();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        log.info("public messages complete.");
        receiveMessagesCheckOrderAndDuplicate(consumerList, messagesToSend);
        // To simulate a consumer crashed
        Consumer<String> crashedConsumer = consumerList.remove(0);
        // wait ack send
        Thread.sleep(3000);
        crashedConsumer.close();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        receiveMessagesCheckOrderAndDuplicate(consumerList, messagesToSend);
        closeConsumers(consumerList);
        log.info("-- Exiting nonPartitionedTopicSendAndReceiveWithFailover test --");
    }

    public void partitionedTopicSendAndReceiveWithFailover(boolean isPersistent) throws Exception {
        log.info("-- Starting partitionedTopicSendAndReceiveWithFailover test --");
        final int partitions = 3;
        String topicName = getPartitionedTopic(admin, "test-partitioned-consume-failover", isPersistent, partitions);
        List<Consumer<String>> consumerList = new ArrayList<>(3);
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        consumerList.add(consumer);
        Consumer<String> standbyConsumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        assertNotNull(standbyConsumer);
        assertTrue(standbyConsumer.isConnected());
        consumerList.add(standbyConsumer);
        assertEquals(consumerList.size(), 2);
        final int messagesToSend = 9;
        final String producerName = "producerForFailover";
        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .producerName(producerName)
                .create();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        log.info("public messages complete.");
        receiveMessagesCheckOrderAndDuplicate(consumerList, messagesToSend);
        // To simulate a consumer crashed
        Consumer<String> crashedConsumer = consumerList.remove(0);
        // wait ack send
        Thread.sleep(3000);
        crashedConsumer.close();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        receiveMessagesCheckOrderAndDuplicate(consumerList, messagesToSend);
        closeConsumers(consumerList);
        log.info("-- Exiting partitionedTopicSendAndReceiveWithFailover test --");
    }

    public void nonPartitionedTopicSendAndReceiveWithShared(boolean isPersistent) throws Exception {
        log.info("-- Starting nonPartitionedTopicSendAndReceiveWithShared test --");
        final String topicName = getNonPartitionedTopic(admin, "test-non-partitioned-consume-shared", isPersistent);
        List<Consumer<String>> consumerList = new ArrayList<>(2);
        final Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
        consumerList.add(consumer);
        Consumer<String> moreConsumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
        assertNotNull(moreConsumer);
        assertTrue(moreConsumer.isConnected());
        consumerList.add(moreConsumer);
        final int messagesToSend = 10;
        final String producerName = "producerForShared";
        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .producerName(producerName)
                .create();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        log.info("public messages complete.");
        receiveMessagesCheckDuplicate(consumerList, messagesToSend);
        // To simulate a consumer crashed
        Consumer<String> crashedConsumer = consumerList.remove(0);
        crashedConsumer.close();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        receiveMessagesCheckDuplicate(consumerList, messagesToSend);
        closeConsumers(consumerList);
        log.info("-- Exiting nonPartitionedTopicSendAndReceiveWithShared test --");
    }

    public void partitionedTopicSendAndReceiveWithShared(boolean isPersistent) throws Exception {
        log.info("-- Starting partitionedTopicSendAndReceiveWithShared test --");
        final int partitions = 3;
        String topicName = getPartitionedTopic(admin, "test-partitioned-consume-shared", isPersistent, partitions);
        List<Consumer<String>> consumerList = new ArrayList<>(3);
        for (int i = 0; i < partitions; i++) {
            Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(topicName)
                    .subscriptionName("test-sub")
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();
            consumerList.add(consumer);
        }
        assertEquals(partitions, consumerList.size());
        final int messagesToSend = 10;
        final String producerName = "producerForFailover";
        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .producerName(producerName)
                .create();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        log.info("public messages complete.");
        receiveMessagesCheckDuplicate(consumerList, messagesToSend);
        // To simulate a consumer crashed
        Consumer<String> crashedConsumer = consumerList.remove(0);
        crashedConsumer.close();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }
        receiveMessagesCheckDuplicate(consumerList, messagesToSend);
        closeConsumers(consumerList);
        log.info("-- Exiting partitionedTopicSendAndReceiveWithShared test --");
    }

    public void nonPartitionedTopicSendAndReceiveWithKeyShared(boolean isPersistent) throws Exception {
        log.info("-- Starting nonPartitionedTopicSendAndReceiveWithKeyShared test --");
        final String topicName = getNonPartitionedTopic(admin, "test-non-partitioned-consume-key-shared", isPersistent);
        List<Consumer<String>> consumerList = new ArrayList<>(2);
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        assertTrue(consumer.isConnected());
        consumerList.add(consumer);
        Consumer<String> moreConsumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        assertNotNull(moreConsumer);
        assertTrue(moreConsumer.isConnected());
        consumerList.add(moreConsumer);
        final int messagesToSend = 10;
        final String producerName = "producerForKeyShared";
        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .producerName(producerName)
                .create();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage()
                    .key(UUID.randomUUID().toString())
                    .value(producer.getProducerName() + "-" + i)
                    .send();
            assertNotNull(messageId);
        }
        log.info("publish messages complete.");
        receiveMessagesCheckStickyKeyAndDuplicate(consumerList, messagesToSend);
        // To simulate a consumer crashed
        Consumer<String> crashedConsumer = consumerList.remove(0);
        crashedConsumer.close();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage()
                    .key(UUID.randomUUID().toString())
                    .value(producer.getProducerName() + "-" + i)
                    .send();
            assertNotNull(messageId);
        }
        receiveMessagesCheckStickyKeyAndDuplicate(consumerList, messagesToSend);
        closeConsumers(consumerList);
        log.info("-- Exiting nonPartitionedTopicSendAndReceiveWithKeyShared test --");
    }

    public void partitionedTopicSendAndReceiveWithKeyShared(boolean isPersistent) throws Exception {
        log.info("-- Starting partitionedTopicSendAndReceiveWithKeyShared test --");
        final int partitions = 3;
        String topicName = getPartitionedTopic(admin, "test-partitioned-consume-key-shared", isPersistent, partitions);
        List<Consumer<String>> consumerList = new ArrayList<>(2);
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        assertTrue(consumer.isConnected());
        consumerList.add(consumer);
        Consumer<String> moreConsumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        assertNotNull(moreConsumer);
        assertTrue(moreConsumer.isConnected());
        consumerList.add(moreConsumer);
        final int messagesToSend = 10;
        final String producerName = "producerForKeyShared";
        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .producerName(producerName)
                .create();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage()
                    .key(UUID.randomUUID().toString())
                    .value(producer.getProducerName() + "-" + i)
                    .send();
            assertNotNull(messageId);
        }
        log.info("publish messages complete.");
        receiveMessagesCheckStickyKeyAndDuplicate(consumerList, messagesToSend);
        // To simulate a consumer crashed
        Consumer<String> crashedConsumer = consumerList.remove(0);
        crashedConsumer.close();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage()
                    .key(UUID.randomUUID().toString())
                    .value(producer.getProducerName() + "-" + i)
                    .send();
            assertNotNull(messageId);
        }
        receiveMessagesCheckStickyKeyAndDuplicate(consumerList, messagesToSend);
        closeConsumers(consumerList);
        log.info("-- Exiting partitionedTopicSendAndReceiveWithKeyShared test --");
    }


    protected static <T extends Comparable<T>> void receiveMessagesCheckOrderAndDuplicate
            (List<Consumer<T>> consumerList, int messagesToReceive) throws PulsarClientException {
        Set<T> messagesReceived = new HashSet<>();
        for (Consumer<T> consumer : consumerList) {
            Message<T> currentReceived;
            Map<String, Message<T>> lastReceivedMap = new HashMap<>();
            while (true) {
                try {
                    currentReceived = consumer.receive(3, TimeUnit.SECONDS);
                } catch (PulsarClientException e) {
                    log.info("no more messages to receive for consumer {}", consumer.getConsumerName());
                    break;
                }
                // Make sure that messages are received in order
                if (currentReceived != null) {
                    consumer.acknowledge(currentReceived);
                    if (lastReceivedMap.containsKey(currentReceived.getTopicName())) {
                        assertTrue(currentReceived.getMessageId().compareTo(
                                        lastReceivedMap.get(currentReceived.getTopicName()).getMessageId()) > 0,
                                "Received messages are not in order.");
                    }
                } else {
                    break;
                }
                lastReceivedMap.put(currentReceived.getTopicName(), currentReceived);
                // Make sure that there are no duplicates
                assertTrue(messagesReceived.add(currentReceived.getValue()),
                        "Received duplicate message " + currentReceived.getValue());
            }
        }
        assertEquals(messagesToReceive, messagesReceived.size());
    }

    protected static <T> void receiveMessagesCheckDuplicate
            (List<Consumer<T>> consumerList, int messagesToReceive) throws PulsarClientException {
        Set<T> messagesReceived = new HashSet<>();
        for (Consumer<T> consumer : consumerList) {
            Message<T> currentReceived = null;
            while (true) {
                try {
                    currentReceived = consumer.receive(3, TimeUnit.SECONDS);
                } catch (PulsarClientException e) {
                    log.info("no more messages to receive for consumer {}", consumer.getConsumerName());
                    break;
                }
                if (currentReceived != null) {
                    consumer.acknowledge(currentReceived);
                    // Make sure that there are no duplicates
                    assertTrue(messagesReceived.add(currentReceived.getValue()),
                            "Received duplicate message " + currentReceived.getValue());
                } else {
                    break;
                }
            }
        }
        assertEquals(messagesReceived.size(), messagesToReceive);
    }

    protected static <T> void receiveMessagesCheckStickyKeyAndDuplicate
            (List<Consumer<T>> consumerList, int messagesToReceive) throws PulsarClientException {
        Map<String, Set<String>> consumerKeys = new HashMap<>();
        Set<T> messagesReceived = new HashSet<>();
        for (Consumer<T> consumer : consumerList) {
            Message<T> currentReceived;
            while (true) {
                try {
                    currentReceived = consumer.receive(3, TimeUnit.SECONDS);
                } catch (PulsarClientException e) {
                    log.info("no more messages to receive for consumer {}", consumer.getConsumerName());
                    break;
                }
                if (currentReceived != null) {
                    consumer.acknowledge(currentReceived);
                    assertNotNull(currentReceived.getKey());
                    consumerKeys.putIfAbsent(consumer.getConsumerName(), new HashSet<>());
                    consumerKeys.get(consumer.getConsumerName()).add(currentReceived.getKey());
                    // Make sure that there are no duplicates
                    assertTrue(messagesReceived.add(currentReceived.getValue()),
                            "Received duplicate message " + currentReceived.getValue());
                } else {
                    break;
                }
            }
        }
        // Make sure key will not be distributed to multiple consumers (except null key)
        Set<String> allKeys = new HashSet<>();
        consumerKeys.forEach((k, v) -> v.stream().filter(Objects::nonNull).forEach(key -> {
            assertTrue(allKeys.add(key),
                    "Key " + key + " is distributed to multiple consumers");
        }));
        assertEquals(messagesReceived.size(), messagesToReceive);
    }

    protected static <T> void closeConsumers(List<Consumer<T>> consumerList) throws PulsarClientException {
        Iterator<Consumer<T>> iterator = consumerList.iterator();
        while (iterator.hasNext()) {
            iterator.next().close();
            iterator.remove();
        }
    }

}
