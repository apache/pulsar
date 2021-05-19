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
package org.apache.pulsar.tests.integration.messaging;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TopicMessagingBase extends MessagingBase {

    protected void nonPartitionedTopicSendAndReceiveWithExclusive(String serviceUrl, boolean isPersistent) throws Exception {
        log.info("-- Starting {} test --", methodName);
        final String topicName = getNonPartitionedTopic("test-non-partitioned-consume-exclusive", isPersistent);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
        @Cleanup
        final Consumer<String> consumer = client.newConsumer(Schema.STRING)
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
        @Cleanup
        final Producer<String> producer = client.newProducer(Schema.STRING)
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
        log.info("-- Exiting {} test --", methodName);
    }

    protected void partitionedTopicSendAndReceiveWithExclusive(String serviceUrl, boolean isPersistent) throws Exception {
        log.info("-- Starting {} test --", methodName);
        final int partitions = 3;
        String topicName = getPartitionedTopic("test-partitioned-consume-exclusive", isPersistent, partitions);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
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
        @Cleanup
        final Producer<String> producer = client.newProducer(Schema.STRING)
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
        log.info("-- Exiting {} test --", methodName);
    }

    protected void nonPartitionedTopicSendAndReceiveWithFailover(String serviceUrl, boolean isPersistent) throws Exception {
        log.info("-- Starting {} test --", methodName);
        final String topicName = getNonPartitionedTopic("test-non-partitioned-consume-failover", isPersistent);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
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
        @Cleanup
        final Producer<String> producer = client.newProducer(Schema.STRING)
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
        log.info("-- Exiting {} test --", methodName);
    }

    protected void partitionedTopicSendAndReceiveWithFailover(String serviceUrl, boolean isPersistent) throws Exception {
        log.info("-- Starting {} test --", methodName);
        final int partitions = 3;
        String topicName = getPartitionedTopic("test-partitioned-consume-failover", isPersistent, partitions);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
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
        @Cleanup
        final Producer<String> producer = client.newProducer(Schema.STRING)
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
        log.info("-- Exiting {} test --", methodName);
    }

    protected void nonPartitionedTopicSendAndReceiveWithShared(String serviceUrl, boolean isPersistent) throws Exception {
        log.info("-- Starting {} test --", methodName);
        final String topicName = getNonPartitionedTopic("test-non-partitioned-consume-shared", isPersistent);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
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
        @Cleanup
        final Producer<String> producer = client.newProducer(Schema.STRING)
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
        log.info("-- Exiting {} test --", methodName);
    }

    protected void partitionedTopicSendAndReceiveWithShared(String serviceUrl, boolean isPersistent) throws Exception {
        log.info("-- Starting {} test --", methodName);
        final int partitions = 3;
        String topicName = getPartitionedTopic("test-partitioned-consume-shared", isPersistent, partitions);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
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
        @Cleanup
        final Producer<String> producer = client.newProducer(Schema.STRING)
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
        log.info("-- Exiting {} test --", methodName);
    }

    protected void nonPartitionedTopicSendAndReceiveWithKeyShared(String serviceUrl, boolean isPersistent) throws Exception {
        log.info("-- Starting {} test --", methodName);
        final String topicName = getNonPartitionedTopic("test-non-partitioned-consume-key-shared", isPersistent);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
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
        @Cleanup
        final Producer<String> producer = client.newProducer(Schema.STRING)
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
        log.info("-- Exiting {} test --", methodName);
    }

    protected void partitionedTopicSendAndReceiveWithKeyShared(String serviceUrl, boolean isPersistent) throws Exception {
        log.info("-- Starting {} test --", methodName);
        final int partitions = 3;
        String topicName = getPartitionedTopic("test-partitioned-consume-key-shared", isPersistent, partitions);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();
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
        @Cleanup
        final Producer<String> producer = client.newProducer(Schema.STRING)
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
        log.info("-- Exiting {} test --", methodName);
    }

}
