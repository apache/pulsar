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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.tests.EnumValuesDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class MessageIdTest extends BrokerTestBase {
    private static final Logger log = LoggerFactory.getLogger(MessageIdTest.class);

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test(timeOut = 10000, dataProviderClass = EnumValuesDataProvider.class, dataProvider = "values")
    public void producerSendAsync(TopicType topicType) throws PulsarClientException, PulsarAdminException {
        // Given
        String key = "producerSendAsync-" + topicType;
        final String topicName = "persistent://prop/cluster/namespace/topic-" + key;
        final String subscriptionName = "my-subscription-" + key;
        final String messagePrefix = "my-message-" + key + "-";
        final int numberOfMessages = 30;
        if (topicType == TopicType.PARTITIONED) {
            int numberOfPartitions = 3;
            admin.topics().createPartitionedTopic(topicName, numberOfPartitions);
        }

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscribe();

        // When
        // Messages are published asynchronously
        List<Future<MessageId>> futures = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePrefix + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }

        // Then
        // expect that the Message Ids of subsequently sent messages are in ascending order
        Set<MessageId> messageIds = new HashSet<>();
        MessageIdImpl previousMessageId = null;
        for (Future<MessageId> f : futures) {
            try {
                MessageIdImpl currentMessageId = (MessageIdImpl) f.get();
                if (previousMessageId != null) {
                    assertTrue(currentMessageId.compareTo(previousMessageId) > 0,
                            "Message Ids should be in ascending order");
                }
                messageIds.add(currentMessageId);
                previousMessageId = currentMessageId;
            } catch (Exception e) {
                fail("Failed to publish message", e);
            }
        }

        // And
        // expect that there's a message id for each sent out message
        // and that all messages have been received by the consumer
        log.info("Message IDs = {}", messageIds);
        assertEquals(messageIds.size(), numberOfMessages, "Not all messages published successfully");

        for (int i = 0; i < numberOfMessages; i++) {
            Message<byte[]> message = consumer.receive();
            assertEquals(new String(message.getData()), messagePrefix + i);
            MessageId messageId = message.getMessageId();
            if (topicType == TopicType.PARTITIONED) {
                messageId = ((TopicMessageIdImpl) messageId).getInnerMessageId();
            }
            assertTrue(messageIds.remove(messageId), "Failed to receive message");
        }
        log.info("Remaining message IDs = {}", messageIds);
        assertEquals(messageIds.size(), 0, "Not all messages received successfully");
        consumer.unsubscribe();
    }

    @Test(timeOut = 10000, dataProviderClass = EnumValuesDataProvider.class, dataProvider = "values")
    public void producerSend(TopicType topicType) throws PulsarClientException, PulsarAdminException {
        // Given
        String key = "producerSend-" + topicType;
        final String topicName = "persistent://prop/cluster/namespace/topic-" + key;
        final String subscriptionName = "my-subscription-" + key;
        final String messagePrefix = "my-message-" + key + "-";
        final int numberOfMessages = 30;
        if (topicType == TopicType.PARTITIONED) {
            int numberOfPartitions = 7;
            admin.topics().createPartitionedTopic(topicName, numberOfPartitions);
        }

        Producer<byte[]> producer = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(topicName)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscribe();

        // When
        // Messages are published
        Set<MessageId> messageIds = new HashSet<>();
        for (int i = 0; i < numberOfMessages; i++) {
            String message = messagePrefix + i;
            messageIds.add(producer.send(message.getBytes()));
        }

        // Then
        // expect that the Message Ids of subsequently sent messages are in ascending order
        log.info("Message IDs = {}", messageIds);
        assertEquals(messageIds.size(), numberOfMessages, "Not all messages published successfully");

        for (int i = 0; i < numberOfMessages; i++) {
            MessageId messageId = consumer.receive().getMessageId();
            if (topicType == TopicType.PARTITIONED) {
                messageId = ((TopicMessageIdImpl) messageId).getInnerMessageId();
            }
            assertTrue(messageIds.remove(messageId), "Failed to receive Message");
        }
        log.info("Remaining message IDs = {}", messageIds);
        assertEquals(messageIds.size(), 0, "Not all messages received successfully");
        consumer.unsubscribe();
    }
}
