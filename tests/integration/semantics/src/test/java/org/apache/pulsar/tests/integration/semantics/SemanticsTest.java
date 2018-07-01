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

import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.topologies.PulsarClusterTestBase;
import org.testng.annotations.Test;

/**
 * Test pulsar produce/consume semantics
 */
@Slf4j
public class SemanticsTest extends PulsarClusterTestBase {

    //
    // Test Basic Publish & Consume Operations
    //

    @Test(dataProvider = "ServiceUrlAndTopics")
    public void testPublishAndConsume(String serviceUrl, boolean isPersistent) throws Exception {
        String topicName = generateTopicName("testpubconsume", isPersistent);

        int numMessages = 10;

        try (PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .build()) {

            try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscribe()) {

                try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topicName)
                    .create()) {

                    for (int i = 0; i < numMessages; i++) {
                        producer.send("smoke-message-" + i);
                    }
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<String> m = consumer.receive();
                    assertEquals("smoke-message-" + i, m.getValue());
                }
            }
        }
    }

    @Test(dataProvider = "ServiceUrls")
    public void testEffectivelyOnceDisabled(String serviceUrl) throws Exception {
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);

        String topicName = generateTopicName(nsName, "testeffectivelyonce", true);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
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
    public void testEffectivelyOnceEnabled(String serviceUrl) throws Exception {
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);
        pulsarCluster.enableDeduplication(nsName, true);

        String topicName = generateTopicName(nsName, "testeffectivelyonce", true);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
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
}
