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
package org.apache.pulsar.broker.service.persistent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DelayedDeliveryTest extends ProducerConsumerBase {

    @Override
    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testDelayedDelivery()
            throws Exception {
        String topic = "testNegativeAcks-" + System.nanoTime();

        @Cleanup
        Consumer<String> failoverConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("failover-sub")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        @Cleanup
        Consumer<String> sharedConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }

        producer.flush();

        // Failover consumer will receive the messages immediately while
        // the shared consumer will get them after the delay
        Message<String> msg = sharedConsumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        for (int i = 0; i < 10; i++) {
            msg = failoverConsumer.receive(100, TimeUnit.MILLISECONDS);
            assertEquals(msg.getValue(), "msg-" + i);
        }

        Set<String> receivedMsgs = new TreeSet<>();
        for (int i = 0; i < 10; i++) {
            msg = sharedConsumer.receive(10, TimeUnit.SECONDS);
            receivedMsgs.add(msg.getValue());
        }

        assertEquals(receivedMsgs.size(), 10);
        for (int i = 0; i < 10; i++) {
            assertTrue(receivedMsgs.contains("msg-" + i));
        }
    }

    @Test
    public void testInterleavedMessages()
            throws Exception {
        String topic = "testInterleavedMessages-" + System.nanoTime();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 10; i++) {
            // Publish 1 message without delay and 1 with delay
            producer.newMessage()
                    .value("immediate-msg-" + i)
                    .sendAsync();

            producer.newMessage()
                    .value("delayed-msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }

        producer.flush();

        // Failover consumer will receive the messages immediately while
        // the shared consumer will get them after the delay
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertEquals(msg.getValue(), "immediate-msg-" + i);
            consumer.acknowledge(msg);
        }

        // Delayed messages might not come in same exact order
        Set<String> delayedMessages = new TreeSet<>();
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive(10, TimeUnit.SECONDS);
            delayedMessages.add(msg.getValue());
            consumer.acknowledge(msg);
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(delayedMessages.contains("delayed-msg-" + i));
        }
    }

    @Test
    public void testEverythingFilteredInMultipleReads()
            throws Exception {
        String topic = "testEverythingFilteredInMultipleReads-" + System.nanoTime();

        @Cleanup
        Consumer<String> sharedConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .send();
        }

        Thread.sleep(1000);

        // Write a 2nd batch of messages
        for (int i = 10; i < 20; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .send();
        }

        Message<String> msg = sharedConsumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        Set<String> receivedMsgs = new TreeSet<>();
        for (int i = 0; i < 20; i++) {
            msg = sharedConsumer.receive(10, TimeUnit.SECONDS);
            receivedMsgs.add(msg.getValue());
        }

        assertEquals(receivedMsgs.size(), 20);
        for (int i = 0; i < 10; i++) {
            assertTrue(receivedMsgs.contains("msg-" + i));
        }
    }

    @Test
    public void testDelayedDeliveryWithMultipleConcurrentReadEntries()
            throws Exception {
        String topic = "persistent://public/default/testDelayedDelivery-" + System.nanoTime();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(1) // Use small prefecthing to simulate the multiple read batches
                .subscribe();

        // Simulate race condition with high frequency of calls to dispatcher.readMoreEntries()
        PersistentDispatcherMultipleConsumers d = (PersistentDispatcherMultipleConsumers) ((PersistentTopic) pulsar
                .getBrokerService().getTopicReference(topic).get()).getSubscription("shared-sub").getDispatcher();
        Thread t = new Thread(() -> {
            while (true) {
                synchronized (d) {
                    d.readMoreEntries();
                }

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        t.start();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        final int N = 1000;

        for (int i = 0; i < N; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(5, TimeUnit.SECONDS)
                    .sendAsync();
        }

        producer.flush();

        Message<String> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        Set<String> receivedMsgs = new TreeSet<>();
        for (int i = 0; i < N; i++) {
            msg = consumer.receive(10, TimeUnit.SECONDS);
            receivedMsgs.add(msg.getValue());
        }

        assertEquals(receivedMsgs.size(), N);
        for (int i = 0; i < N; i++) {
            assertTrue(receivedMsgs.contains("msg-" + i));
        }
        t.interrupt();
    }
}
