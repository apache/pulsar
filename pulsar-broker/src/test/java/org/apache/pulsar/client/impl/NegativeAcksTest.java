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
import static org.testng.Assert.assertNull;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class NegativeAcksTest extends ProducerConsumerBase {

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

    @DataProvider(name = "variations")
    public static Object[][] variations() {
        return new Object[][] {
                // batching / partitions / subscription-type / redelivery-delay-ms / ack-timeout
                { false, false, SubscriptionType.Shared, 100, 0 },
                { false, false, SubscriptionType.Failover, 100, 0 },
                { false, true, SubscriptionType.Shared, 100, 0 },
                { false, true, SubscriptionType.Failover, 100, 0 },
                { true, false, SubscriptionType.Shared, 100, 0 },
                { true, false, SubscriptionType.Failover, 100, 0 },
                { true, true, SubscriptionType.Shared, 100, 0 },
                { true, true, SubscriptionType.Failover, 100, 0 },

                { false, false, SubscriptionType.Shared, 0, 0 },
                { false, false, SubscriptionType.Failover, 0, 0 },
                { false, true, SubscriptionType.Shared, 0, 0 },
                { false, true, SubscriptionType.Failover, 0, 0 },
                { true, false, SubscriptionType.Shared, 0, 0 },
                { true, false, SubscriptionType.Failover, 0, 0 },
                { true, true, SubscriptionType.Shared, 0, 0 },
                { true, true, SubscriptionType.Failover, 0, 0 },

                { false, false, SubscriptionType.Shared, 100, 1000 },
                { false, false, SubscriptionType.Failover, 100, 1000 },
                { false, true, SubscriptionType.Shared, 100, 1000 },
                { false, true, SubscriptionType.Failover, 100, 1000 },
                { true, false, SubscriptionType.Shared, 100, 1000 },
                { true, false, SubscriptionType.Failover, 100, 1000 },
                { true, true, SubscriptionType.Shared, 100, 1000 },
                { true, true, SubscriptionType.Failover, 100, 1000 },

                { false, false, SubscriptionType.Shared, 0, 1000 },
                { false, false, SubscriptionType.Failover, 0, 1000 },
                { false, true, SubscriptionType.Shared, 0, 1000 },
                { false, true, SubscriptionType.Failover, 0, 1000 },
                { true, false, SubscriptionType.Shared, 0, 1000 },
                { true, false, SubscriptionType.Failover, 0, 1000 },
                { true, true, SubscriptionType.Shared, 0, 1000 },
                { true, true, SubscriptionType.Failover, 0, 1000 },
        };
    }

    @Test(dataProvider = "variations")
    public void testNegativeAcks(boolean batching, boolean usePartitions, SubscriptionType subscriptionType,
            int negAcksDelayMillis, int ackTimeout)
            throws Exception {
        log.info("Test negative acks batching={} partitions={} subType={} negAckDelayMs={}", batching, usePartitions,
                subscriptionType, negAcksDelayMillis);
        String topic = "testNegativeAcks-" + System.nanoTime();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub1")
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscriptionType(subscriptionType)
                .negativeAckRedeliveryDelay(negAcksDelayMillis, TimeUnit.MILLISECONDS)
                .ackTimeout(ackTimeout, TimeUnit.MILLISECONDS)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(batching)
                .create();

        Set<String> sentMessages = new HashSet<>();

        final int N = 10;
        for (int i = 0; i < N; i++) {
            String value = "test-" + i;
            producer.sendAsync(value);
            sentMessages.add(value);
        }
        producer.flush();

        for (int i = 0; i < N; i++) {
            Message<String> msg = consumer.receive();
            consumer.negativeAcknowledge(msg);
        }

        Set<String> receivedMessages = new HashSet<>();

        // All the messages should be received again
        for (int i = 0; i < N; i++) {
            Message<String> msg = consumer.receive();
            receivedMessages.add(msg.getValue());
            consumer.acknowledge(msg);
        }

        assertEquals(receivedMessages, sentMessages);

        // There should be no more messages
        assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
        consumer.close();
        producer.close();
    }

    @Test
    public void testBatchNegativeAcks()
            throws Exception {
        
        boolean batching = true;
        SubscriptionType subscriptionType = SubscriptionType.Shared;
        int negAcksDelayMillis = 0;
        int ackTimeout = 0;
        String topic = "testNegativeAcks-" + System.nanoTime();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub1")
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscriptionType(subscriptionType)
                .negativeAckRedeliveryDelay(negAcksDelayMillis, TimeUnit.MILLISECONDS)
                .ackTimeout(ackTimeout, TimeUnit.MILLISECONDS)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(batching)
                .create();

        Set<String> nackedMessages = new HashSet<>();

        final int N = 10;
        final int A = 10;
        for (int i = 0; i < A; i++) {
            String value = "positive-" + i;
            producer.sendAsync(value);
        }
        for (int i = 0; i < N; i++) {
            String value = "negative-" + i;
            producer.sendAsync(value);
        }
        producer.flush();

        // negatively ack negative message
        int nacked = 0;
        for (int i = 0; i < N + A; i++) {
            Message<String> msg = consumer.receive();
            String value = msg.getValue();
            if (value.startsWith("negative")) {
                consumer.negativeAcknowledge(msg);
                nackedMessages.add(value);
                nacked++;
            } else {
                consumer.acknowledge(msg);
            }
        }

        assertEquals(nacked, N);

        Set<String> receivedMessages = new HashSet<>();

        // Only the negatively acknowledged messages should be received again
        for (int i = 0; i < N; i++) {
            Message<String> msg = consumer.receive();
            receivedMessages.add(msg.getValue());
            consumer.acknowledge(msg);
        }

        assertEquals(receivedMessages, nackedMessages);

        // There should be no more messages
        assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
        consumer.close();
        producer.close();
    }
 
    @Test
    public void testBatchNegativeCumulativeAcks()
            throws Exception {
        boolean batching = true;
        SubscriptionType subscriptionType = SubscriptionType.Exclusive;
        int negAcksDelayMillis = 0;
        int ackTimeout = 1000;
        String topic = "testNegativeAcks-" + System.nanoTime();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub1")
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscriptionType(subscriptionType)
                .negativeAckRedeliveryDelay(negAcksDelayMillis, TimeUnit.MILLISECONDS)
                .ackTimeout(ackTimeout, TimeUnit.MILLISECONDS)
                .subscribe();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(batching)
                .create();
        Set<String> nackedMessages = new HashSet<>();
        Message<String> msg = null;
        final int N = 10;
        final int A = 10;
        for (int i = 0; i < A; i++) {
            String value = "positive-" + i;
            producer.sendAsync(value);
        }
        for (int i = 0; i < N; i++) {
            String value = "negative-" + i;
            producer.sendAsync(value);
        }
        producer.flush();
        for (int i = 0; i < A; i++) {
            msg = consumer.receive();
        }
        // Cumulative ack of messages
        consumer.acknowledgeCumulative(msg);
        // negatively ack negative message
        int nacked = 0;
        for (int i = 0; i < N; i++) {
            msg = consumer.receive();
            String value = msg.getValue();
            consumer.negativeAcknowledge(msg);
            nackedMessages.add(value);
            nacked++;
        }
        assertEquals(nacked, N);
        Set<String> receivedMessages = new HashSet<>();
        // Only the negatively acknowledged messages should be received again
        for (int i = 0; i < N; i++) {
            msg = consumer.receive();
            receivedMessages.add(msg.getValue());
        }
        // Cumulative ack of messages
        consumer.acknowledgeCumulative(msg);
        assertEquals(receivedMessages, nackedMessages);
        // Wait for unacked timer to trigger if there are unacked messages
        Thread.sleep(1500);
        // There should be no more messages since they have all been acked
        assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
        consumer.close();
        producer.close();
    }
}
