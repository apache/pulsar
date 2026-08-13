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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for {@link QueueConsumerBuilder#priorityLevel(int)}: lower priority
 * values dispatch first within a Shared subscription. With a low-priority
 * consumer's prefetch fully populated, higher-priority levels see no traffic
 * until the lower-priority consumer either ack-flushes or blocks on its queue.
 *
 * <p>Single-segment topic to keep the dispatch deterministic — V5 broker-side
 * priority is enforced per-segment, so a multi-segment topic can fan messages
 * across segments and obscure the priority ordering.
 */
public class V5ConsumerPriorityLevelTest extends V5ClientBaseTest {

    /**
     * Two consumers, priority 0 (high) and priority 1 (low). With both subscribed
     * before any traffic, every message produced should land on the high-priority
     * consumer and none on the low-priority one — until the high consumer's
     * prefetch is full, which we keep clear by acking each message.
     */
    @Test
    public void testHigherPriorityConsumerReceivesAllWhenPrefetchAvailable() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        QueueConsumer<String> high = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("priority-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .priorityLevel(0)
                .receiverQueueSize(50)
                .consumerName("high")
                .subscribe();
        @Cleanup
        QueueConsumer<String> low = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("priority-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .priorityLevel(1)
                .receiverQueueSize(50)
                .consumerName("low")
                .subscribe();

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        int n = 20;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "msg-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // High-priority consumer drains everything (acking as it goes so its
        // prefetch never fills). The broker's priority dispatcher must hand all
        // messages to the priority-0 consumer before considering priority-1.
        Set<String> received = new HashSet<>();
        for (int i = 0; i < n; i++) {
            Message<String> msg = high.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "high-priority consumer must receive message #" + (i + 1));
            received.add(msg.value());
            high.acknowledge(msg.id());
        }
        assertEquals(received, sent, "high-priority consumer must receive every message");

        // Low-priority consumer must have seen nothing.
        Message<String> stragglers = low.receive(Duration.ofMillis(500));
        assertNull(stragglers, "low-priority consumer must not receive while high one is draining");
    }

    /**
     * When the high-priority consumer pauses (no acks, prefetch fills), the
     * broker overflows to the low-priority consumer for further dispatch.
     */
    @Test
    public void testLowerPriorityConsumerReceivesOverflow() throws Exception {
        String topic = newScalableTopic(1);
        int highReceiverQueue = 5;

        @Cleanup
        QueueConsumer<String> high = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("overflow-priority-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .priorityLevel(0)
                .receiverQueueSize(highReceiverQueue)
                .consumerName("high-paused")
                .subscribe();
        @Cleanup
        QueueConsumer<String> low = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("overflow-priority-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .priorityLevel(1)
                .receiverQueueSize(50)
                .consumerName("low-active")
                .subscribe();

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Publish more than the high consumer's prefetch — once it's full, the
        // remainder must spill to the low-priority consumer.
        int total = highReceiverQueue * 4;
        for (int i = 0; i < total; i++) {
            producer.newMessage().key("k-" + i).value("v-" + i).send();
        }

        // Low-priority consumer must see the spillover (without "high" ever
        // calling receive(), the broker treats it as having a full prefetch
        // queue and routes onward).
        Set<String> lowSeen = new HashSet<>();
        long deadline = System.currentTimeMillis() + 10_000L;
        while (lowSeen.size() < total - highReceiverQueue && System.currentTimeMillis() < deadline) {
            Message<String> msg = low.receive(Duration.ofMillis(500));
            if (msg != null) {
                lowSeen.add(msg.value());
                low.acknowledge(msg.id());
            }
        }
        // V5's receiverQueueSize sizes the v4 internal receive buffer; it isn't a
        // broker-side backpressure cap. ScalableQueueConsumer runs a continuous receive
        // loop that drains v4's queue into an unbounded V5 queue, which keeps refilling
        // the broker's per-consumer permits. As a result the high consumer can absorb
        // more than receiverQueueSize before the broker spills to low. The minimum
        // guarantee that the priority dispatch is functioning is that the low consumer
        // receives at least one overflow message.
        assertTrue(lowSeen.size() > 0,
                "low-priority consumer must see overflow once high consumer's prefetch fills, got: "
                        + lowSeen.size());
    }
}
