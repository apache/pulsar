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
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * QueueConsumer counterpart of {@link V5CumulativeAckTest#testCumulativeAckCoversAllSegments()}.
 *
 * <p>QueueConsumer doesn't expose cumulative ack — each {@link QueueConsumer#acknowledge}
 * call covers exactly one message. We assert that individually acking every message on a
 * multi-segment topic correctly advances the cursor on every per-segment v4 consumer:
 * after a close + re-attach on the same subscription, the new consumer must observe an
 * empty backlog.
 */
public class V5QueueConsumerMultiSegmentAckTest extends V5ClientBaseTest {

    @Test
    public void testIndividualAcksCoverAllSegments() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "queue-multi-seg-ack-sub";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int n = 100;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        Set<String> received = new HashSet<>();
        for (int i = 0; i < n; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed message #" + i);
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertEquals(received, sent, "should drain every produced message");

        // Close before re-attaching so the broker treats this as a fresh consumer.
        consumer.close();

        @Cleanup
        QueueConsumer<String> reopened = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();

        Message<String> stale = reopened.receive(Duration.ofMillis(500));
        assertNull(stale,
                "after individually acking every message on every segment, no message"
                        + " should remain on the subscription");
    }

    /**
     * QueueConsumer counterpart of
     * {@link V5CumulativeAckTest#testCumulativeAckMidStreamReplaysUnackedTail()}. Drain
     * the full stream but only ack the first half. After close + re-attach, the unacked
     * tail must be redelivered exactly — no already-acked message comes back, and
     * nothing from the unacked half is dropped.
     */
    @Test
    public void testPartialAcksReplayUnackedOnReattach() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "queue-partial-ack-sub";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int n = 60;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // Drain all n. Ack the first ackedCount messages; leave the rest unacked.
        int ackedCount = 25;
        Set<String> acked = new HashSet<>();
        Set<String> unacked = new HashSet<>();
        for (int i = 0; i < n; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed message #" + i);
            if (i < ackedCount) {
                consumer.acknowledge(msg.id());
                acked.add(msg.value());
            } else {
                unacked.add(msg.value());
            }
        }
        Set<String> all = new HashSet<>(acked);
        all.addAll(unacked);
        assertEquals(all, sent, "consumer must see every produced message exactly once");

        // Close before re-attaching so the broker treats this as a fresh consumer.
        consumer.close();

        @Cleanup
        QueueConsumer<String> reopened = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();

        // Drain the replay. We expect exactly the unacked set — no acked message comes
        // back, and every unacked message is redelivered.
        Set<String> replayed = new HashSet<>();
        while (true) {
            Message<String> msg = reopened.receive(Duration.ofMillis(500));
            if (msg == null) {
                break;
            }
            replayed.add(msg.value());
            reopened.acknowledge(msg.id());
        }
        assertEquals(replayed, unacked,
                "replay must equal exactly the unacked tail (no acked replay, no drop)");
    }
}
