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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * The stream consumer skips a cumulative ack whose position an earlier plain ack already covered.
 * That must never count an ack issued before the segment's delivery restarted: the broker may
 * not have received it, and the application's re-ack of the redelivered position has to go out.
 */
public class V5StreamConsumerAckWatermarkTest extends V5ClientBaseTest {

    /**
     * A plain ack's future is usually complete by the time the caller has it (immediate acks, no
     * receipts). If the broker rewinds the cursor and redelivers before that completion is
     * observed, observing it later must not resurrect the position as covered.
     */
    @Test
    public void testAckCompletedBeforeARedeliveryDoesNotSuppressTheReack() throws Exception {
        String topic = newScalableTopic(1);
        String subscription = "stale-ack-completion-sub";
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                // Immediate acks: the v4 future is already complete when the v5 layer gets it.
                .acknowledgmentGroupTime(Duration.ZERO)
                .subscribe();
        assertTrue(consumer instanceof ScalableStreamConsumer, "single-topic stream consumer expected");
        ScalableStreamConsumer<String> impl = (ScalableStreamConsumer<String>) consumer;

        int n = 10;
        for (int i = 0; i < n; i++) {
            producer.newMessage().value("v-" + i).send();
        }
        MessageId last = receiveAll(consumer, n);

        var segments = admin.scalableTopics().getStats(topic).getLayout().getSegments().values();
        assertEquals(segments.size(), 1, "single-segment topic");
        var brokerSub = (PersistentSubscription) getTopicReference(segments.iterator().next().getName())
                .orElseThrow().getSubscription(subscription);

        // Between issuing the ack and observing its completion: rewind the cursor to the start,
        // which throws the consumer off and redelivers everything, and drain that redelivery so
        // the receive loop has seen this segment's delivery go backwards.
        impl.beforeAckWatermarkUpdateHook = () -> {
            impl.beforeAckWatermarkUpdateHook = null;
            try {
                // The ack has been written to the connection, but the broker may not have applied it yet.
                // The cursor applies operations in arrival order, so a rewind that overtakes the ack is
                // undone by it: wait until the ack has been applied and persisted before rewinding.
                var cursor = brokerSub.getCursor();
                Awaitility.await().untilAsserted(() -> {
                    assertEquals(subscriptionBacklog(topic, subscription), 0L, "the ack must have been applied");
                    assertEquals(cursor.getPersistentMarkDeletedPosition(), cursor.getMarkDeletedPosition(),
                            "the ack must have been persisted");
                });
                brokerSub.resetCursor(PositionFactory.EARLIEST).get(10, TimeUnit.SECONDS);
                receiveAll(consumer, n);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        consumer.acknowledgeCumulative(last);
        assertNull(impl.beforeAckWatermarkUpdateHook, "the ack must have gone through the seam");
        assertTrue(subscriptionBacklog(topic, subscription) > 0, "the rewind left everything unacked again");

        // Same position, retained id: the completion observed after the rewind must not have
        // marked it as covered.
        consumer.acknowledgeCumulative(last);
        Awaitility.await().untilAsserted(() -> assertEquals(subscriptionBacklog(topic, subscription), 0L,
                "re-acking after the redelivery must advance the cursor"));
    }

    /** Receive {@code n} messages and return the id of the last one. */
    private static MessageId receiveAll(StreamConsumer<String> consumer, int n) throws Exception {
        MessageId last = null;
        for (int i = 0; i < n; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed message #" + i);
            last = msg.id();
        }
        return last;
    }

    /** Backlog of {@code subscription} across the scalable topic's segments, read broker-side. */
    private long subscriptionBacklog(String topic, String subscription) throws Exception {
        long total = 0;
        for (var seg : admin.scalableTopics().getStats(topic).getLayout().getSegments().values()) {
            var ref = getTopicReference(seg.getName());
            if (ref.isEmpty()) {
                continue;
            }
            var sub = ref.get().getSubscription(subscription);
            if (sub != null) {
                total += sub.getNumberOfEntriesInBacklog(true);
            }
        }
        return total;
    }
}
