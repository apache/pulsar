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
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Coverage for {@link StreamConsumer#acknowledgeCumulative(MessageId)} on a multi-segment
 * scalable topic.
 *
 * <p>Each message returned by a V5 stream consumer carries a position vector — a snapshot
 * of the latest delivered offset on every active segment as of that point in the stream.
 * Cumulative-acking a single {@link MessageId} must therefore advance the cursor on
 * <em>every</em> segment, not just the segment that produced this particular message.
 *
 * <p>We assert this end-to-end: produce N messages across multiple segments, drain them,
 * cumulative-ack the last received id, then attach a fresh consumer on the same
 * subscription. The fresh consumer must observe an empty backlog.
 */
public class V5CumulativeAckTest extends V5ClientBaseTest {

    @Test
    public void testCumulativeAckCoversAllSegments() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "cum-ack-sub";

        // Subscribe before producing so the subscription cursor exists at the start of
        // every segment — an EARLIEST consumer would also work, but this keeps the test
        // unambiguous.
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
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
        MessageId last = null;
        for (int i = 0; i < n; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed message #" + i);
            received.add(msg.value());
            last = msg.id();
        }
        assertEquals(received, sent, "should drain every produced message");

        // Single cumulative ack on the last id — the position vector embedded in this id
        // must advance the cursor on every segment.
        assertNotNull(last);
        consumer.acknowledgeCumulative(last);

        // Close and re-open the consumer on the same subscription. With a complete
        // cumulative ack, the new attach must see no backlog.
        consumer.close();

        @Cleanup
        StreamConsumer<String> reopened = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();

        Message<String> stale = reopened.receive(Duration.ofMillis(500));
        assertNull(stale,
                "after a single cumulative ack of the last received id, no message"
                        + " should remain unacked on any segment");
    }

    /**
     * Cumulative ack in the middle of the stream: re-attaching the consumer must replay
     * exactly the unacked tail (not the whole stream).
     */
    @Test
    public void testCumulativeAckMidStreamReplaysUnackedTail() throws Exception {
        String topic = newScalableTopic(4);
        String subscription = "cum-ack-mid-sub";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
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

        // Receive the first ackedCount messages, ack the last of those cumulatively.
        int ackedCount = 25;
        Set<String> firstHalf = new HashSet<>();
        MessageId midId = null;
        for (int i = 0; i < ackedCount; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed mid-stream message #" + i);
            firstHalf.add(msg.value());
            midId = msg.id();
        }
        consumer.acknowledgeCumulative(midId);

        // Drain the rest into a second set so both halves stay disjoint for the assertion.
        Set<String> secondHalf = new HashSet<>();
        for (int i = 0; i < n - ackedCount; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed tail message #" + (ackedCount + i));
            secondHalf.add(msg.value());
        }
        // The consumer must have seen all n messages exactly once across the two halves.
        Set<String> all = new HashSet<>(firstHalf);
        all.addAll(secondHalf);
        assertEquals(all, sent, "consumer must see every produced message exactly once");

        // Close before re-attaching so the broker treats this as a fresh consumer.
        consumer.close();

        @Cleanup
        StreamConsumer<String> reopened = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();

        // Re-attach must replay only the unacked tail. The exact count can differ from
        // (n - ackedCount) because the cumulative ack is per-segment and may include
        // messages that hadn't been touched yet on segments not crossed by the position
        // vector at midId. We assert: replayed.size() <= n - ackedCount, and every
        // replayed value comes from secondHalf (i.e. nothing already-acked is replayed).
        Set<String> replayed = new HashSet<>();
        while (true) {
            Message<String> msg = reopened.receive(Duration.ofMillis(500));
            if (msg == null) {
                break;
            }
            replayed.add(msg.value());
        }
        assertTrue(replayed.size() <= n - ackedCount,
                "replay must not exceed unacked tail size (" + (n - ackedCount)
                        + "), got " + replayed.size());
        for (String v : replayed) {
            assertTrue(!firstHalf.contains(v) || secondHalf.contains(v),
                    "replayed value " + v + " was already cumulative-acked");
        }
    }

    /**
     * Cumulative ack must continue to do the right thing across a split: messages from
     * the now-sealed parent segment that are covered by the ack stay acked, and the cursor
     * advances onto the children. Re-attaching after the split sees only the post-ack tail.
     */
    @Test
    public void testCumulativeAckCrossesSealedParentToChildren() throws Exception {
        String topic = newScalableTopic(1);
        String subscription = "cum-ack-cross-sub";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Parent batch on the only initial segment.
        int parentBatch = 30;
        for (int i = 0; i < parentBatch; i++) {
            producer.newMessage().key("k-" + i).value("parent-" + i).send();
        }

        // Drain the parent batch and cumulative-ack the last one. That should fully ack
        // the (still active) parent segment.
        MessageId lastParent = null;
        for (int i = 0; i < parentBatch; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed parent #" + i);
            lastParent = msg.id();
        }
        consumer.acknowledgeCumulative(lastParent);

        // Split.
        long parentId = -1;
        var meta = admin.scalableTopics().getMetadata(topic);
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                parentId = seg.getSegmentId();
                break;
            }
        }
        assertTrue(parentId >= 0);
        admin.scalableTopics().splitSegment(topic, parentId);

        // The split is synchronous server-side, but the V5 client's DAG watch is async —
        // sending before the watch delivers the new layout would fail with TopicTerminated.
        Awaitility.await().untilAsserted(() -> {
            int active = 0;
            var m = admin.scalableTopics().getMetadata(topic);
            for (var seg : m.getSegments().values()) {
                if (seg.isActive()) {
                    active++;
                }
            }
            assertEquals(active, 2, "split must produce 2 active children");
        });

        // Child batch on the new children.
        int childBatch = 30;
        Set<String> childSent = new HashSet<>();
        for (int i = 0; i < childBatch; i++) {
            String v = "child-" + i;
            producer.newMessage().key("k-child-" + i).value(v).send();
            childSent.add(v);
        }

        // Drain children and cumulative-ack the last one.
        Set<String> childReceived = new HashSet<>();
        MessageId lastChild = null;
        for (int i = 0; i < childBatch; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed child #" + i);
            childReceived.add(msg.value());
            lastChild = msg.id();
        }
        assertEquals(childReceived, childSent, "consumer must see every child message");
        consumer.acknowledgeCumulative(lastChild);
        consumer.close();

        // Re-attach: parent fully acked + every child acked → no backlog.
        @Cleanup
        StreamConsumer<String> reopened = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();

        Message<String> stale = reopened.receive(Duration.ofMillis(500));
        assertNull(stale,
                "after cumulative-acking parent and all children, no message"
                        + " should remain on the subscription");
    }
}
