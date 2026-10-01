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
 * DAG-replay coverage for V5 {@link StreamConsumer}.
 *
 * <p>Companion to the QueueConsumer fix in #25611 ({@code subscribes to active + sealed}).
 * StreamConsumer assignment is controller-driven: the broker's
 * {@link org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator}{@code
 * #computeAssignment} now hands consumers every segment in the DAG, not just the
 * currently-active ones, so a fresh EARLIEST consumer can pick up backlog that lives on
 * already-sealed segments (i.e., messages produced before a split / merge happened).
 *
 * <p>Without this, {@link #testEarliestSubscribePostSplitReadsSealedBacklog()} would only
 * see messages produced after the consumer joined; sealed-segment data would be silently
 * orphaned.
 */
public class V5StreamConsumerDagReplayTest extends V5ClientBaseTest {

    /**
     * Produce N messages into a single-segment topic (everything lands on segment 0),
     * split, then attach a brand-new EARLIEST StreamConsumer. Without the controller
     * including segment 0 in the assignment the consumer would only see post-split sends
     * and the original N messages would be unreachable for this subscription.
     */
    @Test
    public void testEarliestSubscribePostSplitReadsSealedBacklog() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Phase 1: fill segment 0.
        int preSplit = 50;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < preSplit; i++) {
            String v = "pre-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // Split: segment 0 is sealed, two new active children take over.
        long parentSegment = singleActiveSegmentId(topic);
        admin.scalableTopics().splitSegment(topic, parentSegment);
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 2,
                "split must produce 2 active children"));

        // Phase 2: more messages, this time landing on the children.
        int postSplit = 50;
        for (int i = 0; i < postSplit; i++) {
            String v = "post-" + i;
            producer.newMessage().key("kk-" + i).value(v).send();
            sent.add(v);
        }

        // The consumer subscribes for the first time — AFTER both batches and the split.
        // The controller has to include segment 0 (sealed, with backlog) in the initial
        // assignment for this consumer to see the pre-split messages at all.
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("dag-replay-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Ack cumulatively as we go — the broker uses subscription backlog == 0 to decide
        // a sealed segment is drained, so deferring acks until the end would keep the
        // pre-split parent "in backlog" forever and the active children would never get
        // unblocked.
        Set<String> received = new HashSet<>();
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received.size() < sent.size() && System.currentTimeMillis() < deadline) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(1));
            if (msg != null) {
                received.add(msg.value());
                consumer.acknowledgeCumulative(msg.id());
            }
        }
        assertEquals(received, sent,
                "fresh EARLIEST consumer must receive both pre-split (sealed) "
                        + "and post-split (active) messages");
    }

    /**
     * Same shape as the previous test but with a chain of two splits. Sealed-on-sealed
     * (segment 0 → segments 1+2 → segments 3+4 from re-splitting segment 1) must all
     * remain visible to a fresh EARLIEST consumer. Exercises the
     * {@code getAllSegments()} path the assignment now uses, not just direct active
     * descendants.
     */
    @Test
    public void testEarliestSubscribeAfterTwoSplitsReadsAllBacklog() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        Set<String> sent = new HashSet<>();
        // Phase 1: lands on segment 0.
        for (int i = 0; i < 30; i++) {
            String v = "p1-" + i;
            producer.newMessage().key("a-" + i).value(v).send();
            sent.add(v);
        }
        admin.scalableTopics().splitSegment(topic, singleActiveSegmentId(topic));
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 2));

        // Phase 2: lands on the two children.
        for (int i = 0; i < 30; i++) {
            String v = "p2-" + i;
            producer.newMessage().key("b-" + i).value(v).send();
            sent.add(v);
        }

        // Pick one of the two active segments and split it again — produces a deeper DAG.
        long childToSplit = anyActiveSegmentId(topic);
        admin.scalableTopics().splitSegment(topic, childToSplit);
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 3));

        // Phase 3: lands on the now-three active leaves.
        for (int i = 0; i < 30; i++) {
            String v = "p3-" + i;
            producer.newMessage().key("c-" + i).value(v).send();
            sent.add(v);
        }

        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("dag-replay-deep-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Ack cumulatively as we go — the broker uses subscription backlog == 0 to decide
        // a sealed segment is drained, so deferring acks until the end would keep the
        // pre-split parent "in backlog" forever and the active children would never get
        // unblocked.
        Set<String> received = new HashSet<>();
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received.size() < sent.size() && System.currentTimeMillis() < deadline) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(1));
            if (msg != null) {
                received.add(msg.value());
                consumer.acknowledgeCumulative(msg.id());
            }
        }
        assertEquals(received, sent,
                "fresh EARLIEST consumer must receive every message produced across the DAG, "
                        + "including the two sealed generations");
    }

    /**
     * After a fresh EARLIEST consumer drains the sealed backlog, a second consumer joining
     * the same subscription must <em>not</em> re-receive the already-acknowledged sealed
     * messages. The assignment still hands the second consumer some sealed segments
     * (rebalance is by hash range, ties broken by id), but the cursor on those segments
     * is at the end — the v4 layer fires {@code TopicTerminated} immediately and the
     * receive loop closes.
     */
    @Test
    public void testSealedBacklogNotRedeliveredAfterFirstConsumerDrains() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Pre-split batch + split.
        int preSplit = 30;
        for (int i = 0; i < preSplit; i++) {
            producer.newMessage().key("k-" + i).value("pre-" + i).send();
        }
        admin.scalableTopics().splitSegment(topic, singleActiveSegmentId(topic));
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 2));

        // First consumer drains everything (incl. the sealed segment) and acks.
        StreamConsumer<String> first = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("redelivery-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Set<String> firstReceived = new HashSet<>();
        MessageId last = null;
        long deadline = System.currentTimeMillis() + 30_000L;
        while (firstReceived.size() < preSplit && System.currentTimeMillis() < deadline) {
            Message<String> msg = first.receive(Duration.ofSeconds(1));
            if (msg != null) {
                firstReceived.add(msg.value());
                last = msg.id();
            }
        }
        assertEquals(firstReceived.size(), preSplit, "first consumer must drain all pre-split messages");
        assertNotNull(last);
        first.acknowledgeCumulative(last);
        // Give the broker a moment to persist the cumulative ack on every segment.
        Thread.sleep(500);
        first.close();

        // Second consumer joins the same subscription. The sealed segment is in the
        // assignment, but its cursor is past every message — no redelivery.
        @Cleanup
        StreamConsumer<String> second = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("redelivery-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        Message<String> stray = second.receive(Duration.ofSeconds(2));
        assertTrue(stray == null,
                "after the first consumer fully acked the sealed backlog, a fresh "
                        + "subscriber must NOT see those messages, got: "
                        + (stray == null ? "null" : stray.value()));
    }

    // --- Helpers ---

    private long singleActiveSegmentId(String topic) throws Exception {
        var meta = admin.scalableTopics().getMetadata(topic);
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                return seg.getSegmentId();
            }
        }
        throw new AssertionError("no active segment for " + topic);
    }

    private long anyActiveSegmentId(String topic) throws Exception {
        var meta = admin.scalableTopics().getMetadata(topic);
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                return seg.getSegmentId();
            }
        }
        throw new AssertionError("no active segment for " + topic);
    }

    private int activeSegmentCount(String topic) throws Exception {
        int active = 0;
        for (var seg : admin.scalableTopics().getMetadata(topic).getSegments().values()) {
            if (seg.isActive()) {
                active++;
            }
        }
        return active;
    }
}
