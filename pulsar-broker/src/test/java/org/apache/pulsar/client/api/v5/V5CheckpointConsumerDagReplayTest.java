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
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * DAG-replay coverage for V5 {@link CheckpointConsumer} — the checkpoint analogue of
 * {@code V5StreamConsumerDagReplayTest}. A fresh {@link Checkpoint#earliest()} consumer
 * subscribed after a split must still read the messages that ended up on the now-sealed
 * parent segment.
 *
 * <p>Unmanaged {@link CheckpointConsumer} (no {@code consumerGroup(...)}) walks the DAG
 * via {@code DagWatchClient} and subscribes to active + sealed segments directly — it
 * doesn't go through the broker's {@link
 * org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator}. The managed flow
 * (with {@code consumerGroup(...)}) does, so the parent-drain ordering applies there;
 * unlike a {@link StreamConsumer}, a managed {@link CheckpointConsumer} doesn't create
 * subscription cursors on the segment topics (it reads via Readers and tracks position
 * client-side as a {@link Checkpoint}), so the broker treats every sealed segment as
 * effectively drained and unblocks the active children right away.
 */
public class V5CheckpointConsumerDagReplayTest extends V5ClientBaseTest {

    @Test
    public void testEarliestUnmanagedCheckpointPostSplitReadsSealedBacklog() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Phase 1: messages land on segment 0.
        int preSplit = 30;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < preSplit; i++) {
            String v = "pre-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // Split: segment 0 sealed, two new active children.
        long parentSegment = singleActiveSegmentId(topic);
        admin.scalableTopics().splitSegment(topic, parentSegment);
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 2));

        // Phase 2: messages land on the children.
        int postSplit = 30;
        for (int i = 0; i < postSplit; i++) {
            String v = "post-" + i;
            producer.newMessage().key("kk-" + i).value(v).send();
            sent.add(v);
        }

        // Fresh consumer subscribes AFTER split — must see the pre-split sealed backlog
        // alongside the post-split active data. Unmanaged path uses DagWatchClient and
        // covers active + sealed segments, so this works without going through
        // SubscriptionCoordinator at all.
        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        Set<String> received = drainExactly(consumer, sent.size());
        assertEquals(received, sent,
                "unmanaged EARLIEST checkpoint consumer must receive both pre-split "
                        + "(sealed) and post-split (active) messages");
    }

    @Test
    public void testEarliestUnmanagedCheckpointAfterTwoSplitsReadsAllBacklog() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            String v = "p1-" + i;
            producer.newMessage().key("a-" + i).value(v).send();
            sent.add(v);
        }
        admin.scalableTopics().splitSegment(topic, singleActiveSegmentId(topic));
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 2));

        for (int i = 0; i < 20; i++) {
            String v = "p2-" + i;
            producer.newMessage().key("b-" + i).value(v).send();
            sent.add(v);
        }
        admin.scalableTopics().splitSegment(topic, anyActiveSegmentId(topic));
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 3));

        for (int i = 0; i < 20; i++) {
            String v = "p3-" + i;
            producer.newMessage().key("c-" + i).value(v).send();
            sent.add(v);
        }

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        Set<String> received = drainExactly(consumer, sent.size());
        assertEquals(received, sent,
                "unmanaged EARLIEST checkpoint consumer must receive every message produced "
                        + "across the DAG, including both sealed generations");
    }

    /**
     * Same scenario as the unmanaged version, but with {@code consumerGroup(...)}: the
     * broker drives the assignment via {@link
     * org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator}. Checkpoint
     * consumers don't create per-segment subscription cursors, so the broker's drain
     * check sees no subscription on the sealed parent → no backlog to worry about → the
     * active children are unblocked right away.
     */
    @Test
    public void testEarliestManagedCheckpointPostSplitReadsSealedBacklog() throws Exception {
        String topic = newScalableTopic(1);
        String group = "managed-dag-replay";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        int preSplit = 30;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < preSplit; i++) {
            String v = "pre-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        admin.scalableTopics().splitSegment(topic, singleActiveSegmentId(topic));
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 2));

        int postSplit = 30;
        for (int i = 0; i < postSplit; i++) {
            String v = "post-" + i;
            producer.newMessage().key("kk-" + i).value(v).send();
            sent.add(v);
        }

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .consumerGroup(group)
                .startPosition(Checkpoint.earliest())
                .create();

        Set<String> received = drainExactly(consumer, sent.size());
        assertEquals(received, sent,
                "managed EARLIEST checkpoint consumer must receive both pre-split "
                        + "(sealed) and post-split (active) messages");
    }

    @Test
    public void testEarliestManagedCheckpointAfterTwoSplitsReadsAllBacklog() throws Exception {
        String topic = newScalableTopic(1);
        String group = "managed-dag-replay-deep";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            String v = "p1-" + i;
            producer.newMessage().key("a-" + i).value(v).send();
            sent.add(v);
        }
        admin.scalableTopics().splitSegment(topic, singleActiveSegmentId(topic));
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 2));

        for (int i = 0; i < 20; i++) {
            String v = "p2-" + i;
            producer.newMessage().key("b-" + i).value(v).send();
            sent.add(v);
        }
        admin.scalableTopics().splitSegment(topic, anyActiveSegmentId(topic));
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 3));

        for (int i = 0; i < 20; i++) {
            String v = "p3-" + i;
            producer.newMessage().key("c-" + i).value(v).send();
            sent.add(v);
        }

        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .consumerGroup(group)
                .startPosition(Checkpoint.earliest())
                .create();

        Set<String> received = drainExactly(consumer, sent.size());
        assertEquals(received, sent,
                "managed EARLIEST checkpoint consumer must receive every message produced "
                        + "across the DAG, including both sealed generations");
    }

    // --- Helpers ---

    private Set<String> drainExactly(CheckpointConsumer<String> consumer, int expected) throws Exception {
        Set<String> received = new HashSet<>();
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received.size() < expected && System.currentTimeMillis() < deadline) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(1));
            if (msg != null) {
                received.add(msg.value());
            }
        }
        return received;
    }

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
        return singleActiveSegmentId(topic);
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
