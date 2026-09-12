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
import java.util.concurrent.ConcurrentHashMap;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Coverage for {@link CheckpointConsumerBuilder#consumerGroup(String)} — multiple
 * checkpoint consumers in the same group share the topic's segments via the broker's
 * subscription coordinator, identical to the StreamConsumer pattern but with
 * client-side position state instead of broker-side cursors.
 *
 * <p>Two key invariants we assert here that don't apply to StreamConsumer:
 * <ul>
 *   <li><b>Position state is client-side.</b> A checkpoint consumer resumes from
 *       whatever {@link Checkpoint} the application passes at create time. The broker
 *       does not track per-consumer cursors.</li>
 *   <li><b>No leftover metadata.</b> Once every member of a group goes away and the
 *       grace timer has fired, the broker's stats report no subscription for that
 *       group on the topic.</li>
 * </ul>
 */
public class V5CheckpointConsumerGroupTest extends V5ClientBaseTest {

    @Test
    public void testConsumersInGroupShareSegments() throws Exception {
        String topic = newScalableTopic(4);
        String group = "group-share";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Two checkpoint consumers in the same group: each should end up with a
        // disjoint subset of segments via the controller's assignment.
        @Cleanup
        CheckpointConsumer<String> a = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .consumerGroup(group)
                .startPosition(Checkpoint.earliest())
                .create();
        @Cleanup
        CheckpointConsumer<String> b = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .consumerGroup(group)
                .startPosition(Checkpoint.earliest())
                .create();

        int n = 100;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        Set<String> received = ConcurrentHashMap.newKeySet();
        Set<String> aGot = ConcurrentHashMap.newKeySet();
        Set<String> bGot = ConcurrentHashMap.newKeySet();
        Thread t1 = drainTo(a, received, aGot);
        Thread t2 = drainTo(b, received, bGot);
        t1.join();
        t2.join();

        assertEquals(received, sent, "every message must be delivered exactly once across the group");

        Set<String> overlap = new HashSet<>(aGot);
        overlap.retainAll(bGot);
        assertTrue(overlap.isEmpty(), "no message should be delivered to both consumers, overlap=" + overlap);

        assertTrue(!aGot.isEmpty() && !bGot.isEmpty(),
                "controller must split segments across both consumers"
                        + " (a=" + aGot.size() + " b=" + bGot.size() + ")");
    }

    @Test
    public void testGroupScalesUp() throws Exception {
        String topic = newScalableTopic(2);
        String group = "group-scale";

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Phase 1: a single consumer in the group serves both segments.
        CheckpointConsumer<String> c1 = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .consumerGroup(group)
                .startPosition(Checkpoint.earliest())
                .create();

        int phaseN = 30;
        produceBatch(producer, "p1-", phaseN);
        Set<String> p1Received = drainExpected(c1, phaseN);
        assertEquals(p1Received.size(), phaseN, "single consumer should drain its phase exactly");

        // Phase 2: add a second consumer. CheckpointConsumer has no broker-side cursor,
        // so every consumer starts from whatever Checkpoint it provides. Use latest so
        // c2 only reads messages produced after it joins (avoids re-reading phase 1
        // from the segments it's assigned).
        @Cleanup
        CheckpointConsumer<String> c2 = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .consumerGroup(group)
                .startPosition(Checkpoint.latest())
                .create();

        produceBatch(producer, "p2-", phaseN);
        Set<String> c1Phase2 = ConcurrentHashMap.newKeySet();
        Set<String> c2Phase2 = ConcurrentHashMap.newKeySet();
        Set<String> p2Received = ConcurrentHashMap.newKeySet();
        Thread d1 = drainTo(c1, p2Received, c1Phase2);
        Thread d2 = drainTo(c2, p2Received, c2Phase2);
        d1.join();
        d2.join();
        assertEquals(p2Received.size(), phaseN, "phase 2 must be fully delivered across both consumers");
        assertTrue(!c1Phase2.isEmpty() && !c2Phase2.isEmpty(),
                "phase 2 must split across both consumers: c1=" + c1Phase2.size()
                        + " c2=" + c2Phase2.size());

        c1.close();
    }

    @Test
    public void testResumesFromClientProvidedCheckpoint() throws Exception {
        // Single segment so per-iteration value assertions are deterministic — order
        // is preserved within a segment.
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Use the unmanaged (no-group) checkpoint consumer here: this test isolates
        // the "position state lives entirely in the client checkpoint" property.
        // Adding a consumer group would mix in the broker's session-tracking — that's
        // covered by the other tests in this class.
        CheckpointConsumer<String> first = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        int prefix = 30;
        for (int i = 0; i < prefix; i++) {
            producer.newMessage().key("k-" + i).value("v-" + i).send();
        }
        for (int i = 0; i < prefix; i++) {
            Message<String> msg = first.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed prefix message #" + i);
            assertEquals(msg.value(), "v-" + i, "prefix message out of order");
            assertEquals(msg.key().orElse(null), "k-" + i, "prefix key out of order");
        }
        Checkpoint mid = first.checkpoint();

        // Produce more data, then close the consumer entirely.
        int suffix = 30;
        for (int i = 0; i < suffix; i++) {
            producer.newMessage().key("k-" + (prefix + i)).value("v-" + (prefix + i)).send();
        }
        first.close();

        // A fresh consumer that hands the saved checkpoint as its start position must
        // resume from the message immediately after the one that produced the
        // checkpoint — no broker state is involved.
        @Cleanup
        CheckpointConsumer<String> second = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(mid)
                .create();

        for (int i = 0; i < suffix; i++) {
            Message<String> msg = second.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed resumed message #" + i);
            assertEquals(msg.value(), "v-" + (prefix + i),
                    "resume from checkpoint delivered the wrong value at index " + i);
            assertEquals(msg.key().orElse(null), "k-" + (prefix + i),
                    "resume from checkpoint delivered the wrong key at index " + i);
        }
        assertNull(second.receive(Duration.ofMillis(200)),
                "no extra messages should arrive past the produced suffix");
    }

    @Test
    public void testNoMetadataLeftAfterAllGroupMembersClose() throws Exception {
        String topic = newScalableTopic(2);
        String group = "group-cleanup";

        // Open two members of the group on dedicated clients. Closing the client
        // severs the underlying connection, which is what the broker observes as a
        // disconnect — only then does the grace timer arm and eventually evict the
        // session. (consumer.close() alone removes only the local registration.)
        PulsarClient cli1 = newV5Client();
        cli1.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .consumerGroup(group)
                .startPosition(Checkpoint.earliest())
                .create();
        PulsarClient cli2 = newV5Client();
        cli2.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .consumerGroup(group)
                .startPosition(Checkpoint.earliest())
                .create();

        cli1.close();
        cli2.close();

        // Once the broker grace timer fires (default 60s; SharedPulsarCluster will
        // lower this once PR #25619 lands), the controller evicts the sessions and
        // deletes the persisted registrations. After that, the topic stats must report
        // zero consumers in the group.
        Awaitility.await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
            var stats = admin.scalableTopics().getStats(topic);
            var sub = stats.getSubscriptions().get(group);
            assertTrue(sub == null || sub.consumerCount() == 0,
                    "group '" + group + "' must leave no consumers behind, got "
                            + (sub == null ? "null" : sub.consumerCount() + " consumers"));
        });
    }

    // --- Helpers ---

    private void produceBatch(Producer<String> producer, String prefix, int n) throws Exception {
        for (int i = 0; i < n; i++) {
            producer.newMessage().key("k-" + i).value(prefix + i).send();
        }
    }

    private Set<String> drainExpected(CheckpointConsumer<String> consumer, int expected) throws Exception {
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

    private Thread drainTo(CheckpointConsumer<String> consumer, Set<String> all, Set<String> mine) {
        Thread t = new Thread(() -> {
            try {
                while (true) {
                    Message<String> msg = consumer.receive(Duration.ofSeconds(1));
                    if (msg == null) {
                        return;
                    }
                    all.add(msg.value());
                    mine.add(msg.value());
                }
            } catch (Exception ignored) {
            }
        }, "checkpoint-consumer-drainer");
        t.start();
        return t;
    }

    @Test
    public void testReceiveMultiReturnsBatch() throws Exception {
        // Single segment so messages stay in send order.
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        CheckpointConsumer<String> consumer = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        int n = 20;
        for (int i = 0; i < n; i++) {
            producer.newMessage().key("k-" + i).value("v-" + i).send();
        }

        // receiveMulti returns up to maxNumMessages within the timeout, ordered as
        // they arrived from the segment. Drain the full batch in chunks of 8.
        Set<String> received = new HashSet<>();
        long deadline = System.currentTimeMillis() + 10_000L;
        while (received.size() < n && System.currentTimeMillis() < deadline) {
            Messages<String> batch = consumer.receiveMulti(8, Duration.ofSeconds(1));
            assertNotNull(batch);
            for (Message<String> msg : batch) {
                received.add(msg.value());
            }
        }
        assertEquals(received.size(), n,
                "receiveMulti must surface every produced message exactly once");
    }

    @Test
    public void testCheckpointResumeAcrossSplit() throws Exception {
        // Single initial segment so the checkpoint position is unambiguous.
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        // Read the prefix and take a mid-stream checkpoint (still on the original
        // single segment).
        CheckpointConsumer<String> first = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        int prefix = 20;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < prefix; i++) {
            String v = "pre-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }
        Set<String> consumed = new HashSet<>();
        for (int i = 0; i < prefix / 2; i++) {
            Message<String> msg = first.receive(Duration.ofSeconds(5));
            assertNotNull(msg);
            consumed.add(msg.value());
        }
        Checkpoint mid = first.checkpoint();
        first.close();

        // Split the original segment, then produce more messages — these route to the
        // new active children.
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

        int suffix = 20;
        for (int i = 0; i < suffix; i++) {
            String v = "post-" + i;
            producer.newMessage().key("k-suffix-" + i).value(v).send();
            sent.add(v);
        }

        // Resume from the saved checkpoint. The unmanaged consumer subscribes to
        // active + sealed segments, so the saved position on the now-sealed parent
        // still drains its remainder, and the children deliver the post-split data.
        @Cleanup
        CheckpointConsumer<String> resumed = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(mid)
                .create();

        Set<String> received = new HashSet<>();
        // Expected: prefix/2 unread parent messages + all suffix messages.
        int expected = (prefix - prefix / 2) + suffix;
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received.size() < expected && System.currentTimeMillis() < deadline) {
            Message<String> msg = resumed.receive(Duration.ofSeconds(1));
            if (msg != null) {
                received.add(msg.value());
            }
        }
        Set<String> expectedSet = new HashSet<>(sent);
        expectedSet.removeAll(consumed);
        assertEquals(received, expectedSet,
                "resume across split must replay the parent tail and every child message");
    }

    @Test
    public void testCheckpointResumeAcrossMerge() throws Exception {
        // Two initial segments so we can merge them.
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();

        CheckpointConsumer<String> first = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        int prefix = 20;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < prefix; i++) {
            String v = "pre-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }
        Set<String> consumed = new HashSet<>();
        for (int i = 0; i < prefix / 2; i++) {
            Message<String> msg = first.receive(Duration.ofSeconds(5));
            assertNotNull(msg);
            consumed.add(msg.value());
        }
        Checkpoint mid = first.checkpoint();
        first.close();

        // Merge the two segments into one child — the originals seal.
        var meta = admin.scalableTopics().getMetadata(topic);
        java.util.List<Long> activeIds = new java.util.ArrayList<>();
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                activeIds.add(seg.getSegmentId());
            }
        }
        assertEquals(activeIds.size(), 2);
        admin.scalableTopics().mergeSegments(topic, activeIds.get(0), activeIds.get(1));
        Awaitility.await().untilAsserted(() -> {
            int active = 0;
            var m = admin.scalableTopics().getMetadata(topic);
            for (var seg : m.getSegments().values()) {
                if (seg.isActive()) {
                    active++;
                }
            }
            assertEquals(active, 1, "merge must collapse to 1 active segment");
        });

        int suffix = 20;
        for (int i = 0; i < suffix; i++) {
            String v = "post-" + i;
            producer.newMessage().key("k-suffix-" + i).value(v).send();
            sent.add(v);
        }

        @Cleanup
        CheckpointConsumer<String> resumed = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(mid)
                .create();

        Set<String> received = new HashSet<>();
        int expected = (prefix - prefix / 2) + suffix;
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received.size() < expected && System.currentTimeMillis() < deadline) {
            Message<String> msg = resumed.receive(Duration.ofSeconds(1));
            if (msg != null) {
                received.add(msg.value());
            }
        }
        Set<String> expectedSet = new HashSet<>(sent);
        expectedSet.removeAll(consumed);
        assertEquals(received, expectedSet,
                "resume across merge must replay both sealed parents' tails and the merged child");
    }
}
