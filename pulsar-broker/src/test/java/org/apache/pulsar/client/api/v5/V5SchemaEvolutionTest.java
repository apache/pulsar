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
import static org.testng.Assert.fail;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Schema evolution coverage for V5 scalable topics.
 *
 * <p>Schemas on a scalable topic are scoped at the parent topic, not per-segment:
 * {@link org.apache.pulsar.common.naming.TopicName#getSchemaName()} strips the segment
 * descriptor (since {@code segment://t/ns/parent/desc} parses with {@code localName} =
 * {@code parent}), so every segment of the same scalable topic shares one schema id —
 * {@code t/ns/parent}. The schema registry therefore holds a single version history per
 * scalable topic, and split / merge segments inherit it transparently.
 *
 * <p>These tests pin that contract:
 * <ul>
 *   <li>{@link #testSingleSchemaIdAcrossSegments()} — produce v1, split into two children,
 *       produce v1 again across the children. Only one schema entry exists when looked up
 *       at the parent name; the children re-use it rather than registering their own.</li>
 *   <li>{@link #testCompatibleSchemaUpgradeAcrossSplit()} — produce v1, split, produce a
 *       BACKWARD-compatible v2 across the new children. The compatibility check sees the
 *       v1 history that pre-dated the split and accepts v2; the registry ends up with both
 *       versions and the consumer reads every message.</li>
 *   <li>{@link #testIncompatibleSchemaAfterSplitFails()} — same setup, but the post-split
 *       producer attempts an incompatible schema. The pre-split history is still in scope,
 *       so the registration is rejected.</li>
 * </ul>
 */
public class V5SchemaEvolutionTest extends V5ClientBaseTest {

    @Test
    public void testSingleSchemaIdAcrossSegments() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<V1> producer = v5Client.newProducer(Schema.avro(V1.class))
                .topic(topic)
                .create();
        producer.newMessage().key("k-pre").value(new V1(1)).send();

        // Split forces a second active segment to come into play.
        long parentSegment = singleActiveSegmentId(topic);
        admin.scalableTopics().splitSegment(topic, parentSegment);
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 2,
                "split must produce 2 active children"));

        // Send across the new active segments. Same schema, but two distinct underlying
        // segment topics — if schemas were per-segment we'd see one entry per segment.
        for (int i = 0; i < 20; i++) {
            producer.newMessage().key("k-" + i).value(new V1(100 + i)).send();
        }

        // The schemas admin endpoint resolves by tenant/namespace/topic-local-name; the
        // local name strips the segment descriptor, so this query lines up with the
        // schema id the broker wrote. One id, one version.
        var schemas = admin.schemas().getAllSchemas(topic);
        assertEquals(schemas.size(), 1,
                "single schema id should be shared across all segments, got " + schemas);
    }

    @Test
    public void testCompatibleSchemaUpgradeAcrossSplit() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(),
                SchemaCompatibilityStrategy.BACKWARD);

        String topic = newScalableTopic(1);

        // Phase 1: schema v1 on the only initial segment.
        Producer<V1> p1 = v5Client.newProducer(Schema.avro(V1.class))
                .topic(topic)
                .create();
        p1.newMessage().key("v1-a").value(new V1(1)).send();
        p1.newMessage().key("v1-b").value(new V1(2)).send();
        p1.close();

        // Split: parent sealed, two new active children.
        long parentSegment = singleActiveSegmentId(topic);
        admin.scalableTopics().splitSegment(topic, parentSegment);
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 2));

        // Phase 2: a NEW producer attaches with schema v2. v2 adds a nullable field on
        // top of v1 — backward compatible. The compatibility check has to consult v1 (in
        // the pre-split history) for this to pass; if the children carried their own
        // empty schema state it would just be accepted as a new schema v0.
        @Cleanup
        Producer<V2> p2 = v5Client.newProducer(Schema.avro(V2.class))
                .topic(topic)
                .create();
        p2.newMessage().key("v2-a").value(new V2(3, 30)).send();
        p2.newMessage().key("v2-b").value(new V2(4, 40)).send();

        // Two schema versions registered under the same scalable-topic id.
        var schemas = admin.schemas().getAllSchemas(topic);
        assertEquals(schemas.size(), 2,
                "expected v1 and v2 to coexist under one scalable-topic schema id, got " + schemas);

        // Consumer reads everything as v2 — v1 messages decode into V2 with j == null
        // (the nullable default), v2 messages decode straight through.
        @Cleanup
        QueueConsumer<V2> consumer = v5Client.newQueueConsumer(Schema.avro(V2.class))
                .topic(topic)
                .subscriptionName("evolution-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        int seenV1 = 0;
        int seenV2 = 0;
        for (int i = 0; i < 4; i++) {
            Message<V2> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missing message #" + i);
            V2 v = msg.value();
            if (v.j == null) {
                seenV1++;
                assertTrue(v.i == 1 || v.i == 2, "unexpected v1-decoded payload: " + v);
            } else {
                seenV2++;
                assertTrue(v.i == 3 || v.i == 4, "unexpected v2 payload: " + v);
            }
            consumer.acknowledge(msg.id());
        }
        assertEquals(seenV1, 2, "expected exactly the two v1 messages to decode with j == null");
        assertEquals(seenV2, 2, "expected exactly the two v2 messages to decode with j set");
    }

    @Test
    public void testIncompatibleSchemaAfterSplitFails() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(),
                SchemaCompatibilityStrategy.BACKWARD);

        String topic = newScalableTopic(1);

        Producer<V1> p1 = v5Client.newProducer(Schema.avro(V1.class))
                .topic(topic)
                .create();
        p1.newMessage().key("v1").value(new V1(1)).send();
        p1.close();

        long parentSegment = singleActiveSegmentId(topic);
        admin.scalableTopics().splitSegment(topic, parentSegment);
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegmentCount(topic), 2));

        // VBad has the same field shape as V2 but with a non-nullable extra field — that
        // breaks BACKWARD compatibility against V1 (an old reader can't fill a required
        // field from a v1 record). The V5 producer creates v4 segment producers lazily on
        // the first send, so the schema registration / compatibility check happens then;
        // exercise it explicitly here.
        @Cleanup
        Producer<VBad> bad = v5Client.newProducer(Schema.avro(VBad.class))
                .topic(topic)
                .create();
        try {
            bad.newMessage().key("bad").value(new VBad(5, 50)).send();
            fail("expected incompatible schema to be rejected post-split");
        } catch (PulsarClientException expected) {
            // good — the broker preserved v1 history across the split and rejected
            // the registration.
        } catch (CompletionException ce) {
            // Some V5 builders may surface the error wrapped — accept either form.
            assertTrue(ce.getCause() instanceof PulsarClientException,
                    "unexpected wrapped error: " + ce);
        }

        // The registry still shows just v1 — the failed registration didn't leak through.
        var schemas = admin.schemas().getAllSchemas(topic);
        assertEquals(schemas.size(), 1,
                "incompatible schema must not be added to the scalable-topic registry, got "
                        + schemas);
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

    private int activeSegmentCount(String topic) throws Exception {
        int active = 0;
        for (var seg : admin.scalableTopics().getMetadata(topic).getSegments().values()) {
            if (seg.isActive()) {
                active++;
            }
        }
        return active;
    }

    // --- Schema POJOs ---

    /** v1 schema: single int field. */
    public static class V1 {
        public int i;

        public V1() {
        }

        public V1(int i) {
            this.i = i;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof V1 v && v.i == i;
        }

        @Override
        public int hashCode() {
            return Objects.hash(i);
        }
    }

    /** v2 schema: adds a nullable Integer — backward-compatible upgrade. */
    public static class V2 {
        public int i;
        public Integer j;

        public V2() {
        }

        public V2(int i, Integer j) {
            this.i = i;
            this.j = j;
        }

        @Override
        public String toString() {
            return "V2{i=" + i + ", j=" + j + "}";
        }
    }

    /** Incompatible upgrade: the extra field is required, not nullable. */
    public static class VBad {
        public int i;
        public int j;

        public VBad() {
        }

        public VBad(int i, int j) {
            this.i = i;
            this.j = j;
        }
    }
}
