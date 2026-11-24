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
import static org.testng.Assert.expectThrows;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * PIP-486 rebucket rollover: {@code admin.scalableTopics().rebucketSegment(...)} seals a segment
 * and rolls it over to a same-range successor with a new entry-bucket count. The change rides the
 * ordinary seal → successor flow, so producers redirect transparently and ordered consumption
 * preserves per-key order across the boundary.
 */
public class V5SegmentRebucketTest extends V5ClientBaseTest {

    @Test
    public void testRebucketMidFlowPreservesPerKeyOrder() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("rebucket-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            keys.add("key-" + i);
        }
        int perKey = 10;
        Map<String, List<String>> sent = new HashMap<>();
        for (String k : keys) {
            sent.put(k, new ArrayList<>());
        }

        // Phase 1: lands on the initial segment under its original bucketing (N=4 by default).
        for (int i = 0; i < perKey; i++) {
            for (String k : keys) {
                String value = k + "-" + i;
                producer.newMessage().key(k).value(value).send();
                sent.get(k).add(value);
            }
        }

        long parentId = soleActiveSegmentId(topic);
        admin.scalableTopics().rebucketSegment(topic, parentId, 8);

        // The rollover seals the parent and creates one same-range successor with 8 buckets.
        Awaitility.await().untilAsserted(() -> {
            ScalableTopicMetadata md = admin.scalableTopics().getMetadata(topic);
            List<ScalableTopicMetadata.SegmentInfo> active = md.getSegments().values().stream()
                    .filter(ScalableTopicMetadata.SegmentInfo::isActive).toList();
            assertEquals(active.size(), 1, "rollover must keep exactly one active segment");
            ScalableTopicMetadata.SegmentInfo successor = active.get(0);
            assertTrue(successor.getSegmentId() != parentId, "successor must be a new segment");
            assertEquals(successor.getParentIds(), List.of(parentId));
            assertEquals(successor.getEntryBucketSplits().size() + 1, 8,
                    "successor must carry the new bucket count");
            ScalableTopicMetadata.SegmentInfo parent = md.getSegments().get(parentId);
            assertTrue(parent.isSealed(), "parent must be sealed");
            assertEquals(parent.getHashRange().getStart(), successor.getHashRange().getStart());
            assertEquals(parent.getHashRange().getEnd(), successor.getHashRange().getEnd());
        });

        // Phase 2: the producer re-routes to the successor (transparent segment-gone retry)
        // and batches by the new 8-bucket boundaries.
        for (int i = perKey; i < perKey * 2; i++) {
            for (String k : keys) {
                String value = k + "-" + i;
                producer.newMessage().key(k).value(value).send();
                sent.get(k).add(value);
            }
        }

        // Ordered consumption across the rollover: the sealed parent must be fully consumed
        // AND acknowledged before the controller serves the successor, so ack as we go (an
        // order-sensitive application would) — holding every ack would leave the parent
        // undrained and the successor unassigned.
        Map<String, List<String>> received = new HashMap<>();
        int total = keys.size() * perKey * 2;
        for (int i = 0; i < total; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(msg, "missed message #" + i);
            String key = msg.key().orElseThrow(() -> new AssertionError("missing key"));
            received.computeIfAbsent(key, __ -> new ArrayList<>()).add(msg.value());
            consumer.acknowledgeCumulative(msg.id());
        }

        for (String k : keys) {
            assertEquals(received.get(k), sent.get(k),
                    "per-key order across the rebucket rollover for key=" + k);
        }
    }

    @Test
    public void testRebucketRejectsInvalidRequests() throws Exception {
        String topic = newScalableTopic(1);
        long segmentId = soleActiveSegmentId(topic);

        // All segment-level rejections are client errors: HTTP 412, never 500.
        // Out-of-range bucket counts.
        assertEquals(expectThrows(PulsarAdminException.class,
                () -> admin.scalableTopics().rebucketSegment(topic, segmentId, 0))
                .getStatusCode(), 412);
        // Unchanged bucketing (the initial segment already has the budget-derived N=4).
        assertEquals(expectThrows(PulsarAdminException.class,
                () -> admin.scalableTopics().rebucketSegment(topic, segmentId, 4))
                .getStatusCode(), 412);
        // Unknown segment.
        assertEquals(expectThrows(PulsarAdminException.class,
                () -> admin.scalableTopics().rebucketSegment(topic, 12345, 8))
                .getStatusCode(), 412);

        // A valid rollover still works after the rejections, and the parent (now sealed)
        // cannot be rolled over again.
        admin.scalableTopics().rebucketSegment(topic, segmentId, 8);
        assertEquals(expectThrows(PulsarAdminException.class,
                () -> admin.scalableTopics().rebucketSegment(topic, segmentId, 16))
                .getStatusCode(), 412);
    }

    private long soleActiveSegmentId(String topic) throws Exception {
        ScalableTopicMetadata md = admin.scalableTopics().getMetadata(topic);
        List<ScalableTopicMetadata.SegmentInfo> active = md.getSegments().values().stream()
                .filter(ScalableTopicMetadata.SegmentInfo::isActive).toList();
        assertEquals(active.size(), 1, "expected exactly one active segment");
        return active.get(0).getSegmentId();
    }
}
