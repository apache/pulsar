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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;
import org.apache.pulsar.common.policies.data.SegmentTopicStats;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * End-to-end coverage of the scalable-topic stats admin API: the topic-level snapshot
 * ({@code admin.scalableTopics().getStats}) — segment DAG, subscriptions with their backlog
 * across segments, producers — and the per-segment {@link SegmentTopicStats}
 * ({@code admin.scalableTopics().getSegmentStats}).
 */
public class V5ScalableTopicStatsTest extends V5ClientBaseTest {

    @Test
    public void testTopicStatsAggregateAcrossSegments() throws Exception {
        String topic = newScalableTopic(2);
        String subscription = "stats-sub";
        // Pre-create the subscription so every message produced below is retained as backlog.
        admin.scalableTopics().createSubscription(topic, subscription, ScalableSubscriptionType.QUEUE);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .producerName("stats-producer")
                // One entry per message, so the (entry-based) backlog counts messages.
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create();
        int n = 40;
        for (int i = 0; i < n; i++) {
            producer.newMessage().key("k-" + i).value("v-" + i).send();
        }

        ScalableTopicStats stats = admin.scalableTopics().getStats(topic);

        // --- Segment DAG: two active root segments tiling the hash space ---
        ScalableTopicStats.LayoutStats layout = stats.getLayout();
        assertEquals(layout.getEpoch(), 0);
        assertEquals(layout.getSegments().keySet(), Set.of(0L, 1L));
        for (ScalableTopicStats.LayoutSegment segment : layout.getSegments().values()) {
            assertTrue(segment.isActive());
            assertTrue(segment.getParentIds().isEmpty());
            assertTrue(segment.getChildIds().isEmpty());
            assertTrue(segment.getEntryBuckets() >= 1);
            assertNotNull(segment.getOwnerBroker(), "segment stats must have been collected from its owner");
        }
        // The segment name carries the hash range and the segment ID.
        String segmentPrefix = "segment://" + topic.substring("topic://".length());
        assertEquals(layout.getSegments().get(0L).getName(), segmentPrefix + "/0000-7fff-0");
        assertEquals(layout.getSegments().get(1L).getName(), segmentPrefix + "/8000-ffff-1");
        assertTrue(stats.getStorageSize() > 0);

        // --- Producers: the V5 producer's per-segment producers fold into one entry ---
        assertEquals(stats.getProducers().size(), 1, "got " + stats.getProducers());
        ScalableTopicStats.ProducerStats p = stats.getProducers().get(0);
        assertEquals(p.getProducerName(), "stats-producer");
        assertNotNull(p.getAddress());

        // --- Subscription: backlog is the sum over the segments' cursors ---
        ScalableTopicStats.SubscriptionStats sub = stats.getSubscriptions().get(subscription);
        assertNotNull(sub, "subscription missing from " + stats.getSubscriptions().keySet());
        assertEquals(sub.getType(), ScalableSubscriptionType.QUEUE);
        assertEquals(sub.getMsgBacklog(), n);
        assertEquals(sub.getSegments().keySet(), Set.of(0L, 1L));
        long perSegment = sub.getSegments().values().stream()
                .mapToLong(ScalableTopicStats.SegmentSubscriptionStats::getMsgBacklog).sum();
        assertEquals(perSegment, n);
        assertTrue(sub.getConsumers().isEmpty());

        // --- Attach a consumer, drain part of the backlog ---
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("stats-consumer")
                .subscribe();
        int acked = 15;
        for (int i = 0; i < acked; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(msg, "receive timed out at " + i);
            consumer.acknowledge(msg.id());
        }

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ScalableTopicStats.SubscriptionStats s = admin.scalableTopics().getStats(topic)
                    .getSubscriptions().get(subscription);
            assertEquals(s.getMsgBacklog(), n - acked);
            assertEquals(s.getConsumers().size(), 1, "got " + s.getConsumers());
            ScalableTopicStats.ConsumerStats c = s.getConsumers().get(0);
            assertEquals(c.getConsumerName(), "stats-consumer");
            assertTrue(c.isConnected());
            assertEquals(c.getSegmentIds(), List.of(0L, 1L), "a QUEUE consumer attaches to every segment");
            for (ScalableTopicStats.SegmentSubscriptionStats seg : s.getSegments().values()) {
                assertEquals(seg.getConsumerCount(), 1);
            }
        });

        // --- Per-segment stats: the trimmed stats of each backing topic ---
        long backlogFromSegments = 0;
        List<String> segmentProducers = new ArrayList<>();
        for (long segmentId : List.of(0L, 1L)) {
            SegmentTopicStats segmentStats = admin.scalableTopics().getSegmentStats(topic, segmentId);
            assertNotNull(segmentStats.getOwnerBroker());
            assertEquals(segmentStats.getProducers().size(), 1);
            segmentProducers.add(segmentStats.getProducers().get(0).getProducerName());
            var segmentSub = segmentStats.getSubscriptions().get(subscription);
            assertNotNull(segmentSub, "subscription missing on segment " + segmentId);
            assertEquals(segmentSub.getConsumers().size(), 1);
            assertEquals(segmentSub.getConsumers().get(0).getConsumerName(), "stats-consumer-seg-" + segmentId);
            backlogFromSegments += segmentSub.getMsgBacklog();
        }
        assertEquals(segmentProducers, List.of("stats-producer-seg-0", "stats-producer-seg-1"));
        assertEquals(backlogFromSegments, n - acked);

        // A segment is also addressable by the name the layout lists for it.
        SegmentTopicStats byName = admin.scalableTopics().getSegmentStats(layout.getSegments().get(1L).getName());
        assertEquals(byName.getProducers().get(0).getProducerName(), "stats-producer-seg-1");

        assertThrows(PulsarAdminException.NotFoundException.class,
                () -> admin.scalableTopics().getSegmentStats(topic, 42L));
        assertThrows(PulsarAdminException.class,
                () -> admin.scalableTopics().getSegmentStats("persistent://public/default/not-a-segment"));
        assertThrows("a malformed segment descriptor is rejected client-side", PulsarAdminException.class,
                () -> admin.scalableTopics().getSegmentStats("segment://public/default/x/0000-ffff"));
    }

    @Test
    public void testStatsOfMigratedTopicIncludeLegacySegments() throws Exception {
        String base = getNamespace() + "/migrated-" + UUID.randomUUID().toString().substring(0, 8);
        admin.topics().createNonPartitionedTopic("persistent://" + base);

        // Data on the regular topic before the migration, so the legacy segment has storage.
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic("persistent://" + base)
                .create();
        for (int i = 0; i < 10; i++) {
            producer.newMessage().key("k-" + i).value("v-" + i).send();
        }
        admin.scalableTopics().migrateToScalable(base, false);
        String topic = "topic://" + base;

        ScalableTopicStats stats = admin.scalableTopics().getStats(topic);

        // Segment 0 is the sealed legacy parent wrapping the persistent:// topic; segment 1 is
        // the active child that took over.
        ScalableTopicStats.LayoutSegment parent = stats.getLayout().getSegments().get(0L);
        assertTrue(parent.isSealed());
        assertEquals(parent.getName(), "persistent://" + base, "a legacy segment is named after the topic it wraps");
        assertEquals(parent.getChildIds(), List.of(1L));
        assertNotNull(parent.getOwnerBroker(), "the legacy topic's stats are collected like any segment's");
        ScalableTopicStats.LayoutSegment child = stats.getLayout().getSegments().get(1L);
        assertTrue(child.isActive());
        assertTrue(child.getName().startsWith("segment://" + base + "/"), "unexpected child name " + child.getName());
        assertEquals(child.getParentIds(), List.of(0L));
        assertTrue(stats.getStorageSize() > 0, "the legacy segment's storage is part of the aggregate");

        // Per-segment stats of the legacy segment are those of the wrapped persistent:// topic.
        SegmentTopicStats legacy = admin.scalableTopics().getSegmentStats(topic, 0L);
        assertNotNull(legacy.getOwnerBroker());
        assertTrue(legacy.getStorageSize() > 0);
    }

    @Test
    public void testStatsReflectSplitDag() throws Exception {
        String topic = newScalableTopic(1);
        admin.scalableTopics().splitSegment(topic, 0L);

        ScalableTopicStats.LayoutStats layout = admin.scalableTopics().getStats(topic).getLayout();
        assertEquals(layout.getEpoch(), 1);
        assertEquals(layout.getSegments().keySet(), Set.of(0L, 1L, 2L));

        String segmentPrefix = "segment://" + topic.substring("topic://".length());
        ScalableTopicStats.LayoutSegment parent = layout.getSegments().get(0L);
        assertTrue(parent.isSealed());
        assertEquals(parent.getChildIds(), List.of(1L, 2L));
        assertEquals(parent.getName(), segmentPrefix + "/0000-ffff-0");

        ScalableTopicStats.LayoutSegment left = layout.getSegments().get(1L);
        assertTrue(left.isActive());
        assertEquals(left.getParentIds(), List.of(0L));
        assertEquals(left.getName(), segmentPrefix + "/0000-7fff-1");
        ScalableTopicStats.LayoutSegment right = layout.getSegments().get(2L);
        assertEquals(right.getParentIds(), List.of(0L));
        assertEquals(right.getName(), segmentPrefix + "/8000-ffff-2");

        // The sealed (terminated) parent is still a real topic with stats of its own.
        SegmentTopicStats parentStats = admin.scalableTopics().getSegmentStats(topic, 0L);
        assertNotNull(parentStats.getOwnerBroker());
    }

    @Test
    public void testStreamConsumerSessionsInStats() throws Exception {
        String topic = newScalableTopic(2);
        String subscription = "stream-sub";

        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .consumerName("stream-c1")
                .subscribe();

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ScalableTopicStats.SubscriptionStats sub = admin.scalableTopics().getStats(topic)
                    .getSubscriptions().get(subscription);
            assertNotNull(sub);
            assertEquals(sub.getType(), ScalableSubscriptionType.STREAM,
                    "a controller-coordinated subscription is STREAM");
            assertEquals(sub.getConsumers().size(), 1, "got " + sub.getConsumers());
            ScalableTopicStats.ConsumerStats c = sub.getConsumers().get(0);
            assertEquals(c.getConsumerName(), "stream-c1");
            assertTrue(c.isConnected());
            assertEquals(c.getSegmentIds(), List.of(0L, 1L), "the only consumer holds both segments");
        });
    }
}
