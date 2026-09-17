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
package org.apache.pulsar.broker.service.scalable;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;
import org.apache.pulsar.common.policies.data.SegmentTopicStats;
import org.testng.annotations.Test;

/**
 * Unit tests for the folding rules of {@link ScalableTopicStatsBuilder}, driven by
 * hand-built per-segment {@link SegmentTopicStats} fixtures.
 */
public class ScalableTopicStatsBuilderTest {

    private static final TopicName TOPIC = TopicName.get("topic://tenant/ns/stats-topic");

    /**
     * Two initial segments (0, 1) sharing an 8 entry-bucket budget (4 each); segment 0 split
     * into 2 and 3.
     */
    private static SegmentLayout splitLayout() {
        ScalableTopicMetadata md = ScalableTopicController.createInitialMetadata(2, 8, Map.of());
        return SegmentLayout.fromMetadata(md).splitSegment(0, 1_000L);
    }

    private static SegmentTopicStats segmentStats(String owner, double msgRateIn, long storageSize) {
        SegmentTopicStats ts = new SegmentTopicStats();
        ts.setOwnerBroker(owner);
        ts.setMsgRateIn(msgRateIn);
        ts.setByteRateIn(msgRateIn * 100);
        ts.setStorageSize(storageSize);
        ts.setBacklogSize(storageSize / 2);
        return ts;
    }

    private static SegmentTopicStats.ProducerStats producer(String name, double msgRateIn) {
        SegmentTopicStats.ProducerStats p = new SegmentTopicStats.ProducerStats();
        p.setProducerName(name);
        p.setMsgRateIn(msgRateIn);
        p.setByteRateIn(msgRateIn * 50);
        p.setAddress("10.0.0.1:1234");
        p.setClientVersion("v5");
        return p;
    }

    private static SegmentTopicStats.SubscriptionStats subscription(long msgBacklog,
                                                                     SegmentTopicStats.ConsumerStats... consumers) {
        SegmentTopicStats.SubscriptionStats s = new SegmentTopicStats.SubscriptionStats();
        s.setMsgBacklog(msgBacklog);
        s.setBacklogSize(msgBacklog * 10);
        s.setMsgRateOut(msgBacklog);
        for (SegmentTopicStats.ConsumerStats c : consumers) {
            s.getConsumers().add(c);
            s.setUnackedMessages(s.getUnackedMessages() + c.getUnackedMessages());
        }
        return s;
    }

    private static SegmentTopicStats.ConsumerStats consumer(String name, int unacked) {
        SegmentTopicStats.ConsumerStats c = new SegmentTopicStats.ConsumerStats();
        c.setConsumerName(name);
        c.setUnackedMessages(unacked);
        c.setAvailablePermits(100);
        c.setMsgRateOut(1);
        c.setAddress("10.0.0.2:5678");
        return c;
    }

    @Test
    public void testDagAndTopicAggregates() {
        SegmentLayout layout = splitLayout();
        SegmentTopicStats seg1 = segmentStats("broker-a", 2.0, 1_000);
        SegmentTopicStats seg2 = segmentStats("broker-b", 4.0, 3_000);
        // Segment 0 (sealed) and 3 have no stats collected.
        Map<Long, SegmentTopicStats> segmentStats = Map.of(1L, seg1, 2L, seg2);

        ScalableTopicStats stats = ScalableTopicStatsBuilder.build(
                TOPIC, layout, segmentStats, Map.of(), Map.of());

        ScalableTopicStats.LayoutStats dag = stats.getLayout();
        assertEquals(dag.getEpoch(), 1);
        assertEquals(dag.getSegments().keySet(), Set.of(0L, 1L, 2L, 3L));

        ScalableTopicStats.LayoutSegment parent = dag.getSegments().get(0L);
        assertTrue(parent.isSealed());
        assertEquals(parent.getChildIds(), List.of(2L, 3L));
        assertTrue(parent.getParentIds().isEmpty());
        assertEquals(parent.getName(), "segment://tenant/ns/stats-topic/0000-7fff-0");
        assertNull(parent.getOwnerBroker(), "no stats collected → no owner");

        ScalableTopicStats.LayoutSegment child = dag.getSegments().get(2L);
        assertTrue(child.isActive());
        assertEquals(child.getParentIds(), List.of(0L));
        assertTrue(child.getChildIds().isEmpty());
        assertEquals(child.getName(), "segment://tenant/ns/stats-topic/0000-3fff-2");
        assertEquals(child.getOwnerBroker(), "broker-b");
        assertEquals(child.getEntryBuckets(), 2, "a split halves the parent's 4 entry-buckets");

        assertEquals(stats.getMsgRateIn(), 6.0);
        assertEquals(stats.getByteRateIn(), 600.0);
        assertEquals(stats.getAverageMsgSize(), 100.0, "throughput / rate");
        assertEquals(stats.getStorageSize(), 4_000L);
        assertEquals(stats.getBacklogSize(), 2_000L);
        assertTrue(stats.getProducers().isEmpty());
        assertTrue(stats.getSubscriptions().isEmpty());
    }

    @Test
    public void testRatesRoundedToThreeDecimals() {
        SegmentLayout layout = splitLayout();
        SegmentTopicStats seg1 = segmentStats("broker-a", 100.00208351674284, 0);
        seg1.setByteRateIn(5536.915360155018);
        seg1.getProducers().add(producer("p-seg-1", 33.3333333));
        seg1.getSubscriptions().put("s", subscription(0, consumer("c-seg-1", 0)));
        seg1.getSubscriptions().get("s").setMsgRateOut(1.0006);
        seg1.getSubscriptions().get("s").getConsumers().get(0).setMsgRateOut(2.0004);

        ScalableTopicStats stats = ScalableTopicStatsBuilder.build(
                TOPIC, layout, Map.of(1L, seg1), Map.of(), Map.of());

        assertEquals(stats.getMsgRateIn(), 100.002);
        assertEquals(stats.getByteRateIn(), 5536.915);
        assertEquals(stats.getAverageMsgSize(), 55.368);
        assertEquals(stats.getProducers().get(0).getMsgRateIn(), 33.333);
        assertEquals(stats.getProducers().get(0).getAverageMsgSize(), 50.0);
        ScalableTopicStats.SubscriptionStats s = stats.getSubscriptions().get("s");
        assertEquals(s.getMsgRateOut(), 1.001);
        assertEquals(s.getSegments().get(1L).getMsgRateOut(), 1.001);
        assertEquals(s.getConsumers().get(0).getMsgRateOut(), 2.0);
    }

    @Test
    public void testProducersFoldedAcrossSegments() {
        SegmentLayout layout = splitLayout();
        SegmentTopicStats seg1 = segmentStats("broker-a", 1, 0);
        seg1.getProducers().add(producer("app-producer-seg-1", 2.0));
        seg1.getProducers().add(producer("standalone-abc", 0.5));
        SegmentTopicStats seg2 = segmentStats("broker-b", 1, 0);
        seg2.getProducers().add(producer("app-producer-seg-2", 3.0));
        // A name whose suffix names a *different* segment is not the V5 pattern.
        seg2.getProducers().add(producer("other-seg-1", 1.0));

        ScalableTopicStats stats = ScalableTopicStatsBuilder.build(
                TOPIC, layout, Map.of(1L, seg1, 2L, seg2), Map.of(), Map.of());

        assertEquals(stats.getProducers().size(), 3, "got " + stats.getProducers());
        Map<String, ScalableTopicStats.ProducerStats> byName = new HashMap<>();
        stats.getProducers().forEach(p -> byName.put(p.getProducerName(), p));
        assertEquals(byName.keySet(), Set.of("app-producer", "standalone-abc", "other-seg-1"));

        ScalableTopicStats.ProducerStats app = byName.get("app-producer");
        assertEquals(app.getMsgRateIn(), 5.0, "both per-segment producers folded into one");
        assertEquals(app.getByteRateIn(), 250.0);
        assertEquals(app.getAverageMsgSize(), 50.0);
        assertEquals(app.getAddress(), "10.0.0.1:1234");
        assertEquals(app.getClientVersion(), "v5");

        assertEquals(byName.get("standalone-abc").getMsgRateIn(), 0.5);
        assertEquals(byName.get("other-seg-1").getMsgRateIn(), 1.0);
    }

    @Test
    public void testQueueSubscriptionAggregatedFromSegments() {
        SegmentLayout layout = splitLayout();
        SegmentTopicStats seg1 = segmentStats("broker-a", 1, 0);
        seg1.getSubscriptions().put("q", subscription(10, consumer("qc-seg-1", 3)));
        SegmentTopicStats seg2 = segmentStats("broker-b", 1, 0);
        seg2.getSubscriptions().put("q", subscription(5, consumer("qc-seg-2", 1), consumer("random-name", 2)));
        // A segment without this subscription contributes nothing.
        SegmentTopicStats seg3 = segmentStats("broker-a", 1, 0);

        ScalableTopicStats stats = ScalableTopicStatsBuilder.build(
                TOPIC, layout, Map.of(1L, seg1, 2L, seg2, 3L, seg3), Map.of(), Map.of());

        assertEquals(stats.getSubscriptions().keySet(), Set.of("q"));
        ScalableTopicStats.SubscriptionStats q = stats.getSubscriptions().get("q");
        assertEquals(q.getType(), ScalableSubscriptionType.QUEUE,
                "a subscription that only exists on the segments is QUEUE");
        assertEquals(q.getMsgBacklog(), 15L);
        assertEquals(q.getBacklogSize(), 150L);
        assertEquals(q.getMsgRateOut(), 15.0);
        assertEquals(q.getUnackedMessages(), 6L);
        assertEquals(q.getSegments().keySet(), Set.of(1L, 2L));
        assertEquals(q.getSegments().get(1L).getMsgBacklog(), 10L);
        assertEquals(q.getSegments().get(1L).getConsumerCount(), 1);
        assertEquals(q.getSegments().get(2L).getMsgBacklog(), 5L);
        assertEquals(q.getSegments().get(2L).getConsumerCount(), 2);

        Map<String, ScalableTopicStats.ConsumerStats> byName = new HashMap<>();
        q.getConsumers().forEach(c -> byName.put(c.getConsumerName(), c));
        assertEquals(byName.keySet(), Set.of("qc", "random-name"));
        ScalableTopicStats.ConsumerStats qc = byName.get("qc");
        assertTrue(qc.isConnected());
        assertEquals(qc.getSegmentIds(), List.of(1L, 2L));
        assertEquals(qc.getUnackedMessages(), 4L);
        assertEquals(qc.getAvailablePermits(), 200);
        assertEquals(qc.getMsgRateOut(), 2.0);
        assertEquals(qc.getAddress(), "10.0.0.2:5678");
        assertEquals(byName.get("random-name").getSegmentIds(), List.of(2L));
    }

    @Test
    public void testStreamSessionsSeedConsumers() {
        SegmentLayout layout = splitLayout();
        SegmentTopicStats seg1 = segmentStats("broker-a", 1, 0);
        seg1.getSubscriptions().put("s", subscription(7, consumer("c1-seg-1", 2)));
        SegmentTopicStats seg2 = segmentStats("broker-b", 1, 0);
        seg2.getSubscriptions().put("s", subscription(0));
        var sessions = Map.of("s", List.of(
                new ScalableTopicStatsBuilder.StreamConsumer("c1", true, List.of(1L, 2L)),
                // Registered, in its grace period, still holding segment 3.
                new ScalableTopicStatsBuilder.StreamConsumer("c2", false, List.of(3L)),
                // Registered and connected but idle: nothing assigned.
                new ScalableTopicStatsBuilder.StreamConsumer("c3", true, List.of())));

        ScalableTopicStats stats = ScalableTopicStatsBuilder.build(
                TOPIC, layout, Map.of(1L, seg1, 2L, seg2), Map.of(), sessions);

        ScalableTopicStats.SubscriptionStats s = stats.getSubscriptions().get("s");
        assertEquals(s.getType(), ScalableSubscriptionType.STREAM,
                "a subscription the controller coordinates is STREAM");
        assertEquals(s.getMsgBacklog(), 7L);
        assertEquals(s.getConsumers().size(), 3);
        Map<String, ScalableTopicStats.ConsumerStats> byName = new HashMap<>();
        s.getConsumers().forEach(c -> byName.put(c.getConsumerName(), c));

        ScalableTopicStats.ConsumerStats c1 = byName.get("c1");
        assertTrue(c1.isConnected());
        assertEquals(c1.getSegmentIds(), List.of(1L, 2L), "assignment, not just the segments attached so far");
        assertEquals(c1.getUnackedMessages(), 2L);

        ScalableTopicStats.ConsumerStats c2 = byName.get("c2");
        assertFalse(c2.isConnected());
        assertEquals(c2.getSegmentIds(), List.of(3L));
        assertEquals(c2.getUnackedMessages(), 0L);

        ScalableTopicStats.ConsumerStats c3 = byName.get("c3");
        assertTrue(c3.isConnected());
        assertTrue(c3.getSegmentIds().isEmpty());
    }

    @Test
    public void testPersistedTypeAndEmptySubscriptionsAreReported() {
        SegmentLayout layout = splitLayout();
        // Admin-created subscriptions with no cursor materialized anywhere yet.
        Map<String, ScalableSubscriptionType> persisted = Map.of(
                "admin-stream", ScalableSubscriptionType.STREAM,
                "admin-queue", ScalableSubscriptionType.QUEUE);

        ScalableTopicStats stats = ScalableTopicStatsBuilder.build(
                TOPIC, layout, Map.of(), persisted, Map.of());

        assertEquals(stats.getSubscriptions().keySet(), Set.of("admin-stream", "admin-queue"));
        assertEquals(stats.getSubscriptions().get("admin-stream").getType(), ScalableSubscriptionType.STREAM);
        assertEquals(stats.getSubscriptions().get("admin-queue").getType(), ScalableSubscriptionType.QUEUE);
        assertTrue(stats.getSubscriptions().get("admin-queue").getConsumers().isEmpty());
        assertEquals(stats.getSubscriptions().get("admin-queue").getMsgBacklog(), 0L);
    }

    @Test
    public void testStripSegmentSuffix() {
        assertEquals(ScalableTopicStatsBuilder.stripSegmentSuffix("p-seg-12", 12), "p");
        assertEquals(ScalableTopicStatsBuilder.stripSegmentSuffix("p-seg-12", 1), "p-seg-12");
        assertEquals(ScalableTopicStatsBuilder.stripSegmentSuffix("-seg-12", 12), "-seg-12",
                "an empty base name is not folded");
        assertEquals(ScalableTopicStatsBuilder.stripSegmentSuffix("plain", 3), "plain");
        assertNull(ScalableTopicStatsBuilder.stripSegmentSuffix(null, 3));
    }
}
