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
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentLoadStats;
import org.testng.annotations.Test;

/**
 * Unit tests for the pure {@link AutoScalePolicyEvaluator} decision function (PIP-483).
 * All inputs are constructed in-memory; there is no broker or metadata store.
 */
public class AutoScalePolicyEvaluatorTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final long NO_PRIOR = Long.MIN_VALUE;

    // Split thresholds well above merge thresholds — the dead-band is the hysteresis.
    private static final double SPLIT_MSG_IN = 10_000;
    private static final double SPLIT_BYTES_IN = 50_000_000;
    private static final double SPLIT_MSG_OUT = 50_000;
    private static final double SPLIT_BYTES_OUT = 250_000_000;
    private static final double MERGE_MSG_IN = 1_000;
    private static final double MERGE_BYTES_IN = 5_000_000;
    private static final double MERGE_MSG_OUT = 5_000;
    private static final double MERGE_BYTES_OUT = 25_000_000;

    private static AutoScaleConfig.AutoScaleConfigBuilder baseConfig() {
        return AutoScaleConfig.builder()
                .enabled(true)
                .maxSegments(64)
                .minSegments(1)
                .maxDagDepth(10)
                .splitCooldown(Duration.ofMinutes(1))
                .mergeCooldown(Duration.ofMinutes(5))
                .mergeWindow(Duration.ofMinutes(5))
                .splitMsgRateIn(SPLIT_MSG_IN)
                .splitBytesRateIn(SPLIT_BYTES_IN)
                .splitMsgRateOut(SPLIT_MSG_OUT)
                .splitBytesRateOut(SPLIT_BYTES_OUT)
                .mergeMsgRateIn(MERGE_MSG_IN)
                .mergeBytesRateIn(MERGE_BYTES_IN)
                .mergeMsgRateOut(MERGE_MSG_OUT)
                .mergeBytesRateOut(MERGE_BYTES_OUT);
    }

    private static SegmentLayout initialLayout(int segments) {
        return SegmentLayout.fromMetadata(
                ScalableTopicController.createInitialMetadata(segments, 4, Map.of()));
    }

    /** A load sample with the given rates, last modified {@code ageMs} ago. */
    private static SegmentLoadSample sample(double msgIn, double bytesIn, double msgOut,
                                            double bytesOut, long ageMs) {
        return new SegmentLoadSample(
                new SegmentLoadStats(msgIn, bytesIn, msgOut, bytesOut), NOW - ageMs);
    }

    private static SegmentLoadSample cold(long ageMs) {
        return sample(0, 0, 0, 0, ageMs);
    }

    private static long old() {
        return Duration.ofMinutes(10).toMillis();
    }

    private static AutoScaleDecision decide(SegmentLayout layout,
                                            Map<Long, SegmentLoadSample> load,
                                            Map<String, Integer> consumers,
                                            AutoScaleConfig config) {
        return AutoScalePolicyEvaluator.decide(layout, load, consumers, config, NOW,
                NO_PRIOR, NO_PRIOR);
    }

    // --- enable switch ---

    @Test
    public void testDisabledReturnsNoAction() {
        SegmentLayout layout = initialLayout(1);
        Map<Long, SegmentLoadSample> load = Map.of(0L, sample(1_000_000, 0, 0, 0, old()));
        AutoScaleDecision d = decide(layout, load, Map.of("s", 100),
                baseConfig().enabled(false).build());
        assertTrue(d instanceof AutoScaleDecision.NoAction);
    }

    // --- consumer-driven split ---

    @Test
    public void testConsumerDrivenSplitTargetsBusiestSegment() {
        SegmentLayout layout = initialLayout(2);
        Map<Long, SegmentLoadSample> load = Map.of(
                0L, sample(100, 0, 0, 0, old()),
                1L, sample(200, 0, 0, 0, old()));
        // One subscription with 3 consumers but only 2 segments → need a 3rd segment.
        AutoScaleDecision d = decide(layout, load, Map.of("sub", 3), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.Split, d.toString());
        AutoScaleDecision.Split s = (AutoScaleDecision.Split) d;
        assertEquals(s.segmentId(), 1L, "should split the busiest segment by msgRateIn");
        assertEquals(s.reason(), "consumer-count");
    }

    @Test
    public void testConsumerCountUsesPerSubscriptionMaxNotSum() {
        SegmentLayout layout = initialLayout(2);
        // Fresh samples (age 0) so the merge pass can't fire — isolates the consumer check.
        Map<Long, SegmentLoadSample> load = Map.of(0L, cold(0), 1L, cold(0));
        // Two subscriptions, 2 consumers each. Per-subscription max is 2, not 4 → 2 == 2
        // segments, no scale-up needed.
        AutoScaleDecision d = decide(layout, load, Map.of("a", 2, "b", 2), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.NoAction,
                "per-subscription max (2) does not exceed 2 segments");
    }

    @Test
    public void testConsumerDrivenSplitRespectsMaxSegments() {
        SegmentLayout layout = initialLayout(2);
        // Fresh samples so the merge pass can't fire — isolates the split suppression.
        Map<Long, SegmentLoadSample> load = Map.of(0L, cold(0), 1L, cold(0));
        AutoScaleDecision d = decide(layout, load, Map.of("sub", 5),
                baseConfig().maxSegments(2).build());
        assertTrue(d instanceof AutoScaleDecision.NoAction, "at maxSegments, no split");
    }

    @Test
    public void testConsumerDrivenSplitRespectsSplitCooldown() {
        SegmentLayout layout = initialLayout(2);
        Map<Long, SegmentLoadSample> load = Map.of(0L, cold(0), 1L, cold(0));
        long recentSplit = NOW - Duration.ofSeconds(30).toMillis(); // < 1m cooldown
        AutoScaleDecision d = AutoScalePolicyEvaluator.decide(layout, load, Map.of("sub", 3),
                baseConfig().build(), NOW, recentSplit, NO_PRIOR);
        assertTrue(d instanceof AutoScaleDecision.NoAction, "within split cooldown, no split");
    }

    // --- load-driven split ---

    @Test
    public void testLoadDrivenSplitOnMsgRateIn() {
        SegmentLayout layout = initialLayout(1);
        Map<Long, SegmentLoadSample> load = Map.of(0L, sample(SPLIT_MSG_IN + 1, 0, 0, 0, old()));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.Split, d.toString());
        AutoScaleDecision.Split s = (AutoScaleDecision.Split) d;
        assertEquals(s.segmentId(), 0L);
        assertEquals(s.reason(), "msgRateIn");
    }

    @Test
    public void testLoadDrivenSplitOnBytesRateOut() {
        SegmentLayout layout = initialLayout(1);
        Map<Long, SegmentLoadSample> load =
                Map.of(0L, sample(0, 0, 0, SPLIT_BYTES_OUT + 1, old()));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.Split, d.toString());
        assertEquals(((AutoScaleDecision.Split) d).reason(), "bytesRateOut");
    }

    @Test
    public void testLoadDrivenSplitPicksMostOverloaded() {
        SegmentLayout layout = initialLayout(2);
        // seg0 at 1.1x, seg1 at 1.5x of the msgRateIn split threshold.
        Map<Long, SegmentLoadSample> load = Map.of(
                0L, sample(SPLIT_MSG_IN * 1.1, 0, 0, 0, old()),
                1L, sample(SPLIT_MSG_IN * 1.5, 0, 0, 0, old()));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.Split, d.toString());
        assertEquals(((AutoScaleDecision.Split) d).segmentId(), 1L,
                "should split the more overloaded segment");
    }

    @Test
    public void testNoSplitWhenAllUnderThreshold() {
        SegmentLayout layout = initialLayout(1);
        // Just below every split threshold, freshly updated → not merge-eligible either.
        Map<Long, SegmentLoadSample> load = Map.of(0L,
                sample(SPLIT_MSG_IN - 1, SPLIT_BYTES_IN - 1, SPLIT_MSG_OUT - 1,
                        SPLIT_BYTES_OUT - 1, 0));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.NoAction);
    }

    // --- merge ---

    @Test
    public void testMergeColdAdjacentPair() {
        SegmentLayout layout = initialLayout(2);
        Map<Long, SegmentLoadSample> load = Map.of(0L, cold(old()), 1L, cold(old()));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.Merge, d.toString());
        AutoScaleDecision.Merge m = (AutoScaleDecision.Merge) d;
        assertTrue((m.segmentId1() == 0L && m.segmentId2() == 1L)
                || (m.segmentId1() == 1L && m.segmentId2() == 0L));
    }

    @Test
    public void testMergeRespectsMinSegments() {
        SegmentLayout layout = initialLayout(2);
        Map<Long, SegmentLoadSample> load = Map.of(0L, cold(old()), 1L, cold(old()));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().minSegments(2).build());
        assertTrue(d instanceof AutoScaleDecision.NoAction, "at minSegments, no merge");
    }

    @Test
    public void testMergeRespectsMergeCooldown() {
        SegmentLayout layout = initialLayout(2);
        Map<Long, SegmentLoadSample> load = Map.of(0L, cold(old()), 1L, cold(old()));
        long recentMerge = NOW - Duration.ofMinutes(1).toMillis(); // < 5m cooldown
        AutoScaleDecision d = AutoScalePolicyEvaluator.decide(layout, load, Map.of(),
                baseConfig().build(), NOW, NO_PRIOR, recentMerge);
        assertTrue(d instanceof AutoScaleDecision.NoAction, "within merge cooldown, no merge");
    }

    @Test
    public void testMergeRequiresColdForFullWindow() {
        SegmentLayout layout = initialLayout(2);
        // Cold values, but only updated 1 minute ago — window is 5 minutes.
        long recent = Duration.ofMinutes(1).toMillis();
        Map<Long, SegmentLoadSample> load = Map.of(0L, cold(recent), 1L, cold(recent));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.NoAction,
                "segment must stay cold for the full mergeWindow");
    }

    @Test
    public void testMergeRequiresLoadRecordPresent() {
        SegmentLayout layout = initialLayout(2);
        // No load records at all → no evidence of durable coldness → never merge.
        AutoScaleDecision d = decide(layout, new HashMap<>(), Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.NoAction);
    }

    @Test
    public void testHysteresisDeadBandNoSplitNoMerge() {
        SegmentLayout layout = initialLayout(2);
        // msgRateIn sits between the merge threshold and the split threshold for seg0:
        // not hot enough to split, not cold enough to merge.
        Map<Long, SegmentLoadSample> load = Map.of(
                0L, sample(MERGE_MSG_IN + 1, 0, 0, 0, old()),
                1L, cold(old()));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.NoAction,
                "in the dead-band, neither split nor merge");
    }

    @Test
    public void testSplitTakesPriorityOverMerge() {
        SegmentLayout layout = initialLayout(2);
        // seg0 hot (would split), seg0+seg1 also a cold-ish adjacent pair — split wins.
        Map<Long, SegmentLoadSample> load = Map.of(
                0L, sample(SPLIT_MSG_IN + 1, 0, 0, 0, old()),
                1L, cold(old()));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.Split, "split has priority");
    }

    @Test
    public void testMergeRespectsMaxDagDepth() {
        // Build a layout whose two active segments each already have merge depth 1:
        // split(0) → {1,2}; merge(1,2) → {3}; split(3) → {4,5}. Segments 4 and 5 each have
        // one merge (node 3) in their ancestry.
        SegmentLayout layout = initialLayout(1)
                .splitSegment(0, NOW)
                .mergeSegments(1, 2, NOW)
                .splitSegment(3, NOW);
        List<Long> active = layout.getActiveSegments().keySet().stream().sorted().toList();
        assertEquals(active, List.of(4L, 5L));
        assertEquals(layout.mergeDepth(4L), 1);
        assertEquals(layout.mergeDepth(5L), 1);

        Map<Long, SegmentLoadSample> load = Map.of(4L, cold(old()), 5L, cold(old()));

        // maxDagDepth=1: both at the cap → no merge.
        AutoScaleDecision blocked = decide(layout, load, Map.of(),
                baseConfig().maxDagDepth(1).build());
        assertTrue(blocked instanceof AutoScaleDecision.NoAction, "merge blocked at maxDagDepth");

        // maxDagDepth=2: under the cap → merge allowed.
        AutoScaleDecision allowed = decide(layout, load, Map.of(),
                baseConfig().maxDagDepth(2).build());
        assertTrue(allowed instanceof AutoScaleDecision.Merge, "merge allowed below maxDagDepth");
    }

    @Test
    public void testMergePicksColdestAdjacentPair() {
        SegmentLayout layout = initialLayout(4); // ranges tile [0,MAX] in 4 adjacent quarters
        // seg0+seg1 combined hotter than seg2+seg3; all below merge thresholds and old.
        Map<Long, SegmentLoadSample> load = Map.of(
                0L, sample(900, 0, 0, 0, old()),
                1L, sample(900, 0, 0, 0, old()),
                2L, sample(10, 0, 0, 0, old()),
                3L, sample(10, 0, 0, 0, old()));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.Merge, d.toString());
        AutoScaleDecision.Merge m = (AutoScaleDecision.Merge) d;
        // The coldest adjacent pair is {2,3}.
        assertTrue((m.segmentId1() == 2L && m.segmentId2() == 3L)
                || (m.segmentId1() == 3L && m.segmentId2() == 2L),
                "should pick the coldest adjacent pair {2,3}, got " + m);
    }

    @Test
    public void testMergedPairIsAlwaysAdjacent() {
        SegmentLayout layout = initialLayout(4);
        Map<Long, SegmentLoadSample> load = Map.of(
                0L, cold(old()), 1L, cold(old()), 2L, cold(old()), 3L, cold(old()));
        AutoScaleDecision d = decide(layout, load, Map.of(), baseConfig().build());
        assertTrue(d instanceof AutoScaleDecision.Merge, d.toString());
        AutoScaleDecision.Merge m = (AutoScaleDecision.Merge) d;
        SegmentInfo a = layout.getAllSegments().get(m.segmentId1());
        SegmentInfo b = layout.getAllSegments().get(m.segmentId2());
        assertTrue(a.hashRange().isAdjacentTo(b.hashRange()),
                "merged pair must be hash-range adjacent");
    }
}
