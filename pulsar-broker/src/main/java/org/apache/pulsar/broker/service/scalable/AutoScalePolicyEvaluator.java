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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentLoadStats;

/**
 * Pure, side-effect-free decision function for scalable-topic auto split/merge (PIP-483).
 *
 * <p>Given a snapshot of the current layout, per-segment load samples, per-subscription
 * stream/checkpoint consumer counts, the resolved policy, and the current time, it returns
 * exactly one {@link AutoScaleDecision}. It performs no I/O and holds no state — the caller
 * (the controller leader) collects the inputs and dispatches the result.
 *
 * <p>It runs two passes and emits at most one action:
 * <ol>
 *   <li><b>Split</b> (fast, lightly coalesced by {@code splitCooldown}): consumer-count
 *       scale-up first, then traffic-driven scale-up.</li>
 *   <li><b>Merge</b> (lazy, gated by {@code mergeCooldown} + {@code mergeWindow} +
 *       {@code maxDagDepth}): only if no split fired. A merge whose clamped bucket count
 *       ({@code maxEntryBucketsPerSegment}) would drop total consumer capacity below the
 *       parallelism live consumers currently get is skipped — merging must never idle a
 *       consumer that owns a bucket today, since the rebucket lane cannot grow past the
 *       ceiling to win the capacity back.</li>
 * </ol>
 */
public final class AutoScalePolicyEvaluator {

    private AutoScalePolicyEvaluator() {
    }

    /**
     * Decide whether to split, merge, or do nothing.
     *
     * @param layout              the current segment layout
     * @param loadBySegment       per active-segment load sample; a missing entry is treated
     *                            as zero load with no age (never merge-eligible)
     * @param streamConsumerCount per-subscription count of STREAM/CHECKPOINT (controller-managed)
     *                            consumers; QUEUE subscriptions are excluded by the caller
     * @param config              the resolved policy
     * @param nowMs               current wall-clock time, epoch millis
     * @param lastSplitAtMs       epoch millis of the last split on this topic (manual or auto),
     *                            or {@code Long.MIN_VALUE} if none
     * @param lastMergeAtMs       epoch millis of the last merge on this topic (manual or auto),
     *                            or {@code Long.MIN_VALUE} if none
     * @return the decision
     */
    public static AutoScaleDecision decide(
            SegmentLayout layout,
            Map<Long, SegmentLoadSample> loadBySegment,
            Map<String, Integer> streamConsumerCount,
            AutoScaleConfig config,
            long nowMs,
            long lastSplitAtMs,
            long lastMergeAtMs,
            long lastRebucketAtMs) {

        if (!config.enabled()) {
            return AutoScaleDecision.NONE;
        }

        List<SegmentInfo> active = new ArrayList<>(layout.getActiveSegments().values());

        AutoScaleDecision consumerScale = tryConsumerScale(active, loadBySegment,
                streamConsumerCount, config, nowMs, lastSplitAtMs, lastRebucketAtMs);
        if (!(consumerScale instanceof AutoScaleDecision.NoAction)) {
            return consumerScale;
        }

        AutoScaleDecision split = trySplit(active, loadBySegment, config, nowMs, lastSplitAtMs);
        if (!(split instanceof AutoScaleDecision.NoAction)) {
            return split;
        }

        return tryMerge(active, layout, loadBySegment, streamConsumerCount, config, nowMs,
                lastMergeAtMs);
    }

    // --- Consumer-driven scale-up: segments vs entry-buckets (PIP-486) ---

    /**
     * Serve surplus consumers (a subscription with more consumers than active segments) by
     * adding capacity on one of two axes:
     * <ul>
     *   <li><b>Split</b> when traffic justifies a physical segment: the busiest segment's
     *       inbound rate is at or above {@code splitVsRebucketMinMsgRateIn} and the topic is
     *       under {@code maxSegments} — today's "segments first" behavior.</li>
     *   <li><b>Rebucket-up</b> otherwise (a low-throughput topic, or the topic is at the
     *       segment cap): if the existing entry-bucket capacity cannot absorb the surplus,
     *       roll the smallest-bucketed segment over to the smallest power of two that lets
     *       every consumer own a bucket, capped at {@code maxEntryBucketsPerSegment}.
     *       Raising is fast (one rollover sized to the surplus); lowering is deliberately
     *       not automated here — spiky consumer counts must not flap the bucketing.</li>
     * </ul>
     */
    private static AutoScaleDecision tryConsumerScale(
            List<SegmentInfo> active,
            Map<Long, SegmentLoadSample> loadBySegment,
            Map<String, Integer> streamConsumerCount,
            AutoScaleConfig config,
            long nowMs,
            long lastSplitAtMs,
            long lastRebucketAtMs) {

        int consumers = streamConsumerCount.values().stream()
                .mapToInt(Integer::intValue).max().orElse(0);
        int segments = active.size();
        if (consumers <= segments) {
            return AutoScaleDecision.NONE;
        }

        SegmentInfo busiest = busiestByMsgRateIn(active, loadBySegment);
        if (busiest == null) {
            return AutoScaleDecision.NONE;
        }
        boolean atSegmentCap = segments >= config.maxSegments();
        boolean belowSplitFloor = statsOf(busiest.segmentId(), loadBySegment).msgRateIn()
                < config.splitVsRebucketMinMsgRateIn();

        if (!atSegmentCap && !belowSplitFloor) {
            // Traffic justifies a physical segment.
            if (withinCooldown(nowMs, lastSplitAtMs, config.splitCooldown().toMillis())) {
                return AutoScaleDecision.NONE;
            }
            return new AutoScaleDecision.Split(busiest.segmentId(), "consumer-count");
        }

        // Bucket lane: absorb the surplus with entry-buckets.
        long capacity = 0;
        for (SegmentInfo segment : active) {
            capacity += segment.bucketCount();
        }
        if (consumers <= capacity) {
            // The existing buckets already absorb the surplus (broker-side fan-out).
            return AutoScaleDecision.NONE;
        }
        if (withinCooldown(nowMs, lastRebucketAtMs, config.rebucketCooldown().toMillis())) {
            return AutoScaleDecision.NONE;
        }
        // One shot: bring every segment below the common per-segment target up to it in a
        // single decision, so the topic converges to a uniform bucketing in one evaluation —
        // never one segment per cooldown, and no arrival-history-dependent skew.
        int target = Math.min(nextPowerOfTwo(ceilDiv(consumers, segments)),
                config.maxEntryBucketsPerSegment());
        List<Long> below = new ArrayList<>();
        for (SegmentInfo segment : active) {
            if (segment.bucketCount() < target) {
                below.add(segment.segmentId());
            }
        }
        if (below.isEmpty()) {
            // Bucket capacity is maxed out; the remaining surplus stays idle.
            return AutoScaleDecision.NONE;
        }
        below.sort(Long::compareTo);
        return new AutoScaleDecision.Rebucket(below, target,
                atSegmentCap ? "at-max-segments" : "below-split-rate-floor");
    }

    // --- Split pass ---

    private static AutoScaleDecision trySplit(
            List<SegmentInfo> active,
            Map<Long, SegmentLoadSample> loadBySegment,
            AutoScaleConfig config,
            long nowMs,
            long lastSplitAtMs) {

        if (active.size() >= config.maxSegments()) {
            return AutoScaleDecision.NONE;
        }
        if (withinCooldown(nowMs, lastSplitAtMs, config.splitCooldown().toMillis())) {
            return AutoScaleDecision.NONE;
        }

        // Load-driven: split the segment with the highest overload score among those over
        // at least one split threshold.
        SegmentInfo hottest = null;
        double hottestScore = 1.0; // strictly over threshold means a per-metric ratio > 1.0
        String hottestReason = null;
        for (SegmentInfo segment : active) {
            SegmentLoadStats stats = statsOf(segment.segmentId(), loadBySegment);
            double score = 0.0;
            String reason = null;
            double[] ratios = {
                    stats.msgRateIn() / config.splitMsgRateIn(),
                    stats.bytesRateIn() / config.splitBytesRateIn(),
                    stats.msgRateOut() / config.splitMsgRateOut(),
                    stats.bytesRateOut() / config.splitBytesRateOut(),
            };
            String[] reasons = {"msgRateIn", "bytesRateIn", "msgRateOut", "bytesRateOut"};
            for (int i = 0; i < ratios.length; i++) {
                if (ratios[i] > score) {
                    score = ratios[i];
                    reason = reasons[i];
                }
            }
            if (score > 1.0 && score > hottestScore) {
                hottestScore = score;
                hottest = segment;
                hottestReason = reason;
            }
        }
        if (hottest != null) {
            return new AutoScaleDecision.Split(hottest.segmentId(), hottestReason);
        }

        return AutoScaleDecision.NONE;
    }

    // --- Merge pass ---

    private static AutoScaleDecision tryMerge(
            List<SegmentInfo> active,
            SegmentLayout layout,
            Map<Long, SegmentLoadSample> loadBySegment,
            Map<String, Integer> streamConsumerCount,
            AutoScaleConfig config,
            long nowMs,
            long lastMergeAtMs) {

        if (active.size() <= config.minSegments()) {
            return AutoScaleDecision.NONE;
        }
        if (withinCooldown(nowMs, lastMergeAtMs, config.mergeCooldown().toMillis())) {
            return AutoScaleDecision.NONE;
        }

        long mergeWindowMs = config.mergeWindow().toMillis();

        // A merged segment's bucket count is the clamped sum of the pair's (the same clamp
        // mergeSegments applies), so an at-ceiling merge shrinks total consumer capacity —
        // and the rebucket lane cannot grow past the ceiling to recover it. Skip any pair
        // whose merge would leave less capacity than the parallelism consumers get today:
        // min(consumers, capacity), so an already over-subscribed topic can still take a
        // capacity-preserving merge, and an idle one can always consolidate.
        int consumers = streamConsumerCount.values().stream()
                .mapToInt(Integer::intValue).max().orElse(0);
        long totalCapacity = 0;
        for (SegmentInfo segment : active) {
            totalCapacity += segment.bucketCount();
        }
        long usedParallelism = Math.min(consumers, totalCapacity);

        AutoScaleDecision.Merge coldest = null;
        double coldestCombined = Double.MAX_VALUE;
        for (int i = 0; i < active.size(); i++) {
            for (int j = i + 1; j < active.size(); j++) {
                SegmentInfo a = active.get(i);
                SegmentInfo b = active.get(j);
                if (!a.hashRange().isAdjacentTo(b.hashRange())) {
                    continue;
                }
                if (layout.mergeDepth(a.segmentId()) >= config.maxDagDepth()
                        || layout.mergeDepth(b.segmentId()) >= config.maxDagDepth()) {
                    continue;
                }
                long pairCapacity = (long) a.bucketCount() + b.bucketCount();
                long mergedCapacity = Math.min(pairCapacity, config.maxEntryBucketsPerSegment());
                if (totalCapacity - pairCapacity + mergedCapacity < usedParallelism) {
                    continue;
                }
                if (!coldEnough(a.segmentId(), loadBySegment, config, nowMs, mergeWindowMs)
                        || !coldEnough(b.segmentId(), loadBySegment, config, nowMs, mergeWindowMs)) {
                    continue;
                }
                double combined = combinedRate(a.segmentId(), loadBySegment)
                        + combinedRate(b.segmentId(), loadBySegment);
                if (combined < coldestCombined) {
                    coldestCombined = combined;
                    coldest = new AutoScaleDecision.Merge(a.segmentId(), b.segmentId(), "cold");
                }
            }
        }
        return coldest != null ? coldest : AutoScaleDecision.NONE;
    }

    /**
     * A segment is cold enough to merge only if it has a load record that has stayed below
     * every merge threshold for at least {@code mergeWindowMs}. A missing record means we
     * have no evidence the segment is durably cold, so it is never merge-eligible.
     *
     * <p>Note that {@code nowMs} is the controller broker's clock while the sample's
     * {@code modifiedAtMs} is the metadata store's server-side timestamp; clock skew between
     * the two shifts the effective window. Acceptable for a lazy-merge heuristic — skew is
     * normally seconds against a multi-minute window.
     */
    private static boolean coldEnough(long segmentId, Map<Long, SegmentLoadSample> loadBySegment,
                                      AutoScaleConfig config, long nowMs, long mergeWindowMs) {
        SegmentLoadSample sample = loadBySegment.get(segmentId);
        if (sample == null) {
            return false;
        }
        if (nowMs - sample.modifiedAtMs() < mergeWindowMs) {
            return false;
        }
        SegmentLoadStats stats = sample.stats();
        return stats.msgRateIn() < config.mergeMsgRateIn()
                && stats.bytesRateIn() < config.mergeBytesRateIn()
                && stats.msgRateOut() < config.mergeMsgRateOut()
                && stats.bytesRateOut() < config.mergeBytesRateOut();
    }

    // --- Helpers ---

    private static boolean withinCooldown(long nowMs, long lastAtMs, long cooldownMs) {
        return lastAtMs != Long.MIN_VALUE && nowMs - lastAtMs < cooldownMs;
    }

    private static SegmentLoadStats statsOf(long segmentId, Map<Long, SegmentLoadSample> load) {
        SegmentLoadSample sample = load.get(segmentId);
        return sample != null ? sample.stats() : SegmentLoadStats.ZERO;
    }

    private static double combinedRate(long segmentId, Map<Long, SegmentLoadSample> load) {
        SegmentLoadStats s = statsOf(segmentId, load);
        return s.msgRateIn() + s.bytesRateIn() + s.msgRateOut() + s.bytesRateOut();
    }

    /** Ceiling integer division for positive operands. */
    private static int ceilDiv(int a, int b) {
        return (a + b - 1) / b;
    }

    /** The smallest power of two {@code >= v} (for {@code v >= 1}). */
    private static int nextPowerOfTwo(int v) {
        int highest = Integer.highestOneBit(v);
        return highest == v ? v : highest << 1;
    }

    private static SegmentInfo busiestByMsgRateIn(List<SegmentInfo> active,
                                                  Map<Long, SegmentLoadSample> load) {
        SegmentInfo best = null;
        double bestRate = -1.0;
        for (SegmentInfo segment : active) {
            double rate = statsOf(segment.segmentId(), load).msgRateIn();
            // Tie-break deterministically on segment id so the choice is stable across ticks.
            if (rate > bestRate || (rate == bestRate && best != null
                    && segment.segmentId() < best.segmentId())) {
                bestRate = rate;
                best = segment;
            }
        }
        return best;
    }
}
