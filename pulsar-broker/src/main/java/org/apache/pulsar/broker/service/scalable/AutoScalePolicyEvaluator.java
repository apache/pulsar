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
 *       {@code maxDagDepth}): only if no split fired.</li>
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
            long lastMergeAtMs) {

        if (!config.enabled()) {
            return AutoScaleDecision.NONE;
        }

        List<SegmentInfo> active = new ArrayList<>(layout.getActiveSegments().values());

        AutoScaleDecision split = trySplit(active, loadBySegment, streamConsumerCount,
                config, nowMs, lastSplitAtMs);
        if (!(split instanceof AutoScaleDecision.NoAction)) {
            return split;
        }

        return tryMerge(active, layout, loadBySegment, config, nowMs, lastMergeAtMs);
    }

    // --- Split pass ---

    private static AutoScaleDecision trySplit(
            List<SegmentInfo> active,
            Map<Long, SegmentLoadSample> loadBySegment,
            Map<String, Integer> streamConsumerCount,
            AutoScaleConfig config,
            long nowMs,
            long lastSplitAtMs) {

        if (active.size() >= config.maxSegments()) {
            return AutoScaleDecision.NONE;
        }
        if (withinCooldown(nowMs, lastSplitAtMs, config.splitCooldown().toMillis())) {
            return AutoScaleDecision.NONE;
        }

        // (a) Consumer-driven: per-subscription max. If any managed subscription has more
        // consumers than there are active segments, add a segment so the 1:1 assignment can
        // give the extra consumer its own segment. Split the busiest segment by msgRateIn so
        // the new pair lands where it relieves the most ingest.
        int requiredConsumers = streamConsumerCount.values().stream()
                .mapToInt(Integer::intValue).max().orElse(0);
        if (requiredConsumers > active.size()) {
            SegmentInfo target = busiestByMsgRateIn(active, loadBySegment);
            if (target != null) {
                return new AutoScaleDecision.Split(target.segmentId(), "consumer-count");
            }
        }

        // (b) Load-driven: split the segment with the highest overload score among those over
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
