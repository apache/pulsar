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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleSupplier;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentLoadStats;

/**
 * Writes per-segment {@link SegmentLoadStats} to the metadata store on behalf of the broker
 * that owns a segment's {@code segment://} topic (PIP-483).
 *
 * <p>To keep metadata write volume bounded, a sample is persisted only when it differs
 * materially from the last value this broker wrote for that segment — i.e. some rate moved
 * by more than the configured relative threshold (default 25%), or crossed to/from zero. A
 * steady-state segment therefore writes once and then stays quiet; the controller's
 * windowing relies on the stored record's {@code Stat} modification time staying put while
 * load is unchanged.
 *
 * <p><b>Known blind spot</b> — the materiality band is anchored at the last <em>written</em>
 * value, not at the policy thresholds. A true rate that settles inside the band never
 * produces a new record, so a segment can sustain up to {@code threshold} beyond a split or
 * merge threshold indefinitely without the controller seeing it. This is path-dependent
 * (whether a given load triggers depends on what was last written) but one-directional: it
 * can only delay an action, never cause a spurious one, and its magnitude is bounded by the
 * configured threshold. Accepted as the cost of bounded write volume; operators wanting
 * tighter tracking lower {@code scalableTopicLoadReportRateChangeThreshold}.
 *
 * <p>This class owns only the materiality decision and the last-written cache. Sampling the
 * live {@code TopicStats} and scheduling the periodic sweep are wired in by the broker.
 */
public class SegmentLoadReporter {

    private final ScalableTopicResources resources;
    /** Re-read on every sample so the broker config knob is honored dynamically. */
    private final DoubleSupplier rateChangeThreshold;

    /** Last value written per load-record path, so we can skip immaterial updates. */
    private final ConcurrentHashMap<String, SegmentLoadStats> lastWritten = new ConcurrentHashMap<>();

    public SegmentLoadReporter(ScalableTopicResources resources, DoubleSupplier rateChangeThreshold) {
        this.resources = resources;
        this.rateChangeThreshold = rateChangeThreshold;
    }

    public SegmentLoadReporter(ScalableTopicResources resources, double rateChangeThreshold) {
        this(resources, () -> rateChangeThreshold);
    }

    /**
     * Report a segment's current load, writing to the store only if it changed materially
     * since the last write (or has never been written).
     *
     * <p>On a local cache miss (broker restart, or segment ownership just moved here) the
     * baseline is seeded from the record already in the store, and the materiality gate is
     * applied against that. Without this, the first sample after every ownership move would
     * write unconditionally and reset the record's modification time — which the controller
     * uses as "cold since" for the merge window — starving merges under frequent rebalancing.
     *
     * @return a future completing with {@code true} if a write happened, {@code false} if the
     *         sample was immaterial and skipped
     */
    public CompletableFuture<Boolean> reportIfChanged(TopicName topic, long segmentId,
                                                      SegmentLoadStats current) {
        String path = resources.segmentLoadPath(topic, segmentId);
        double threshold = rateChangeThreshold.getAsDouble();
        SegmentLoadStats last = lastWritten.get(path);
        if (last == null) {
            return resources.getSegmentLoadAsync(topic, segmentId).thenCompose(stored -> {
                stored.ifPresent(result -> lastWritten.putIfAbsent(path, result.getValue()));
                SegmentLoadStats baseline = lastWritten.get(path);
                if (baseline != null && !isMaterialChange(baseline, current, threshold)) {
                    return CompletableFuture.completedFuture(false);
                }
                return write(topic, segmentId, path, current);
            });
        }
        if (!isMaterialChange(last, current, threshold)) {
            return CompletableFuture.completedFuture(false);
        }
        return write(topic, segmentId, path, current);
    }

    private CompletableFuture<Boolean> write(TopicName topic, long segmentId, String path,
                                             SegmentLoadStats current) {
        return resources.reportSegmentLoadAsync(topic, segmentId, current)
                .thenApply(__ -> {
                    lastWritten.put(path, current);
                    return true;
                });
    }

    /**
     * Drop the cached last-written value for a segment — call when this broker stops owning
     * the segment topic (unload, seal, or delete) so the cache doesn't grow unboundedly with
     * segment churn. A later re-acquire re-seeds the baseline from the stored record.
     */
    public void forget(TopicName topic, long segmentId) {
        lastWritten.remove(resources.segmentLoadPath(topic, segmentId));
    }

    /**
     * True if any of the four rates changed by more than {@code threshold} (relative), or
     * crossed to/from zero.
     */
    static boolean isMaterialChange(SegmentLoadStats last, SegmentLoadStats current,
                                    double threshold) {
        return changed(last.msgRateIn(), current.msgRateIn(), threshold)
                || changed(last.bytesRateIn(), current.bytesRateIn(), threshold)
                || changed(last.msgRateOut(), current.msgRateOut(), threshold)
                || changed(last.bytesRateOut(), current.bytesRateOut(), threshold);
    }

    private static boolean changed(double last, double current, double threshold) {
        if (last == 0.0) {
            // Any move off zero (a segment going from idle to active) is always material;
            // staying at zero is not.
            return current != 0.0;
        }
        return Math.abs(current - last) / last > threshold;
    }
}
