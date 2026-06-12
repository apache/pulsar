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

import java.time.Duration;
import lombok.Builder;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;

/**
 * Fully-resolved auto split/merge policy for a single scalable topic (PIP-483).
 *
 * <p>This is the flattened result of merging broker config defaults with any namespace and
 * topic overrides. The {@link AutoScalePolicyEvaluator} reads it directly — it never sees
 * the partial override objects or the broker config.
 *
 * <p>All thresholds are absolute (msg/s and bytes/s). Split thresholds must sit strictly
 * above the corresponding merge thresholds: the dead-band between them is the hysteresis
 * that prevents a just-merged segment from immediately re-qualifying for a split.
 *
 * @param enabled           whether auto split/merge is active for this topic; when false the
 *                          evaluator always returns {@code NoAction}
 * @param maxSegments       hard ceiling on active segments; splits stop once reached
 * @param minSegments       hard floor on active segments; merges stop once reached
 * @param maxDagDepth       max merges allowed in a segment's lineage; a pair is merge-eligible
 *                          only while neither side has reached this depth (splits are unaffected)
 * @param splitCooldown     minimum time between automatic splits on the topic; short, only to
 *                          coalesce a burst of near-simultaneous triggers
 * @param mergeCooldown     minimum time between automatic merges on the topic
 * @param mergeWindow       how long a segment must continuously stay below every merge threshold
 *                          before it becomes merge-eligible (measured from the load record's
 *                          metadata-store last-modified time)
 * @param splitMsgRateIn    inbound msg/s above which a segment is split
 * @param splitBytesRateIn  inbound bytes/s above which a segment is split
 * @param splitMsgRateOut   outbound (dispatched) msg/s above which a segment is split
 * @param splitBytesRateOut outbound bytes/s above which a segment is split
 * @param mergeMsgRateIn    inbound msg/s below which a segment counts as cold for merging
 * @param mergeBytesRateIn  inbound bytes/s below which a segment counts as cold for merging
 * @param mergeMsgRateOut   outbound msg/s below which a segment counts as cold for merging
 * @param mergeBytesRateOut outbound bytes/s below which a segment counts as cold for merging
 */
@Builder(toBuilder = true)
public record AutoScaleConfig(
        boolean enabled,
        int maxSegments,
        int minSegments,
        int maxDagDepth,
        Duration splitCooldown,
        Duration mergeCooldown,
        Duration mergeWindow,
        double splitMsgRateIn,
        double splitBytesRateIn,
        double splitMsgRateOut,
        double splitBytesRateOut,
        double mergeMsgRateIn,
        double mergeBytesRateIn,
        double mergeMsgRateOut,
        double mergeBytesRateOut
) {

    /**
     * Build the cluster-wide default policy from broker configuration. Per-namespace and
     * per-topic overrides (when added) are layered on top of this via {@code toBuilder()}.
     *
     * @param conf the broker service configuration
     * @return the resolved policy reflecting the {@code scalableTopic*} settings
     */
    public static AutoScaleConfig fromBrokerConfig(ServiceConfiguration conf) {
        return brokerDefaults(conf).validated();
    }

    /**
     * Resolve the effective policy for a topic: broker defaults, overlaid with the namespace
     * override, overlaid with the topic override (most-specific wins per field; {@code null}
     * override fields fall through).
     *
     * @param conf              the broker configuration (cluster-wide defaults)
     * @param namespaceOverride the namespace-level override, or {@code null} if none
     * @param topicOverride     the topic-level override, or {@code null} if none
     * @return the validated effective policy
     * @throws IllegalArgumentException if the resolved policy violates an invariant
     */
    public static AutoScaleConfig resolve(ServiceConfiguration conf,
                                          AutoScalePolicyOverride namespaceOverride,
                                          AutoScalePolicyOverride topicOverride) {
        AutoScaleConfig config = brokerDefaults(conf);
        config = applyOverride(config, namespaceOverride);
        config = applyOverride(config, topicOverride);
        return config.validated();
    }

    private static AutoScaleConfig brokerDefaults(ServiceConfiguration conf) {
        return AutoScaleConfig.builder()
                .enabled(conf.isScalableTopicAutoScaleEnabled())
                .maxSegments(conf.getScalableTopicMaxSegments())
                .minSegments(conf.getScalableTopicMinSegments())
                .maxDagDepth(conf.getScalableTopicMaxDagDepth())
                .splitCooldown(Duration.ofSeconds(conf.getScalableTopicSplitCooldownSeconds()))
                .mergeCooldown(Duration.ofSeconds(conf.getScalableTopicMergeCooldownSeconds()))
                .mergeWindow(Duration.ofSeconds(conf.getScalableTopicMergeWindowSeconds()))
                .splitMsgRateIn(conf.getScalableTopicSplitMsgRateInThreshold())
                .splitBytesRateIn(conf.getScalableTopicSplitBytesRateInThreshold())
                .splitMsgRateOut(conf.getScalableTopicSplitMsgRateOutThreshold())
                .splitBytesRateOut(conf.getScalableTopicSplitBytesRateOutThreshold())
                .mergeMsgRateIn(conf.getScalableTopicMergeMsgRateInThreshold())
                .mergeBytesRateIn(conf.getScalableTopicMergeBytesRateInThreshold())
                .mergeMsgRateOut(conf.getScalableTopicMergeMsgRateOutThreshold())
                .mergeBytesRateOut(conf.getScalableTopicMergeBytesRateOutThreshold())
                .build();
    }

    private static AutoScaleConfig applyOverride(AutoScaleConfig base, AutoScalePolicyOverride o) {
        if (o == null) {
            return base;
        }
        AutoScaleConfigBuilder b = base.toBuilder();
        if (o.getEnabled() != null) {
            b.enabled(o.getEnabled());
        }
        if (o.getMaxSegments() != null) {
            b.maxSegments(o.getMaxSegments());
        }
        if (o.getMinSegments() != null) {
            b.minSegments(o.getMinSegments());
        }
        if (o.getMaxDagDepth() != null) {
            b.maxDagDepth(o.getMaxDagDepth());
        }
        if (o.getSplitCooldownSeconds() != null) {
            b.splitCooldown(Duration.ofSeconds(o.getSplitCooldownSeconds()));
        }
        if (o.getMergeCooldownSeconds() != null) {
            b.mergeCooldown(Duration.ofSeconds(o.getMergeCooldownSeconds()));
        }
        if (o.getMergeWindowSeconds() != null) {
            b.mergeWindow(Duration.ofSeconds(o.getMergeWindowSeconds()));
        }
        if (o.getSplitMsgRateInThreshold() != null) {
            b.splitMsgRateIn(o.getSplitMsgRateInThreshold());
        }
        if (o.getSplitBytesRateInThreshold() != null) {
            b.splitBytesRateIn(o.getSplitBytesRateInThreshold());
        }
        if (o.getSplitMsgRateOutThreshold() != null) {
            b.splitMsgRateOut(o.getSplitMsgRateOutThreshold());
        }
        if (o.getSplitBytesRateOutThreshold() != null) {
            b.splitBytesRateOut(o.getSplitBytesRateOutThreshold());
        }
        if (o.getMergeMsgRateInThreshold() != null) {
            b.mergeMsgRateIn(o.getMergeMsgRateInThreshold());
        }
        if (o.getMergeBytesRateInThreshold() != null) {
            b.mergeBytesRateIn(o.getMergeBytesRateInThreshold());
        }
        if (o.getMergeMsgRateOutThreshold() != null) {
            b.mergeMsgRateOut(o.getMergeMsgRateOutThreshold());
        }
        if (o.getMergeBytesRateOutThreshold() != null) {
            b.mergeBytesRateOut(o.getMergeBytesRateOutThreshold());
        }
        return b.build();
    }

    /**
     * Validate the invariants the evaluator depends on; returns {@code this} for chaining.
     *
     * <p>In particular every split threshold must be strictly positive — the evaluator
     * scores overload as {@code rate / splitThreshold}, and a zero threshold would make any
     * positive rate score {@code Infinity} (permanent split pressure) while a zero rate
     * scores {@code NaN} (silently ignored). Catching misconfiguration here surfaces a
     * clear error at the policy-resolution layer instead.
     *
     * @throws IllegalArgumentException if any invariant is violated
     */
    public AutoScaleConfig validated() {
        check(minSegments >= 1, "minSegments must be >= 1");
        check(maxSegments >= minSegments, "maxSegments must be >= minSegments");
        check(maxDagDepth >= 0, "maxDagDepth must be >= 0");
        check(!splitCooldown.isNegative(), "splitCooldown must not be negative");
        check(!mergeCooldown.isNegative(), "mergeCooldown must not be negative");
        check(!mergeWindow.isNegative(), "mergeWindow must not be negative");
        check(splitMsgRateIn > 0, "splitMsgRateInThreshold must be > 0");
        check(splitBytesRateIn > 0, "splitBytesRateInThreshold must be > 0");
        check(splitMsgRateOut > 0, "splitMsgRateOutThreshold must be > 0");
        check(splitBytesRateOut > 0, "splitBytesRateOutThreshold must be > 0");
        check(mergeMsgRateIn >= 0, "mergeMsgRateInThreshold must be >= 0");
        check(mergeBytesRateIn >= 0, "mergeBytesRateInThreshold must be >= 0");
        check(mergeMsgRateOut >= 0, "mergeMsgRateOutThreshold must be >= 0");
        check(mergeBytesRateOut >= 0, "mergeBytesRateOutThreshold must be >= 0");
        check(splitMsgRateIn > mergeMsgRateIn,
                "splitMsgRateInThreshold must be > mergeMsgRateInThreshold (hysteresis)");
        check(splitBytesRateIn > mergeBytesRateIn,
                "splitBytesRateInThreshold must be > mergeBytesRateInThreshold (hysteresis)");
        check(splitMsgRateOut > mergeMsgRateOut,
                "splitMsgRateOutThreshold must be > mergeMsgRateOutThreshold (hysteresis)");
        check(splitBytesRateOut > mergeBytesRateOut,
                "splitBytesRateOutThreshold must be > mergeBytesRateOutThreshold (hysteresis)");
        return this;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("Invalid auto split/merge configuration: " + message);
        }
    }
}
