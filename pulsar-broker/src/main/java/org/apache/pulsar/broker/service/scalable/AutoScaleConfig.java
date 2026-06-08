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
}
