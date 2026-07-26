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
package org.apache.pulsar.common.policies.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Override of the scalable-topic auto split/merge policy (PIP-483), settable at the
 * namespace level (field on {@link Policies}) and at the topic level (field on the
 * scalable-topic metadata).
 *
 * <p>Every field is optional: an unset ({@code null}) field falls through to the next
 * resolution layer — topic override → namespace override → broker configuration. Setting
 * {@code enabled = false} opts the namespace or topic out of auto split/merge entirely.
 *
 * <p>The resolved policy must satisfy the same invariants as the broker configuration
 * (positive split thresholds, split thresholds strictly above merge thresholds,
 * {@code minSegments <= maxSegments}, non-negative cooldowns); an override that would
 * violate them is rejected when it is set.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public final class AutoScalePolicyOverride {

    /** Master switch; {@code false} opts this namespace/topic out of auto split/merge. */
    private Boolean enabled;

    /** Hard ceiling on active segments; splits stop once reached. */
    private Integer maxSegments;

    /** Hard floor on active segments; merges stop once reached. */
    private Integer minSegments;

    /** Max merges in a segment's lineage before merges are disabled for it. */
    private Integer maxDagDepth;

    /** Minimum seconds between automatic splits (coalesces bursts). */
    private Long splitCooldownSeconds;

    /** Minimum seconds between automatic merges. */
    private Long mergeCooldownSeconds;

    /** Seconds a segment pair must stay cold before becoming merge-eligible. */
    private Long mergeWindowSeconds;

    /** Inbound msg/s above which a segment is split. */
    private Double splitMsgRateInThreshold;

    /** Inbound bytes/s above which a segment is split. */
    private Long splitBytesRateInThreshold;

    /** Outbound (dispatched) msg/s above which a segment is split. */
    private Double splitMsgRateOutThreshold;

    /** Outbound bytes/s above which a segment is split. */
    private Long splitBytesRateOutThreshold;

    /** Inbound msg/s below which a segment counts as cold for merging. */
    private Double mergeMsgRateInThreshold;

    /** Inbound bytes/s below which a segment counts as cold for merging. */
    private Long mergeBytesRateInThreshold;

    /** Outbound msg/s below which a segment counts as cold for merging. */
    private Double mergeMsgRateOutThreshold;

    /** Outbound bytes/s below which a segment counts as cold for merging. */
    private Long mergeBytesRateOutThreshold;
}
