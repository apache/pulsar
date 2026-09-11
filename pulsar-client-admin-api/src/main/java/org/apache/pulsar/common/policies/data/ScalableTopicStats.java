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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Aggregated stats for a scalable topic.
 *
 * <p>Combines segment-DAG state (from the topic metadata) with per-subscription consumer
 * counts (from the persisted consumer registrations). Per-segment throughput rates are
 * not yet populated — that requires collecting stats from each segment-owning broker and
 * will be added in a follow-up.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScalableTopicStats {

    /** Current layout epoch. */
    private long epoch;

    /** Total number of segments in the DAG (active + sealed). */
    private int totalSegments;

    /** Number of segments currently in ACTIVE state. */
    private int activeSegments;

    /** Number of segments currently in SEALED state. */
    private int sealedSegments;

    /** Per-segment stats keyed by segment ID. */
    @Builder.Default
    private Map<Long, SegmentStats> segments = new LinkedHashMap<>();

    /** Per-subscription stats keyed by subscription name. */
    @Builder.Default
    private Map<String, SubscriptionStats> subscriptions = new LinkedHashMap<>();

    /**
     * Per-segment stats.
     *
     * @param name  full segment topic name (e.g. {@code segment://tenant/ns/topic/<descriptor>}),
     *              which already encodes the segment ID and hash range
     * @param state segment state: "ACTIVE" or "SEALED"
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SegmentStats(String name, String state) {
    }

    /**
     * Per-subscription stats.
     *
     * @param consumerCount number of persisted consumer registrations
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SubscriptionStats(int consumerCount) {
    }
}
