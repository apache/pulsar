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
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Metadata for a scalable topic, as returned by the admin API.
 *
 * <p>A scalable topic is composed of a DAG of segments, each covering an inclusive
 * hash range. The epoch is incremented on every layout change (split/merge).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScalableTopicMetadata {

    /** Incremented on every layout change (split/merge/prune). */
    private long epoch;

    /** Next available segment ID (monotonically increasing). */
    private long nextSegmentId;

    /** All segments: active + historical (keyed by segmentId). */
    @Builder.Default
    private Map<Long, SegmentInfo> segments = new LinkedHashMap<>();

    /** User-defined topic properties. */
    @Builder.Default
    private Map<String, String> properties = Map.of();

    /**
     * Describes a single segment in a scalable topic's DAG.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SegmentInfo {
        private long segmentId;
        private HashRange hashRange;
        private String state;
        private List<Long> parentIds;
        private List<Long> childIds;
        private long createdAtEpoch;
        private long sealedAtEpoch;

        public boolean isActive() {
            return "ACTIVE".equals(state);
        }

        public boolean isSealed() {
            return "SEALED".equals(state);
        }
    }

    /**
     * An inclusive hash range [start, end] within a 16-bit hash space.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class HashRange {
        private int start;
        private int end;

        @Override
        public String toString() {
            return "[" + String.format("%04x", start) + ", " + String.format("%04x", end) + "]";
        }
    }
}
