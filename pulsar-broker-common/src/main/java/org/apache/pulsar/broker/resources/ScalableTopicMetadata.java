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
package org.apache.pulsar.broker.resources;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.scalable.SegmentInfo;

/**
 * Metadata for a scalable topic, stored in the metadata store at
 * {@code /topics/{tenant}/{namespace}/{topic}}.
 *
 * <p>The segments map represents the full DAG of the topic's segment history.
 * The epoch is incremented on every layout change (split/merge/prune).
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
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
     * Per-topic auto split/merge policy override (PIP-483). {@code null} means no override:
     * the namespace policy and then the broker configuration apply.
     */
    private AutoScalePolicyOverride autoScalePolicy;

    /**
     * Set while the topic is being deleted, like {@code PartitionedTopicMetadata.deleted}. No
     * controller elects itself for a topic marked deleted, so the delete can remove the
     * controller lock along with the rest of the topic's metadata. A delete that fails after
     * marking the topic leaves it marked, until a retried delete removes it.
     */
    private boolean deleted;

    /** Private identity of this topic record's lifetime, assigned once for records predating it. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String incarnationId;

    /**
     * Durable intent reserved before creating successors or terminating parents. It is kept in the
     * same record as the published DAG so that publishing the replacement and clearing the intent
     * are one CAS. Missing on records written before transition recovery was introduced.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private PendingLayoutTransition pendingLayoutTransition;

    /** The fixed plan of an irreversible layout operation, including its original source fingerprints. */
    public record PendingLayoutTransition(String id, String incarnationId, String operation, String trigger,
                                          long baseEpoch, long baseNextSegmentId, long startedAtMs,
                                          Map<Long, SegmentInfo> sources,
                                          Map<Long, SegmentInfo> replacements, long nextSegmentId) {
        public PendingLayoutTransition {
            sources = Collections.unmodifiableMap(new LinkedHashMap<>(sources));
            replacements = Collections.unmodifiableMap(new LinkedHashMap<>(replacements));
        }
    }

    /** The admin metadata response exposes only the published layout, never a broker's recovery plan. */
    public ScalableTopicMetadata withoutRecoveryMetadata() {
        return toBuilder().incarnationId(null).pendingLayoutTransition(null).build();
    }
}
