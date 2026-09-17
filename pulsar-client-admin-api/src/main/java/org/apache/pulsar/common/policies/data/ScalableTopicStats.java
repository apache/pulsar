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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.ProducerAccessMode;

/**
 * Stats for a scalable topic as a whole: the segment layout (the DAG), the traffic and
 * storage aggregated across every segment, the producers attached to the topic, and the
 * subscriptions with their backlog broken down across segments.
 *
 * <p>Rates are rounded to three decimals. Per-segment numbers come from each segment's
 * owning broker; a segment whose stats could not be collected keeps its layout entry but
 * reports no owner (see {@link SegmentStats#getOwnerBroker()}) and contributes nothing to
 * the aggregates.
 *
 * <p>The stats of a single segment's underlying topic — its own rates and storage, cursors,
 * per-consumer permits and so on — are served separately as a regular {@link TopicStats}
 * by {@code ScalableTopics.getSegmentStats(topic, segmentId)}.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScalableTopicStats {

    /** The segment DAG. */
    private LayoutStats layout = new LayoutStats();

    /** Total rate of messages published on the topic (msg/s), summed across segments. */
    private double msgRateIn;

    /** Total rate of bytes published on the topic (byte/s), summed across segments. */
    private double byteRateIn;

    /** Total rate of messages dispatched for the topic (msg/s), summed across segments. */
    private double msgRateOut;

    /** Total rate of bytes dispatched for the topic (byte/s), summed across segments. */
    private double byteRateOut;

    /** Average size of published messages (bytes): {@code byteRateIn / msgRateIn}. */
    private double averageMsgSize;

    /** Space used to store the messages of every segment (bytes). */
    private long storageSize;

    /** Estimated total unconsumed (backlog) size across every segment (bytes). */
    private long backlogSize;

    /** Producers attached to the topic. */
    private List<ProducerStats> producers = new ArrayList<>();

    /** Per-subscription stats keyed by subscription name. */
    private Map<String, SubscriptionStats> subscriptions = new LinkedHashMap<>();

    /** The segment DAG: the layout epoch and every segment, active or sealed. */
    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LayoutStats {

        /** Current layout epoch, incremented on every split, merge, rebucket or prune. */
        private long epoch;

        /** The segments, keyed by segment ID. */
        private Map<Long, SegmentStats> segments = new LinkedHashMap<>();
    }

    /**
     * One node of the segment DAG: the segment's name (which encodes its hash range and ID),
     * its state, its edges and the broker serving it.
     */
    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SegmentStats {

        /**
         * The segment's name: {@code segment://tenant/ns/topic/<hashStart>-<hashEnd>-<segmentId>},
         * or the {@code persistent://...} topic wrapped by a legacy segment of a migrated topic.
         */
        private String name;

        /** Segment state: "ACTIVE" or "SEALED". */
        private String state;

        /** Parent segment IDs; omitted from the JSON form when empty (an initial segment). */
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private List<Long> parentIds = new ArrayList<>();

        /** Child segment IDs; omitted from the JSON form when empty (an active segment). */
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private List<Long> childIds = new ArrayList<>();

        /** Number of entry-buckets the segment is divided into (PIP-486). */
        private int entryBuckets;

        /**
         * Broker currently serving the segment's topic, or {@code null} when its stats could
         * not be collected.
         */
        private String ownerBroker;

        @JsonIgnore
        public boolean isActive() {
            return "ACTIVE".equals(state);
        }

        @JsonIgnore
        public boolean isSealed() {
            return "SEALED".equals(state);
        }
    }

    /**
     * A producer attached to the topic. A scalable-topic producer publishes through one
     * underlying producer per active segment; those are folded into a single entry when
     * they carry the producer's name (suffixed per segment), otherwise each underlying
     * producer is listed on its own.
     */
    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProducerStats {

        /** Producer name. */
        private String producerName;

        /** Total rate of messages published by this producer (msg/s). */
        private double msgRateIn;

        /** Total rate of bytes published by this producer (byte/s). */
        private double byteRateIn;

        /** Average message size published by this producer (bytes). */
        private double averageMsgSize;

        /** Producer access mode. */
        private ProducerAccessMode accessMode;

        /** Address of this producer. */
        private String address;

        /** Timestamp of connection. */
        private String connectedSince;

        /** Client library version. */
        private String clientVersion;
    }

    /**
     * A subscription on the scalable topic, aggregated across every segment that holds a
     * cursor for it.
     */
    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SubscriptionStats {

        /**
         * Subscription type: {@code STREAM} (controller-managed, ordered) or {@code QUEUE}
         * (consumers attach to every segment directly).
         */
        private ScalableSubscriptionType type;

        /** Number of entries in the backlog, summed across segments. */
        private long msgBacklog;

        /** Size of the backlog in bytes, summed across segments. */
        private long backlogSize;

        /** Messages delivered but not yet acknowledged, summed across segments. */
        private long unackedMessages;

        /** Total rate of messages delivered on this subscription (msg/s). */
        private double msgRateOut;

        /** Total rate of bytes delivered on this subscription (byte/s). */
        private double byteRateOut;

        /** Total rate of messages redelivered on this subscription (msg/s). */
        private double msgRateRedeliver;

        /** Total rate of message acknowledgements (msg/s). */
        private double messageAckRate;

        /** Per-segment breakdown of the subscription, keyed by segment ID. */
        private Map<Long, SegmentSubscriptionStats> segments = new LinkedHashMap<>();

        /** Consumers on this subscription. */
        private List<ConsumerStats> consumers = new ArrayList<>();
    }

    /** The share of a subscription that lives on one segment. */
    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SegmentSubscriptionStats {

        /** Number of entries in the backlog on this segment. */
        private long msgBacklog;

        /** Size of the backlog on this segment in bytes. */
        private long backlogSize;

        /** Messages delivered from this segment but not yet acknowledged. */
        private long unackedMessages;

        /** Rate of messages delivered from this segment (msg/s). */
        private double msgRateOut;

        /** Rate of bytes delivered from this segment (byte/s). */
        private double byteRateOut;

        /** Number of consumers attached to the subscription on this segment. */
        private int consumerCount;
    }

    /**
     * A consumer on a subscription. Per-segment underlying consumers that carry the
     * consumer's name (suffixed per segment) are folded into one entry; on a STREAM
     * subscription the entry also reflects the controller's session for the consumer, so a
     * registered consumer shows up even while disconnected (within its grace period) or
     * idle (assigned no segment).
     */
    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ConsumerStats {

        /** Consumer name. */
        private String consumerName;

        /** Whether the consumer is currently connected. */
        private boolean connected;

        /**
         * IDs of the segments this consumer is attached to; on a STREAM subscription, the
         * segments the controller has assigned to it.
         */
        private List<Long> segmentIds = new ArrayList<>();

        /** Total rate of messages delivered to the consumer (msg/s). */
        private double msgRateOut;

        /** Total rate of bytes delivered to the consumer (byte/s). */
        private double byteRateOut;

        /** Messages delivered to the consumer but not yet acknowledged. */
        private long unackedMessages;

        /** Number of available message permits, summed across segments. */
        private int availablePermits;

        /** Address of this consumer. */
        private String address;

        /** Timestamp of connection. */
        private String connectedSince;

        /** Client library version. */
        private String clientVersion;
    }
}
