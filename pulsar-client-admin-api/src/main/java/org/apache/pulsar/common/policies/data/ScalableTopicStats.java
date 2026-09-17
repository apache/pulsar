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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.ProducerAccessMode;

/**
 * Stats for a scalable topic as a whole: the segment DAG with per-segment load, the
 * subscriptions with their backlog broken down across segments, and the producers
 * attached to the topic.
 *
 * <p>Rates and counters are aggregated across every segment in the DAG (active and
 * sealed). Per-segment numbers come from each segment's owning broker; a segment whose
 * stats could not be collected keeps its DAG entry but reports no owner and zero load
 * (see {@link SegmentStats#getOwnerBroker()}).
 *
 * <p>The stats of a single segment's underlying topic — cursors, per-consumer permits,
 * ledger details and so on — are served separately as a regular {@link TopicStats} by
 * {@code ScalableTopics.getSegmentStats(topic, segmentId)}.
 */
@Data
@NoArgsConstructor
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

    /** Total rate of messages published on the topic (msg/s), summed across segments. */
    private double msgRateIn;

    /** Total throughput of messages published on the topic (byte/s), summed across segments. */
    private double msgThroughputIn;

    /** Total rate of messages dispatched for the topic (msg/s), summed across segments. */
    private double msgRateOut;

    /** Total throughput of messages dispatched for the topic (byte/s), summed across segments. */
    private double msgThroughputOut;

    /** Total messages published to the topic since its segments were loaded. */
    private long msgInCounter;

    /** Total bytes published to the topic since its segments were loaded. */
    private long bytesInCounter;

    /** Total messages delivered to consumers since the segments were loaded. */
    private long msgOutCounter;

    /** Total bytes delivered to consumers since the segments were loaded. */
    private long bytesOutCounter;

    /** Average size of published messages (bytes): {@code msgThroughputIn / msgRateIn}. */
    private double averageMsgSize;

    /** Space used to store the messages of every segment (bytes). */
    private long storageSize;

    /** Estimated total unconsumed (backlog) size across every segment (bytes). */
    private long backlogSize;

    /** The segment DAG, keyed by segment ID. */
    private Map<Long, SegmentStats> segments = new LinkedHashMap<>();

    /** Producers attached to the topic. */
    private List<ProducerStats> producers = new ArrayList<>();

    /** Per-subscription stats keyed by subscription name. */
    private Map<String, SubscriptionStats> subscriptions = new LinkedHashMap<>();

    /**
     * One node of the segment DAG: its identity, its edges, its lifecycle and the load the
     * owning broker reports for it.
     */
    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SegmentStats {

        /** Segment ID, unique within the topic. */
        private long segmentId;

        /**
         * The topic backing this segment: {@code segment://tenant/ns/topic/<descriptor>}, or the
         * {@code persistent://...} topic wrapped by a legacy segment of a migrated topic.
         */
        private String topic;

        /** Inclusive hash range [start, end] this segment covers. */
        private ScalableTopicMetadata.HashRange hashRange;

        /** Segment state: "ACTIVE" or "SEALED". */
        private String state;

        /** Parent segment IDs (empty for the initial segments). */
        private List<Long> parentIds = new ArrayList<>();

        /** Child segment IDs (empty for active segments). */
        private List<Long> childIds = new ArrayList<>();

        /** Wall-clock millis at which the segment was created. */
        private long createdAtMs;

        /** Wall-clock millis at which the segment was sealed, or -1 while active. */
        private long sealedAtMs;

        /** Number of entry-buckets the segment is divided into (PIP-486). */
        private int entryBuckets;

        /**
         * Broker currently serving the segment's topic, or {@code null} when its stats could
         * not be collected — in which case the load fields below are zero.
         */
        private String ownerBroker;

        /** Rate of messages published on this segment (msg/s). */
        private double msgRateIn;

        /** Throughput of messages published on this segment (byte/s). */
        private double msgThroughputIn;

        /** Rate of messages dispatched from this segment (msg/s). */
        private double msgRateOut;

        /** Throughput of messages dispatched from this segment (byte/s). */
        private double msgThroughputOut;

        /** Space used to store this segment's messages (bytes). */
        private long storageSize;

        public boolean isActive() {
            return "ACTIVE".equals(state);
        }

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

        /** IDs of the segments this producer is attached to. */
        private List<Long> segmentIds = new ArrayList<>();

        /** Total rate of messages published by this producer (msg/s). */
        private double msgRateIn;

        /** Total throughput of messages published by this producer (byte/s). */
        private double msgThroughputIn;

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

        /** Total throughput delivered on this subscription (byte/s). */
        private double msgThroughputOut;

        /** Total rate of messages redelivered on this subscription (msg/s). */
        private double msgRateRedeliver;

        /** Total rate of message acknowledgements (msg/s). */
        private double messageAckRate;

        /** Total messages delivered to consumers (msg). */
        private long msgOutCounter;

        /** Total bytes delivered to consumers (bytes). */
        private long bytesOutCounter;

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

        /** Throughput delivered from this segment (byte/s). */
        private double msgThroughputOut;

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

        /** Total throughput delivered to the consumer (byte/s). */
        private double msgThroughputOut;

        /** Total messages delivered to the consumer (msg). */
        private long msgOutCounter;

        /** Total bytes delivered to the consumer (bytes). */
        private long bytesOutCounter;

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
