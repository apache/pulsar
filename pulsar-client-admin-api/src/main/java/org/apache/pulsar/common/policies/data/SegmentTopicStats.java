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
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.ProducerAccessMode;

/**
 * Stats of a single segment of a scalable topic: the traffic, storage and backlog of the
 * topic backing the segment, the producers attached to it, and its subscriptions with their
 * consumers. This is the regular topic stats trimmed to what matters for a segment — no
 * lifetime counters, quotas, offload, compaction, transaction, Key_Shared or throttling
 * internals — with rates rounded to three decimals and times reported as ISO-8601 dates.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentTopicStats {

    /** Broker currently serving the segment. */
    private String ownerBroker;

    /** Rate of messages published on the segment (msg/s). */
    private double msgRateIn;

    /** Rate of bytes published on the segment (byte/s). */
    private double byteRateIn;

    /** Rate of messages dispatched from the segment (msg/s). */
    private double msgRateOut;

    /** Rate of bytes dispatched from the segment (byte/s). */
    private double byteRateOut;

    /** Average size of published messages (bytes). */
    private double averageMsgSize;

    /** Space used to store the segment's messages (bytes). */
    private long storageSize;

    /** Estimated total unconsumed (backlog) size (bytes). */
    private long backlogSize;

    /** Space used by the segment's offloaded messages (bytes). */
    private long offloadedStorageSize;

    /**
     * Age in seconds of the oldest unacknowledged message across the segment's subscriptions
     * as of the last backlog check, or -1 when unknown.
     */
    private long oldestBacklogMessageAgeSeconds;

    /** Producers waiting for exclusive access to the segment. */
    private int waitingPublishers;

    /** When the segment's topic was created (ISO-8601), or absent when unknown. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String topicCreationTime;

    /** Producers attached to the segment. */
    private List<ProducerStats> producers = new ArrayList<>();

    /** Subscriptions on the segment, keyed by name. */
    private Map<String, SubscriptionStats> subscriptions = new LinkedHashMap<>();

    /** A producer attached to the segment. */
    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProducerStats {

        /** Producer name. */
        private String producerName;

        /** Producer access mode. */
        private ProducerAccessMode accessMode;

        /** Rate of messages published by this producer (msg/s). */
        private double msgRateIn;

        /** Rate of bytes published by this producer (byte/s). */
        private double byteRateIn;

        /** Average message size published by this producer (bytes). */
        private double averageMsgSize;

        /** Address of this producer. */
        private String address;

        /** Timestamp of connection. */
        private String connectedSince;

        /** Client library version. */
        private String clientVersion;
    }

    /** A subscription on the segment. */
    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SubscriptionStats {

        /** The subscription type on the segment's topic (Exclusive, Shared, Key_Shared, ...). */
        private String type;

        /** Rate of messages delivered on this subscription (msg/s). */
        private double msgRateOut;

        /** Rate of bytes delivered on this subscription (byte/s). */
        private double byteRateOut;

        /** Rate of messages redelivered on this subscription (msg/s). */
        private double msgRateRedeliver;

        /** Rate of message acknowledgements (msg/s). */
        private double messageAckRate;

        /** Number of entries in the backlog. */
        private long msgBacklog;

        /** Size of the backlog in bytes. */
        private long backlogSize;

        /** Messages delivered but not yet acknowledged. */
        private long unackedMessages;

        /** Age in seconds of the oldest unacknowledged message, or -1 when unknown. */
        private long oldestBacklogMessageAgeSeconds;

        /** Whether dispatching is blocked because the unacked-message threshold was reached. */
        private boolean blockedSubscriptionOnUnackedMsgs;

        /** When a message was last delivered (ISO-8601), or absent if never. */
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private String lastConsumedTime;

        /** When a message was last acknowledged (ISO-8601), or absent if never. */
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private String lastAckedTime;

        /** Consumers attached to this subscription. */
        private List<ConsumerStats> consumers = new ArrayList<>();
    }

    /** A consumer attached to a subscription on the segment. */
    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ConsumerStats {

        /** Consumer name. */
        private String consumerName;

        /** Rate of messages delivered to the consumer (msg/s). */
        private double msgRateOut;

        /** Rate of bytes delivered to the consumer (byte/s). */
        private double byteRateOut;

        /** Rate of messages redelivered to the consumer (msg/s). */
        private double msgRateRedeliver;

        /** Rate of message acknowledgements by the consumer (msg/s). */
        private double messageAckRate;

        /** Number of available message permits. */
        private int availablePermits;

        /** Messages delivered to the consumer but not yet acknowledged. */
        private long unackedMessages;

        /** Whether the consumer is blocked because the unacked-message threshold was reached. */
        private boolean blockedConsumerOnUnackedMsgs;

        /** Address of this consumer. */
        private String address;

        /** Timestamp of connection. */
        private String connectedSince;

        /** Client library version. */
        private String clientVersion;

        /** When a message was last delivered to the consumer (ISO-8601), or absent if never. */
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private String lastConsumedTime;

        /** When the consumer last acknowledged a message (ISO-8601), or absent if never. */
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private String lastAckedTime;
    }
}
