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

import java.util.List;
import java.util.Map;

/**
 * Consumer statistics.
 */
public interface ConsumerStats {
    /** the app id. */
    String getAppId();

    /** Total rate of messages delivered to the consumer (msg/s). */
    double getMsgRateOut();

    /** Total throughput delivered to the consumer (bytes/s). */
    double getMsgThroughputOut();

    /** Total bytes delivered to consumer (bytes). */
    long getBytesOutCounter();

    /** Total messages delivered to consumer (msg). */
    long getMsgOutCounter();

    /** Total rate of messages redelivered by this consumer (msg/s). */
    double getMsgRateRedeliver();

    /**
     * Total rate of message ack(msg/s).
     */
    double getMessageAckRate();

    /** The total rate of chunked messages delivered to this consumer. */
    double getChunkedMessageRate();

    /** Name of the consumer. */
    String getConsumerName();

    /** Number of available message permits for the consumer. */
    int getAvailablePermits();

    /**
     * Number of unacknowledged messages for the consumer, where an unacknowledged message is one that has been
     * sent to the consumer but not yet acknowledged. This field is only meaningful when using a
     * {@link org.apache.pulsar.client.api.SubscriptionType} that tracks individual message acknowledgement, like
     * {@link org.apache.pulsar.client.api.SubscriptionType#Shared} or
     * {@link org.apache.pulsar.client.api.SubscriptionType#Key_Shared}.
     */
    int getUnackedMessages();

    /** Number of average messages per entry for the consumer consumed. */
    int getAvgMessagesPerEntry();

    /** Flag to verify if consumer is blocked due to reaching threshold of unacked messages. */
    boolean isBlockedConsumerOnUnackedMsgs();

    /** The read position of the cursor when the consumer joining. */
    @Deprecated
    String getReadPositionWhenJoining();

    /**
     * For Key_Shared subscription in AUTO_SPLIT ordered mode:
     * Retrieves the current number of hashes in the draining state for this consumer.
     *
     * @return the current number of hashes in the draining state for this consumer
     */
    int getDrainingHashesCount();

    /**
     * For Key_Shared subscription in AUTO_SPLIT ordered mode:
     * Retrieves the total number of hashes cleared from the draining state since the consumer connected.
     *
     * @return the total number of hashes cleared from the draining state since the consumer connected
     */
    long getDrainingHashesClearedTotal();

    /**
     * For Key_Shared subscription in AUTO_SPLIT ordered mode:
     * Retrieves the total number of unacked messages for all draining hashes for this consumer.
     *
     * @return the total number of unacked messages for all draining hashes for this consumer
     */
    int getDrainingHashesUnackedMessages();

    /**
     * For Key_Shared subscription in AUTO_SPLIT ordered mode:
     * Retrieves the draining hashes for this consumer.
     *
     * @return a list of draining hashes for this consumer
     */
    List<DrainingHash> getDrainingHashes();

    /** Address of this consumer. */
    String getAddress();

    /** Timestamp of connection. */
    String getConnectedSince();

    /** Client library version. */
    String getClientVersion();

    long getLastAckedTimestamp();
    long getLastConsumedTimestamp();
    long getLastConsumedFlowTimestamp();

    /**
     * Hash ranges assigned to this consumer if in Key_Shared subscription mode.
     * This format and field is used when `subscriptionKeySharedUseClassicPersistentImplementation` is set to `false`
     * (default).
     */
    List<int[]> getKeyHashRangeArrays();

    /**
     * Hash ranges assigned to this consumer if in Key_Shared subscription mode.
     * This format and field is used when `subscriptionKeySharedUseClassicPersistentImplementation` is set to `true`.
     */
    @Deprecated
    List<String> getKeyHashRanges();

    /** Metadata (key/value strings) associated with this consumer. */
    Map<String, String> getMetadata();
}