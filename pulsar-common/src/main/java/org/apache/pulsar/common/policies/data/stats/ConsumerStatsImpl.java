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
package org.apache.pulsar.common.policies.data.stats;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Data;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.util.DateFormatter;

/**
 * Consumer statistics.
 */
@Data
public class ConsumerStatsImpl implements ConsumerStats {
    /** Total rate of messages delivered to the consumer (msg/s). */
    public double msgRateOut;

    /** Total throughput delivered to the consumer (bytes/s). */
    public double msgThroughputOut;

    /** Total bytes delivered to consumer (bytes). */
    public long bytesOutCounter;

    /** Total messages delivered to consumer (msg). */
    public long msgOutCounter;

    /** Total rate of messages redelivered by this consumer (msg/s). */
    public double msgRateRedeliver;

    /**
     * Total rate of message ack (msg/s).
     */
    public double messageAckRate;

    /** The total rate of chunked messages delivered to this consumer. */
    public double chunkedMessageRate;

    /** Name of the consumer. */
    public String consumerName;

    /** Number of available message permits for the consumer. */
    public int availablePermits;

    /**
     * Number of unacknowledged messages for the consumer, where an unacknowledged message is one that has been
     * sent to the consumer but not yet acknowledged. This field is only meaningful when using a
     * {@link org.apache.pulsar.client.api.SubscriptionType} that tracks individual message acknowledgement, like
     * {@link org.apache.pulsar.client.api.SubscriptionType#Shared} or
     * {@link org.apache.pulsar.client.api.SubscriptionType#Key_Shared}.
     */
    public int unackedMessages;

    /** Number of average messages per entry for the consumer consumed. */
    public int avgMessagesPerEntry;

    /** Flag to verify if consumer is blocked due to reaching threshold of unacked messages. */
    public boolean blockedConsumerOnUnackedMsgs;

    /** The read position of the cursor when the consumer joining. */
    public String readPositionWhenJoining;

    /** Address of this consumer. */
    private String address;
    /** Timestamp of connection. */
    private String connectedSince;
    /** Client library version. */
    private String clientVersion;

    // ignore this json field to skip from stats in future release. replaced with readable #getLastAckedTime().
    @Deprecated
    public long lastAckedTimestamp;
    // ignore this json field to skip from stats in future release. replaced with readable #getLastConsumedTime().
    @Deprecated
    public long lastConsumedTimestamp;

    public long lastConsumedFlowTimestamp;

    /** Hash ranges assigned to this consumer if is Key_Shared sub mode. **/
    public List<String> keyHashRanges;

    /** Metadata (key/value strings) associated with this consumer. */
    public Map<String, String> metadata;

    public ConsumerStatsImpl add(ConsumerStatsImpl stats) {
        Objects.requireNonNull(stats);
        this.msgRateOut += stats.msgRateOut;
        this.messageAckRate += stats.messageAckRate;
        this.msgThroughputOut += stats.msgThroughputOut;
        this.bytesOutCounter += stats.bytesOutCounter;
        this.msgOutCounter += stats.msgOutCounter;
        this.msgRateRedeliver += stats.msgRateRedeliver;
        this.availablePermits += stats.availablePermits;
        this.unackedMessages += stats.unackedMessages;
        this.blockedConsumerOnUnackedMsgs = stats.blockedConsumerOnUnackedMsgs;
        this.readPositionWhenJoining = stats.readPositionWhenJoining;
        return this;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getConnectedSince() {
        return connectedSince;
    }

    public void setConnectedSince(String connectedSince) {
        this.connectedSince = connectedSince;
    }

    public String getClientVersion() {
        return clientVersion;
    }

    public void setClientVersion(String clientVersion) {
        this.clientVersion = clientVersion;
    }

    public String getReadPositionWhenJoining() {
        return readPositionWhenJoining;
    }

    public String getLastAckedTime() {
        return DateFormatter.format(lastAckedTimestamp);
    }

    public String getLastConsumedTime() {
        return DateFormatter.format(lastConsumedTimestamp);
    }
}
