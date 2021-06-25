/**
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    /** Total chunked messages dispatched. */
    public double chunkedMessageRate;

    /** Name of the consumer. */
    public String consumerName;

    /** Number of available message permits for the consumer. */
    public int availablePermits;

    /** Number of unacknowledged messages for the consumer. */
    public int unackedMessages;

    /** Number of average messages per entry for the consumer consumed. */
    public int avgMessagesPerEntry;

    /** Flag to verify if consumer is blocked due to reaching threshold of unacked messages. */
    public boolean blockedConsumerOnUnackedMsgs;

    /** The read position of the cursor when the consumer joining. */
    public String readPositionWhenJoining;

    /** Address of this consumer. */
    @JsonIgnore
    private int addressOffset = -1;
    @JsonIgnore
    private int addressLength;

    /** Timestamp of connection. */
    @JsonIgnore
    private int connectedSinceOffset = -1;
    @JsonIgnore
    private int connectedSinceLength;

    /** Client library version. */
    @JsonIgnore
    private int clientVersionOffset = -1;
    @JsonIgnore
    private int clientVersionLength;

    public long lastAckedTimestamp;
    public long lastConsumedTimestamp;

    /** Hash ranges assigned to this consumer if is Key_Shared sub mode. **/
    public List<String> keyHashRanges;

    /** Metadata (key/value strings) associated with this consumer. */
    public Map<String, String> metadata;

    /**
     * In order to prevent multiple string object allocation under stats: create a string-buffer
     * that stores data for all string place-holders.
     */
    @JsonIgnore
    private StringBuilder stringBuffer = new StringBuilder();

    public ConsumerStatsImpl add(ConsumerStatsImpl stats) {
        Objects.requireNonNull(stats);
        this.msgRateOut += stats.msgRateOut;
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
        return addressOffset == -1 ? null : stringBuffer.substring(addressOffset, addressOffset + addressLength);
    }

    public void setAddress(String address) {
        if (address == null) {
            this.addressOffset = -1;
            return;
        }
        this.addressOffset = this.stringBuffer.length();
        this.addressLength = address.length();
        this.stringBuffer.append(address);
    }

    public String getConnectedSince() {
        return connectedSinceOffset == -1 ? null
                : stringBuffer.substring(connectedSinceOffset, connectedSinceOffset + connectedSinceLength);
    }

    public void setConnectedSince(String connectedSince) {
        if (connectedSince == null) {
            this.connectedSinceOffset = -1;
            return;
        }
        this.connectedSinceOffset = this.stringBuffer.length();
        this.connectedSinceLength = connectedSince.length();
        this.stringBuffer.append(connectedSince);
    }

    public String getClientVersion() {
        return clientVersionOffset == -1 ? null
                : stringBuffer.substring(clientVersionOffset, clientVersionOffset + clientVersionLength);
    }

    public void setClientVersion(String clientVersion) {
        if (clientVersion == null) {
            this.clientVersionOffset = -1;
            return;
        }
        this.clientVersionOffset = this.stringBuffer.length();
        this.clientVersionLength = clientVersion.length();
        this.stringBuffer.append(clientVersion);
    }

    public String getReadPositionWhenJoining() {
        return readPositionWhenJoining;
    }
}
