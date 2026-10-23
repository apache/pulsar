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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Map;
import lombok.Data;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.stats.Rate;

/**
 * Statistics about a publisher.
 */
@Data
public class PublisherStatsImpl implements PublisherStats {
    @JsonIgnore
    private int count;

    public ProducerAccessMode accessMode;

    /** Total rate of messages published by this publisher (msg/s). */
    public double msgRateIn;

    /** Total throughput of messages published by this publisher (byte/s). */
    public double msgThroughputIn;

    /** Average message size published by this publisher. */
    public double averageMsgSize;

    /** The total rate of chunked messages published by this publisher. **/
    public double chunkedMessageRate;

    /** Id of this publisher. */
    public long producerId;

    /** Whether partial producer is supported at client. */
    public boolean supportsPartialProducer;

    /** Producer name. */
    private String producerName;
    /** Address of this publisher. */
    private String address;
    /** Timestamp of connection. */
    private String connectedSince;
    /** Client library version. */
    private String clientVersion;

    /** Metadata (key/value strings) associated with this publisher. */
    public Map<String, String> metadata;

    @JsonIgnore
    private final Rate msgIn = new Rate();
    @JsonIgnore
    private final Rate msgChunkIn = new Rate();

    public PublisherStatsImpl add(PublisherStatsImpl stats) {
        if (stats == null) {
            throw new IllegalArgumentException("stats can't be null");
        }
        this.count++;
        this.msgRateIn += stats.msgRateIn;
        this.msgThroughputIn += stats.msgThroughputIn;
        double newAverageMsgSize = (this.averageMsgSize * (this.count - 1) + stats.averageMsgSize) / this.count;
        this.averageMsgSize = newAverageMsgSize;
        return this;
    }

    public String getProducerName() {
        return producerName;
    }

    public void setProducerName(String producerName) {
        this.producerName = producerName;
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

    public void calculateRates() {
        msgIn.calculateRate();
        msgChunkIn.calculateRate();

        msgRateIn = msgIn.getRate();
        msgThroughputIn = msgIn.getValueRate();
        averageMsgSize = msgIn.getAverageValue();
        chunkedMessageRate = msgChunkIn.getRate();
    }

    public void recordMsgIn(long messageCount, long byteCount) {
        msgIn.recordMultipleEvents(messageCount, byteCount);
    }

    @JsonIgnore
    public long getMsgInCounter() {
        return msgIn.getTotalCount();
    }

    @JsonIgnore
    public long getBytesInCounter() {
        return msgIn.getTotalValue();
    }

    public void recordChunkedMsgIn() {
        msgChunkIn.recordEvent();
    }

    @JsonIgnore
    public long getChunkedMsgInCounter() {
        return msgChunkIn.getTotalCount();
    }
}
