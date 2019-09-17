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
package org.apache.pulsar.common.policies.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

/**
 * Statistics about a publisher.
 */
public class PublisherStats {
    private int count;

    /** Total rate of messages published by this publisher (msg/s). */
    public double msgRateIn;

    /** Total throughput of messages published by this publisher (byte/s). */
    public double msgThroughputIn;

    /** Average message size published by this publisher. */
    public double averageMsgSize;

    /** Id of this publisher. */
    public long producerId;

    /** Producer name. */
    private int producerNameOffset = -1;
    private int producerNameLength;

    /** Address of this publisher. */
    private int addressOffset = -1;
    private int addressLength;

    /** Timestamp of connection. */
    private int connectedSinceOffset = -1;
    private int connectedSinceLength;

    /** Client library version. */
    private int clientVersionOffset = -1;
    private int clientVersionLength;

    /**
     * In order to prevent multiple string objects under stats: create a string-buffer that stores data for all string
     * place-holders.
     */
    private StringBuilder stringBuffer = new StringBuilder();

    /** Metadata (key/value strings) associated with this publisher. */
    public Map<String, String> metadata;

    public PublisherStats add(PublisherStats stats) {
        checkNotNull(stats);
        this.count++;
        this.msgRateIn += stats.msgRateIn;
        this.msgThroughputIn += stats.msgThroughputIn;
        double newAverageMsgSize = (this.averageMsgSize * (this.count - 1) + stats.averageMsgSize) / this.count;
        this.averageMsgSize = newAverageMsgSize;
        return this;
    }

    public String getProducerName() {
        return producerNameOffset == -1 ? null
                : stringBuffer.substring(producerNameOffset, producerNameOffset + producerNameLength);
    }

    public void setProducerName(String producerName) {
        if (producerName == null) {
            this.producerNameOffset = -1;
            return;
        }
        this.producerNameOffset = this.stringBuffer.length();
        this.producerNameLength = producerName.length();
        this.stringBuffer.append(producerName);
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
}
