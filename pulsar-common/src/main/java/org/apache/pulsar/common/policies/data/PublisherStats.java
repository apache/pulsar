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

/**
 */
public class PublisherStats {
    /** Total rate of messages published by this publisher. msg/s */
    public double msgRateIn;

    /** Total throughput of messages published by this publisher. byte/s */
    public double msgThroughputIn;

    /** Average message size published by this publisher */
    public double averageMsgSize;

    /** Id of this publisher */
    public long producerId;

    /** Producer name */
    public String producerName;

    /** Address of this publisher */
    public String address;

    /** Timestamp of connection */
    public String connectedSince;
    
    /** Client library version */
    public String clientVersion;

    public PublisherStats add(PublisherStats stats) {
        checkNotNull(stats);
        this.msgRateIn += stats.msgRateIn;
        this.msgThroughputIn += stats.msgThroughputIn;
        this.averageMsgSize += stats.averageMsgSize;
        return this;
    }
}
