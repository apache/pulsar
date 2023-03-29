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

import java.util.Objects;
import lombok.Data;
import org.apache.pulsar.common.policies.data.ReplicatorStats;

/**
 * Statistics about a replicator.
 */
@Data
public class ReplicatorStatsImpl implements ReplicatorStats {

    /** Total rate of messages received from the remote cluster (msg/s). */
    public double msgRateIn;

    /** Total throughput received from the remote cluster (bytes/s). */
    public double msgThroughputIn;

    /** Total rate of messages delivered to the replication-subscriber (msg/s). */
    public double msgRateOut;

    /** Total throughput delivered to the replication-subscriber (bytes/s). */
    public double msgThroughputOut;

    /** Total rate of messages expired (msg/s). */
    public double msgRateExpired;

    /** Number of messages pending to be replicated to remote cluster. */
    public long replicationBacklog;

    /** is the replication-subscriber up and running to replicate to remote cluster. */
    public boolean connected;

    /** Time in seconds from the time a message was produced to the time when it is about to be replicated. */
    public long replicationDelayInSeconds;

    /** Address of incoming replication connection. */
    public String inboundConnection;

    /** Timestamp of incoming connection establishment time. */
    public String inboundConnectedSince;

    /** Address of outbound replication connection. */
    public String outboundConnection;

    /** Timestamp of outbound connection establishment time. */
    public String outboundConnectedSince;

    public ReplicatorStatsImpl add(ReplicatorStatsImpl stats) {
        Objects.requireNonNull(stats);
        this.msgRateIn += stats.msgRateIn;
        this.msgThroughputIn += stats.msgThroughputIn;
        this.msgRateOut += stats.msgRateOut;
        this.msgThroughputOut += stats.msgThroughputOut;
        this.msgRateExpired += stats.msgRateExpired;
        this.replicationBacklog += stats.replicationBacklog;
        if (this.connected) {
            this.connected &= stats.connected;
        }
        this.replicationDelayInSeconds = Math.max(this.replicationDelayInSeconds, stats.replicationDelayInSeconds);
        return this;
    }
}
