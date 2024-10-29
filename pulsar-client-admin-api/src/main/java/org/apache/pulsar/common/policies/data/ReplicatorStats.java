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

/**
 * Statistics about a replicator.
 */
public interface ReplicatorStats {

    /** Total rate of messages received from the remote cluster (msg/s). */
    double getMsgRateIn();

    /** Total throughput received from the remote cluster (bytes/s). */
    double getMsgThroughputIn();

    /** Total rate of messages delivered to the replication-subscriber (msg/s). */
    double getMsgRateOut();

    /** Total throughput delivered to the replication-subscriber (bytes/s). */
    double getMsgThroughputOut();

    /**
     * Total bytes delivered to the replication-subscriber (bytes).
     */
    long getBytesOutCounter();

    /**
     * Total messages delivered to the replication-subscriber (msg).
     */
    long getMsgOutCounter();

    /** Total rate of messages expired (msg/s). */
    double getMsgRateExpired();

    /** Number of messages pending to be replicated to remote cluster. */
    long getReplicationBacklog();

    /** is the replication-subscriber up and running to replicate to remote cluster. */
    boolean isConnected();

    /** Time in seconds from the time a message was produced to the time when it is about to be replicated. */
    long getReplicationDelayInSeconds();

    /** Address of incoming replication connection. */
    String getInboundConnection();

    /** Timestamp of incoming connection establishment time. */
    String getInboundConnectedSince();

    /** Address of outbound replication connection. */
    String getOutboundConnection();

    /** Timestamp of outbound connection establishment time. */
    String getOutboundConnectedSince();
}
