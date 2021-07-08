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
package org.apache.pulsar.broker.stats.prometheus;

public class AggregatedReplicationStats {

    /** Total rate of messages received from the remote cluster (msg/s). */
    public double msgRateIn;

    /** Total throughput received from the remote cluster. bytes/s */
    public double msgThroughputIn;

    /** Total rate of messages delivered to the replication-subscriber. msg/s */
    public double msgRateOut;

    /** Total throughput delivered to the replication-subscriber. bytes/s */
    public double msgThroughputOut;

    /** Total rate of messages expired (msg/s). */
    public double msgRateExpired;

    /** Number of messages pending to be replicated to remote cluster. */
    public long replicationBacklog;

    /** The count of replication-subscriber up and running to replicate to remote cluster. */
    public long connectedCount;

    /** Time in seconds from the time a message was produced to the time when it is about to be replicated. */
    public long replicationDelayInSeconds;

}
