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

import java.util.List;
import java.util.Map;

/**
 * Consumer statistics.
 */
public interface ConsumerStats {
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

    /** Total chunked messages dispatched. */
    double getChunkedMessageRate();

    /** Name of the consumer. */
    String getConsumerName();

    /** Number of available message permits for the consumer. */
    int getAvailablePermits();

    /** Number of unacknowledged messages for the consumer. */
    int getUnackedMessages();

    /** Number of average messages per entry for the consumer consumed. */
    int getAvgMessagesPerEntry();

    /** Flag to verify if consumer is blocked due to reaching threshold of unacked messages. */
    boolean isBlockedConsumerOnUnackedMsgs();

    /** The read position of the cursor when the consumer joining. */
    String getReadPositionWhenJoining();

    /** Address of this consumer. */
    String getAddress();

    /** Timestamp of connection. */
    String getConnectedSince();

    /** Client library version. */
    String getClientVersion();

    long getLastAckedTimestamp();
    long getLastConsumedTimestamp();

    /** Hash ranges assigned to this consumer if is Key_Shared sub mode. **/
    List<String> getKeyHashRanges();

    /** Metadata (key/value strings) associated with this consumer. */
    Map<String, String> getMetadata();
}
