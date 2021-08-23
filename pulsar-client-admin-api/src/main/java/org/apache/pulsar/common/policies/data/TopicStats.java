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
 * Statistics for a Pulsar topic.
 */
public interface TopicStats {
    /** Total rate of messages published on the topic (msg/s). */
    double getMsgRateIn();

    /** Total throughput of messages published on the topic (byte/s). */
    double getMsgThroughputIn();

    /** Total rate of messages dispatched for the topic (msg/s). */
    double getMsgRateOut();

    /** Total throughput of messages dispatched for the topic (byte/s). */
    double getMsgThroughputOut();

    /** Total bytes published to the topic (bytes). */
    long getBytesInCounter();

    /** Total messages published to the topic (msg). */
    long getMsgInCounter();

    /** Total bytes delivered to consumer (bytes). */
    long getBytesOutCounter();

    /** Total messages delivered to consumer (msg). */
    long getMsgOutCounter();

    /** Average size of published messages (bytes). */
    double getAverageMsgSize();

    /** Topic has chunked message published on it. */
    boolean isMsgChunkPublished();

    /** Space used to store the messages for the topic (bytes). */
    long getStorageSize();

    /** Get estimated total unconsumed or backlog size in bytes. */
    long getBacklogSize();

    /** Space used to store the offloaded messages for the topic/. */
    long getOffloadedStorageSize();

    /** List of connected publishers on this topic w/ their stats. */
    List<? extends PublisherStats> getPublishers();

    int getWaitingPublishers();

    /** Map of subscriptions with their individual statistics. */
    Map<String, ? extends SubscriptionStats> getSubscriptions();

    /** Map of replication statistics by remote cluster context. */
    Map<String, ? extends ReplicatorStats> getReplication();

    String getDeduplicationStatus();

    /** The topic epoch or empty if not set. */
    Long getTopicEpoch();

    /** The number of non-contiguous deleted messages ranges. */
    int getNonContiguousDeletedMessagesRanges();

    /** The serialized size of non-contiguous deleted messages ranges. */
    int getNonContiguousDeletedMessagesRangesSerializedSize();

    /** The compaction stats. */
    CompactionStats getCompaction();
}
