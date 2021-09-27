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
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Statistics for a Pulsar topic.
 */
@Data
public class TopicStatsImpl implements TopicStats {
    @JsonIgnore
    private int count;

    /** Total rate of messages published on the topic (msg/s). */
    public double msgRateIn;

    /** Total throughput of messages published on the topic (byte/s). */
    public double msgThroughputIn;

    /** Total rate of messages dispatched for the topic (msg/s). */
    public double msgRateOut;

    /** Total throughput of messages dispatched for the topic (byte/s). */
    public double msgThroughputOut;

    /** Total bytes published to the topic (bytes). */
    public long bytesInCounter;

    /** Total messages published to the topic (msg). */
    public long msgInCounter;

    /** Total bytes delivered to consumer (bytes). */
    public long bytesOutCounter;

    /** Total messages delivered to consumer (msg). */
    public long msgOutCounter;

    /** Average size of published messages (bytes). */
    public double averageMsgSize;

    /** Topic has chunked message published on it. */
    public boolean msgChunkPublished;

    /** Space used to store the messages for the topic (bytes). */
    public long storageSize;

    /** Get estimated total unconsumed or backlog size in bytes. */
    public long backlogSize;

    /** Space used to store the offloaded messages for the topic/. */
    public long offloadedStorageSize;

    /** record last successful offloaded ledgerId. If no offload ledger, the value should be 0 */
    public long lastOffloadLedgerId;

    /** record last successful offloaded timestamp. If no successful offload, the value should be 0 */
    public long lastOffloadSuccessTimeStamp;

    /** record last failed offloaded timestamp. If no failed offload, the value should be 0 */
    public long lastOffloadFailureTimeStamp;

    /** List of connected publishers on this topic w/ their stats. */
    @Getter(AccessLevel.NONE)
    public List<PublisherStatsImpl> publishers;

    public int waitingPublishers;

    /** Map of subscriptions with their individual statistics. */
    @Getter(AccessLevel.NONE)
    public Map<String, SubscriptionStatsImpl> subscriptions;

    /** Map of replication statistics by remote cluster context. */
    @Getter(AccessLevel.NONE)
    public Map<String, ReplicatorStatsImpl> replication;

    public String deduplicationStatus;

    /** The topic epoch or empty if not set. */
    public Long topicEpoch;

    /** The number of non-contiguous deleted messages ranges. */
    public int nonContiguousDeletedMessagesRanges;

    /** The serialized size of non-contiguous deleted messages ranges. */
    public int nonContiguousDeletedMessagesRangesSerializedSize;

    /** The compaction stats */
    public CompactionStatsImpl compaction;

    public List<? extends PublisherStats> getPublishers() {
        return publishers;
    }

    public Map<String, ? extends SubscriptionStats> getSubscriptions() {
        return subscriptions;
    }

    public Map<String, ? extends ReplicatorStats> getReplication() {
        return replication;
    }

    public TopicStatsImpl() {
        this.publishers = new ArrayList<>();
        this.subscriptions = new HashMap<>();
        this.replication = new TreeMap<>();
        this.compaction = new CompactionStatsImpl();
    }

    public void reset() {
        this.count = 0;
        this.msgRateIn = 0;
        this.msgThroughputIn = 0;
        this.msgRateOut = 0;
        this.msgThroughputOut = 0;
        this.averageMsgSize = 0;
        this.storageSize = 0;
        this.backlogSize = 0;
        this.bytesInCounter = 0;
        this.msgInCounter = 0;
        this.bytesOutCounter = 0;
        this.msgOutCounter = 0;
        this.publishers.clear();
        this.subscriptions.clear();
        this.waitingPublishers = 0;
        this.replication.clear();
        this.deduplicationStatus = null;
        this.topicEpoch = null;
        this.nonContiguousDeletedMessagesRanges = 0;
        this.nonContiguousDeletedMessagesRangesSerializedSize = 0;
        this.offloadedStorageSize = 0;
        this.lastOffloadLedgerId = 0;
        this.lastOffloadFailureTimeStamp = 0;
        this.lastOffloadSuccessTimeStamp = 0;
        this.compaction.reset();
    }

    // if the stats are added for the 1st time, we will need to make a copy of these stats and add it to the current
    // stats.
    public TopicStatsImpl add(TopicStats ts) {
        TopicStatsImpl stats = (TopicStatsImpl) ts;

        this.count++;
        this.msgRateIn += stats.msgRateIn;
        this.msgThroughputIn += stats.msgThroughputIn;
        this.msgRateOut += stats.msgRateOut;
        this.msgThroughputOut += stats.msgThroughputOut;
        this.bytesInCounter += stats.bytesInCounter;
        this.msgInCounter += stats.msgInCounter;
        this.bytesOutCounter += stats.bytesOutCounter;
        this.msgOutCounter += stats.msgOutCounter;
        this.waitingPublishers += stats.waitingPublishers;
        double newAverageMsgSize = (this.averageMsgSize * (this.count - 1) + stats.averageMsgSize) / this.count;
        this.averageMsgSize = newAverageMsgSize;
        this.storageSize += stats.storageSize;
        this.backlogSize += stats.backlogSize;
        this.offloadedStorageSize += stats.offloadedStorageSize;
        this.nonContiguousDeletedMessagesRanges += stats.nonContiguousDeletedMessagesRanges;
        this.nonContiguousDeletedMessagesRangesSerializedSize += stats.nonContiguousDeletedMessagesRangesSerializedSize;
        if (this.publishers.size() != stats.publishers.size()) {
            for (int i = 0; i < stats.publishers.size(); i++) {
                PublisherStatsImpl publisherStats = new PublisherStatsImpl();
                this.publishers.add(publisherStats.add(stats.publishers.get(i)));
            }
        } else {
            for (int i = 0; i < stats.publishers.size(); i++) {
                this.publishers.get(i).add(stats.publishers.get(i));
            }
        }
        if (this.subscriptions.size() != stats.subscriptions.size()) {
            for (String subscription : stats.subscriptions.keySet()) {
                SubscriptionStatsImpl subscriptionStats = new SubscriptionStatsImpl();
                this.subscriptions.put(subscription, subscriptionStats.add(stats.subscriptions.get(subscription)));
            }
        } else {
            for (String subscription : stats.subscriptions.keySet()) {
                if (this.subscriptions.get(subscription) != null) {
                    this.subscriptions.get(subscription).add(stats.subscriptions.get(subscription));
                } else {
                    SubscriptionStatsImpl subscriptionStats = new SubscriptionStatsImpl();
                    this.subscriptions.put(subscription, subscriptionStats.add(stats.subscriptions.get(subscription)));
                }
            }
        }
        if (this.replication.size() != stats.replication.size()) {
            for (String repl : stats.replication.keySet()) {
                ReplicatorStatsImpl replStats = new ReplicatorStatsImpl();
                this.replication.put(repl, replStats.add(stats.replication.get(repl)));
            }
        } else {
            for (String repl : stats.replication.keySet()) {
                if (this.replication.get(repl) != null) {
                    this.replication.get(repl).add(stats.replication.get(repl));
                } else {
                    ReplicatorStatsImpl replStats = new ReplicatorStatsImpl();
                    this.replication.put(repl, replStats.add(stats.replication.get(repl)));
                }
            }
        }
        return this;
    }

}
