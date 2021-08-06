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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Statistics for a Pulsar topic.
 */
public class TopicStats {
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

    /** List of connected publishers on this topic w/ their stats. */
    public List<PublisherStats> publishers;

    public int waitingPublishers;

    /** Map of subscriptions with their individual statistics. */
    public Map<String, SubscriptionStats> subscriptions;

    /** Map of replication statistics by remote cluster context. */
    public Map<String, ReplicatorStats> replication;

    public String deduplicationStatus;

    /** The topic epoch or empty if not set. */
    public Long topicEpoch;

    /** The number of non-contiguous deleted messages ranges. */
    public int nonContiguousDeletedMessagesRanges;

    /** The serialized size of non-contiguous deleted messages ranges. */
    public int nonContiguousDeletedMessagesRangesSerializedSize;

    public TopicStats() {
        this.publishers = new ArrayList<>();
        this.subscriptions = new HashMap<>();
        this.replication = new TreeMap<>();
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
    }

    // if the stats are added for the 1st time, we will need to make a copy of these stats and add it to the current
    // stats.
    public TopicStats add(TopicStats stats) {
        Objects.requireNonNull(stats);
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
                PublisherStats publisherStats = new PublisherStats();
                this.publishers.add(publisherStats.add(stats.publishers.get(i)));
            }
        } else {
            for (int i = 0; i < stats.publishers.size(); i++) {
                this.publishers.get(i).add(stats.publishers.get(i));
            }
        }
        if (this.subscriptions.size() != stats.subscriptions.size()) {
            for (String subscription : stats.subscriptions.keySet()) {
                SubscriptionStats subscriptionStats = new SubscriptionStats();
                this.subscriptions.put(subscription, subscriptionStats.add(stats.subscriptions.get(subscription)));
            }
        } else {
            for (String subscription : stats.subscriptions.keySet()) {
                if (this.subscriptions.get(subscription) != null) {
                    this.subscriptions.get(subscription).add(stats.subscriptions.get(subscription));
                } else {
                    SubscriptionStats subscriptionStats = new SubscriptionStats();
                    this.subscriptions.put(subscription, subscriptionStats.add(stats.subscriptions.get(subscription)));
                }
            }
        }
        if (this.replication.size() != stats.replication.size()) {
            for (String repl : stats.replication.keySet()) {
                ReplicatorStats replStats = new ReplicatorStats();
                this.replication.put(repl, replStats.add(stats.replication.get(repl)));
            }
        } else {
            for (String repl : stats.replication.keySet()) {
                if (this.replication.get(repl) != null) {
                    this.replication.get(repl).add(stats.replication.get(repl));
                } else {
                    ReplicatorStats replStats = new ReplicatorStats();
                    this.replication.put(repl, replStats.add(stats.replication.get(repl)));
                }
            }
        }
        return this;
    }

}
