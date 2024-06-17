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

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

/**
 * Statistics for a Pulsar topic.
 * This class is not thread-safe.
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

    /** Total bytes published to the system topic (bytes). */
    public long systemTopicBytesInCounter;

    /** Total bytes delivered to consumer (bytes). */
    public long bytesOutCounter;

    /** Total messages delivered to consumer (msg). */
    public long msgOutCounter;

    /** Total bytes delivered to internal cursors. */
    public long bytesOutInternalCounter;

    /** Average size of published messages (bytes). */
    public double averageMsgSize;

    /** Topic has chunked message published on it. */
    public boolean msgChunkPublished;

    /** Space used to store the messages for the topic (bytes). */
    public long storageSize;

    /** Get estimated total unconsumed or backlog size in bytes. */
    public long backlogSize;

    /** the size in bytes of the topic backlog quota. */
    public long backlogQuotaLimitSize;

    /** the topic backlog age quota, in seconds. */
    public long backlogQuotaLimitTime;

    /**
     * Age of oldest unacknowledged message, as recorded in last backlog quota check interval.
     * <p>
     * The age of the oldest unacknowledged (i.e. backlog) message, measured by the time elapsed from its published
     * time, in seconds. This value is recorded every backlog quota check interval, hence it represents the value
     * seen in the last check.
     * </p>
     */
    public long oldestBacklogMessageAgeSeconds;

    /**
     * The subscription name containing oldest unacknowledged message as recorded in last backlog quota check.
     * <p>
     * The name of the subscription containing the oldest unacknowledged message. This value is recorded every backlog
     * quota check interval, hence it represents the value seen in the last check.
     * </p>
     */
    public String oldestBacklogMessageSubscriptionName;

    /** The number of times the publishing rate limit was triggered. */
    public long publishRateLimitedTimes;

    /** Get the publish time of the earliest message over all the backlogs. */
    public long earliestMsgPublishTimeInBacklogs;

    /** Space used to store the offloaded messages for the topic/. */
    public long offloadedStorageSize;

    /** record last successful offloaded ledgerId. If no offload ledger, the value should be 0 */
    public long lastOffloadLedgerId;

    /** record last successful offloaded timestamp. If no successful offload, the value should be 0 */
    public long lastOffloadSuccessTimeStamp;

    /** record last failed offloaded timestamp. If no failed offload, the value should be 0 */
    public long lastOffloadFailureTimeStamp;

    public long ongoingTxnCount;
    public long abortedTxnCount;
    public long committedTxnCount;

    /** List of connected publishers on this topic w/ their stats. */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private List<PublisherStatsImpl> publishers;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, PublisherStatsImpl> publishersMap;

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

    /** The size of DelayedDeliveryTracer memory usage. */
    public long delayedMessageIndexSizeInBytes;

    /** Map of bucket delayed index statistics. */
    @JsonIgnore
    public Map<String, TopicMetricBean> bucketDelayedIndexStats;

    /** The compaction stats. */
    public CompactionStatsImpl compaction;

    /** The broker that owns this topic. */
    public String ownerBroker;

    public List<? extends PublisherStats> getPublishers() {
        return Stream.concat(publishers.stream().sorted(
                                Comparator.comparing(PublisherStatsImpl::getProducerName, nullsLast(naturalOrder()))),
                        publishersMap.values().stream().sorted(
                                Comparator.comparing(PublisherStatsImpl::getProducerName, nullsLast(naturalOrder()))))
                .collect(Collectors.toList());
    }

    public void setPublishers(List<? extends PublisherStats> statsList) {
        this.publishers.clear();
        this.publishersMap.clear();
        statsList.forEach(s -> addPublisher((PublisherStatsImpl) s));
    }

    public void addPublisher(PublisherStatsImpl stats) {
        if (stats.isSupportsPartialProducer() && stats.getProducerName() != null) {
            publishersMap.put(stats.getProducerName(), stats);
        } else {
            stats.setSupportsPartialProducer(false); // setter method with side effect
            publishers.add(stats);
        }
    }

    public Map<String, ? extends SubscriptionStats> getSubscriptions() {
        return subscriptions;
    }

    public Map<String, ? extends ReplicatorStats> getReplication() {
        return replication;
    }

    public TopicStatsImpl() {
        this.publishers = new ArrayList<>();
        this.publishersMap = new ConcurrentHashMap<>();
        this.subscriptions = new HashMap<>();
        this.replication = new TreeMap<>();
        this.compaction = new CompactionStatsImpl();
        this.bucketDelayedIndexStats = new HashMap<>();
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
        this.publishersMap.clear();
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
        this.publishRateLimitedTimes = 0L;
        this.earliestMsgPublishTimeInBacklogs = 0L;
        this.delayedMessageIndexSizeInBytes = 0;
        this.compaction.reset();
        this.ownerBroker = null;
        this.bucketDelayedIndexStats.clear();
        this.backlogQuotaLimitSize = 0;
        this.backlogQuotaLimitTime = 0;
        this.oldestBacklogMessageAgeSeconds = -1;
        this.oldestBacklogMessageSubscriptionName = null;
    }

    // if the stats are added for the 1st time, we will need to make a copy of these stats and add it to the current
    // stats. This stat addition is not thread-safe.
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
        this.publishRateLimitedTimes += stats.publishRateLimitedTimes;
        this.offloadedStorageSize += stats.offloadedStorageSize;
        this.nonContiguousDeletedMessagesRanges += stats.nonContiguousDeletedMessagesRanges;
        this.nonContiguousDeletedMessagesRangesSerializedSize += stats.nonContiguousDeletedMessagesRangesSerializedSize;
        this.delayedMessageIndexSizeInBytes += stats.delayedMessageIndexSizeInBytes;
        this.ongoingTxnCount = stats.ongoingTxnCount;
        this.abortedTxnCount = stats.abortedTxnCount;
        this.committedTxnCount = stats.committedTxnCount;
        this.backlogQuotaLimitTime = stats.backlogQuotaLimitTime;
        this.backlogQuotaLimitSize = stats.backlogQuotaLimitSize;
        if (stats.oldestBacklogMessageAgeSeconds > this.oldestBacklogMessageAgeSeconds) {
            this.oldestBacklogMessageAgeSeconds = stats.oldestBacklogMessageAgeSeconds;
            this.oldestBacklogMessageSubscriptionName = stats.oldestBacklogMessageSubscriptionName;
        }

        stats.bucketDelayedIndexStats.forEach((k, v) -> {
            TopicMetricBean topicMetricBean =
                    this.bucketDelayedIndexStats.computeIfAbsent(k, __ -> new TopicMetricBean());
            topicMetricBean.name = v.name;
            topicMetricBean.labelsAndValues = v.labelsAndValues;
            topicMetricBean.value += v.value;
        });

        List<? extends PublisherStats> publisherStats = stats.getPublishers();
        for (int index = 0; index < publisherStats.size(); index++) {
           PublisherStats s = publisherStats.get(index);
           if (s.isSupportsPartialProducer() && s.getProducerName() != null) {
               this.publishersMap.computeIfAbsent(s.getProducerName(), key -> {
                   final PublisherStatsImpl newStats = new PublisherStatsImpl();
                   newStats.setSupportsPartialProducer(true);
                   newStats.setProducerName(s.getProducerName());
                   return newStats;
               }).add((PublisherStatsImpl) s);
           } else {
               // Add a publisher stat entry to this.publishers
               // if this.publishers.size() is smaller than
               // the input stats.publishers.size().
               // Here, index == this.publishers.size() means
               // this.publishers.size() is smaller than the input stats.publishers.size()
               if (index == this.publishers.size()) {
                   PublisherStatsImpl newStats = new PublisherStatsImpl();
                   newStats.setSupportsPartialProducer(false);
                   this.publishers.add(newStats);
               }
               this.publishers.get(index)
                       .add((PublisherStatsImpl) s);
           }
        }

        for (Map.Entry<String, SubscriptionStatsImpl> entry : stats.subscriptions.entrySet()) {
            SubscriptionStatsImpl subscriptionStats =
                    this.subscriptions.computeIfAbsent(entry.getKey(), k -> new SubscriptionStatsImpl());
            subscriptionStats.add(entry.getValue());
        }

        for (Map.Entry<String, ReplicatorStatsImpl> entry : stats.replication.entrySet()) {
            ReplicatorStatsImpl replStats =
                    this.replication.computeIfAbsent(entry.getKey(), k -> {
                        ReplicatorStatsImpl r = new ReplicatorStatsImpl();
                        r.setConnected(true);
                        return r;
                    });
            replStats.add(entry.getValue());
        }

        if (earliestMsgPublishTimeInBacklogs != 0 && ((TopicStatsImpl) ts).earliestMsgPublishTimeInBacklogs != 0) {
            earliestMsgPublishTimeInBacklogs = Math.min(
                    earliestMsgPublishTimeInBacklogs,
                    ((TopicStatsImpl) ts).earliestMsgPublishTimeInBacklogs
            );
        } else {
            earliestMsgPublishTimeInBacklogs = Math.max(
                    earliestMsgPublishTimeInBacklogs,
                    ((TopicStatsImpl) ts).earliestMsgPublishTimeInBacklogs
            );
        }
        return this;
    }
}
