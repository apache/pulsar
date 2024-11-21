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
package org.apache.pulsar.broker.stats.prometheus;

import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.compaction.CompactionRecord;

public class AggregatedNamespaceStats {
    public int topicsCount;
    public int subscriptionsCount;
    public int producersCount;
    public int consumersCount;
    public double rateIn;
    public double rateOut;
    public double throughputIn;
    public double throughputOut;

    public double messageAckRate;
    public long bytesInCounter;
    public long msgInCounter;
    public long bytesOutCounter;
    public long msgOutCounter;

    public ManagedLedgerStats managedLedgerStats = new ManagedLedgerStats();
    public long msgBacklog;
    public long msgDelayed;
    public long msgInReplay;

    public long ongoingTxnCount;
    public long abortedTxnCount;
    public long committedTxnCount;

    long backlogQuotaLimit;
    long backlogQuotaLimitTime;

    public long sizeBasedBacklogQuotaExceededEvictionCount;
    public long timeBasedBacklogQuotaExceededEvictionCount;

    public Map<String, AggregatedReplicationStats> replicationStats = new HashMap<>();

    public Map<String, AggregatedSubscriptionStats> subscriptionStats = new HashMap<>();

    long compactionRemovedEventCount;
    long compactionSucceedCount;
    long compactionFailedCount;
    long compactionDurationTimeInMills;
    double compactionReadThroughput;
    double compactionWriteThroughput;
    long compactionCompactedEntriesCount;
    long compactionCompactedEntriesSize;
    StatsBuckets compactionLatencyBuckets = new StatsBuckets(CompactionRecord.WRITE_LATENCY_BUCKETS_USEC);
    long delayedMessageIndexSizeInBytes;

    Map<String, TopicMetricBean> bucketDelayedIndexStats = new HashMap<>();

    @SuppressWarnings("DuplicatedCode")
    void updateStats(TopicStats stats) {
        topicsCount++;

        subscriptionsCount += stats.subscriptionsCount;
        producersCount += stats.producersCount;
        consumersCount += stats.consumersCount;

        rateIn += stats.rateIn;
        rateOut += stats.rateOut;
        throughputIn += stats.throughputIn;
        throughputOut += stats.throughputOut;

        bytesInCounter += stats.bytesInCounter;
        msgInCounter += stats.msgInCounter;
        bytesOutCounter += stats.bytesOutCounter;
        msgOutCounter += stats.msgOutCounter;
        delayedMessageIndexSizeInBytes += stats.delayedMessageIndexSizeInBytes;

        stats.bucketDelayedIndexStats.forEach((k, v) -> {
            TopicMetricBean topicMetricBean =
                    bucketDelayedIndexStats.computeIfAbsent(k, __ -> new TopicMetricBean());
            topicMetricBean.name = v.name;
            topicMetricBean.labelsAndValues = v.labelsAndValues;
            topicMetricBean.value += v.value;
        });

        this.ongoingTxnCount += stats.ongoingTxnCount;
        this.abortedTxnCount += stats.abortedTxnCount;
        this.committedTxnCount += stats.committedTxnCount;

        managedLedgerStats.storageSize += stats.managedLedgerStats.storageSize;
        managedLedgerStats.storageLogicalSize += stats.managedLedgerStats.storageLogicalSize;
        managedLedgerStats.backlogSize += stats.managedLedgerStats.backlogSize;
        managedLedgerStats.offloadedStorageUsed += stats.managedLedgerStats.offloadedStorageUsed;
        backlogQuotaLimit = Math.max(backlogQuotaLimit, stats.backlogQuotaLimit);
        backlogQuotaLimitTime = Math.max(backlogQuotaLimitTime, stats.backlogQuotaLimitTime);

        sizeBasedBacklogQuotaExceededEvictionCount += stats.sizeBasedBacklogQuotaExceededEvictionCount;
        timeBasedBacklogQuotaExceededEvictionCount += stats.timeBasedBacklogQuotaExceededEvictionCount;

        managedLedgerStats.storageWriteRate += stats.managedLedgerStats.storageWriteRate;
        managedLedgerStats.storageReadRate += stats.managedLedgerStats.storageReadRate;
        managedLedgerStats.storageReadCacheMissesRate += stats.managedLedgerStats.storageReadCacheMissesRate;

        msgBacklog += stats.msgBacklog;

        managedLedgerStats.storageWriteLatencyBuckets.addAll(stats.managedLedgerStats.storageWriteLatencyBuckets);
        managedLedgerStats.storageLedgerWriteLatencyBuckets
                .addAll(stats.managedLedgerStats.storageLedgerWriteLatencyBuckets);
        managedLedgerStats.entrySizeBuckets.addAll(stats.managedLedgerStats.entrySizeBuckets);

        stats.replicationStats.forEach((n, as) -> {
            AggregatedReplicationStats replStats =
                    replicationStats.computeIfAbsent(n, k -> new AggregatedReplicationStats());
            replStats.msgRateIn += as.msgRateIn;
            replStats.msgRateOut += as.msgRateOut;
            replStats.msgThroughputIn += as.msgThroughputIn;
            replStats.msgThroughputOut += as.msgThroughputOut;
            replStats.replicationBacklog += as.replicationBacklog;
            replStats.msgRateExpired += as.msgRateExpired;
            replStats.connectedCount += as.connectedCount;
            replStats.disconnectedCount += as.disconnectedCount;
            replStats.replicationDelayInSeconds += as.replicationDelayInSeconds;
        });

        stats.subscriptionStats.forEach((n, as) -> {
            AggregatedSubscriptionStats subsStats =
                    subscriptionStats.computeIfAbsent(n, k -> new AggregatedSubscriptionStats());
            msgDelayed += as.msgDelayed;
            msgInReplay += as.msgInReplay;
            subsStats.blockedSubscriptionOnUnackedMsgs = as.blockedSubscriptionOnUnackedMsgs;
            subsStats.msgBacklog += as.msgBacklog;
            subsStats.msgBacklogNoDelayed += as.msgBacklogNoDelayed;
            subsStats.msgDelayed += as.msgDelayed;
            subsStats.msgInReplay += as.msgInReplay;
            subsStats.msgRateRedeliver += as.msgRateRedeliver;
            subsStats.unackedMessages += as.unackedMessages;
            subsStats.filterProcessedMsgCount += as.filterProcessedMsgCount;
            subsStats.filterAcceptedMsgCount += as.filterAcceptedMsgCount;
            subsStats.filterRejectedMsgCount += as.filterRejectedMsgCount;
            subsStats.filterRescheduledMsgCount += as.filterRescheduledMsgCount;
            subsStats.delayedMessageIndexSizeInBytes += as.delayedMessageIndexSizeInBytes;
            as.bucketDelayedIndexStats.forEach((k, v) -> {
                TopicMetricBean topicMetricBean =
                        subsStats.bucketDelayedIndexStats.computeIfAbsent(k, __ -> new TopicMetricBean());
                topicMetricBean.name = v.name;
                topicMetricBean.labelsAndValues = v.labelsAndValues;
                topicMetricBean.value += v.value;
            });
            as.consumerStat.forEach((c, v) -> {
                AggregatedConsumerStats consumerStats =
                        subsStats.consumerStat.computeIfAbsent(c, k -> new AggregatedConsumerStats());
                consumerStats.blockedSubscriptionOnUnackedMsgs = v.blockedSubscriptionOnUnackedMsgs;
                consumerStats.msgRateRedeliver += v.msgRateRedeliver;
                consumerStats.unackedMessages += v.unackedMessages;
                messageAckRate += v.msgAckRate;
            });
        });

        compactionRemovedEventCount += stats.compactionRemovedEventCount;
        compactionSucceedCount += stats.compactionSucceedCount;
        compactionFailedCount += stats.compactionFailedCount;
        compactionDurationTimeInMills += stats.compactionDurationTimeInMills;
        compactionReadThroughput += stats.compactionReadThroughput;
        compactionWriteThroughput += stats.compactionWriteThroughput;
        compactionCompactedEntriesCount += stats.compactionCompactedEntriesCount;
        compactionCompactedEntriesSize += stats.compactionCompactedEntriesSize;
        compactionLatencyBuckets.addAll(stats.compactionLatencyBuckets);
    }

    @SuppressWarnings("DuplicatedCode")
    public void reset() {
        managedLedgerStats.reset();
        topicsCount = 0;
        subscriptionsCount = 0;
        producersCount = 0;
        consumersCount = 0;
        rateIn = 0;
        rateOut = 0;
        throughputIn = 0;
        throughputOut = 0;
        messageAckRate = 0;
        bytesInCounter = 0;
        msgInCounter = 0;

        bytesOutCounter = 0;
        msgOutCounter = 0;

        msgBacklog = 0;
        msgDelayed = 0;
        msgInReplay = 0;
        ongoingTxnCount = 0;
        abortedTxnCount = 0;
        committedTxnCount = 0;

        backlogQuotaLimit = 0;
        backlogQuotaLimitTime = -1;

        replicationStats.clear();
        subscriptionStats.clear();

        sizeBasedBacklogQuotaExceededEvictionCount = 0;
        timeBasedBacklogQuotaExceededEvictionCount = 0;

        compactionRemovedEventCount = 0;
        compactionSucceedCount = 0;
        compactionFailedCount = 0;
        compactionDurationTimeInMills = 0;
        compactionReadThroughput = 0;
        compactionWriteThroughput = 0;
        compactionCompactedEntriesCount = 0;
        compactionCompactedEntriesSize = 0;

        delayedMessageIndexSizeInBytes = 0;
        bucketDelayedIndexStats.clear();
    }
}
