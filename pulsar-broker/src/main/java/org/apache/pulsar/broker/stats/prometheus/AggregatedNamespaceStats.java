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

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.util.StatsBuckets;

public class AggregatedNamespaceStats {
    public int topicsCount;
    public int subscriptionsCount;
    public int producersCount;
    public int consumersCount;
    public double rateIn;
    public double rateOut;
    public double throughputIn;
    public double throughputOut;

    public long storageSize;
    public long msgBacklog;
    public long msgDelayed;

    long backlogSize;
    long offloadedStorageUsed;
    long backlogQuotaLimit;

    public StatsBuckets storageWriteLatencyBuckets = new StatsBuckets(
            ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC);
    public StatsBuckets entrySizeBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES);

    public double storageWriteRate;
    public double storageReadRate;

    public Map<String, AggregatedReplicationStats> replicationStats = new HashMap<>();

    public Map<String, AggregatedSubscriptionStats> subscriptionStats = new HashMap<>();

    void updateStats(TopicStats stats) {
        topicsCount++;

        subscriptionsCount += stats.subscriptionsCount;
        producersCount += stats.producersCount;
        consumersCount += stats.consumersCount;

        rateIn += stats.rateIn;
        rateOut += stats.rateOut;
        throughputIn += stats.throughputIn;
        throughputOut += stats.throughputOut;

        storageSize += stats.storageSize;
        backlogSize += stats.backlogSize;
        offloadedStorageUsed += stats.offloadedStorageUsed;

        storageWriteRate += stats.storageWriteRate;
        storageReadRate += stats.storageReadRate;

        msgBacklog += stats.msgBacklog;

        storageWriteLatencyBuckets.addAll(stats.storageWriteLatencyBuckets);
        entrySizeBuckets.addAll(stats.entrySizeBuckets);

        stats.replicationStats.forEach((n, as) -> {
            AggregatedReplicationStats replStats =
                    replicationStats.computeIfAbsent(n,  k -> new AggregatedReplicationStats());
            replStats.msgRateIn += as.msgRateIn;
            replStats.msgRateOut += as.msgRateOut;
            replStats.msgThroughputIn += as.msgThroughputIn;
            replStats.msgThroughputOut += as.msgThroughputOut;
            replStats.replicationBacklog += as.replicationBacklog;
        });

        stats.subscriptionStats.forEach((n, as) -> {
            AggregatedSubscriptionStats subsStats =
                    subscriptionStats.computeIfAbsent(n, k -> new AggregatedSubscriptionStats());
            msgDelayed += as.msgDelayed;
            subsStats.blockedSubscriptionOnUnackedMsgs = as.blockedSubscriptionOnUnackedMsgs;
            subsStats.msgBacklog += as.msgBacklog;
            subsStats.msgBacklogNoDelayed += as.msgBacklogNoDelayed;
            subsStats.msgDelayed += as.msgDelayed;
            subsStats.msgRateRedeliver += as.msgRateRedeliver;
            subsStats.unackedMessages += as.unackedMessages;
            as.consumerStat.forEach((c, v) -> {
                AggregatedConsumerStats consumerStats =
                        subsStats.consumerStat.computeIfAbsent(c, k -> new AggregatedConsumerStats());
                consumerStats.blockedSubscriptionOnUnackedMsgs = v.blockedSubscriptionOnUnackedMsgs;
                consumerStats.msgRateRedeliver += v.msgRateRedeliver;
                consumerStats.unackedMessages += v.unackedMessages;
            });
        });
    }

    public void reset() {
        topicsCount = 0;
        subscriptionsCount = 0;
        producersCount = 0;
        consumersCount = 0;
        rateIn = 0;
        rateOut = 0;
        throughputIn = 0;
        throughputOut = 0;

        storageSize = 0;
        msgBacklog = 0;
        msgDelayed = 0;
        storageWriteRate = 0;
        storageReadRate = 0;

        replicationStats.clear();
        subscriptionStats.clear();

        storageWriteLatencyBuckets.reset();
        entrySizeBuckets.reset();
    }
}
