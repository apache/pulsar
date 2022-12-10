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

import io.netty.util.concurrent.FastThreadLocal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentSubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.compaction.CompactedTopicContext;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.CompactorMXBean;

@Slf4j
public class NamespaceStatsAggregator {

    private static final FastThreadLocal<AggregatedNamespaceStats> localNamespaceStats =
            new FastThreadLocal<>() {
                @Override
                protected AggregatedNamespaceStats initialValue() {
                    return new AggregatedNamespaceStats();
                }
            };

    private static final FastThreadLocal<TopicStats> localTopicStats = new FastThreadLocal<>() {
        @Override
        protected TopicStats initialValue() {
            return new TopicStats();
        }
    };

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                boolean includeProducerMetrics, boolean splitTopicAndPartitionIndexLabel,
                                PrometheusMetricStreams stream) {
        String cluster = pulsar.getConfiguration().getClusterName();
        AggregatedNamespaceStats namespaceStats = localNamespaceStats.get();
        TopicStats topicStats = localTopicStats.get();
        Optional<CompactorMXBean> compactorMXBean = getCompactorMXBean(pulsar);
        LongAdder topicsCount = new LongAdder();
        Map<String, Long> localNamespaceTopicCount = new HashMap<>();

        printDefaultBrokerStats(stream, cluster);

        pulsar.getBrokerService().getMultiLayerTopicMap().forEach((namespace, bundlesMap) -> {
            namespaceStats.reset();
            topicsCount.reset();

            bundlesMap.forEach((bundle, topicsMap) -> topicsMap.forEach((name, topic) -> {
                getTopicStats(topic, topicStats, includeConsumerMetrics, includeProducerMetrics,
                        pulsar.getConfiguration().isExposePreciseBacklogInPrometheus(),
                        pulsar.getConfiguration().isExposeSubscriptionBacklogSizeInPrometheus(),
                        compactorMXBean
                );

                if (includeTopicMetrics) {
                    topicsCount.add(1);
                    TopicStats.printTopicStats(stream, topicStats, compactorMXBean, cluster, namespace, name,
                            splitTopicAndPartitionIndexLabel);
                } else {
                    namespaceStats.updateStats(topicStats);
                }
            }));

            if (!includeTopicMetrics) {
                // Only include namespace level stats if we don't have the per-topic, otherwise we're going to
                // report the same data twice, and it will make the aggregation difficult
                printNamespaceStats(stream, namespaceStats, cluster, namespace);
            } else {
                localNamespaceTopicCount.put(namespace, topicsCount.sum());
            }
        });

        if (includeTopicMetrics) {
            printTopicsCountStats(stream, localNamespaceTopicCount, cluster);
        }
    }

    private static Optional<CompactorMXBean> getCompactorMXBean(PulsarService pulsar) {
        Compactor compactor = pulsar.getNullableCompactor();
        return Optional.ofNullable(compactor).map(Compactor::getStats);
    }

    private static void aggregateTopicStats(TopicStats stats, SubscriptionStatsImpl subscriptionStats,
                                            AggregatedSubscriptionStats subsStats) {
        stats.subscriptionsCount++;
        stats.msgBacklog += subscriptionStats.msgBacklog;
        subsStats.bytesOutCounter = subscriptionStats.bytesOutCounter;
        subsStats.msgOutCounter = subscriptionStats.msgOutCounter;
        subsStats.msgBacklog = subscriptionStats.msgBacklog;
        subsStats.msgDelayed = subscriptionStats.msgDelayed;
        subsStats.msgRateExpired = subscriptionStats.msgRateExpired;
        subsStats.totalMsgExpired = subscriptionStats.totalMsgExpired;
        subsStats.msgBacklogNoDelayed = subsStats.msgBacklog - subsStats.msgDelayed;
        subsStats.lastExpireTimestamp = subscriptionStats.lastExpireTimestamp;
        subsStats.lastAckedTimestamp = subscriptionStats.lastAckedTimestamp;
        subsStats.lastConsumedFlowTimestamp = subscriptionStats.lastConsumedFlowTimestamp;
        subsStats.lastConsumedTimestamp = subscriptionStats.lastConsumedTimestamp;
        subsStats.lastMarkDeleteAdvancedTimestamp = subscriptionStats.lastMarkDeleteAdvancedTimestamp;
        subsStats.consumersCount = subscriptionStats.consumers.size();
        subsStats.blockedSubscriptionOnUnackedMsgs = subscriptionStats.blockedSubscriptionOnUnackedMsgs;
        subscriptionStats.consumers.forEach(cStats -> {
            stats.consumersCount++;
            subsStats.unackedMessages += cStats.unackedMessages;
            subsStats.msgRateRedeliver += cStats.msgRateRedeliver;
            subsStats.msgRateOut += cStats.msgRateOut;
            subsStats.messageAckRate += cStats.messageAckRate;
            subsStats.msgThroughputOut += cStats.msgThroughputOut;
        });
        stats.rateOut += subsStats.msgRateOut;
        stats.throughputOut += subsStats.msgThroughputOut;
        subsStats.filterProcessedMsgCount = subscriptionStats.filterProcessedMsgCount;
        subsStats.filterAcceptedMsgCount = subscriptionStats.filterAcceptedMsgCount;
        subsStats.filterRejectedMsgCount = subscriptionStats.filterRejectedMsgCount;
        subsStats.filterRescheduledMsgCount = subscriptionStats.filterRescheduledMsgCount;
    }

    private static void getTopicStats(Topic topic, TopicStats stats, boolean includeConsumerMetrics,
                                      boolean includeProducerMetrics, boolean getPreciseBacklog,
                                      boolean subscriptionBacklogSize, Optional<CompactorMXBean> compactorMXBean) {
        stats.reset();

        if (topic instanceof PersistentTopic) {
            // Managed Ledger stats
            ManagedLedger ml = ((PersistentTopic) topic).getManagedLedger();
            ManagedLedgerMBeanImpl mlStats = (ManagedLedgerMBeanImpl) ml.getStats();

            stats.managedLedgerStats.storageSize = mlStats.getStoredMessagesSize();
            stats.managedLedgerStats.storageLogicalSize = mlStats.getStoredMessagesLogicalSize();
            stats.managedLedgerStats.backlogSize = ml.getEstimatedBacklogSize();
            stats.managedLedgerStats.offloadedStorageUsed = ml.getOffloadedSize();
            stats.backlogQuotaLimit = topic
                    .getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage).getLimitSize();
            stats.backlogQuotaLimitTime = topic
                    .getBacklogQuota(BacklogQuota.BacklogQuotaType.message_age).getLimitTime();

            stats.managedLedgerStats.storageWriteLatencyBuckets
                    .addAll(mlStats.getInternalAddEntryLatencyBuckets());
            stats.managedLedgerStats.storageWriteLatencyBuckets.refresh();
            stats.managedLedgerStats.storageLedgerWriteLatencyBuckets
                    .addAll(mlStats.getInternalLedgerAddEntryLatencyBuckets());
            stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.refresh();

            stats.managedLedgerStats.entrySizeBuckets.addAll(mlStats.getInternalEntrySizeBuckets());
            stats.managedLedgerStats.entrySizeBuckets.refresh();

            stats.managedLedgerStats.storageWriteRate = mlStats.getAddEntryMessagesRate();
            stats.managedLedgerStats.storageReadRate = mlStats.getReadEntriesRate();
        }
        TopicStatsImpl tStatus = topic.getStats(getPreciseBacklog, subscriptionBacklogSize, false);
        stats.msgInCounter = tStatus.msgInCounter;
        stats.bytesInCounter = tStatus.bytesInCounter;
        stats.msgOutCounter = tStatus.msgOutCounter;
        stats.bytesOutCounter = tStatus.bytesOutCounter;
        stats.averageMsgSize = tStatus.averageMsgSize;
        stats.publishRateLimitedTimes = tStatus.publishRateLimitedTimes;
        stats.delayedTrackerMemoryUsage = tStatus.delayedMessageIndexSizeInBytes;
        stats.abortedTxnCount = tStatus.abortedTxnCount;
        stats.ongoingTxnCount = tStatus.ongoingTxnCount;
        stats.committedTxnCount = tStatus.committedTxnCount;

        stats.producersCount = 0;
        topic.getProducers().values().forEach(producer -> {
            if (producer.isRemote()) {
                AggregatedReplicationStats replStats = stats.replicationStats
                        .computeIfAbsent(producer.getRemoteCluster(), k -> new AggregatedReplicationStats());

                replStats.msgRateIn += producer.getStats().msgRateIn;
                replStats.msgThroughputIn += producer.getStats().msgThroughputIn;
            } else {
                // Local producer
                stats.producersCount++;
                stats.rateIn += producer.getStats().msgRateIn;
                stats.throughputIn += producer.getStats().msgThroughputIn;

                if (includeProducerMetrics) {
                    AggregatedProducerStats producerStats = stats.producerStats.computeIfAbsent(
                            producer.getProducerName(), k -> new AggregatedProducerStats());
                    producerStats.producerId = producer.getStats().producerId;
                    producerStats.msgRateIn = producer.getStats().msgRateIn;
                    producerStats.msgThroughputIn = producer.getStats().msgThroughputIn;
                    producerStats.averageMsgSize = producer.getStats().averageMsgSize;
                }
            }
        });

        if (topic instanceof PersistentTopic) {
            tStatus.subscriptions.forEach((subName, subscriptionStats) -> {
                AggregatedSubscriptionStats subsStats = stats.subscriptionStats
                        .computeIfAbsent(subName, k -> new AggregatedSubscriptionStats());
                aggregateTopicStats(stats, subscriptionStats, subsStats);
            });
        } else {
            ((NonPersistentTopicStatsImpl) tStatus).getNonPersistentSubscriptions()
                    .forEach((subName, nonPersistentSubscriptionStats) -> {
                        NonPersistentSubscriptionStatsImpl subscriptionStats =
                                (NonPersistentSubscriptionStatsImpl) nonPersistentSubscriptionStats;
                        AggregatedSubscriptionStats subsStats = stats.subscriptionStats
                                .computeIfAbsent(subName, k -> new AggregatedSubscriptionStats());
                        aggregateTopicStats(stats, subscriptionStats, subsStats);
                        subsStats.msgDropRate += subscriptionStats.getMsgDropRate();
                    });
        }

        // Consumer stats can be a lot if a subscription has many consumers
        if (includeConsumerMetrics) {
            topic.getSubscriptions().forEach((name, subscription) -> {
                AggregatedSubscriptionStats subsStats = stats.subscriptionStats
                        .computeIfAbsent(name, k -> new AggregatedSubscriptionStats());
                subscription.getConsumers().forEach(consumer -> {
                    ConsumerStatsImpl conStats = consumer.getStats();

                    AggregatedConsumerStats consumerStats = subsStats.consumerStat
                            .computeIfAbsent(consumer, k -> new AggregatedConsumerStats());

                    consumerStats.unackedMessages = conStats.unackedMessages;
                    consumerStats.msgRateRedeliver = conStats.msgRateRedeliver;
                    consumerStats.msgRateOut = conStats.msgRateOut;
                    consumerStats.msgAckRate = conStats.messageAckRate;
                    consumerStats.msgThroughputOut = conStats.msgThroughputOut;
                    consumerStats.bytesOutCounter = conStats.bytesOutCounter;
                    consumerStats.msgOutCounter = conStats.msgOutCounter;
                    consumerStats.availablePermits = conStats.availablePermits;
                    consumerStats.blockedSubscriptionOnUnackedMsgs = conStats.blockedConsumerOnUnackedMsgs;
                });
            });
        }

        topic.getReplicators().forEach((cluster, replicator) -> {
            ReplicatorStatsImpl replStats = replicator.getStats();
            AggregatedReplicationStats aggReplStats = stats.replicationStats.get(replicator.getRemoteCluster());
            if (aggReplStats == null) {
                aggReplStats = new AggregatedReplicationStats();
                stats.replicationStats.put(replicator.getRemoteCluster(), aggReplStats);
                aggReplStats.msgRateIn = replStats.msgRateIn;
                aggReplStats.msgThroughputIn = replStats.msgThroughputIn;
            }

            aggReplStats.msgRateOut += replStats.msgRateOut;
            aggReplStats.msgThroughputOut += replStats.msgThroughputOut;
            aggReplStats.replicationBacklog += replStats.replicationBacklog;
            aggReplStats.msgRateExpired += replStats.msgRateExpired;
            aggReplStats.connectedCount += replStats.connected ? 1 : 0;
            aggReplStats.replicationDelayInSeconds += replStats.replicationDelayInSeconds;
        });

        compactorMXBean
                .flatMap(mxBean -> mxBean.getCompactionRecordForTopic(topic.getName()))
                .map(compactionRecord -> {
                    stats.compactionRemovedEventCount = compactionRecord.getCompactionRemovedEventCount();
                    stats.compactionSucceedCount = compactionRecord.getCompactionSucceedCount();
                    stats.compactionFailedCount = compactionRecord.getCompactionFailedCount();
                    stats.compactionDurationTimeInMills = compactionRecord.getCompactionDurationTimeInMills();
                    stats.compactionReadThroughput = compactionRecord.getCompactionReadThroughput();
                    stats.compactionWriteThroughput = compactionRecord.getCompactionWriteThroughput();
                    stats.compactionLatencyBuckets.addAll(compactionRecord.getCompactionLatencyStats());
                    stats.compactionLatencyBuckets.refresh();
                    PersistentTopic persistentTopic = (PersistentTopic) topic;
                    Optional<CompactedTopicContext> compactedTopicContext = persistentTopic
                            .getCompactedTopicContext();
                    if (compactedTopicContext.isPresent()) {
                        LedgerHandle ledger = compactedTopicContext.get().getLedger();
                        long entries = ledger.getLastAddConfirmed() + 1;
                        long size = ledger.getLength();

                        stats.compactionCompactedEntriesCount = entries;
                        stats.compactionCompactedEntriesSize = size;
                    }
                    return compactionRecord;
                });
    }

    private static void printDefaultBrokerStats(PrometheusMetricStreams stream, String cluster) {
        // Print metrics with 0 values. This is necessary to have the available brokers being
        // reported in the brokers dashboard even if they don't have any topic or traffic
        writeMetric(stream, "pulsar_topics_count", 0, cluster);
        writeMetric(stream, "pulsar_subscriptions_count", 0, cluster);
        writeMetric(stream, "pulsar_producers_count", 0, cluster);
        writeMetric(stream, "pulsar_consumers_count", 0, cluster);
        writeMetric(stream, "pulsar_rate_in", 0, cluster);
        writeMetric(stream, "pulsar_rate_out", 0, cluster);
        writeMetric(stream, "pulsar_throughput_in", 0, cluster);
        writeMetric(stream, "pulsar_throughput_out", 0, cluster);
        writeMetric(stream, "pulsar_storage_size", 0, cluster);
        writeMetric(stream, "pulsar_storage_logical_size", 0, cluster);
        writeMetric(stream, "pulsar_storage_write_rate", 0, cluster);
        writeMetric(stream, "pulsar_storage_read_rate", 0, cluster);
        writeMetric(stream, "pulsar_msg_backlog", 0, cluster);
    }

    private static void printTopicsCountStats(PrometheusMetricStreams stream, Map<String, Long> namespaceTopicsCount,
                                              String cluster) {
        namespaceTopicsCount.forEach(
                (ns, topicCount) -> writeMetric(stream, "pulsar_topics_count", topicCount, cluster, ns)
        );
    }

    private static void printNamespaceStats(PrometheusMetricStreams stream, AggregatedNamespaceStats stats,
                                            String cluster, String namespace) {
        writeMetric(stream, "pulsar_topics_count", stats.topicsCount, cluster, namespace);
        writeMetric(stream, "pulsar_subscriptions_count", stats.subscriptionsCount, cluster,
                namespace);
        writeMetric(stream, "pulsar_producers_count", stats.producersCount, cluster, namespace);
        writeMetric(stream, "pulsar_consumers_count", stats.consumersCount, cluster, namespace);

        writeMetric(stream, "pulsar_rate_in", stats.rateIn, cluster, namespace);
        writeMetric(stream, "pulsar_rate_out", stats.rateOut, cluster, namespace);
        writeMetric(stream, "pulsar_throughput_in", stats.throughputIn, cluster, namespace);
        writeMetric(stream, "pulsar_throughput_out", stats.throughputOut, cluster, namespace);
        writeMetric(stream, "pulsar_txn_tb_active_total", stats.ongoingTxnCount, cluster, namespace);
        writeMetric(stream, "pulsar_txn_tb_aborted_total", stats.abortedTxnCount, cluster, namespace);
        writeMetric(stream, "pulsar_txn_tb_committed_total", stats.committedTxnCount, cluster, namespace);
        writeMetric(stream, "pulsar_consumer_msg_ack_rate", stats.messageAckRate, cluster, namespace);

        writeMetric(stream, "pulsar_in_bytes_total", stats.bytesInCounter, cluster, namespace);
        writeMetric(stream, "pulsar_in_messages_total", stats.msgInCounter, cluster, namespace);
        writeMetric(stream, "pulsar_out_bytes_total", stats.bytesOutCounter, cluster, namespace);
        writeMetric(stream, "pulsar_out_messages_total", stats.msgOutCounter, cluster, namespace);

        writeMetric(stream, "pulsar_storage_size", stats.managedLedgerStats.storageSize, cluster,
                namespace);
        writeMetric(stream, "pulsar_storage_logical_size",
                stats.managedLedgerStats.storageLogicalSize, cluster, namespace);
        writeMetric(stream, "pulsar_storage_backlog_size", stats.managedLedgerStats.backlogSize, cluster,
                namespace);
        writeMetric(stream, "pulsar_storage_offloaded_size",
                stats.managedLedgerStats.offloadedStorageUsed, cluster, namespace);

        writeMetric(stream, "pulsar_storage_write_rate", stats.managedLedgerStats.storageWriteRate,
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_read_rate", stats.managedLedgerStats.storageReadRate,
                cluster, namespace);

        writeMetric(stream, "pulsar_subscription_delayed", stats.msgDelayed, cluster, namespace);

        writeMetric(stream, "pulsar_delayed_message_index_size_bytes", stats.delayedTrackerMemoryUsage, cluster,
                namespace);

        writePulsarMsgBacklog(stream, stats.msgBacklog, cluster, namespace);

        stats.managedLedgerStats.storageWriteLatencyBuckets.refresh();
        long[] latencyBuckets = stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets();
        writeMetric(stream, "pulsar_storage_write_latency_le_0_5", latencyBuckets[0], cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_le_1", latencyBuckets[1], cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_le_5", latencyBuckets[2], cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_le_10", latencyBuckets[3], cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_le_20", latencyBuckets[4], cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_le_50", latencyBuckets[5], cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_le_100", latencyBuckets[6], cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_le_200", latencyBuckets[7], cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_le_1000", latencyBuckets[8], cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_overflow", latencyBuckets[9], cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_count",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getCount(), cluster, namespace);
        writeMetric(stream, "pulsar_storage_write_latency_sum",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getSum(), cluster, namespace);

        stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.refresh();
        long[] ledgerWriteLatencyBuckets = stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets();
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_0_5", ledgerWriteLatencyBuckets[0],
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_1", ledgerWriteLatencyBuckets[1],
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_5", ledgerWriteLatencyBuckets[2],
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_10", ledgerWriteLatencyBuckets[3],
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_20", ledgerWriteLatencyBuckets[4],
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_50", ledgerWriteLatencyBuckets[5],
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_100", ledgerWriteLatencyBuckets[6],
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_200", ledgerWriteLatencyBuckets[7],
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_1000", ledgerWriteLatencyBuckets[8],
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_overflow", ledgerWriteLatencyBuckets[9],
                cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_count",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount(), cluster, namespace);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_sum",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum(), cluster, namespace);

        stats.managedLedgerStats.entrySizeBuckets.refresh();
        long[] entrySizeBuckets = stats.managedLedgerStats.entrySizeBuckets.getBuckets();
        writeMetric(stream, "pulsar_entry_size_le_128", entrySizeBuckets[0], cluster, namespace);
        writeMetric(stream, "pulsar_entry_size_le_512", entrySizeBuckets[1], cluster, namespace);
        writeMetric(stream, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2], cluster, namespace);
        writeMetric(stream, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3], cluster, namespace);
        writeMetric(stream, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4], cluster, namespace);
        writeMetric(stream, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5], cluster, namespace);
        writeMetric(stream, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6], cluster, namespace);
        writeMetric(stream, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7], cluster, namespace);
        writeMetric(stream, "pulsar_entry_size_le_overflow", entrySizeBuckets[8], cluster, namespace);
        writeMetric(stream, "pulsar_entry_size_count", stats.managedLedgerStats.entrySizeBuckets.getCount(),
                cluster, namespace);
        writeMetric(stream, "pulsar_entry_size_sum", stats.managedLedgerStats.entrySizeBuckets.getSum(),
                cluster, namespace);

        writeReplicationStat(stream, "pulsar_replication_rate_in", stats,
                replStats -> replStats.msgRateIn, cluster, namespace);
        writeReplicationStat(stream, "pulsar_replication_rate_out", stats,
                replStats -> replStats.msgRateOut, cluster, namespace);
        writeReplicationStat(stream, "pulsar_replication_throughput_in", stats,
                replStats -> replStats.msgThroughputIn, cluster, namespace);
        writeReplicationStat(stream, "pulsar_replication_throughput_out", stats,
                replStats -> replStats.msgThroughputOut, cluster, namespace);
        writeReplicationStat(stream, "pulsar_replication_backlog", stats,
                replStats -> replStats.replicationBacklog, cluster, namespace);
        writeReplicationStat(stream, "pulsar_replication_connected_count", stats,
                replStats -> replStats.connectedCount, cluster, namespace);
        writeReplicationStat(stream, "pulsar_replication_rate_expired", stats,
                replStats -> replStats.msgRateExpired, cluster, namespace);
        writeReplicationStat(stream, "pulsar_replication_delay_in_seconds", stats,
                replStats -> replStats.replicationDelayInSeconds, cluster, namespace);
    }

    private static void writePulsarMsgBacklog(PrometheusMetricStreams stream, Number value,
                                              String cluster, String namespace) {
        stream.writeSample("pulsar_msg_backlog", value, "cluster", cluster, "namespace", namespace,
                "remote_cluster",
                "local");
    }

    private static void writeMetric(PrometheusMetricStreams stream, String metricName, Number value,
                                    String cluster) {
        stream.writeSample(metricName, value, "cluster", cluster);
    }

    private static void writeMetric(PrometheusMetricStreams stream, String metricName, Number value, String cluster,
                                    String namespace) {
        stream.writeSample(metricName, value, "cluster", cluster, "namespace", namespace);
    }

    private static void writeReplicationStat(PrometheusMetricStreams stream, String metricName,
                                             AggregatedNamespaceStats namespaceStats,
                                             Function<AggregatedReplicationStats, Number> sampleValueFunction,
                                             String cluster, String namespace) {
        if (!namespaceStats.replicationStats.isEmpty()) {
            namespaceStats.replicationStats.forEach((remoteCluster, replStats) ->
                    stream.writeSample(metricName, sampleValueFunction.apply(replStats),
                            "cluster", cluster,
                            "namespace", namespace,
                            "remote_cluster", remoteCluster)
            );
        }
    }


}
