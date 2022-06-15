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

import static org.apache.pulsar.broker.stats.prometheus.TopicStats.addMetric;
import io.netty.util.concurrent.FastThreadLocal;
import io.prometheus.client.Collector;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.prometheus.metrics.PrometheusTextFormatUtil;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentSubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.compaction.CompactedTopicContext;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.CompactorMXBean;

@Slf4j
public class NamespaceStatsAggregator {

    private static FastThreadLocal<AggregatedNamespaceStats> localNamespaceStats =
            new FastThreadLocal<>() {
                @Override
                protected AggregatedNamespaceStats initialValue() throws Exception {
                    return new AggregatedNamespaceStats();
                }
            };

    private static FastThreadLocal<TopicStats> localTopicStats = new FastThreadLocal<>() {
        @Override
        protected TopicStats initialValue() throws Exception {
            return new TopicStats();
        }
    };

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                boolean includeProducerMetrics, boolean splitTopicAndPartitionIndexLabel,
                                SimpleTextOutputStream stream) {
        String cluster = pulsar.getConfiguration().getClusterName();
        AggregatedNamespaceStats namespaceStats = localNamespaceStats.get();
        TopicStats.resetTypes();
        TopicStats topicStats = localTopicStats.get();

        Map<String, Collector.MetricFamilySamples> metrics = new HashMap<>();

        buildDefaultBrokerStats(metrics, cluster);

        Optional<CompactorMXBean> compactorMXBean = getCompactorMXBean(pulsar);
        LongAdder topicsCount = new LongAdder();
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
                    TopicStats.buildTopicStats(metrics, cluster, namespace, name, topicStats, compactorMXBean,
                            splitTopicAndPartitionIndexLabel);
                } else {
                    namespaceStats.updateStats(topicStats);
                }
            }));

            if (!includeTopicMetrics) {
                // Only include namespace level stats if we don't have the per-topic, otherwise we're going to report
                // the same data twice, and it will make the aggregation difficult
                printNamespaceStats(metrics, cluster, namespace, namespaceStats);
            } else {
                printTopicsCountStats(metrics, cluster, namespace, topicsCount);
            }
        });

        PrometheusTextFormatUtil.writeMetrics(stream, metrics.values());
    }

    private static Optional<CompactorMXBean> getCompactorMXBean(PulsarService pulsar) {
        Compactor compactor = null;
        try {
            compactor = pulsar.getCompactor(false);
        } catch (PulsarServerException e) {
            log.error("get compactor error", e);
        }
        return Optional.ofNullable(compactor).map(Compactor::getStats);
    }

    private static void aggregateTopicStats(TopicStats stats, SubscriptionStatsImpl subscriptionStats,
                                            AggregatedSubscriptionStats subsStats) {
        stats.subscriptionsCount++;
        stats.msgBacklog += subscriptionStats.msgBacklog;
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
        subscriptionStats.consumers.forEach(cStats -> {
            stats.consumersCount++;
            subsStats.unackedMessages += cStats.unackedMessages;
            subsStats.msgRateRedeliver += cStats.msgRateRedeliver;
            subsStats.msgRateOut += cStats.msgRateOut;
            subsStats.messageAckRate += cStats.messageAckRate;
            subsStats.msgThroughputOut += cStats.msgThroughputOut;
            subsStats.bytesOutCounter += cStats.bytesOutCounter;
            subsStats.msgOutCounter += cStats.msgOutCounter;
            if (!subsStats.blockedSubscriptionOnUnackedMsgs && cStats.blockedConsumerOnUnackedMsgs) {
                subsStats.blockedSubscriptionOnUnackedMsgs = true;
            }
        });
        stats.rateOut += subsStats.msgRateOut;
        stats.throughputOut += subsStats.msgThroughputOut;

    }

    private static void getTopicStats(Topic topic, TopicStats stats, boolean includeConsumerMetrics,
                                      boolean includeProducerMetrics, boolean getPreciseBacklog,
                                      boolean subscriptionBacklogSize,
                                      Optional<CompactorMXBean> compactorMXBean) {
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

    private static void buildDefaultBrokerStats(Map<String, Collector.MetricFamilySamples> metrics, String cluster) {
        // Print metrics with 0 values. This is necessary to have the available brokers being
        // reported in the brokers dashboard even if they don't have any topic or traffic
        metric(metrics, cluster, "pulsar_topics_count", 0);
        metric(metrics, cluster, "pulsar_subscriptions_count", 0);
        metric(metrics, cluster, "pulsar_producers_count", 0);
        metric(metrics, cluster, "pulsar_consumers_count", 0);
        metric(metrics, cluster, "pulsar_rate_in", 0);
        metric(metrics, cluster, "pulsar_rate_out", 0);
        metric(metrics, cluster, "pulsar_throughput_in", 0);
        metric(metrics, cluster, "pulsar_throughput_out", 0);
        metric(metrics, cluster, "pulsar_storage_size", 0);
        metric(metrics, cluster, "pulsar_storage_logical_size", 0);
        metric(metrics, cluster, "pulsar_storage_write_rate", 0);
        metric(metrics, cluster, "pulsar_storage_read_rate", 0);
        metric(metrics, cluster, "pulsar_msg_backlog", 0);
    }

    private static void printTopicsCountStats(Map<String, Collector.MetricFamilySamples> metrics, String cluster,
                                              String namespace,
                                              LongAdder topicsCount) {
        metric(metrics, cluster, namespace, "pulsar_topics_count", topicsCount.sum());
    }

    private static void printNamespaceStats(Map<String, Collector.MetricFamilySamples> metrics, String cluster,
                                            String namespace,
                                            AggregatedNamespaceStats stats) {
        metric(metrics, cluster, namespace, "pulsar_topics_count", stats.topicsCount);
        metric(metrics, cluster, namespace, "pulsar_subscriptions_count", stats.subscriptionsCount);
        metric(metrics, cluster, namespace, "pulsar_producers_count", stats.producersCount);
        metric(metrics, cluster, namespace, "pulsar_consumers_count", stats.consumersCount);

        metric(metrics, cluster, namespace, "pulsar_rate_in", stats.rateIn);
        metric(metrics, cluster, namespace, "pulsar_rate_out", stats.rateOut);
        metric(metrics, cluster, namespace, "pulsar_throughput_in", stats.throughputIn);
        metric(metrics, cluster, namespace, "pulsar_throughput_out", stats.throughputOut);
        metric(metrics, cluster, namespace, "pulsar_consumer_msg_ack_rate", stats.messageAckRate);

        metric(metrics, cluster, namespace, "pulsar_in_bytes_total", stats.bytesInCounter);
        metric(metrics, cluster, namespace, "pulsar_in_messages_total", stats.msgInCounter);
        metric(metrics, cluster, namespace, "pulsar_out_bytes_total", stats.bytesOutCounter);
        metric(metrics, cluster, namespace, "pulsar_out_messages_total", stats.msgOutCounter);

        metric(metrics, cluster, namespace, "pulsar_storage_size", stats.managedLedgerStats.storageSize);
        metric(metrics, cluster, namespace, "pulsar_storage_logical_size", stats.managedLedgerStats.storageLogicalSize);
        metric(metrics, cluster, namespace, "pulsar_storage_backlog_size", stats.managedLedgerStats.backlogSize);
        metric(metrics, cluster, namespace, "pulsar_storage_offloaded_size",
                stats.managedLedgerStats.offloadedStorageUsed);

        metric(metrics, cluster, namespace, "pulsar_storage_write_rate", stats.managedLedgerStats.storageWriteRate);
        metric(metrics, cluster, namespace, "pulsar_storage_read_rate", stats.managedLedgerStats.storageReadRate);

        metric(metrics, cluster, namespace, "pulsar_subscription_delayed", stats.msgDelayed);

        metricWithRemoteCluster(metrics, cluster, namespace, "pulsar_msg_backlog", "local", stats.msgBacklog);

        stats.managedLedgerStats.storageWriteLatencyBuckets.refresh();
        long[] latencyBuckets = stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets();
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_le_0_5", latencyBuckets[0]);
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_le_1", latencyBuckets[1]);
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_le_5", latencyBuckets[2]);
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_le_10", latencyBuckets[3]);
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_le_20", latencyBuckets[4]);
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_le_50", latencyBuckets[5]);
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_le_100", latencyBuckets[6]);
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_le_200", latencyBuckets[7]);
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_le_1000", latencyBuckets[8]);
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_overflow", latencyBuckets[9]);
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_count",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getCount());
        metric(metrics, cluster, namespace, "pulsar_storage_write_latency_sum",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getSum());

        stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.refresh();
        long[] ledgerWritelatencyBuckets = stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets();
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_le_0_5", ledgerWritelatencyBuckets[0]);
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_le_1", ledgerWritelatencyBuckets[1]);
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_le_5", ledgerWritelatencyBuckets[2]);
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_le_10", ledgerWritelatencyBuckets[3]);
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_le_20", ledgerWritelatencyBuckets[4]);
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_le_50", ledgerWritelatencyBuckets[5]);
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_le_100", ledgerWritelatencyBuckets[6]);
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_le_200", ledgerWritelatencyBuckets[7]);
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_le_1000",
                ledgerWritelatencyBuckets[8]);
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_overflow",
                ledgerWritelatencyBuckets[9]);
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_count",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount());
        metric(metrics, cluster, namespace, "pulsar_storage_ledger_write_latency_sum",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum());

        stats.managedLedgerStats.entrySizeBuckets.refresh();
        long[] entrySizeBuckets = stats.managedLedgerStats.entrySizeBuckets.getBuckets();
        metric(metrics, cluster, namespace, "pulsar_entry_size_le_128", entrySizeBuckets[0]);
        metric(metrics, cluster, namespace, "pulsar_entry_size_le_512", entrySizeBuckets[1]);
        metric(metrics, cluster, namespace, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2]);
        metric(metrics, cluster, namespace, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3]);
        metric(metrics, cluster, namespace, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4]);
        metric(metrics, cluster, namespace, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5]);
        metric(metrics, cluster, namespace, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6]);
        metric(metrics, cluster, namespace, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7]);
        metric(metrics, cluster, namespace, "pulsar_entry_size_le_overflow", entrySizeBuckets[8]);
        metric(metrics, cluster, namespace, "pulsar_entry_size_count",
                stats.managedLedgerStats.entrySizeBuckets.getCount());
        metric(metrics, cluster, namespace, "pulsar_entry_size_sum",
                stats.managedLedgerStats.entrySizeBuckets.getSum());

        if (!stats.replicationStats.isEmpty()) {
            stats.replicationStats.forEach((remoteCluster, replStats) -> {
                metricWithRemoteCluster(metrics, cluster, namespace, "pulsar_replication_rate_in", remoteCluster,
                        replStats.msgRateIn);
                metricWithRemoteCluster(metrics, cluster, namespace, "pulsar_replication_rate_out", remoteCluster,
                        replStats.msgRateOut);
                metricWithRemoteCluster(metrics, cluster, namespace, "pulsar_replication_throughput_in", remoteCluster,
                        replStats.msgThroughputIn);
                metricWithRemoteCluster(metrics, cluster, namespace, "pulsar_replication_throughput_out", remoteCluster,
                        replStats.msgThroughputOut);
                metricWithRemoteCluster(metrics, cluster, namespace, "pulsar_replication_backlog", remoteCluster,
                        replStats.replicationBacklog);
                metricWithRemoteCluster(metrics, cluster, namespace, "pulsar_replication_connected_count",
                        remoteCluster,
                        replStats.connectedCount);
                metricWithRemoteCluster(metrics, cluster, namespace, "pulsar_replication_rate_expired", remoteCluster,
                        replStats.msgRateExpired);
                metricWithRemoteCluster(metrics, cluster, namespace, "pulsar_replication_delay_in_seconds",
                        remoteCluster, replStats.replicationDelayInSeconds);
            });
        }
    }

    private static void metric(Map<String, Collector.MetricFamilySamples> metrics, String cluster, String name,
                               long value) {
        addMetric(metrics, Map.of("cluster", cluster), name, value);
    }

    private static void metric(Map<String, Collector.MetricFamilySamples> metrics, String cluster, String namespace,
                               String name, long value) {
        addMetric(metrics, Map.of("cluster", cluster, "namespace", namespace), name, value);
    }

    private static void metric(Map<String, Collector.MetricFamilySamples> metrics, String cluster, String namespace,
                               String name, double value) {
        addMetric(metrics, Map.of("cluster", cluster, "namespace", namespace), name, value);
    }

    private static void metricWithRemoteCluster(Map<String, Collector.MetricFamilySamples> metrics, String cluster,
                                                String namespace, String name, String remoteCluster, double value) {
        addMetric(metrics, Map.of("cluster", cluster, "namespace", namespace, "remote_cluster", remoteCluster), name,
                value);
    }
}
