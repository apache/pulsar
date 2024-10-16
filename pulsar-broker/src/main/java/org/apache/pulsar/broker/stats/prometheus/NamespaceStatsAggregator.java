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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopicMetrics;
import org.apache.pulsar.broker.service.persistent.PersistentTopicMetrics.BacklogQuotaMetrics;
import org.apache.pulsar.broker.stats.prometheus.metrics.PrometheusLabels;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
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

    private static final FastThreadLocal<AggregatedBrokerStats> localBrokerStats =
            new FastThreadLocal<>() {
                @Override
                protected AggregatedBrokerStats initialValue() {
                    return new AggregatedBrokerStats();
                }
            };

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
        AggregatedBrokerStats brokerStats = localBrokerStats.get();
        brokerStats.reset();
        AggregatedNamespaceStats namespaceStats = localNamespaceStats.get();
        TopicStats topicStats = localTopicStats.get();
        Optional<CompactorMXBean> compactorMXBean = getCompactorMXBean(pulsar);
        LongAdder topicsCount = new LongAdder();
        Map<String, Long> localNamespaceTopicCount = new HashMap<>();
        pulsar.getBrokerService().getMultiLayerTopicsMap().forEach((namespace, bundlesMap) -> {
            namespaceStats.reset();
            topicsCount.reset();

            bundlesMap.forEach((bundle, topicsMap) -> topicsMap.forEach((name, topic) -> {
                getTopicStats(topic, topicStats, includeConsumerMetrics, includeProducerMetrics,
                        pulsar.getConfiguration().isExposePreciseBacklogInPrometheus(),
                        pulsar.getConfiguration().isExposeSubscriptionBacklogSizeInPrometheus(),
                        compactorMXBean
                );

                brokerStats.updateStats(topicStats);

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

        printBrokerStats(stream, cluster, brokerStats);
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
        subsStats.msgInReplay = subscriptionStats.msgInReplay;
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
        subsStats.delayedMessageIndexSizeInBytes = subscriptionStats.delayedMessageIndexSizeInBytes;
        subsStats.bucketDelayedIndexStats = subscriptionStats.bucketDelayedIndexStats;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static void getTopicStats(Topic topic, TopicStats stats, boolean includeConsumerMetrics,
                                      boolean includeProducerMetrics, boolean getPreciseBacklog,
                                      boolean subscriptionBacklogSize, Optional<CompactorMXBean> compactorMXBean) {
        stats.reset();

        if (topic instanceof PersistentTopic persistentTopic) {
            // Managed Ledger stats
            ManagedLedger ml = persistentTopic.getManagedLedger();
            ManagedLedgerMBeanImpl mlStats = (ManagedLedgerMBeanImpl) ml.getStats();

            stats.managedLedgerStats.storageSize = mlStats.getStoredMessagesSize();
            stats.managedLedgerStats.storageLogicalSize = mlStats.getStoredMessagesLogicalSize();
            stats.managedLedgerStats.backlogSize = ml.getEstimatedBacklogSize();
            stats.managedLedgerStats.offloadedStorageUsed = ml.getOffloadedSize();
            stats.backlogQuotaLimit = topic
                    .getBacklogQuota(BacklogQuotaType.destination_storage).getLimitSize();
            stats.backlogQuotaLimitTime = topic
                    .getBacklogQuota(BacklogQuotaType.message_age).getLimitTime();
            stats.backlogAgeSeconds = topic.getBestEffortOldestUnacknowledgedMessageAgeSeconds();

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
            stats.managedLedgerStats.storageReadCacheMissesRate = mlStats.getReadEntriesOpsCacheMissesRate();

            // Topic Stats
            PersistentTopicMetrics persistentTopicMetrics = persistentTopic.getPersistentTopicMetrics();

            BacklogQuotaMetrics backlogQuotaMetrics = persistentTopicMetrics.getBacklogQuotaMetrics();
            stats.sizeBasedBacklogQuotaExceededEvictionCount =
                    backlogQuotaMetrics.getSizeBasedBacklogQuotaExceededEvictionCount();
            stats.timeBasedBacklogQuotaExceededEvictionCount =
                    backlogQuotaMetrics.getTimeBasedBacklogQuotaExceededEvictionCount();
        }

        TopicStatsImpl tStatus = topic.getStats(getPreciseBacklog, subscriptionBacklogSize, false);
        stats.msgInCounter = tStatus.msgInCounter;
        stats.bytesInCounter = tStatus.bytesInCounter;
        stats.msgOutCounter = tStatus.msgOutCounter;
        stats.systemTopicBytesInCounter = tStatus.systemTopicBytesInCounter;
        stats.bytesOutInternalCounter = tStatus.getBytesOutInternalCounter();
        stats.bytesOutCounter = tStatus.bytesOutCounter;
        stats.averageMsgSize = tStatus.averageMsgSize;
        stats.publishRateLimitedTimes = tStatus.publishRateLimitedTimes;
        stats.delayedMessageIndexSizeInBytes = tStatus.delayedMessageIndexSizeInBytes;
        stats.bucketDelayedIndexStats = tStatus.bucketDelayedIndexStats;
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
            ReplicatorStatsImpl replStats = replicator.computeStats();
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
            if (replStats.connected) {
                aggReplStats.connectedCount += 1;
            } else {
                aggReplStats.disconnectedCount += 1;
            }
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

    private static void printBrokerStats(PrometheusMetricStreams stream, String cluster,
                                         AggregatedBrokerStats brokerStats) {
        // Print metrics values. This is necessary to have the available brokers being
        // reported in the brokers dashboard even if they don't have any topic or traffic
        writeMetric(stream, "pulsar_broker_topics_count", brokerStats.topicsCount, cluster);
        writeMetric(stream, "pulsar_broker_subscriptions_count", brokerStats.subscriptionsCount, cluster);
        writeMetric(stream, "pulsar_broker_producers_count", brokerStats.producersCount, cluster);
        writeMetric(stream, "pulsar_broker_consumers_count", brokerStats.consumersCount, cluster);
        writeMetric(stream, "pulsar_broker_rate_in", brokerStats.rateIn, cluster);
        writeMetric(stream, "pulsar_broker_rate_out", brokerStats.rateOut, cluster);
        writeMetric(stream, "pulsar_broker_throughput_in", brokerStats.throughputIn, cluster);
        writeMetric(stream, "pulsar_broker_throughput_out", brokerStats.throughputOut, cluster);
        writeMetric(stream, "pulsar_broker_storage_size", brokerStats.storageSize, cluster);
        writeMetric(stream, "pulsar_broker_storage_logical_size", brokerStats.storageLogicalSize, cluster);
        writeMetric(stream, "pulsar_broker_storage_write_rate", brokerStats.storageWriteRate, cluster);
        writeMetric(stream, "pulsar_broker_storage_read_rate", brokerStats.storageReadRate, cluster);
        writeMetric(stream, "pulsar_broker_storage_read_cache_misses_rate",
                brokerStats.storageReadCacheMissesRate, cluster);

        writePulsarBacklogQuotaMetricBrokerLevel(stream,
                "pulsar_broker_storage_backlog_quota_exceeded_evictions_total",
                brokerStats.sizeBasedBacklogQuotaExceededEvictionCount, cluster, BacklogQuotaType.destination_storage);
        writePulsarBacklogQuotaMetricBrokerLevel(stream,
                "pulsar_broker_storage_backlog_quota_exceeded_evictions_total",
                brokerStats.timeBasedBacklogQuotaExceededEvictionCount, cluster, BacklogQuotaType.message_age);

        writeMetric(stream, "pulsar_broker_msg_backlog", brokerStats.msgBacklog, cluster);
        long userOutBytes = brokerStats.bytesOutCounter - brokerStats.bytesOutInternalCounter;
        writeMetric(stream, "pulsar_broker_out_bytes_total",
                userOutBytes, cluster, "system_subscription", "false");
        writeMetric(stream, "pulsar_broker_out_bytes_total",
                brokerStats.bytesOutInternalCounter, cluster, "system_subscription", "true");
        long userTopicInBytes = brokerStats.bytesInCounter - brokerStats.systemTopicBytesInCounter;
        writeMetric(stream, "pulsar_broker_in_bytes_total",
                userTopicInBytes, cluster, "system_topic", "false");
        writeMetric(stream, "pulsar_broker_in_bytes_total",
                brokerStats.systemTopicBytesInCounter, cluster, "system_topic", "true");
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
        writeMetric(stream, "pulsar_storage_read_cache_misses_rate",
                stats.managedLedgerStats.storageReadCacheMissesRate, cluster, namespace);

        writeMetric(stream, "pulsar_subscription_delayed", stats.msgDelayed, cluster, namespace);

        writeMetric(stream, "pulsar_subscription_in_replay", stats.msgInReplay, cluster, namespace);

        writeMetric(stream, "pulsar_delayed_message_index_size_bytes", stats.delayedMessageIndexSizeInBytes, cluster,
                namespace);

        stats.bucketDelayedIndexStats.forEach((k, metric) -> {
            String[] labels = ArrayUtils.addAll(new String[]{"namespace", namespace}, metric.labelsAndValues);
            writeMetric(stream, metric.name, metric.value, cluster, labels);
        });

        writePulsarMsgBacklog(stream, stats.msgBacklog, cluster, namespace);
        writePulsarBacklogQuotaMetricNamespaceLevel(stream,
                "pulsar_storage_backlog_quota_exceeded_evictions_total",
                stats.sizeBasedBacklogQuotaExceededEvictionCount, cluster, namespace,
                BacklogQuotaType.destination_storage);
        writePulsarBacklogQuotaMetricNamespaceLevel(stream,
                "pulsar_storage_backlog_quota_exceeded_evictions_total",
                stats.timeBasedBacklogQuotaExceededEvictionCount, cluster, namespace,
                BacklogQuotaType.message_age);

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
        writeReplicationStat(stream, "pulsar_replication_disconnected_count", stats,
                replStats -> replStats.disconnectedCount, cluster, namespace);
        writeReplicationStat(stream, "pulsar_replication_rate_expired", stats,
                replStats -> replStats.msgRateExpired, cluster, namespace);
        writeReplicationStat(stream, "pulsar_replication_delay_in_seconds", stats,
                replStats -> replStats.replicationDelayInSeconds, cluster, namespace);
    }

    @SuppressWarnings("SameParameterValue")
    private static void writePulsarBacklogQuotaMetricBrokerLevel(PrometheusMetricStreams stream, String metricName,
                                                                 Number value, String cluster,
                                                                 BacklogQuotaType backlogQuotaType) {
        String quotaTypeLabelValue = PrometheusLabels.backlogQuotaTypeLabel(backlogQuotaType);
        stream.writeSample(metricName, value, "cluster", cluster,
                "quota_type", quotaTypeLabelValue);
    }

    @SuppressWarnings("SameParameterValue")
    private static void writePulsarBacklogQuotaMetricNamespaceLevel(PrometheusMetricStreams stream, String metricName,
                                                                    Number value, String cluster, String namespace,
                                                                    BacklogQuotaType backlogQuotaType) {
        String quotaTypeLabelValue = PrometheusLabels.backlogQuotaTypeLabel(backlogQuotaType);
        stream.writeSample(metricName, value, "cluster", cluster,
                "namespace", namespace,
                "quota_type", quotaTypeLabelValue);
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

    private static void writeMetric(PrometheusMetricStreams stream, String metricName, Number value,
                                    String cluster, String... extraLabelsAndValues) {
        String[] labels = ArrayUtils.addAll(new String[]{"cluster", cluster}, extraLabelsAndValues);
        stream.writeSample(metricName, value, labels);
    }


    private static void writeMetric(PrometheusMetricStreams stream, String metricName, Number value, String cluster,
                                    String namespace) {
        String[] labels = new String[]{"cluster", cluster, "namespace", namespace};
        stream.writeSample(metricName, value, labels);
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
