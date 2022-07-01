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

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator.MAX_COMPONENTS;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
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
import org.apache.pulsar.broker.PulsarServerException;
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
import org.apache.pulsar.common.util.NumberFormat;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
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
                                SimpleTextOutputStream stream) {
        String cluster = pulsar.getConfiguration().getClusterName();
        AggregatedNamespaceStats namespaceStats = localNamespaceStats.get();
        TopicStats topicStats = localTopicStats.get();
        Optional<CompactorMXBean> compactorMXBean = getCompactorMXBean(pulsar);
        LongAdder topicsCount = new LongAdder();
        Map<String, ByteBuf> allMetrics = new HashMap<>();
        Map<String, Long> localNamespaceTopicCount = new HashMap<>();
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
                    TopicStats.printTopicStats(allMetrics, topicStats, compactorMXBean, cluster, namespace, name,
                            splitTopicAndPartitionIndexLabel);
                } else {
                    namespaceStats.updateStats(topicStats);
                }
            }));

            if (!includeTopicMetrics) {
                // Only include namespace level stats if we don't have the per-topic, otherwise we're going to
                // report the same data twice, and it will make the aggregation difficult
                printNamespaceStats(allMetrics, namespaceStats, cluster, namespace);
            } else {
                localNamespaceTopicCount.put(namespace, topicsCount.sum());
            }
        });

        if (includeTopicMetrics) {
            printTopicsCountStats(allMetrics, localNamespaceTopicCount, cluster);
        }

        allMetrics.values().forEach(stream::write);
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

    private static void printTopicsCountStats(Map<String, ByteBuf> allMetrics, Map<String, Long> namespaceTopicsCount,
                                              String cluster) {
        namespaceTopicsCount.forEach((ns, topicCount) ->
                writeMetricWithBrokerDefault(allMetrics, "pulsar_topics_count", topicCount, cluster, ns)
        );
    }

    private static void printNamespaceStats(Map<String, ByteBuf> allMetrics, AggregatedNamespaceStats stats,
                                            String cluster, String namespace) {
        writeMetricWithBrokerDefault(allMetrics, "pulsar_topics_count", stats.topicsCount, cluster, namespace);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_subscriptions_count", stats.subscriptionsCount, cluster,
                namespace);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_producers_count", stats.producersCount, cluster, namespace);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_consumers_count", stats.consumersCount, cluster, namespace);

        writeMetricWithBrokerDefault(allMetrics, "pulsar_rate_in", stats.rateIn, cluster, namespace);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_rate_out", stats.rateOut, cluster, namespace);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_throughput_in", stats.throughputIn, cluster, namespace);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_throughput_out", stats.throughputOut, cluster, namespace);
        writeMetric(allMetrics, "pulsar_consumer_msg_ack_rate", stats.messageAckRate, cluster, namespace);

        writeMetric(allMetrics, "pulsar_in_bytes_total", stats.bytesInCounter, cluster, namespace);
        writeMetric(allMetrics, "pulsar_in_messages_total", stats.msgInCounter, cluster, namespace);
        writeMetric(allMetrics, "pulsar_out_bytes_total", stats.bytesOutCounter, cluster, namespace);
        writeMetric(allMetrics, "pulsar_out_messages_total", stats.msgOutCounter, cluster, namespace);

        writeMetricWithBrokerDefault(allMetrics, "pulsar_storage_size", stats.managedLedgerStats.storageSize, cluster,
                namespace);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_storage_logical_size",
                stats.managedLedgerStats.storageLogicalSize, cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_backlog_size", stats.managedLedgerStats.backlogSize, cluster,
                namespace);
        writeMetric(allMetrics, "pulsar_storage_offloaded_size",
                stats.managedLedgerStats.offloadedStorageUsed, cluster, namespace);

        writeMetricWithBrokerDefault(allMetrics, "pulsar_storage_write_rate", stats.managedLedgerStats.storageWriteRate,
                cluster, namespace);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_storage_read_rate", stats.managedLedgerStats.storageReadRate,
                cluster, namespace);

        writeMetric(allMetrics, "pulsar_subscription_delayed", stats.msgDelayed, cluster, namespace);

        writePulsarMsgBacklog(allMetrics, stats.msgBacklog, cluster, namespace);

        stats.managedLedgerStats.storageWriteLatencyBuckets.refresh();
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_0_5",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[0], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_1",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[1], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_5",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[2], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_10",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[3], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_20",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[4], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_50",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[5], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_100",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[6], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_200",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[7], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_1000",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[8], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_overflow",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[9], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_count",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getCount(), cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_write_latency_sum",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getSum(), cluster, namespace);

        stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.refresh();
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_0_5",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[0], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_1",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[1], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_5",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[2], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_10",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[3], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_20",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[4], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_50",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[5], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_100",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[6], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_200",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[7], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_1000",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[8], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_overflow",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[9], cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_count",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount(), cluster, namespace);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_sum",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum(), cluster, namespace);

        stats.managedLedgerStats.entrySizeBuckets.refresh();
        writeMetric(allMetrics, "pulsar_entry_size_le_128", stats.managedLedgerStats.entrySizeBuckets.getBuckets()[0],
                cluster, namespace);
        writeMetric(allMetrics, "pulsar_entry_size_le_512", stats.managedLedgerStats.entrySizeBuckets.getBuckets()[1],
                cluster, namespace);
        writeMetric(allMetrics, "pulsar_entry_size_le_1_kb", stats.managedLedgerStats.entrySizeBuckets.getBuckets()[2],
                cluster, namespace);
        writeMetric(allMetrics, "pulsar_entry_size_le_2_kb", stats.managedLedgerStats.entrySizeBuckets.getBuckets()[3],
                cluster, namespace);
        writeMetric(allMetrics, "pulsar_entry_size_le_4_kb", stats.managedLedgerStats.entrySizeBuckets.getBuckets()[4],
                cluster, namespace);
        writeMetric(allMetrics, "pulsar_entry_size_le_16_kb", stats.managedLedgerStats.entrySizeBuckets.getBuckets()[5],
                cluster, namespace);
        writeMetric(allMetrics, "pulsar_entry_size_le_100_kb",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[6], cluster, namespace);
        writeMetric(allMetrics, "pulsar_entry_size_le_1_mb", stats.managedLedgerStats.entrySizeBuckets.getBuckets()[7],
                cluster, namespace);
        writeMetric(allMetrics, "pulsar_entry_size_le_overflow",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[8], cluster, namespace);
        writeMetric(allMetrics, "pulsar_entry_size_count",
                stats.managedLedgerStats.entrySizeBuckets.getCount(), cluster, namespace);
        writeMetric(allMetrics, "pulsar_entry_size_sum",
                stats.managedLedgerStats.entrySizeBuckets.getSum(), cluster, namespace);

        writeReplicationStat(allMetrics, "pulsar_replication_rate_in", stats,
                replStats -> replStats.msgRateIn, cluster, namespace);
        writeReplicationStat(allMetrics, "pulsar_replication_rate_out", stats,
                replStats -> replStats.msgRateOut, cluster, namespace);
        writeReplicationStat(allMetrics, "pulsar_replication_throughput_in", stats,
                replStats -> replStats.msgThroughputIn, cluster, namespace);
        writeReplicationStat(allMetrics, "pulsar_replication_throughput_out", stats,
                replStats -> replStats.msgThroughputOut, cluster, namespace);
        writeReplicationStat(allMetrics, "pulsar_replication_backlog", stats,
                replStats -> replStats.replicationBacklog, cluster, namespace);
        writeReplicationStat(allMetrics, "pulsar_replication_connected_count", stats,
                replStats -> replStats.connectedCount, cluster, namespace);
        writeReplicationStat(allMetrics, "pulsar_replication_rate_expired", stats,
                replStats -> replStats.msgRateExpired, cluster, namespace);
        writeReplicationStat(allMetrics, "pulsar_replication_delay_in_seconds", stats,
                replStats -> replStats.replicationDelayInSeconds, cluster, namespace);
    }

    private static void writeMetricWithBrokerDefault(Map<String, ByteBuf> allMetrics, String metricName, Number value,
                                                     String cluster, String namespace) {
        ByteBuf buffer = writeGaugeTypeWithBrokerDefault(allMetrics, metricName, cluster);
        writeSample(buffer, metricName, value, "cluster", cluster, "namespace", namespace);
    }

    private static void writePulsarMsgBacklog(Map<String, ByteBuf> allMetrics, Number value,
                                              String cluster, String namespace) {
        ByteBuf buffer = writeGaugeTypeWithBrokerDefault(allMetrics, "pulsar_msg_backlog", cluster);
        writeSample(buffer, "pulsar_msg_backlog", value, "cluster", cluster, "namespace", namespace, "remote_cluster",
                "local");
    }

    private static void writeMetric(Map<String, ByteBuf> allMetrics, String metricName, Number value, String cluster,
                                    String namespace) {
        ByteBuf buffer = writeGaugeType(allMetrics, metricName);
        writeSample(buffer, metricName, value, "cluster", cluster, "namespace", namespace);
    }

    private static void writeReplicationStat(Map<String, ByteBuf> allMetrics, String metricName,
                                             AggregatedNamespaceStats namespaceStats,
                                             Function<AggregatedReplicationStats, Number> replStatsFunction,
                                             String cluster, String namespace) {
        if (!namespaceStats.replicationStats.isEmpty()) {
            ByteBuf buffer = writeGaugeType(allMetrics, metricName);
            namespaceStats.replicationStats.forEach((remoteCluster, replStats) ->
                    writeSample(buffer, metricName, replStatsFunction.apply(replStats),
                            "cluster", cluster,
                            "namespace", namespace,
                            "remote_cluster", remoteCluster)
            );
        }
    }

    static ByteBuf writeGaugeType(Map<String, ByteBuf> allMetrics, String metricName) {
        if (!allMetrics.containsKey(metricName)) {
            ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.compositeDirectBuffer(MAX_COMPONENTS);
            write(buffer, "# TYPE ");
            write(buffer, metricName);
            write(buffer, " gauge\n");
            allMetrics.put(metricName, buffer);
        }
        return allMetrics.get(metricName);
    }

    static ByteBuf writeGaugeTypeWithBrokerDefault(Map<String, ByteBuf> allMetrics, String metricName, String cluster) {
        if (!allMetrics.containsKey(metricName)) {
            ByteBuf buffer = writeGaugeType(allMetrics, metricName);
            writeSample(buffer, metricName, 0, "cluster", cluster);
        }
        return allMetrics.get(metricName);
    }

    static void writeSample(ByteBuf buffer, String metricName, Number value, String... labelsAndValuesArray) {
        write(buffer, metricName);
        write(buffer, '{');
        for (int i = 0; i < labelsAndValuesArray.length; i += 2) {
            write(buffer, labelsAndValuesArray[i]);
            write(buffer, "=\"");
            write(buffer, labelsAndValuesArray[i + 1]);
            write(buffer, '\"');
            if (labelsAndValuesArray.length != i + 2) {
                write(buffer, ',');
            }
        }
        write(buffer, "\"} ");
        write(buffer, value);
        write(buffer, ' ');
        write(buffer, System.currentTimeMillis());
        write(buffer, '\n');
    }

    private static void write(ByteBuf buffer, String s) {
        if (s == null) {
            return;
        }
        int len = s.length();
        for (int i = 0; i < len; i++) {
            write(buffer, s.charAt(i));
        }
    }

    private static void write(ByteBuf buffer, Number n) {
        if (n instanceof Integer) {
            write(buffer, n.intValue());
        } else if (n instanceof Long) {
            write(buffer, n.longValue());
        } else if (n instanceof Double) {
            write(buffer, n.doubleValue());
        } else {
            write(buffer, n.toString());
        }
    }

    private static void write(ByteBuf buffer, long n) {
        NumberFormat.format(buffer, n);
    }

    private static void write(ByteBuf buffer, double d) {
        long i = (long) d;
        write(buffer, i);

        long r = Math.abs((long) (1000 * (d - i)));
        write(buffer, '.');
        if (r == 0) {
            write(buffer, '0');
        }

        if (r < 100) {
            write(buffer, '0');
        }

        if (r < 10) {
            write(buffer, '0');
        }

        write(buffer, r);
    }

    private static void write(ByteBuf buffer, char c) {
        buffer.writeByte((byte) c);
    }
}
