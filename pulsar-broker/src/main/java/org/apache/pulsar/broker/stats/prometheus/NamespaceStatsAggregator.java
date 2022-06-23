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

import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.compaction.CompactedTopicContext;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.CompactorMXBean;

@Slf4j
public class NamespaceStatsAggregator {

    private static final FastThreadLocal<List<AggregatedNamespaceStats>> localAllNamespaceStats =
            new FastThreadLocal<>() {
                @Override
                protected List<AggregatedNamespaceStats> initialValue() {
                    return new ArrayList<>();
                }
            };

    private static final FastThreadLocal<List<TopicStats>> localAllTopicStats = new FastThreadLocal<>() {
        @Override
        protected List<TopicStats> initialValue() {
            return new ArrayList<>();
        }
    };

    private static final FastThreadLocal<Map<String, Long>> localNamespaceTopicCount = new FastThreadLocal<>() {
        @Override
        protected Map<String, Long> initialValue() {
            return new HashMap<>();
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
        Optional<CompactorMXBean> compactorMXBean = getCompactorMXBean(pulsar);
        LongAdder topicsCount = new LongAdder();
        try {
            pulsar.getBrokerService().getMultiLayerTopicMap().forEach((namespace, bundlesMap) -> {
                // only need to create if we are not including topic metrics
                AggregatedNamespaceStats namespaceStats = includeTopicMetrics ? null : new AggregatedNamespaceStats();
                topicsCount.reset();

                bundlesMap.forEach((bundle, topicsMap) -> topicsMap.forEach((name, topic) -> {
                    //If not includeTopicMetrics then use a single thread local, so not assigning lots of objects
                    TopicStats topicStats = includeTopicMetrics ? new TopicStats() : localTopicStats.get();
                    topicStats.reset();
                    topicStats.name = name;
                    topicStats.namespace = namespace;
                    getTopicStats(topic, topicStats, includeConsumerMetrics, includeProducerMetrics,
                            pulsar.getConfiguration().isExposePreciseBacklogInPrometheus(),
                            pulsar.getConfiguration().isExposeSubscriptionBacklogSizeInPrometheus(),
                            compactorMXBean
                    );

                    if (includeTopicMetrics) {
                        topicsCount.add(1);
                        localAllTopicStats.get().add(topicStats);
                    } else {
                        namespaceStats.updateStats(topicStats);
                    }
                }));

                if (!includeTopicMetrics) {
                    // Only include namespace level stats if we don't have the per-topic, otherwise we're going to
                    // report the same data twice, and it will make the aggregation difficult
                    namespaceStats.name = namespace;
                    localAllNamespaceStats.get().add(namespaceStats);
                } else {
                    localNamespaceTopicCount.get().put(namespace, topicsCount.sum());
                }
            });

            if (includeTopicMetrics) {
                printTopicsCountStats(stream, cluster, localNamespaceTopicCount.get());
                TopicStats.printTopicStats(stream, cluster, localAllTopicStats.get(), compactorMXBean,
                        splitTopicAndPartitionIndexLabel);
            } else {
                printNamespaceStats(stream, cluster, localAllNamespaceStats.get());
            }
        } finally {
            if (includeTopicMetrics) {
                localNamespaceTopicCount.get().clear();
                localAllTopicStats.get().clear();
            } else {
                localAllNamespaceStats.get().clear();
            }
        }
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
        stats.name = topic.getName();

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

    private static void printTopicsCountStats(SimpleTextOutputStream stream, String cluster,
                                              Map<String, Long> namespaceTopicsCount) {
        stream.write("# TYPE ").write("pulsar_topics_count").write(" gauge\n");
        stream.write("pulsar_topics_count")
                .write("{cluster=\"").write(cluster).write("\"} ")
                .write(0).write(' ').write(System.currentTimeMillis())
                .write('\n');
        namespaceTopicsCount.forEach((ns, topicCount) -> stream.write("pulsar_topics_count")
                .write("{cluster=\"").write(cluster)
                .write("\",namespace=\"").write(ns)
                .write("\"} ")
                .write(topicCount).write(' ').write(System.currentTimeMillis())
                .write('\n')
        );
    }

    private static void printNamespaceStats(SimpleTextOutputStream stream, String cluster,
                                            List<AggregatedNamespaceStats> stats) {
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_topics_count", stats, s -> s.topicsCount);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_subscriptions_count", stats, s -> s.subscriptionsCount);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_producers_count", stats, s -> s.producersCount);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_consumers_count", stats, s -> s.consumersCount);

        writeMetricWithBrokerDefault(stream, cluster, "pulsar_rate_in", stats, s -> s.rateIn);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_rate_out", stats, s -> s.rateOut);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_throughput_in", stats, s -> s.throughputIn);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_throughput_out", stats, s -> s.throughputOut);
        writeMetric(stream, cluster, "pulsar_consumer_msg_ack_rate", stats, s -> s.messageAckRate);

        writeMetric(stream, cluster, "pulsar_in_bytes_total", stats, s -> s.bytesInCounter);
        writeMetric(stream, cluster, "pulsar_in_messages_total", stats, s -> s.msgInCounter);
        writeMetric(stream, cluster, "pulsar_out_bytes_total", stats, s -> s.bytesOutCounter);
        writeMetric(stream, cluster, "pulsar_out_messages_total", stats, s -> s.msgOutCounter);

        writeMetricWithBrokerDefault(stream, cluster, "pulsar_storage_size", stats,
                s -> s.managedLedgerStats.storageSize);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_storage_logical_size", stats,
                s -> s.managedLedgerStats.storageLogicalSize);
        writeMetric(stream, cluster, "pulsar_storage_backlog_size", stats, s -> s.managedLedgerStats.backlogSize);
        writeMetric(stream, cluster, "pulsar_storage_offloaded_size",
                stats, s -> s.managedLedgerStats.offloadedStorageUsed);

        writeMetricWithBrokerDefault(stream, cluster, "pulsar_storage_write_rate", stats,
                s -> s.managedLedgerStats.storageWriteRate);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_storage_read_rate", stats,
                s -> s.managedLedgerStats.storageReadRate);

        writeMetric(stream, cluster, "pulsar_subscription_delayed", stats, s -> s.msgDelayed);

        writeMsgBacklog(stream, cluster, stats, s -> s.msgBacklog);

        stats.forEach(s -> s.managedLedgerStats.storageWriteLatencyBuckets.refresh());
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_0_5", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[0]);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_1", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[1]);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_5", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[2]);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_10", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[3]);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_20", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[4]);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_50", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[5]);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_100", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[6]);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_200", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[7]);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_1000", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[8]);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_overflow", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[9]);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_count",
                stats, s -> s.managedLedgerStats.storageWriteLatencyBuckets.getCount());
        writeMetric(stream, cluster, "pulsar_storage_write_latency_sum",
                stats, s -> s.managedLedgerStats.storageWriteLatencyBuckets.getSum());

        stats.forEach(s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.refresh());
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_0_5", stats,
                s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[0]);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_1", stats,
                s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[1]);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_5", stats,
                s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[2]);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_10", stats,
                s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[3]);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_20", stats,
                s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[4]);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_50", stats,
                s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[5]);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_100", stats,
                s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[6]);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_200", stats,
                s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[7]);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_1000",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[8]);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_overflow",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[9]);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_count",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount());
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_sum",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum());

        stats.forEach(s -> s.managedLedgerStats.entrySizeBuckets.refresh());
        writeMetric(stream, cluster, "pulsar_entry_size_le_128", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[0]);
        writeMetric(stream, cluster, "pulsar_entry_size_le_512", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[1]);
        writeMetric(stream, cluster, "pulsar_entry_size_le_1_kb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[2]);
        writeMetric(stream, cluster, "pulsar_entry_size_le_2_kb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[3]);
        writeMetric(stream, cluster, "pulsar_entry_size_le_4_kb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[4]);
        writeMetric(stream, cluster, "pulsar_entry_size_le_16_kb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[5]);
        writeMetric(stream, cluster, "pulsar_entry_size_le_100_kb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[6]);
        writeMetric(stream, cluster, "pulsar_entry_size_le_1_mb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[7]);
        writeMetric(stream, cluster, "pulsar_entry_size_le_overflow", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[8]);
        writeMetric(stream, cluster, "pulsar_entry_size_count",
                stats, s -> s.managedLedgerStats.entrySizeBuckets.getCount());
        writeMetric(stream, cluster, "pulsar_entry_size_sum",
                stats, s -> s.managedLedgerStats.entrySizeBuckets.getSum());

        writeReplicationStat(stream, cluster, "pulsar_replication_rate_in", stats,
                replStats -> replStats.msgRateIn);
        writeReplicationStat(stream, cluster, "pulsar_replication_rate_out", stats,
                replStats -> replStats.msgRateOut);
        writeReplicationStat(stream, cluster, "pulsar_replication_throughput_in", stats,
                replStats -> replStats.msgThroughputIn);
        writeReplicationStat(stream, cluster, "pulsar_replication_throughput_out", stats,
                replStats -> replStats.msgThroughputOut);
        writeReplicationStat(stream, cluster, "pulsar_replication_backlog", stats,
                replStats -> replStats.replicationBacklog);
        writeReplicationStat(stream, cluster, "pulsar_replication_connected_count", stats,
                replStats -> replStats.connectedCount);
        writeReplicationStat(stream, cluster, "pulsar_replication_rate_expired", stats,
                replStats -> replStats.msgRateExpired);
        writeReplicationStat(stream, cluster, "pulsar_replication_delay_in_seconds", stats,
                replStats -> replStats.replicationDelayInSeconds);
    }

    private static void writeMetricWithBrokerDefault(SimpleTextOutputStream stream, String cluster, String name,
                                                     List<AggregatedNamespaceStats> allNamespaceStats,
                                                     Function<AggregatedNamespaceStats, Number> namespaceFunction) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        stream.write(name)
                .write("{cluster=\"").write(cluster).write("\"} ")
                .write(0).write(' ').write(System.currentTimeMillis())
                .write('\n');
        writeNamespaceStats(stream, cluster, name, allNamespaceStats, namespaceFunction);
    }

    private static void writeMetric(SimpleTextOutputStream stream, String cluster, String name,
                                    List<AggregatedNamespaceStats> allNamespaceStats,
                                    Function<AggregatedNamespaceStats, Number> namespaceFunction) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        writeNamespaceStats(stream, cluster, name, allNamespaceStats, namespaceFunction);
    }

    private static void writeNamespaceStats(SimpleTextOutputStream stream, String cluster, String name,
                                            List<AggregatedNamespaceStats> allNamespaceStats,
                                            Function<AggregatedNamespaceStats, Number> namespaceFunction) {
        allNamespaceStats.forEach(n -> {
            Number value = namespaceFunction.apply(n);
            if (value != null) {
                stream.write(name)
                        .write("{cluster=\"").write(cluster)
                        .write("\",namespace=\"").write(n.name)
                        .write("\"} ")
                        .write(value).write(' ').write(System.currentTimeMillis())
                        .write('\n');
            }
        });
    }

    private static void writeMsgBacklog(SimpleTextOutputStream stream, String cluster,
                                        List<AggregatedNamespaceStats> allNamespaceStats,
                                        Function<AggregatedNamespaceStats, Number> namespaceFunction) {
        stream.write("# TYPE ").write("pulsar_msg_backlog").write(" gauge\n");
        stream.write("pulsar_msg_backlog")
                .write("{cluster=\"").write(cluster).write("\"} ")
                .write(0).write(' ').write(System.currentTimeMillis())
                .write('\n');
        allNamespaceStats.forEach(n -> {
            Number value = namespaceFunction.apply(n);
            if (value != null) {
                stream.write("pulsar_msg_backlog")
                        .write("{cluster=\"").write(cluster)
                        .write("\",namespace=\"").write(n.name)
                        .write("\",remote_cluster=\"").write("local")
                        .write("\"} ")
                        .write(value).write(' ').write(System.currentTimeMillis())
                        .write('\n');
            }
        });
    }

    private static void writeReplicationStat(SimpleTextOutputStream stream, String cluster, String name,
                                             List<AggregatedNamespaceStats> allNamespaceStats,
                                             Function<AggregatedReplicationStats, Number> replStatsFunction) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        allNamespaceStats.forEach(n -> {
            if (!n.replicationStats.isEmpty()) {
                n.replicationStats.forEach((remoteCluster, replStats) ->
                        stream.write(name)
                                .write("{cluster=\"").write(cluster)
                                .write("\",namespace=\"").write(n.name)
                                .write("\",remote_cluster=\"").write(remoteCluster)
                                .write("\"} ")
                                .write(replStatsFunction.apply(replStats)).write(' ').write(System.currentTimeMillis())
                                .write('\n')
                );
            }
        });
    }

}
