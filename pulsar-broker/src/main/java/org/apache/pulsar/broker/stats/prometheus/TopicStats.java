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

import static org.apache.pulsar.broker.stats.prometheus.NamespaceStatsAggregator.writeGaugeType;
import static org.apache.pulsar.broker.stats.prometheus.NamespaceStatsAggregator.writeGaugeTypeWithBrokerDefault;
import static org.apache.pulsar.broker.stats.prometheus.NamespaceStatsAggregator.writeSample;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.compaction.CompactionRecord;
import org.apache.pulsar.compaction.CompactorMXBean;

class TopicStats {
    int subscriptionsCount;
    int producersCount;
    int consumersCount;
    double rateIn;
    double rateOut;
    double throughputIn;
    double throughputOut;
    long msgInCounter;
    long bytesInCounter;
    long msgOutCounter;
    long bytesOutCounter;
    double averageMsgSize;

    public long msgBacklog;

    long publishRateLimitedTimes;

    long backlogQuotaLimit;
    long backlogQuotaLimitTime;

    ManagedLedgerStats managedLedgerStats = new ManagedLedgerStats();

    Map<String, AggregatedReplicationStats> replicationStats = new HashMap<>();
    Map<String, AggregatedSubscriptionStats> subscriptionStats = new HashMap<>();
    Map<String, AggregatedProducerStats> producerStats = new HashMap<>();

    // For compaction
    long compactionRemovedEventCount;
    long compactionSucceedCount;
    long compactionFailedCount;
    long compactionDurationTimeInMills;
    double compactionReadThroughput;
    double compactionWriteThroughput;
    long compactionCompactedEntriesCount;
    long compactionCompactedEntriesSize;
    StatsBuckets compactionLatencyBuckets = new StatsBuckets(CompactionRecord.WRITE_LATENCY_BUCKETS_USEC);

    public void reset() {
        subscriptionsCount = 0;
        producersCount = 0;
        consumersCount = 0;
        rateIn = 0;
        rateOut = 0;
        throughputIn = 0;
        throughputOut = 0;
        bytesInCounter = 0;
        msgInCounter = 0;
        bytesOutCounter = 0;
        msgOutCounter = 0;

        managedLedgerStats.reset();
        msgBacklog = 0;
        publishRateLimitedTimes = 0L;
        backlogQuotaLimit = 0;
        backlogQuotaLimitTime = -1;

        replicationStats.clear();
        subscriptionStats.clear();
        producerStats.clear();

        compactionRemovedEventCount = 0;
        compactionSucceedCount = 0;
        compactionFailedCount = 0;
        compactionDurationTimeInMills = 0;
        compactionReadThroughput = 0;
        compactionWriteThroughput = 0;
        compactionCompactedEntriesCount = 0;
        compactionCompactedEntriesSize = 0;
        compactionLatencyBuckets.reset();
    }

    public static void printTopicStats(Map<String, ByteBuf> allMetrics, TopicStats stats,
                                       Optional<CompactorMXBean> compactorMXBean, String cluster, String namespace,
                                       String name, boolean splitTopicAndPartitionIndexLabel) {
        writeMetricWithBrokerDefault(allMetrics, "pulsar_subscriptions_count", stats.subscriptionsCount,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_producers_count", stats.producersCount,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_consumers_count", stats.consumersCount,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);

        writeMetricWithBrokerDefault(allMetrics, "pulsar_rate_in", stats.rateIn,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_rate_out", stats.rateOut,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_throughput_in", stats.throughputIn,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_throughput_out", stats.throughputOut,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_average_msg_size", stats.averageMsgSize,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);

        writeMetricWithBrokerDefault(allMetrics, "pulsar_storage_size", stats.managedLedgerStats.storageSize,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_storage_logical_size",
                stats.managedLedgerStats.storageLogicalSize, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(allMetrics, "pulsar_msg_backlog", stats.msgBacklog,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_backlog_size", stats.managedLedgerStats.backlogSize,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_publish_rate_limit_times", stats.publishRateLimitedTimes,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_offloaded_size", stats.managedLedgerStats
                .offloadedStorageUsed, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_backlog_quota_limit", stats.backlogQuotaLimit,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_backlog_quota_limit_time", stats.backlogQuotaLimitTime,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);

        writeMetric(allMetrics, "pulsar_storage_write_latency_le_0_5",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[0],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_1",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[1],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_5",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[2],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_10",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[3],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_20",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[4],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_50",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[5],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_100",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[6],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_200",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[7],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_le_1000",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[8],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_overflow",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[9],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_count",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getCount(),
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_write_latency_sum",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getSum(), cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);

        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_0_5",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[0],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_1",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[1],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_5",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[2],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_10",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[3],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_20",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[4],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_50",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[5],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_100",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[6],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_200",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[7],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_le_1000",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[8],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_overflow",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[9],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_count",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount(),
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_storage_ledger_write_latency_sum",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum(),
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);

        writeMetric(allMetrics, "pulsar_entry_size_le_128",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[0],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_entry_size_le_512",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[1],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_entry_size_le_1_kb",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[2],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_entry_size_le_2_kb",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[3],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_entry_size_le_4_kb",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[4],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_entry_size_le_16_kb",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[5],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_entry_size_le_100_kb",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[6],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_entry_size_le_1_mb",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[7],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_entry_size_le_overflow",
                stats.managedLedgerStats.entrySizeBuckets.getBuckets()[8],
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_entry_size_count", stats.managedLedgerStats.entrySizeBuckets.getCount(),
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_entry_size_sum", stats.managedLedgerStats.entrySizeBuckets.getSum(),
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);

        writeProducerStat(allMetrics, "pulsar_producer_msg_rate_in", stats,
                p -> p.msgRateIn, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeProducerStat(allMetrics, "pulsar_producer_msg_throughput_in", stats,
                p -> p.msgThroughputIn, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeProducerStat(allMetrics, "pulsar_producer_msg_average_Size", stats,
                p -> p.averageMsgSize, cluster, namespace, name, splitTopicAndPartitionIndexLabel);


        writeSubscriptionStat(allMetrics, "pulsar_subscription_back_log", stats, s -> s.msgBacklog,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_back_log_no_delayed",
                stats, s -> s.msgBacklogNoDelayed, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_delayed",
                stats, s -> s.msgDelayed, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_msg_rate_redeliver",
                stats, s -> s.msgRateRedeliver, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_unacked_messages",
                stats, s -> s.unackedMessages, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_blocked_on_unacked_messages",
                stats, s -> s.blockedSubscriptionOnUnackedMsgs ? 1 : 0, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_msg_rate_out",
                stats, s -> s.msgRateOut, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_msg_ack_rate",
                stats, s -> s.messageAckRate, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_msg_throughput_out",
                stats, s -> s.msgThroughputOut, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_out_bytes_total",
                stats, s -> s.bytesOutCounter, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_out_messages_total",
                stats, s -> s.msgOutCounter, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_last_expire_timestamp",
                stats, s -> s.lastExpireTimestamp, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_last_acked_timestamp",
                stats, s -> s.lastAckedTimestamp, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_last_consumed_flow_timestamp",
                stats, s -> s.lastConsumedFlowTimestamp, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_last_consumed_timestamp",
                stats, s -> s.lastConsumedTimestamp, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_last_mark_delete_advanced_timestamp",
                stats, s -> s.lastMarkDeleteAdvancedTimestamp, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_msg_rate_expired",
                stats, s -> s.msgRateExpired, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(allMetrics, "pulsar_subscription_total_msg_expired",
                stats, s -> s.totalMsgExpired, cluster, namespace, name, splitTopicAndPartitionIndexLabel);

        writeConsumerStat(allMetrics, "pulsar_consumer_msg_rate_redeliver", stats, c -> c.msgRateRedeliver,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeConsumerStat(allMetrics, "pulsar_consumer_unacked_messages", stats, c -> c.unackedMessages,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeConsumerStat(allMetrics, "pulsar_consumer_blocked_on_unacked_messages",
                stats, c -> c.blockedSubscriptionOnUnackedMsgs ? 1 : 0,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeConsumerStat(allMetrics, "pulsar_consumer_msg_rate_out", stats, c -> c.msgRateOut,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);

        writeConsumerStat(allMetrics, "pulsar_consumer_msg_ack_rate", stats, c -> c.msgAckRate,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);

        writeConsumerStat(allMetrics, "pulsar_consumer_msg_throughput_out", stats, c -> c.msgThroughputOut,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeConsumerStat(allMetrics, "pulsar_consumer_available_permits", stats, c -> c.availablePermits,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeConsumerStat(allMetrics, "pulsar_out_bytes_total", stats, c -> c.bytesOutCounter,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeConsumerStat(allMetrics, "pulsar_out_messages_total", stats, c -> c.msgOutCounter,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);

        writeReplicationStat(allMetrics, "pulsar_replication_rate_in", stats, r -> r.msgRateIn,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeReplicationStat(allMetrics, "pulsar_replication_rate_out", stats, r -> r.msgRateOut,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeReplicationStat(allMetrics, "pulsar_replication_throughput_in", stats, r -> r.msgThroughputIn,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeReplicationStat(allMetrics, "pulsar_replication_throughput_out", stats, r -> r.msgThroughputOut,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeReplicationStat(allMetrics, "pulsar_replication_backlog", stats, r -> r.replicationBacklog,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeReplicationStat(allMetrics, "pulsar_replication_connected_count", stats, r -> r.connectedCount,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeReplicationStat(allMetrics, "pulsar_replication_rate_expired", stats, r -> r.msgRateExpired,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeReplicationStat(allMetrics, "pulsar_replication_delay_in_seconds", stats,
                r -> r.replicationDelayInSeconds, cluster, namespace, name, splitTopicAndPartitionIndexLabel);

        writeMetric(allMetrics, "pulsar_in_bytes_total", stats.bytesInCounter, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeMetric(allMetrics, "pulsar_in_messages_total", stats.msgInCounter, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);

        // Compaction

        writeCompactionStat(allMetrics, "pulsar_compaction_removed_event_count", stats.compactionRemovedEventCount,
                compactorMXBean, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_succeed_count", stats.compactionSucceedCount,
                compactorMXBean, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_failed_count", stats.compactionFailedCount, compactorMXBean,
                cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_duration_time_in_mills", stats.compactionDurationTimeInMills,
                compactorMXBean, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_read_throughput", stats.compactionReadThroughput,
                compactorMXBean, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_write_throughput", stats.compactionWriteThroughput,
                compactorMXBean, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_compacted_entries_count",
                stats.compactionCompactedEntriesCount, compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_compacted_entries_size",
                stats.compactionCompactedEntriesSize, compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_le_0_5",
                stats.compactionLatencyBuckets.getBuckets()[0], compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_le_1",
                stats.compactionLatencyBuckets.getBuckets()[1], compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_le_5",
                stats.compactionLatencyBuckets.getBuckets()[2], compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_le_10",
                stats.compactionLatencyBuckets.getBuckets()[3], compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_le_20",
                stats.compactionLatencyBuckets.getBuckets()[4], compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_le_50",
                stats.compactionLatencyBuckets.getBuckets()[5], compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_le_100",
                stats.compactionLatencyBuckets.getBuckets()[6], compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_le_200",
                stats.compactionLatencyBuckets.getBuckets()[7], compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_le_1000",
                stats.compactionLatencyBuckets.getBuckets()[8], compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_overflow",
                stats.compactionLatencyBuckets.getBuckets()[9], compactorMXBean, cluster, namespace, name,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_sum", stats.compactionLatencyBuckets.getSum(),
                compactorMXBean, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(allMetrics, "pulsar_compaction_latency_count", stats.compactionLatencyBuckets.getCount(),
                compactorMXBean, cluster, namespace, name, splitTopicAndPartitionIndexLabel);
    }

    private static void writeMetric(Map<String, ByteBuf> allMetrics, String metricName, Number value, String cluster,
                                    String namespace, String topic, boolean splitTopicAndPartitionIndexLabel) {
        ByteBuf buffer = writeGaugeType(allMetrics, metricName);
        writeTopicSample(buffer, metricName, value, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
    }

    private static void writeProducerStat(Map<String, ByteBuf> allMetrics, String metricName, TopicStats topicStats,
                                          Function<AggregatedProducerStats, Number> valueFunction,
                                          String cluster, String namespace, String topic,
                                          boolean splitTopicAndPartitionIndexLabel) {
        ByteBuf buffer = writeGaugeType(allMetrics, metricName);
        topicStats.producerStats.forEach((p, producerStats) ->
                writeTopicSample(buffer, metricName, valueFunction.apply(producerStats), cluster, namespace, topic,
                        splitTopicAndPartitionIndexLabel, "producer_name", p, "producer_id",
                        String.valueOf(producerStats.producerId)));
    }

    private static void writeSubscriptionStat(Map<String, ByteBuf> allMetrics, String metricName, TopicStats topicStats,
                                              Function<AggregatedSubscriptionStats, Number> valueFunction,
                                              String cluster, String namespace, String topic,
                                              boolean splitTopicAndPartitionIndexLabel) {
        ByteBuf buffer = writeGaugeType(allMetrics, metricName);
        topicStats.subscriptionStats.forEach((s, subStats) ->
                writeTopicSample(buffer, metricName, valueFunction.apply(subStats), cluster, namespace, topic,
                        splitTopicAndPartitionIndexLabel, "subscription", s));
    }

    private static void writeConsumerStat(Map<String, ByteBuf> allMetrics, String metricName, TopicStats topicStats,
                                          Function<AggregatedConsumerStats, Number> valueFunction,
                                          String cluster, String namespace, String topic,
                                          boolean splitTopicAndPartitionIndexLabel) {
        ByteBuf buffer = writeGaugeType(allMetrics, metricName);
        topicStats.subscriptionStats.forEach((s, subStats) ->
                subStats.consumerStat.forEach((c, conStats) ->
                        writeTopicSample(buffer, metricName, valueFunction.apply(conStats), cluster, namespace, topic,
                                splitTopicAndPartitionIndexLabel, "subscription", s, "consumer_name", c.consumerName(),
                                "consumer_id", String.valueOf(c.consumerId()))
                ));
    }


    private static void writeReplicationStat(Map<String, ByteBuf> allMetrics, String metricName, TopicStats topicStats,
                                             Function<AggregatedReplicationStats, Number> valueFunction,
                                             String cluster, String namespace, String topic,
                                             boolean splitTopicAndPartitionIndexLabel) {
        if (!topicStats.replicationStats.isEmpty()) {
            ByteBuf buffer = writeGaugeType(allMetrics, metricName);
            topicStats.replicationStats.forEach((remoteCluster, replStats) ->
                    writeTopicSample(buffer, metricName, valueFunction.apply(replStats), cluster, namespace, topic,
                            splitTopicAndPartitionIndexLabel, "remote_cluster", remoteCluster)
            );
        }
    }

    private static void writeCompactionStat(Map<String, ByteBuf> allMetrics, String metricName,
                                            Number value, Optional<CompactorMXBean> compactorMXBean,
                                            String cluster, String namespace, String topic,
                                            boolean splitTopicAndPartitionIndexLabel) {
        boolean hasCompaction = compactorMXBean.flatMap(mxBean -> mxBean.getCompactionRecordForTopic(topic))
                .isPresent();
        if (hasCompaction) {
            ByteBuf buffer = writeGaugeType(allMetrics, metricName);
            writeTopicSample(buffer, metricName, value, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        }
    }

    static void writeMetricWithBrokerDefault(Map<String, ByteBuf> allMetrics, String metricName, Number value,
                                             String cluster, String namespace, String topic,
                                             boolean splitTopicAndPartitionIndexLabel) {
        ByteBuf buffer = writeGaugeTypeWithBrokerDefault(allMetrics, metricName, cluster);
        writeTopicSample(buffer, metricName, value, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
    }

    static void writeTopicSample(ByteBuf buffer, String metricName, Number value, String cluster,
                                 String namespace, String topic, boolean splitTopicAndPartitionIndexLabel,
                                 String... extraLabelsAndValues) {
        List<String> labelsAndValues = new ArrayList<>();
        labelsAndValues.add("cluster");
        labelsAndValues.add(cluster);
        labelsAndValues.add("namespace");
        labelsAndValues.add(namespace);
        labelsAndValues.add("topic");
        if (splitTopicAndPartitionIndexLabel) {
            int index = topic.indexOf(PARTITIONED_TOPIC_SUFFIX);
            if (index > 0) {
                labelsAndValues.add(topic.substring(0, index));
                labelsAndValues.add("partition");
                labelsAndValues.add(topic.substring(index + PARTITIONED_TOPIC_SUFFIX.length()));
            } else {
                labelsAndValues.add(topic);
                labelsAndValues.add("partition");
                labelsAndValues.add("-1");
            }
        } else {
            labelsAndValues.add(topic);
        }
        if (extraLabelsAndValues != null) {
            labelsAndValues.addAll(List.of(extraLabelsAndValues));
        }
        writeSample(buffer, metricName, value, labelsAndValues.toArray(new String[0]));
    }
}
