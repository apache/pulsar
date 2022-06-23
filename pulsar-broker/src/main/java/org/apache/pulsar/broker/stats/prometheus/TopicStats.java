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

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.compaction.CompactionRecord;
import org.apache.pulsar.compaction.CompactorMXBean;

class TopicStats {
    String namespace;
    String name;

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

    public static void printTopicStats(SimpleTextOutputStream stream, String cluster, List<TopicStats> stats,
                                       Optional<CompactorMXBean> compactorMXBean,
                                       boolean splitTopicAndPartitionIndexLabel) {
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_subscriptions_count", stats, s -> s.subscriptionsCount,
                splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_producers_count", stats, s -> s.producersCount,
                splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_consumers_count", stats, s -> s.consumersCount,
                splitTopicAndPartitionIndexLabel);

        writeMetricWithBrokerDefault(stream, cluster, "pulsar_rate_in", stats, s -> s.rateIn,
                splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_rate_out", stats, s -> s.rateOut,
                splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_throughput_in", stats, s -> s.throughputIn,
                splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_throughput_out", stats, s -> s.throughputOut,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_average_msg_size", stats, s -> s.averageMsgSize,
                splitTopicAndPartitionIndexLabel);

        writeMetricWithBrokerDefault(stream, cluster, "pulsar_storage_size", stats,
                s -> s.managedLedgerStats.storageSize,
                splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_storage_logical_size",
                stats, s -> s.managedLedgerStats.storageLogicalSize, splitTopicAndPartitionIndexLabel);
        writeMetricWithBrokerDefault(stream, cluster, "pulsar_msg_backlog", stats, s -> s.msgBacklog,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_backlog_size",
                stats, s -> s.managedLedgerStats.backlogSize, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_rate", stats, s -> s.managedLedgerStats.storageWriteRate,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_read_rate", stats, s -> s.managedLedgerStats.storageReadRate,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_publish_rate_limit_times", stats, s -> s.publishRateLimitedTimes,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_offloaded_size", stats, s -> s.managedLedgerStats
                .offloadedStorageUsed, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_backlog_quota_limit", stats, s -> s.backlogQuotaLimit,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_backlog_quota_limit_time",
                stats, s -> s.backlogQuotaLimitTime, splitTopicAndPartitionIndexLabel);

        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_0_5", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[0],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_1", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[1],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_5", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[2],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_10", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[3],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_20", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[4],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_50", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[5],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_100", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[6],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_200", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[7],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_le_1000", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[8],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_overflow", stats,
                s -> s.managedLedgerStats.storageWriteLatencyBuckets.getBuckets()[9],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_count",
                stats, s -> s.managedLedgerStats.storageWriteLatencyBuckets.getCount(),
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_write_latency_sum",
                stats, s -> s.managedLedgerStats.storageWriteLatencyBuckets.getSum(), splitTopicAndPartitionIndexLabel);

        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_0_5",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[0],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_1",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[1],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_5",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[2],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_10",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[3],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_20",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[4],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_50",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[5],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_100",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[6],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_200",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[7],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_le_1000",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[8],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_overflow",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets()[9],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_count",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount(),
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_storage_ledger_write_latency_sum",
                stats, s -> s.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum(),
                splitTopicAndPartitionIndexLabel);

        writeMetric(stream, cluster, "pulsar_entry_size_le_128", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[0],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_entry_size_le_512", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[1],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_entry_size_le_1_kb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[2],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_entry_size_le_2_kb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[3],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_entry_size_le_4_kb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[4],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_entry_size_le_16_kb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[5],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_entry_size_le_100_kb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[6],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_entry_size_le_1_mb", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[7],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_entry_size_le_overflow", stats,
                s -> s.managedLedgerStats.entrySizeBuckets.getBuckets()[8],
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_entry_size_count",
                stats, s -> s.managedLedgerStats.entrySizeBuckets.getCount(), splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_entry_size_sum",
                stats, s -> s.managedLedgerStats.entrySizeBuckets.getSum(), splitTopicAndPartitionIndexLabel);

        writeProducerStat(stream, cluster, "pulsar_producer_msg_rate_in", stats,
                p -> p.msgRateIn, splitTopicAndPartitionIndexLabel);
        writeProducerStat(stream, cluster, "pulsar_producer_msg_throughput_in", stats,
                p -> p.msgThroughputIn, splitTopicAndPartitionIndexLabel);
        writeProducerStat(stream, cluster, "pulsar_producer_msg_average_Size", stats,
                p -> p.averageMsgSize, splitTopicAndPartitionIndexLabel);


        writeSubscriptionStat(stream, cluster, "pulsar_subscription_back_log", stats,
                s -> s.msgBacklog, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_back_log_no_delayed",
                stats, s -> s.msgBacklogNoDelayed, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_delayed",
                stats, s -> s.msgDelayed, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_msg_rate_redeliver",
                stats, s -> s.msgRateRedeliver, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_unacked_messages",
                stats, s -> s.unackedMessages, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_blocked_on_unacked_messages",
                stats, s -> s.blockedSubscriptionOnUnackedMsgs ? 1 : 0, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_msg_rate_out",
                stats, s -> s.msgRateOut, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_msg_ack_rate",
                stats, s -> s.messageAckRate, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_msg_throughput_out",
                stats, s -> s.msgThroughputOut, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_out_bytes_total",
                stats, s -> s.bytesOutCounter, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_out_messages_total",
                stats, s -> s.msgOutCounter, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_last_expire_timestamp",
                stats, s -> s.lastExpireTimestamp, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_last_acked_timestamp",
                stats, s -> s.lastAckedTimestamp, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_last_consumed_flow_timestamp",
                stats, s -> s.lastConsumedFlowTimestamp, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_last_consumed_timestamp",
                stats, s -> s.lastConsumedTimestamp, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_last_mark_delete_advanced_timestamp",
                stats, s -> s.lastMarkDeleteAdvancedTimestamp, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_msg_rate_expired",
                stats, s -> s.msgRateExpired, splitTopicAndPartitionIndexLabel);
        writeSubscriptionStat(stream, cluster, "pulsar_subscription_total_msg_expired",
                stats, s -> s.totalMsgExpired, splitTopicAndPartitionIndexLabel);

        writeConsumerStat(stream, cluster, "pulsar_consumer_msg_rate_redeliver", stats, c -> c.msgRateRedeliver,
                splitTopicAndPartitionIndexLabel);
        writeConsumerStat(stream, cluster, "pulsar_consumer_unacked_messages", stats, c -> c.unackedMessages,
                splitTopicAndPartitionIndexLabel);
        writeConsumerStat(stream, cluster, "pulsar_consumer_blocked_on_unacked_messages",
                stats, c -> c.blockedSubscriptionOnUnackedMsgs ? 1 : 0,
                splitTopicAndPartitionIndexLabel);
        writeConsumerStat(stream, cluster, "pulsar_consumer_msg_rate_out", stats, c -> c.msgRateOut,
                splitTopicAndPartitionIndexLabel);

        writeConsumerStat(stream, cluster, "pulsar_consumer_msg_ack_rate", stats, c -> c.msgAckRate,
                splitTopicAndPartitionIndexLabel);

        writeConsumerStat(stream, cluster, "pulsar_consumer_msg_throughput_out", stats, c -> c.msgThroughputOut,
                splitTopicAndPartitionIndexLabel);
        writeConsumerStat(stream, cluster, "pulsar_consumer_available_permits", stats, c -> c.availablePermits,
                splitTopicAndPartitionIndexLabel);
        writeConsumerStat(stream, cluster, "pulsar_out_bytes_total", stats, c -> c.bytesOutCounter,
                splitTopicAndPartitionIndexLabel);
        writeConsumerStat(stream, cluster, "pulsar_out_messages_total", stats, c -> c.msgOutCounter,
                splitTopicAndPartitionIndexLabel);

        writeReplicationStat(stream, cluster, "pulsar_replication_rate_in", stats, r -> r.msgRateIn,
                splitTopicAndPartitionIndexLabel);
        writeReplicationStat(stream, cluster, "pulsar_replication_rate_out", stats, r -> r.msgRateOut,
                splitTopicAndPartitionIndexLabel);
        writeReplicationStat(stream, cluster, "pulsar_replication_throughput_in", stats, r -> r.msgThroughputIn,
                splitTopicAndPartitionIndexLabel);
        writeReplicationStat(stream, cluster, "pulsar_replication_throughput_out", stats, r -> r.msgThroughputOut,
                splitTopicAndPartitionIndexLabel);
        writeReplicationStat(stream, cluster, "pulsar_replication_backlog", stats, r -> r.replicationBacklog,
                splitTopicAndPartitionIndexLabel);
        writeReplicationStat(stream, cluster, "pulsar_replication_connected_count", stats, r -> r.connectedCount,
                splitTopicAndPartitionIndexLabel);
        writeReplicationStat(stream, cluster, "pulsar_replication_rate_expired", stats, r -> r.msgRateExpired,
                splitTopicAndPartitionIndexLabel);
        writeReplicationStat(stream, cluster, "pulsar_replication_delay_in_seconds", stats,
                r -> r.replicationDelayInSeconds, splitTopicAndPartitionIndexLabel);

        writeMetric(stream, cluster, "pulsar_in_bytes_total", stats, s -> s.bytesInCounter,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, cluster, "pulsar_in_messages_total", stats, s -> s.msgInCounter,
                splitTopicAndPartitionIndexLabel);

        // Compaction

        writeCompactionStat(stream, cluster, "pulsar_compaction_removed_event_count",
                stats, s -> s.compactionRemovedEventCount, compactorMXBean, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_succeed_count",
                stats, s -> s.compactionSucceedCount, compactorMXBean, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_failed_count",
                stats, s -> s.compactionFailedCount, compactorMXBean, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_duration_time_in_mills",
                stats, s -> s.compactionDurationTimeInMills, compactorMXBean, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_read_throughput",
                stats, s -> s.compactionReadThroughput, compactorMXBean, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_write_throughput",
                stats, s -> s.compactionWriteThroughput, compactorMXBean, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_compacted_entries_count",
                stats, s -> s.compactionCompactedEntriesCount, compactorMXBean, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_compacted_entries_size",
                stats, s -> s.compactionCompactedEntriesSize, compactorMXBean, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_le_0_5",
                stats, s -> s.compactionLatencyBuckets.getBuckets()[0], compactorMXBean,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_le_1",
                stats, s -> s.compactionLatencyBuckets.getBuckets()[1], compactorMXBean,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_le_5",
                stats, s -> s.compactionLatencyBuckets.getBuckets()[2], compactorMXBean,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_le_10",
                stats, s -> s.compactionLatencyBuckets.getBuckets()[3], compactorMXBean,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_le_20",
                stats, s -> s.compactionLatencyBuckets.getBuckets()[4], compactorMXBean,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_le_50",
                stats, s -> s.compactionLatencyBuckets.getBuckets()[5], compactorMXBean,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_le_100",
                stats, s -> s.compactionLatencyBuckets.getBuckets()[6], compactorMXBean,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_le_200",
                stats, s -> s.compactionLatencyBuckets.getBuckets()[7], compactorMXBean,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_le_1000",
                stats, s -> s.compactionLatencyBuckets.getBuckets()[8], compactorMXBean,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_overflow",
                stats, s -> s.compactionLatencyBuckets.getBuckets()[9], compactorMXBean,
                splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_sum",
                stats, s -> s.compactionLatencyBuckets.getSum(), compactorMXBean, splitTopicAndPartitionIndexLabel);
        writeCompactionStat(stream, cluster, "pulsar_compaction_latency_count",
                stats, s -> s.compactionLatencyBuckets.getCount(), compactorMXBean, splitTopicAndPartitionIndexLabel);
    }

    private static void writeMetricWithBrokerDefault(SimpleTextOutputStream stream, String cluster, String name,
                                                     List<TopicStats> allTopicStats,
                                                     Function<TopicStats, Number> topicFunction,
                                                     boolean splitTopicAndPartitionIndexLabel) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        stream.write(name)
                .write("{cluster=\"").write(cluster).write("\"} ")
                .write(0).write(' ').write(System.currentTimeMillis())
                .write('\n');
        writeTopicStats(stream, cluster, name, allTopicStats, topicFunction, splitTopicAndPartitionIndexLabel);
    }

    private static void writeMetric(SimpleTextOutputStream stream, String cluster, String name,
                                    List<TopicStats> allTopicStats,
                                    Function<TopicStats, Number> topicFunction,
                                    boolean splitTopicAndPartitionIndexLabel) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        writeTopicStats(stream, cluster, name, allTopicStats, topicFunction, splitTopicAndPartitionIndexLabel);
    }

    private static void writeTopicStats(SimpleTextOutputStream stream, String cluster, String name,
                                        List<TopicStats> allTopicStats, Function<TopicStats, Number> valueFunction,
                                        boolean splitTopicAndPartitionIndexLabel) {
        allTopicStats.forEach(t -> {
            writeCommonLabels(stream, cluster, t.namespace, t.name, name, splitTopicAndPartitionIndexLabel);
            stream.write("\"} ")
                    .write(valueFunction.apply(t)).write(' ')
                    .write(System.currentTimeMillis())
                    .write('\n');
        });
    }

    private static void writeProducerStat(SimpleTextOutputStream stream, String cluster, String name,
                                          List<TopicStats> allTopicStats,
                                          Function<AggregatedProducerStats, Number> valueFunction,
                                          boolean splitTopicAndPartitionIndexLabel) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        allTopicStats.forEach(t ->
                t.producerStats.forEach((p, producerStats) -> {
                    writeCommonLabels(stream, cluster, t.namespace, t.name, name,
                            splitTopicAndPartitionIndexLabel);
                    stream.write("\",producer_name=\"").write(p)
                            .write("\",producer_id=\"").write(String.valueOf(producerStats.producerId))
                            .write("\"} ")
                            .write(valueFunction.apply(producerStats)).write(' ').write(System.currentTimeMillis())
                            .write('\n');
                }));
    }

    private static void writeSubscriptionStat(SimpleTextOutputStream stream, String cluster, String name,
                                              List<TopicStats> allTopicStats,
                                              Function<AggregatedSubscriptionStats, Number> valueFunction,
                                              boolean splitTopicAndPartitionIndexLabel) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        allTopicStats.forEach(t ->
                t.subscriptionStats.forEach((s, subStats) -> {
                    writeCommonLabels(stream, cluster, t.namespace, t.name, name,
                            splitTopicAndPartitionIndexLabel);
                    stream.write("\",subscription=\"").write(s)
                            .write("\"} ")
                            .write(valueFunction.apply(subStats)).write(' ').write(System.currentTimeMillis())
                            .write('\n');
                }));
    }

    private static void writeConsumerStat(SimpleTextOutputStream stream, String cluster, String name,
                                          List<TopicStats> allTopicStats,
                                          Function<AggregatedConsumerStats, Number> valueFunction,
                                          boolean splitTopicAndPartitionIndexLabel) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        allTopicStats.forEach(t ->
                t.subscriptionStats.forEach((s, subStats) ->
                        subStats.consumerStat.forEach((c, conStats) -> {
                            writeCommonLabels(stream, cluster, t.namespace, t.name, name,
                                    splitTopicAndPartitionIndexLabel);
                            stream.write("\",subscription=\"").write(s)
                                    .write("\",consumer_name=\"").write(c.consumerName())
                                    .write("\",consumer_id=\"").write(String.valueOf(c.consumerId()))
                                    .write("\"} ")
                                    .write(valueFunction.apply(conStats)).write(' ').write(System.currentTimeMillis())
                                    .write('\n');
                        })));
    }


    private static void writeReplicationStat(SimpleTextOutputStream stream, String cluster, String name,
                                             List<TopicStats> allTopicStats,
                                             Function<AggregatedReplicationStats, Number> replStatsFunction,
                                             boolean splitTopicAndPartitionIndexLabel) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        allTopicStats.forEach(t -> {
            if (!t.replicationStats.isEmpty()) {
                t.replicationStats.forEach((remoteCluster, replStats) -> {
                            stream.write(name)
                                    .write("{cluster=\"").write(cluster)
                                    .write("\",namespace=\"").write(t.namespace);
                            if (splitTopicAndPartitionIndexLabel) {
                                int index = t.name.indexOf(PARTITIONED_TOPIC_SUFFIX);
                                if (index > 0) {
                                    stream.write("\",topic=\"").write(t.name.substring(0, index));
                                    stream.write("\",partition=\"")
                                            .write(t.name.substring(
                                                    index + PARTITIONED_TOPIC_SUFFIX.length()));
                                } else {
                                    stream.write("\",topic=\"").write(t.name);
                                    stream.write("\",partition=\"").write("-1");
                                }
                            } else {
                                stream.write("\",topic=\"").write(t.name);
                            }
                            stream.write("\",remote_cluster=\"").write(remoteCluster)
                                    .write("\"} ")
                                    .write(replStatsFunction.apply(replStats)).write(' ')
                                    .write(System.currentTimeMillis())
                                    .write('\n');
                        }
                );
            }
        });
    }

    private static void writeCompactionStat(SimpleTextOutputStream stream, String cluster, String name,
                                            List<TopicStats> allTopicStats,
                                            Function<TopicStats, Number> valueFunction,
                                            Optional<CompactorMXBean> compactorMXBean,
                                            boolean splitTopicAndPartitionIndexLabel) {
        stream.write("# TYPE ").write(name).write(" gauge\n");
        allTopicStats.forEach(t -> {
                    boolean hasCompaction =
                            compactorMXBean.flatMap(mxBean -> mxBean.getCompactionRecordForTopic(t.name))
                                    .isPresent();
                    if (hasCompaction) {
                        writeCommonLabels(stream, cluster, t.namespace, t.name, name,
                                splitTopicAndPartitionIndexLabel);
                        stream.write("\"} ")
                                .write(valueFunction.apply(t)).write(' ')
                                .write(System.currentTimeMillis())
                                .write('\n');
                    }
                }
        );
    }

    private static void writeCommonLabels(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                                          String metricName, boolean splitTopicAndPartitionIndexLabel) {
        stream.write(metricName)
                .write("{cluster=\"").write(cluster)
                .write("\",namespace=\"").write(namespace);
        if (splitTopicAndPartitionIndexLabel) {
            int index = topic.indexOf(PARTITIONED_TOPIC_SUFFIX);
            if (index > 0) {
                stream.write("\",topic=\"").write(topic.substring(0, index));
                stream.write("\",partition=\"")
                        .write(topic.substring(index + PARTITIONED_TOPIC_SUFFIX.length()));
            } else {
                stream.write("\",topic=\"").write(topic);
                stream.write("\",partition=\"").write("-1");
            }
        } else {
            stream.write("\",topic=\"").write(topic);
        }
    }
}
