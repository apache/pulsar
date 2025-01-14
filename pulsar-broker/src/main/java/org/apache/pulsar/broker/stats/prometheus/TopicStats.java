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

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.stats.prometheus.metrics.PrometheusLabels;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
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

    long ongoingTxnCount;
    long abortedTxnCount;
    long committedTxnCount;

    public long msgBacklog;
    long publishRateLimitedTimes;

    long backlogQuotaLimit;
    long backlogQuotaLimitTime;
    long backlogAgeSeconds;

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
    public long delayedMessageIndexSizeInBytes;

    Map<String, TopicMetricBean> bucketDelayedIndexStats = new HashMap<>();

    public long sizeBasedBacklogQuotaExceededEvictionCount;
    public long timeBasedBacklogQuotaExceededEvictionCount;


    @SuppressWarnings("DuplicatedCode")
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

        ongoingTxnCount = 0;
        abortedTxnCount = 0;
        committedTxnCount = 0;

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
        delayedMessageIndexSizeInBytes = 0;
        bucketDelayedIndexStats.clear();

        timeBasedBacklogQuotaExceededEvictionCount = 0;
        sizeBasedBacklogQuotaExceededEvictionCount = 0;
        backlogAgeSeconds = -1;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static void printTopicStats(PrometheusMetricStreams stream, TopicStats stats,
                                       Optional<CompactorMXBean> compactorMXBean, String cluster, String namespace,
                                       String topic, boolean splitTopicAndPartitionIndexLabel) {
        writeMetric(stream, "pulsar_subscriptions_count", stats.subscriptionsCount,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_producers_count", stats.producersCount,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_consumers_count", stats.consumersCount,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);

        writeMetric(stream, "pulsar_rate_in", stats.rateIn,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_rate_out", stats.rateOut,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_throughput_in", stats.throughputIn,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_throughput_out", stats.throughputOut,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_average_msg_size", stats.averageMsgSize,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);

        writeMetric(stream, "pulsar_txn_tb_active_total", stats.ongoingTxnCount,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_txn_tb_aborted_total", stats.abortedTxnCount,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_txn_tb_committed_total", stats.committedTxnCount,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);

        writeMetric(stream, "pulsar_storage_size", stats.managedLedgerStats.storageSize,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_logical_size",
                stats.managedLedgerStats.storageLogicalSize, cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_msg_backlog", stats.msgBacklog,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_rate", stats.managedLedgerStats.storageWriteRate,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_read_rate", stats.managedLedgerStats.storageReadRate,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_read_cache_misses_rate",
                stats.managedLedgerStats.storageReadCacheMissesRate,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_backlog_size", stats.managedLedgerStats.backlogSize,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_publish_rate_limit_times", stats.publishRateLimitedTimes,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_offloaded_size", stats.managedLedgerStats
                .offloadedStorageUsed, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_backlog_quota_limit", stats.backlogQuotaLimit,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_backlog_quota_limit_time", stats.backlogQuotaLimitTime,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_backlog_age_seconds", stats.backlogAgeSeconds,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeBacklogQuotaMetric(stream, "pulsar_storage_backlog_quota_exceeded_evictions_total",
                stats.sizeBasedBacklogQuotaExceededEvictionCount, cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel, BacklogQuotaType.destination_storage);
        writeBacklogQuotaMetric(stream, "pulsar_storage_backlog_quota_exceeded_evictions_total",
                stats.timeBasedBacklogQuotaExceededEvictionCount, cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel, BacklogQuotaType.message_age);

        writeMetric(stream, "pulsar_delayed_message_index_size_bytes", stats.delayedMessageIndexSizeInBytes,
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);

        for (TopicMetricBean topicMetricBean : stats.bucketDelayedIndexStats.values()) {
            writeTopicMetric(stream, topicMetricBean.name, topicMetricBean.value, cluster, namespace,
                    topic, splitTopicAndPartitionIndexLabel, topicMetricBean.labelsAndValues);
        }

        long[] latencyBuckets = stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets();
        writeMetric(stream, "pulsar_storage_write_latency_le_0_5",
                latencyBuckets[0], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_le_1",
                latencyBuckets[1], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_le_5",
                latencyBuckets[2], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_le_10",
                latencyBuckets[3], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_le_20",
                latencyBuckets[4], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_le_50",
                latencyBuckets[5], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_le_100",
                latencyBuckets[6], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_le_200",
                latencyBuckets[7], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_le_1000",
                latencyBuckets[8], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_overflow",
                latencyBuckets[9], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_count",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getCount(),
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_write_latency_sum",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getSum(), cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);

        long[] ledgerWriteLatencyBuckets = stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets();
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_0_5",
                ledgerWriteLatencyBuckets[0], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_1",
                ledgerWriteLatencyBuckets[1], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_5",
                ledgerWriteLatencyBuckets[2], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_10",
                ledgerWriteLatencyBuckets[3], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_20",
                ledgerWriteLatencyBuckets[4], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_50",
                ledgerWriteLatencyBuckets[5], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_100",
                ledgerWriteLatencyBuckets[6], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_200",
                ledgerWriteLatencyBuckets[7], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_1000",
                ledgerWriteLatencyBuckets[8], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_overflow",
                ledgerWriteLatencyBuckets[9], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_count",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount(),
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_sum",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum(),
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);

        long[] entrySizeBuckets = stats.managedLedgerStats.entrySizeBuckets.getBuckets();
        writeMetric(stream, "pulsar_entry_size_le_128", entrySizeBuckets[0], cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_entry_size_le_512", entrySizeBuckets[1], cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2], cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3], cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4], cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5], cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6], cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7], cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_entry_size_le_overflow", entrySizeBuckets[8], cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_entry_size_count", stats.managedLedgerStats.entrySizeBuckets.getCount(),
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_entry_size_sum", stats.managedLedgerStats.entrySizeBuckets.getSum(),
                cluster, namespace, topic, splitTopicAndPartitionIndexLabel);

        stats.producerStats.forEach((p, producerStats) -> {
            writeProducerMetric(stream, "pulsar_producer_msg_rate_in", producerStats.msgRateIn,
                    cluster, namespace, topic, p, producerStats.producerId, splitTopicAndPartitionIndexLabel);
            writeProducerMetric(stream, "pulsar_producer_msg_throughput_in", producerStats.msgThroughputIn,
                    cluster, namespace, topic, p, producerStats.producerId, splitTopicAndPartitionIndexLabel);
            writeProducerMetric(stream, "pulsar_producer_msg_average_Size", producerStats.averageMsgSize,
                    cluster, namespace, topic, p, producerStats.producerId, splitTopicAndPartitionIndexLabel);
        });

        stats.subscriptionStats.forEach((sub, subsStats) -> {
            writeSubscriptionMetric(stream, "pulsar_subscription_back_log", subsStats.msgBacklog,
                    cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_back_log_no_delayed",
                    subsStats.msgBacklogNoDelayed, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_delayed",
                    subsStats.msgDelayed, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_in_replay",
                    subsStats.msgInReplay, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_msg_rate_redeliver",
                    subsStats.msgRateRedeliver, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_unacked_messages",
                    subsStats.unackedMessages, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_blocked_on_unacked_messages",
                    subsStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0, cluster, namespace, topic, sub,
                    splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_msg_rate_out",
                    subsStats.msgRateOut, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_msg_ack_rate",
                    subsStats.messageAckRate, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_msg_throughput_out",
                    subsStats.msgThroughputOut, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_out_bytes_total",
                    subsStats.bytesOutCounter, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_out_messages_total",
                    subsStats.msgOutCounter, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_last_expire_timestamp",
                    subsStats.lastExpireTimestamp, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_last_acked_timestamp",
                    subsStats.lastAckedTimestamp, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_last_consumed_flow_timestamp",
                    subsStats.lastConsumedFlowTimestamp, cluster, namespace, topic, sub,
                    splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_last_consumed_timestamp",
                    subsStats.lastConsumedTimestamp, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_last_mark_delete_advanced_timestamp",
                    subsStats.lastMarkDeleteAdvancedTimestamp, cluster, namespace, topic, sub,
                    splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_msg_rate_expired",
                    subsStats.msgRateExpired, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_total_msg_expired",
                    subsStats.totalMsgExpired, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_msg_drop_rate",
                    subsStats.msgDropRate, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_consumers_count",
                    subsStats.consumersCount, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);

            writeSubscriptionMetric(stream, "pulsar_subscription_filter_processed_msg_count",
                    subsStats.filterProcessedMsgCount, cluster, namespace, topic, sub,
                    splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_filter_accepted_msg_count",
                    subsStats.filterAcceptedMsgCount, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_filter_rejected_msg_count",
                    subsStats.filterRejectedMsgCount, cluster, namespace, topic, sub, splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_filter_rescheduled_msg_count",
                    subsStats.filterRescheduledMsgCount, cluster, namespace, topic, sub,
                    splitTopicAndPartitionIndexLabel);
            writeSubscriptionMetric(stream, "pulsar_subscription_delayed_message_index_size_bytes",
                    subsStats.delayedMessageIndexSizeInBytes, cluster, namespace, topic, sub,
                    splitTopicAndPartitionIndexLabel);

            final String[] subscriptionLabel = {"subscription", sub};
            for (TopicMetricBean topicMetricBean : subsStats.bucketDelayedIndexStats.values()) {
                String[] labelsAndValues = ArrayUtils.addAll(subscriptionLabel, topicMetricBean.labelsAndValues);
                writeTopicMetric(stream, topicMetricBean.name, topicMetricBean.value, cluster, namespace,
                        topic, splitTopicAndPartitionIndexLabel, labelsAndValues);
            }

            subsStats.consumerStat.forEach((c, consumerStats) -> {
                writeConsumerMetric(stream, "pulsar_consumer_msg_rate_redeliver", consumerStats.msgRateRedeliver,
                        cluster, namespace, topic, sub, c, splitTopicAndPartitionIndexLabel);
                writeConsumerMetric(stream, "pulsar_consumer_unacked_messages", consumerStats.unackedMessages,
                        cluster, namespace, topic, sub, c, splitTopicAndPartitionIndexLabel);
                writeConsumerMetric(stream, "pulsar_consumer_blocked_on_unacked_messages",
                        consumerStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0,
                        cluster, namespace, topic, sub, c, splitTopicAndPartitionIndexLabel);
                writeConsumerMetric(stream, "pulsar_consumer_msg_rate_out", consumerStats.msgRateOut,
                        cluster, namespace, topic, sub, c, splitTopicAndPartitionIndexLabel);

                writeConsumerMetric(stream, "pulsar_consumer_msg_ack_rate", consumerStats.msgAckRate,
                        cluster, namespace, topic, sub, c, splitTopicAndPartitionIndexLabel);

                writeConsumerMetric(stream, "pulsar_consumer_msg_throughput_out", consumerStats.msgThroughputOut,
                        cluster, namespace, topic, sub, c, splitTopicAndPartitionIndexLabel);
                writeConsumerMetric(stream, "pulsar_consumer_available_permits", consumerStats.availablePermits,
                        cluster, namespace, topic, sub, c, splitTopicAndPartitionIndexLabel);
                writeConsumerMetric(stream, "pulsar_out_bytes_total", consumerStats.bytesOutCounter,
                        cluster, namespace, topic, sub, c, splitTopicAndPartitionIndexLabel);
                writeConsumerMetric(stream, "pulsar_out_messages_total", consumerStats.msgOutCounter,
                        cluster, namespace, topic, sub, c, splitTopicAndPartitionIndexLabel);
            });
        });

        if (!stats.replicationStats.isEmpty()) {
            stats.replicationStats.forEach((remoteCluster, replStats) -> {
                writeMetric(stream, "pulsar_replication_rate_in", replStats.msgRateIn,
                        cluster, namespace, topic, remoteCluster, splitTopicAndPartitionIndexLabel);
                writeMetric(stream, "pulsar_replication_rate_out", replStats.msgRateOut,
                        cluster, namespace, topic, remoteCluster, splitTopicAndPartitionIndexLabel);
                writeMetric(stream, "pulsar_replication_throughput_in", replStats.msgThroughputIn,
                        cluster, namespace, topic, remoteCluster, splitTopicAndPartitionIndexLabel);
                writeMetric(stream, "pulsar_replication_throughput_out", replStats.msgThroughputOut,
                        cluster, namespace, topic, remoteCluster, splitTopicAndPartitionIndexLabel);
                writeMetric(stream, "pulsar_replication_backlog", replStats.replicationBacklog,
                        cluster, namespace, topic, remoteCluster, splitTopicAndPartitionIndexLabel);
                writeMetric(stream, "pulsar_replication_connected_count", replStats.connectedCount,
                        cluster, namespace, topic, remoteCluster, splitTopicAndPartitionIndexLabel);
                writeMetric(stream, "pulsar_replication_disconnected_count", replStats.disconnectedCount,
                        cluster, namespace, topic, remoteCluster, splitTopicAndPartitionIndexLabel);
                writeMetric(stream, "pulsar_replication_rate_expired", replStats.msgRateExpired,
                        cluster, namespace, topic, remoteCluster, splitTopicAndPartitionIndexLabel);
                writeMetric(stream, "pulsar_replication_delay_in_seconds", replStats.replicationDelayInSeconds,
                        cluster, namespace, topic, remoteCluster, splitTopicAndPartitionIndexLabel);
            });
        }

        writeMetric(stream, "pulsar_in_bytes_total", stats.bytesInCounter, cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);
        writeMetric(stream, "pulsar_in_messages_total", stats.msgInCounter, cluster, namespace, topic,
                splitTopicAndPartitionIndexLabel);

        // Compaction
        boolean hasCompaction = compactorMXBean.flatMap(mxBean -> mxBean.getCompactionRecordForTopic(topic))
                .isPresent();
        if (hasCompaction) {
            writeMetric(stream, "pulsar_compaction_removed_event_count",
                    stats.compactionRemovedEventCount, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_succeed_count",
                    stats.compactionSucceedCount, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_failed_count",
                    stats.compactionFailedCount, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_duration_time_in_mills",
                    stats.compactionDurationTimeInMills, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_read_throughput",
                    stats.compactionReadThroughput, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_write_throughput",
                    stats.compactionWriteThroughput, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_compacted_entries_count",
                    stats.compactionCompactedEntriesCount, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_compacted_entries_size",
                    stats.compactionCompactedEntriesSize, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);

            long[] compactionBuckets = stats.compactionLatencyBuckets.getBuckets();
            writeMetric(stream, "pulsar_compaction_latency_le_0_5",
                    compactionBuckets[0], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_le_1",
                    compactionBuckets[1], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_le_5",
                    compactionBuckets[2], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_le_10",
                    compactionBuckets[3], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_le_20",
                    compactionBuckets[4], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_le_50",
                    compactionBuckets[5], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_le_100",
                    compactionBuckets[6], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_le_200",
                    compactionBuckets[7], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_le_1000",
                    compactionBuckets[8], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_overflow",
                    compactionBuckets[9], cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_sum",
                    stats.compactionLatencyBuckets.getSum(), cluster, namespace, topic,
                    splitTopicAndPartitionIndexLabel);
            writeMetric(stream, "pulsar_compaction_latency_count",
                    stats.compactionLatencyBuckets.getCount(), cluster, namespace, topic,
                    splitTopicAndPartitionIndexLabel);

            for (TopicMetricBean topicMetricBean : stats.bucketDelayedIndexStats.values()) {
                String[] labelsAndValues = topicMetricBean.labelsAndValues;
                writeTopicMetric(stream, topicMetricBean.name, topicMetricBean.value, cluster, namespace,
                        topic, splitTopicAndPartitionIndexLabel, labelsAndValues);
            }
        }
    }

    private static void writeMetric(PrometheusMetricStreams stream, String metricName, Number value, String cluster,
                                    String namespace, String topic, boolean splitTopicAndPartitionIndexLabel) {
        writeTopicMetric(stream, metricName, value, cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
    }

    @SuppressWarnings("SameParameterValue")
    private static void writeBacklogQuotaMetric(PrometheusMetricStreams stream, String metricName, Number value,
                                                String cluster, String namespace, String topic,
                                                boolean splitTopicAndPartitionIndexLabel,
                                                BacklogQuotaType backlogQuotaType) {

        String quotaTypeLabelValue = PrometheusLabels.backlogQuotaTypeLabel(backlogQuotaType);
        writeTopicMetric(stream, metricName, value, cluster, namespace, topic, splitTopicAndPartitionIndexLabel,
                "quota_type", quotaTypeLabelValue);
    }

    private static void writeMetric(PrometheusMetricStreams stream, String metricName, Number value, String cluster,
                                    String namespace, String topic, String remoteCluster,
                                    boolean splitTopicAndPartitionIndexLabel) {
        writeTopicMetric(stream, metricName, value, cluster, namespace, topic, splitTopicAndPartitionIndexLabel,
                "remote_cluster", remoteCluster);
    }

    private static void writeProducerMetric(PrometheusMetricStreams stream, String metricName, Number value,
                                            String cluster, String namespace, String topic, String producer,
                                            long producerId, boolean splitTopicAndPartitionIndexLabel) {
        writeTopicMetric(stream, metricName, value, cluster, namespace, topic, splitTopicAndPartitionIndexLabel,
                "producer_name", producer, "producer_id", String.valueOf(producerId));
    }


    private static void writeSubscriptionMetric(PrometheusMetricStreams stream, String metricName, Number value,
                                                String cluster, String namespace, String topic, String subscription,
                                                boolean splitTopicAndPartitionIndexLabel) {
        writeTopicMetric(stream, metricName, value, cluster, namespace, topic, splitTopicAndPartitionIndexLabel,
                "subscription", subscription);
    }

    private static void writeConsumerMetric(PrometheusMetricStreams stream, String metricName, Number value,
                                            String cluster, String namespace, String topic, String subscription,
                                            Consumer consumer, boolean splitTopicAndPartitionIndexLabel) {
        writeTopicMetric(stream, metricName, value, cluster, namespace, topic, splitTopicAndPartitionIndexLabel,
                "subscription", subscription, "consumer_name", consumer.consumerName(),
                "consumer_id", String.valueOf(consumer.consumerId()));
    }

    static void writeTopicMetric(PrometheusMetricStreams stream, String metricName, Number value, String cluster,
                                 String namespace, String topic, boolean splitTopicAndPartitionIndexLabel,
                                 String... extraLabelsAndValues) {
        int baseLabelCount = splitTopicAndPartitionIndexLabel ? 8 : 6;
        String[] labelsAndValues =
                new String[baseLabelCount + (extraLabelsAndValues != null ? extraLabelsAndValues.length : 0)];
        labelsAndValues[0] = "cluster";
        labelsAndValues[1] = cluster;
        labelsAndValues[2] = "namespace";
        labelsAndValues[3] = namespace;
        labelsAndValues[4] = "topic";
        if (splitTopicAndPartitionIndexLabel) {
            int index = topic.indexOf(PARTITIONED_TOPIC_SUFFIX);
            if (index > 0) {
                labelsAndValues[5] = topic.substring(0, index);
                labelsAndValues[6] = "partition";
                labelsAndValues[7] = topic.substring(index + PARTITIONED_TOPIC_SUFFIX.length());
            } else {
                labelsAndValues[5] = topic;
                labelsAndValues[6] = "partition";
                labelsAndValues[7] = "-1";
            }
        } else {
            labelsAndValues[5] = topic;
        }
        if (extraLabelsAndValues != null) {
            for (int i = 0; i < extraLabelsAndValues.length; i++) {
                labelsAndValues[baseLabelCount + i] = extraLabelsAndValues[i];
            }
        }
        stream.writeSample(metricName, value, labelsAndValues);
    }
}
