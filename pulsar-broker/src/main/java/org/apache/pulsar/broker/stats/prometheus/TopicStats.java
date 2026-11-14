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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.stats.OpenTelemetryTopicStats;
import org.apache.pulsar.broker.stats.prometheus.metrics.PrometheusLabels;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.compaction.CompactionRecord;
import org.apache.pulsar.compaction.CompactorMXBean;
import org.apache.pulsar.opentelemetry.annotations.PulsarDeprecatedMetric;

class TopicStats {
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.SUBSCRIPTION_COUNTER)
    int subscriptionsCount;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.PRODUCER_COUNTER)
    int producersCount;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.CONSUMER_COUNTER)
    int consumersCount;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.MESSAGE_IN_COUNTER)
    double rateIn;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.MESSAGE_OUT_COUNTER)
    double rateOut;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.BYTES_IN_COUNTER)
    double throughputIn;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.BYTES_OUT_COUNTER)
    double throughputOut;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.MESSAGE_IN_COUNTER)
    long msgInCounter;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.BYTES_IN_COUNTER)
    long bytesInCounter;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.MESSAGE_OUT_COUNTER)
    long msgOutCounter;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.BYTES_OUT_COUNTER)
    long bytesOutCounter;
    long systemTopicBytesInCounter;
    long bytesOutInternalCounter;
    @PulsarDeprecatedMetric // Can be derived from MESSAGE_IN_COUNTER and BYTES_IN_COUNTER
    double averageMsgSize;

    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.TRANSACTION_COUNTER)
    long ongoingTxnCount;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.TRANSACTION_COUNTER)
    long abortedTxnCount;
    @PulsarDeprecatedMetric(newMetricName = OpenTelemetryTopicStats.TRANSACTION_COUNTER)
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
                                       String topic, boolean splitTopicAndPartitionIndexLabel,
                                       boolean exposeSubscriptionBacklogAge,
                                       String[] customLabelsAndValues) {
        String[] topicLabels = createTopicLabels(cluster, namespace, topic, splitTopicAndPartitionIndexLabel,
                customLabelsAndValues);
        writeMetric(stream, "pulsar_subscriptions_count", stats.subscriptionsCount, topicLabels);
        writeMetric(stream, "pulsar_producers_count", stats.producersCount, topicLabels);
        writeMetric(stream, "pulsar_consumers_count", stats.consumersCount, topicLabels);

        writeMetric(stream, "pulsar_rate_in", stats.rateIn, topicLabels);
        writeMetric(stream, "pulsar_rate_out", stats.rateOut, topicLabels);
        writeMetric(stream, "pulsar_throughput_in", stats.throughputIn, topicLabels);
        writeMetric(stream, "pulsar_throughput_out", stats.throughputOut, topicLabels);
        writeMetric(stream, "pulsar_average_msg_size", stats.averageMsgSize, topicLabels);

        writeMetric(stream, "pulsar_txn_tb_active_total", stats.ongoingTxnCount, topicLabels);
        writeMetric(stream, "pulsar_txn_tb_aborted_total", stats.abortedTxnCount, topicLabels);
        writeMetric(stream, "pulsar_txn_tb_committed_total", stats.committedTxnCount, topicLabels);

        writeMetric(stream, "pulsar_storage_size", stats.managedLedgerStats.storageSize, topicLabels);
        writeMetric(stream, "pulsar_storage_logical_size", stats.managedLedgerStats.storageLogicalSize, topicLabels);
        writeMetric(stream, "pulsar_msg_backlog", stats.msgBacklog, topicLabels);
        writeMetric(stream, "pulsar_storage_write_rate", stats.managedLedgerStats.storageWriteRate, topicLabels);
        writeMetric(stream, "pulsar_storage_read_rate", stats.managedLedgerStats.storageReadRate, topicLabels);
        writeMetric(stream, "pulsar_storage_read_cache_misses_rate",
            stats.managedLedgerStats.storageReadCacheMissesRate,
            topicLabels);
        writeMetric(stream, "pulsar_storage_backlog_size", stats.managedLedgerStats.backlogSize, topicLabels);
        writeMetric(stream, "pulsar_publish_rate_limit_times", stats.publishRateLimitedTimes, topicLabels);
        writeMetric(stream, "pulsar_storage_offloaded_size", stats.managedLedgerStats
            .offloadedStorageUsed, topicLabels);
        writeMetric(stream, "pulsar_storage_backlog_quota_limit", stats.backlogQuotaLimit, topicLabels);
        writeMetric(stream, "pulsar_storage_backlog_quota_limit_time", stats.backlogQuotaLimitTime, topicLabels);
        writeMetric(stream, "pulsar_storage_backlog_age_seconds", stats.backlogAgeSeconds, topicLabels);
        writeBacklogQuotaMetric(stream, "pulsar_storage_backlog_quota_exceeded_evictions_total",
            stats.sizeBasedBacklogQuotaExceededEvictionCount, topicLabels, BacklogQuotaType.destination_storage);
        writeBacklogQuotaMetric(stream, "pulsar_storage_backlog_quota_exceeded_evictions_total",
            stats.timeBasedBacklogQuotaExceededEvictionCount, topicLabels, BacklogQuotaType.message_age);

        writeMetric(stream, "pulsar_delayed_message_index_size_bytes", stats.delayedMessageIndexSizeInBytes,
            topicLabels);

        for (TopicMetricBean topicMetricBean : stats.bucketDelayedIndexStats.values()) {
            stream.writeSample(topicMetricBean.name, topicMetricBean.value, topicLabels,
                    topicMetricBean.labelsAndValues);
        }

        long[] latencyBuckets = stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets();
        writeMetric(stream, "pulsar_storage_write_latency_le_0_5", latencyBuckets[0], topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_le_1", latencyBuckets[1], topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_le_5", latencyBuckets[2], topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_le_10", latencyBuckets[3], topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_le_20", latencyBuckets[4], topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_le_50", latencyBuckets[5], topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_le_100", latencyBuckets[6], topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_le_200", latencyBuckets[7], topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_le_1000", latencyBuckets[8], topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_overflow", latencyBuckets[9], topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_count",
            stats.managedLedgerStats.storageWriteLatencyBuckets.getCount(),
            topicLabels);
        writeMetric(stream, "pulsar_storage_write_latency_sum",
            stats.managedLedgerStats.storageWriteLatencyBuckets.getSum(), topicLabels);

        long[] ledgerWriteLatencyBuckets = stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets();
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_0_5", ledgerWriteLatencyBuckets[0], topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_1", ledgerWriteLatencyBuckets[1], topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_5", ledgerWriteLatencyBuckets[2], topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_10", ledgerWriteLatencyBuckets[3], topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_20", ledgerWriteLatencyBuckets[4], topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_50", ledgerWriteLatencyBuckets[5], topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_100", ledgerWriteLatencyBuckets[6], topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_200", ledgerWriteLatencyBuckets[7], topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_1000", ledgerWriteLatencyBuckets[8], topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_overflow", ledgerWriteLatencyBuckets[9], topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_count",
            stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount(),
            topicLabels);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_sum",
            stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum(),
            topicLabels);

        long[] entrySizeBuckets = stats.managedLedgerStats.entrySizeBuckets.getBuckets();
        writeMetric(stream, "pulsar_entry_size_le_128", entrySizeBuckets[0], topicLabels);
        writeMetric(stream, "pulsar_entry_size_le_512", entrySizeBuckets[1], topicLabels);
        writeMetric(stream, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2], topicLabels);
        writeMetric(stream, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3], topicLabels);
        writeMetric(stream, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4], topicLabels);
        writeMetric(stream, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5], topicLabels);
        writeMetric(stream, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6], topicLabels);
        writeMetric(stream, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7], topicLabels);
        writeMetric(stream, "pulsar_entry_size_le_overflow", entrySizeBuckets[8], topicLabels);
        writeMetric(stream, "pulsar_entry_size_count", stats.managedLedgerStats.entrySizeBuckets.getCount(),
            topicLabels);
        writeMetric(stream, "pulsar_entry_size_sum", stats.managedLedgerStats.entrySizeBuckets.getSum(), topicLabels);

        stats.producerStats.forEach((p, producerStats) -> {
            String[] producerLabels = appendLabels(topicLabels, "producer_name", p,
                    "producer_id", String.valueOf(producerStats.producerId));
            writeMetric(stream, "pulsar_producer_msg_rate_in", producerStats.msgRateIn, producerLabels);
            writeMetric(stream, "pulsar_producer_msg_throughput_in", producerStats.msgThroughputIn, producerLabels);
            writeMetric(stream, "pulsar_producer_msg_average_Size", producerStats.averageMsgSize, producerLabels);
        });

        stats.subscriptionStats.forEach((sub, subsStats) -> {
            String[] subscriptionLabels = appendLabels(topicLabels, "subscription", sub);
            writeMetric(stream, "pulsar_subscription_back_log", subsStats.msgBacklog, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_back_log_no_delayed",
                subsStats.msgBacklogNoDelayed, subscriptionLabels);
            if (exposeSubscriptionBacklogAge && subsStats.backlogAgeSeconds >= 0) {
                writeMetric(stream, "pulsar_subscription_storage_backlog_age_seconds",
                    subsStats.backlogAgeSeconds, subscriptionLabels);
            }
            writeMetric(stream, "pulsar_subscription_delayed", subsStats.msgDelayed, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_in_replay", subsStats.msgInReplay, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_msg_rate_redeliver",
                subsStats.msgRateRedeliver, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_unacked_messages", subsStats.unackedMessages, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_blocked_on_unacked_messages",
                subsStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_msg_rate_out", subsStats.msgRateOut, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_msg_ack_rate", subsStats.messageAckRate, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_msg_throughput_out",
                subsStats.msgThroughputOut, subscriptionLabels);
            writeMetric(stream, "pulsar_out_bytes_total", subsStats.bytesOutCounter, subscriptionLabels);
            writeMetric(stream, "pulsar_out_messages_total", subsStats.msgOutCounter, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_last_expire_timestamp",
                subsStats.lastExpireTimestamp, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_last_acked_timestamp",
                subsStats.lastAckedTimestamp, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_last_consumed_flow_timestamp",
                subsStats.lastConsumedFlowTimestamp, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_last_consumed_timestamp",
                subsStats.lastConsumedTimestamp, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_last_mark_delete_advanced_timestamp",
                subsStats.lastMarkDeleteAdvancedTimestamp, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_msg_rate_expired", subsStats.msgRateExpired, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_total_msg_expired", subsStats.totalMsgExpired, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_msg_drop_rate", subsStats.msgDropRate, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_consumers_count", subsStats.consumersCount, subscriptionLabels);

            writeMetric(stream, "pulsar_subscription_filter_processed_msg_count",
                subsStats.filterProcessedMsgCount, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_filter_accepted_msg_count",
                subsStats.filterAcceptedMsgCount, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_filter_rejected_msg_count",
                subsStats.filterRejectedMsgCount, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_filter_rescheduled_msg_count",
                subsStats.filterRescheduledMsgCount, subscriptionLabels);
            writeMetric(stream, "pulsar_subscription_delayed_message_index_size_bytes",
                subsStats.delayedMessageIndexSizeInBytes, subscriptionLabels);

            // write dispatch throttling metrics with `reason` labels to identify specific throttling
            // causes: by subscription limit, by topic limit, or by broker limit.
            stream.writeSample("pulsar_subscription_dispatch_throttled_msg_events",
                subsStats.dispatchThrottledMsgEventsBySubscriptionLimit, subscriptionLabels, "reason", "subscription");
            stream.writeSample("pulsar_subscription_dispatch_throttled_bytes_events",
                subsStats.dispatchThrottledBytesEventsBySubscriptionLimit, subscriptionLabels,
                "reason", "subscription");
            stream.writeSample("pulsar_subscription_dispatch_throttled_msg_events",
                subsStats.dispatchThrottledMsgEventsByTopicLimit, subscriptionLabels, "reason", "topic");
            stream.writeSample("pulsar_subscription_dispatch_throttled_bytes_events",
                subsStats.dispatchThrottledBytesEventsByTopicLimit, subscriptionLabels, "reason", "topic");
            stream.writeSample("pulsar_subscription_dispatch_throttled_msg_events",
                subsStats.dispatchThrottledMsgEventsByBrokerLimit, subscriptionLabels, "reason", "broker");
            stream.writeSample("pulsar_subscription_dispatch_throttled_bytes_events",
                subsStats.dispatchThrottledBytesEventsByBrokerLimit, subscriptionLabels, "reason", "broker");

            for (TopicMetricBean topicMetricBean : subsStats.bucketDelayedIndexStats.values()) {
                stream.writeSample(topicMetricBean.name, topicMetricBean.value, subscriptionLabels,
                        topicMetricBean.labelsAndValues);
            }

            subsStats.consumerStat.forEach((c, consumerStats) -> {
                String[] consumerLabels = appendLabels(subscriptionLabels, "consumer_name", c.consumerName(),
                        "consumer_id", String.valueOf(c.consumerId()));
                writeMetric(stream, "pulsar_consumer_msg_rate_redeliver", consumerStats.msgRateRedeliver,
                    consumerLabels);
                writeMetric(stream, "pulsar_consumer_unacked_messages", consumerStats.unackedMessages, consumerLabels);
                writeMetric(stream, "pulsar_consumer_blocked_on_unacked_messages",
                    consumerStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0,
                    consumerLabels);
                writeMetric(stream, "pulsar_consumer_msg_rate_out", consumerStats.msgRateOut, consumerLabels);

                writeMetric(stream, "pulsar_consumer_msg_ack_rate", consumerStats.msgAckRate, consumerLabels);

                writeMetric(stream, "pulsar_consumer_msg_throughput_out", consumerStats.msgThroughputOut,
                    consumerLabels);
                writeMetric(stream, "pulsar_consumer_available_permits", consumerStats.availablePermits,
                    consumerLabels);
                writeMetric(stream, "pulsar_out_bytes_total", consumerStats.bytesOutCounter, consumerLabels);
                writeMetric(stream, "pulsar_out_messages_total", consumerStats.msgOutCounter, consumerLabels);
            });
        });

        if (!stats.replicationStats.isEmpty()) {
            stats.replicationStats.forEach((remoteCluster, replStats) -> {
                String[] replicationLabels = appendLabels(topicLabels, "remote_cluster", remoteCluster);
                writeMetric(stream, "pulsar_replication_rate_in", replStats.msgRateIn, replicationLabels);
                writeMetric(stream, "pulsar_replication_rate_out", replStats.msgRateOut, replicationLabels);
                writeMetric(stream, "pulsar_replication_throughput_in", replStats.msgThroughputIn, replicationLabels);
                writeMetric(stream, "pulsar_replication_throughput_out", replStats.msgThroughputOut, replicationLabels);
                writeMetric(stream, "pulsar_replication_backlog", replStats.replicationBacklog, replicationLabels);
                writeMetric(stream, "pulsar_replication_connected_count", replStats.connectedCount, replicationLabels);
                writeMetric(stream, "pulsar_replication_disconnected_count", replStats.disconnectedCount,
                    replicationLabels);
                writeMetric(stream, "pulsar_replication_rate_expired", replStats.msgRateExpired, replicationLabels);
                writeMetric(stream, "pulsar_replication_delay_in_seconds", replStats.replicationDelayInSeconds,
                    replicationLabels);
            });
        }

        writeMetric(stream, "pulsar_in_bytes_total", stats.bytesInCounter, topicLabels);
        writeMetric(stream, "pulsar_in_messages_total", stats.msgInCounter, topicLabels);

        // Compaction
        boolean hasCompaction = compactorMXBean.flatMap(mxBean -> mxBean.getCompactionRecordForTopic(topic))
            .isPresent();
        if (hasCompaction) {
            writeMetric(stream, "pulsar_compaction_removed_event_count",
                stats.compactionRemovedEventCount, topicLabels);
            writeMetric(stream, "pulsar_compaction_succeed_count", stats.compactionSucceedCount, topicLabels);
            writeMetric(stream, "pulsar_compaction_failed_count", stats.compactionFailedCount, topicLabels);
            writeMetric(stream, "pulsar_compaction_duration_time_in_mills",
                stats.compactionDurationTimeInMills, topicLabels);
            writeMetric(stream, "pulsar_compaction_read_throughput", stats.compactionReadThroughput, topicLabels);
            writeMetric(stream, "pulsar_compaction_write_throughput", stats.compactionWriteThroughput, topicLabels);
            writeMetric(stream, "pulsar_compaction_compacted_entries_count",
                stats.compactionCompactedEntriesCount, topicLabels);
            writeMetric(stream, "pulsar_compaction_compacted_entries_size",
                stats.compactionCompactedEntriesSize, topicLabels);

            long[] compactionBuckets = stats.compactionLatencyBuckets.getBuckets();
            writeMetric(stream, "pulsar_compaction_latency_le_0_5", compactionBuckets[0], topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_le_1", compactionBuckets[1], topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_le_5", compactionBuckets[2], topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_le_10", compactionBuckets[3], topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_le_20", compactionBuckets[4], topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_le_50", compactionBuckets[5], topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_le_100", compactionBuckets[6], topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_le_200", compactionBuckets[7], topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_le_1000", compactionBuckets[8], topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_overflow", compactionBuckets[9], topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_sum", stats.compactionLatencyBuckets.getSum(), topicLabels);
            writeMetric(stream, "pulsar_compaction_latency_count",
                stats.compactionLatencyBuckets.getCount(), topicLabels);

            for (TopicMetricBean topicMetricBean : stats.bucketDelayedIndexStats.values()) {
                stream.writeSample(topicMetricBean.name, topicMetricBean.value, topicLabels,
                        topicMetricBean.labelsAndValues);
            }
        }
    }

    private static void writeMetric(PrometheusMetricStreams stream, String metricName, long value,
                                    String[] labels) {
        stream.writeSample(metricName, value, labels);
    }

    private static void writeMetric(PrometheusMetricStreams stream, String metricName, double value,
                                    String[] labels) {
        stream.writeSample(metricName, value, labels);
    }

    @SuppressWarnings("SameParameterValue")
    private static void writeBacklogQuotaMetric(PrometheusMetricStreams stream, String metricName, long value,
                                                String[] labels, BacklogQuotaType backlogQuotaType) {
        stream.writeSample(metricName, value, labels, "quota_type",
                PrometheusLabels.backlogQuotaTypeLabel(backlogQuotaType));
    }

    private static String[] appendLabels(String[] labels, String label, String value) {
        String[] result = Arrays.copyOf(labels, labels.length + 2);
        result[labels.length] = label;
        result[labels.length + 1] = value;
        return result;
    }

    private static String[] appendLabels(String[] labels, String firstLabel, String firstValue,
                                         String secondLabel, String secondValue) {
        String[] result = Arrays.copyOf(labels, labels.length + 4);
        result[labels.length] = firstLabel;
        result[labels.length + 1] = firstValue;
        result[labels.length + 2] = secondLabel;
        result[labels.length + 3] = secondValue;
        return result;
    }

    private static String[] createTopicLabels(String cluster, String namespace, String topic,
                                               boolean splitTopicAndPartitionIndexLabel,
                                               String[] extraLabelsAndValues) {
        int baseLabelCount = splitTopicAndPartitionIndexLabel ? 8 : 6;
        int extraLabelsCount = (extraLabelsAndValues != null ? extraLabelsAndValues.length : 0);
        String[] labelsAndValues = new String[baseLabelCount + extraLabelsCount];

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

        // Add extra labels (including custom metric labels)
        if (extraLabelsAndValues != null) {
            System.arraycopy(extraLabelsAndValues, 0, labelsAndValues, baseLabelCount, extraLabelsAndValues.length);
        }

        return labelsAndValues;
    }
}
