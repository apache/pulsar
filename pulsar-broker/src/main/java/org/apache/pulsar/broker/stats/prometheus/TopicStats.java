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
import io.prometheus.client.Collector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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

    // Used for tracking duplicate TYPE definitions
    static Map<String, String> metricWithTypeDefinition = new HashMap<>();

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

    static void resetTypes() {
        metricWithTypeDefinition.clear();
    }

    static void buildTopicStats(Map<String, Collector.MetricFamilySamples> metrics, String cluster, String namespace,
                                String topic,
                                TopicStats stats, Optional<CompactorMXBean> compactorMXBean,
                                boolean splitTopicAndPartitionIndexLabel) {
        metric(metrics, cluster, namespace, topic, "pulsar_subscriptions_count", stats.subscriptionsCount,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_producers_count", stats.producersCount,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_consumers_count", stats.consumersCount,
                splitTopicAndPartitionIndexLabel);

        metric(metrics, cluster, namespace, topic, "pulsar_rate_in", stats.rateIn,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_rate_out", stats.rateOut,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_throughput_in", stats.throughputIn,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_throughput_out", stats.throughputOut,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_average_msg_size", stats.averageMsgSize,
                splitTopicAndPartitionIndexLabel);

        metric(metrics, cluster, namespace, topic, "pulsar_storage_size", stats.managedLedgerStats.storageSize,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_logical_size",
                stats.managedLedgerStats.storageLogicalSize, splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_msg_backlog", stats.msgBacklog,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_backlog_size",
                stats.managedLedgerStats.backlogSize, splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_publish_rate_limit_times", stats.publishRateLimitedTimes,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_offloaded_size", stats.managedLedgerStats
                .offloadedStorageUsed, splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_backlog_quota_limit", stats.backlogQuotaLimit,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_backlog_quota_limit_time",
                stats.backlogQuotaLimitTime, splitTopicAndPartitionIndexLabel);

        long[] latencyBuckets = stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets();
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_le_0_5", latencyBuckets[0],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_le_1", latencyBuckets[1],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_le_5", latencyBuckets[2],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_le_10", latencyBuckets[3],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_le_20", latencyBuckets[4],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_le_50", latencyBuckets[5],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_le_100", latencyBuckets[6],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_le_200", latencyBuckets[7],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_le_1000", latencyBuckets[8],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_overflow", latencyBuckets[9],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_count",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getCount(), splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_write_latency_sum",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getSum(), splitTopicAndPartitionIndexLabel);

        long[] ledgerWriteLatencyBuckets = stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets();
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_0_5",
                ledgerWriteLatencyBuckets[0], splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_1",
                ledgerWriteLatencyBuckets[1], splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_5",
                ledgerWriteLatencyBuckets[2], splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_10",
                ledgerWriteLatencyBuckets[3], splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_20",
                ledgerWriteLatencyBuckets[4], splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_50",
                ledgerWriteLatencyBuckets[5], splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_100",
                ledgerWriteLatencyBuckets[6], splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_200",
                ledgerWriteLatencyBuckets[7], splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_1000",
                ledgerWriteLatencyBuckets[8], splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_overflow",
                ledgerWriteLatencyBuckets[9], splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_count",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount(),
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_sum",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum(),
                splitTopicAndPartitionIndexLabel);

        long[] entrySizeBuckets = stats.managedLedgerStats.entrySizeBuckets.getBuckets();
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_le_128", entrySizeBuckets[0],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_le_512", entrySizeBuckets[1],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_le_overflow", entrySizeBuckets[8],
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_count",
                stats.managedLedgerStats.entrySizeBuckets.getCount(), splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_entry_size_sum",
                stats.managedLedgerStats.entrySizeBuckets.getSum(), splitTopicAndPartitionIndexLabel);

        stats.producerStats.forEach((p, producerStats) -> {
            metric(metrics, cluster, namespace, topic, p, producerStats.producerId, "pulsar_producer_msg_rate_in",
                    producerStats.msgRateIn, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, p, producerStats.producerId, "pulsar_producer_msg_throughput_in",
                    producerStats.msgThroughputIn, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, p, producerStats.producerId, "pulsar_producer_msg_average_Size",
                    producerStats.averageMsgSize, splitTopicAndPartitionIndexLabel);
        });

        stats.subscriptionStats.forEach((n, subsStats) -> {
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_back_log",
                    subsStats.msgBacklog, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_back_log_no_delayed",
                    subsStats.msgBacklogNoDelayed, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_delayed",
                    subsStats.msgDelayed, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_redeliver",
                    subsStats.msgRateRedeliver, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_unacked_messages",
                    subsStats.unackedMessages, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_blocked_on_unacked_messages",
                    subsStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_out",
                    subsStats.msgRateOut, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_msg_ack_rate",
                    subsStats.messageAckRate, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_msg_throughput_out",
                    subsStats.msgThroughputOut, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_out_bytes_total",
                    subsStats.bytesOutCounter, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_out_messages_total",
                    subsStats.msgOutCounter, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_last_expire_timestamp",
                    subsStats.lastExpireTimestamp, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_last_acked_timestamp",
                    subsStats.lastAckedTimestamp, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_last_consumed_flow_timestamp",
                    subsStats.lastConsumedFlowTimestamp, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_last_consumed_timestamp",
                    subsStats.lastConsumedTimestamp, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_last_mark_delete_advanced_timestamp",
                    subsStats.lastMarkDeleteAdvancedTimestamp, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_expired",
                    subsStats.msgRateExpired, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_total_msg_expired",
                    subsStats.totalMsgExpired, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_msg_drop_rate",
                    subsStats.msgDropRate, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, n, "pulsar_subscription_consumers_count",
                    subsStats.consumersCount, splitTopicAndPartitionIndexLabel);
            subsStats.consumerStat.forEach((c, consumerStats) -> {
                metric(metrics, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_rate_redeliver", consumerStats.msgRateRedeliver,
                        splitTopicAndPartitionIndexLabel);
                metric(metrics, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_unacked_messages", consumerStats.unackedMessages,
                        splitTopicAndPartitionIndexLabel);
                metric(metrics, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_blocked_on_unacked_messages",
                        consumerStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0,
                        splitTopicAndPartitionIndexLabel);
                metric(metrics, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_rate_out", consumerStats.msgRateOut,
                        splitTopicAndPartitionIndexLabel);

                metric(metrics, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_ack_rate", consumerStats.msgAckRate,
                        splitTopicAndPartitionIndexLabel);

                metric(metrics, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_throughput_out", consumerStats.msgThroughputOut,
                        splitTopicAndPartitionIndexLabel);
                metric(metrics, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_available_permits", consumerStats.availablePermits,
                        splitTopicAndPartitionIndexLabel);
                metric(metrics, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_out_bytes_total", consumerStats.bytesOutCounter,
                        splitTopicAndPartitionIndexLabel);
                metric(metrics, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_out_messages_total", consumerStats.msgOutCounter,
                        splitTopicAndPartitionIndexLabel);
            });
        });

        if (!stats.replicationStats.isEmpty()) {
            stats.replicationStats.forEach((remoteCluster, replStats) -> {
                metricWithRemoteCluster(metrics, cluster, namespace, topic, "pulsar_replication_rate_in", remoteCluster,
                        replStats.msgRateIn, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(metrics, cluster, namespace, topic, "pulsar_replication_rate_out",
                        remoteCluster,
                        replStats.msgRateOut, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(metrics, cluster, namespace, topic, "pulsar_replication_throughput_in",
                        remoteCluster, replStats.msgThroughputIn, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(metrics, cluster, namespace, topic, "pulsar_replication_throughput_out",
                        remoteCluster, replStats.msgThroughputOut, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(metrics, cluster, namespace, topic, "pulsar_replication_backlog", remoteCluster,
                        replStats.replicationBacklog, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(metrics, cluster, namespace, topic, "pulsar_replication_connected_count",
                        remoteCluster, replStats.connectedCount, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(metrics, cluster, namespace, topic, "pulsar_replication_rate_expired",
                        remoteCluster, replStats.msgRateExpired, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(metrics, cluster, namespace, topic, "pulsar_replication_delay_in_seconds",
                        remoteCluster, replStats.replicationDelayInSeconds, splitTopicAndPartitionIndexLabel);
            });
        }

        metric(metrics, cluster, namespace, topic, "pulsar_in_bytes_total", stats.bytesInCounter,
                splitTopicAndPartitionIndexLabel);
        metric(metrics, cluster, namespace, topic, "pulsar_in_messages_total", stats.msgInCounter,
                splitTopicAndPartitionIndexLabel);

        // Compaction
        boolean hasCompaction =
                compactorMXBean.flatMap(mxBean -> mxBean.getCompactionRecordForTopic(topic)).isPresent();
        if (hasCompaction) {
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_removed_event_count",
                    stats.compactionRemovedEventCount, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_succeed_count",
                    stats.compactionSucceedCount, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_failed_count",
                    stats.compactionFailedCount, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_duration_time_in_mills",
                    stats.compactionDurationTimeInMills, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_read_throughput",
                    stats.compactionReadThroughput, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_write_throughput",
                    stats.compactionWriteThroughput, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_compacted_entries_count",
                    stats.compactionCompactedEntriesCount, splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_compacted_entries_size",
                    stats.compactionCompactedEntriesSize, splitTopicAndPartitionIndexLabel);
            long[] compactionLatencyBuckets = stats.compactionLatencyBuckets.getBuckets();
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_le_0_5",
                    compactionLatencyBuckets[0], splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_le_1",
                    compactionLatencyBuckets[1], splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_le_5",
                    compactionLatencyBuckets[2], splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_le_10",
                    compactionLatencyBuckets[3], splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_le_20",
                    compactionLatencyBuckets[4], splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_le_50",
                    compactionLatencyBuckets[5], splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_le_100",
                    compactionLatencyBuckets[6], splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_le_200",
                    compactionLatencyBuckets[7], splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_le_1000",
                    compactionLatencyBuckets[8], splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_overflow",
                    compactionLatencyBuckets[9], splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_sum",
                    stats.compactionLatencyBuckets.getSum(), splitTopicAndPartitionIndexLabel);
            metric(metrics, cluster, namespace, topic, "pulsar_compaction_latency_count",
                    stats.compactionLatencyBuckets.getCount(), splitTopicAndPartitionIndexLabel);
        }
    }

    private static void metric(Map<String, Collector.MetricFamilySamples> metrics, String cluster, String namespace,
                               String topic, String name, double value, boolean splitTopicAndPartitionIndexLabel) {
        Map<String, String> labels = buildLabels(cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        addMetric(metrics, labels, name, value);
    }

    private static void metric(Map<String, Collector.MetricFamilySamples> metrics, String cluster, String namespace,
                               String topic, String subscription, String name, long value,
                               boolean splitTopicAndPartitionIndexLabel) {
        Map<String, String> labels = buildLabels(cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        labels.put("subscription", subscription);
        addMetric(metrics, labels, name, value);
    }

    private static void metric(Map<String, Collector.MetricFamilySamples> metrics, String cluster, String namespace,
                               String topic, String producerName, long produceId, String name, double value,
                               boolean splitTopicAndPartitionIndexLabel) {
        Map<String, String> labels = buildLabels(cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        labels.put("producer_name", producerName);
        labels.put("producer_id", String.valueOf(produceId));
        addMetric(metrics, labels, name, value);
    }

    private static void metric(Map<String, Collector.MetricFamilySamples> metrics, String cluster, String namespace,
                               String topic, String subscription, String name, double value,
                               boolean splitTopicAndPartitionIndexLabel) {
        Map<String, String> labels = buildLabels(cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        labels.put("subscription", subscription);
        addMetric(metrics, labels, name, value);
    }

    private static void metric(Map<String, Collector.MetricFamilySamples> metrics, String cluster, String namespace,
                               String topic, String subscription, String consumerName, long consumerId, String name,
                               double value, boolean splitTopicAndPartitionIndexLabel) {
        Map<String, String> labels = buildLabels(cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        labels.put("subscription", subscription);
        labels.put("consumer_name", consumerName);
        labels.put("consumer_id", String.valueOf(consumerId));
        addMetric(metrics, labels, name, value);
    }

    private static void metricWithRemoteCluster(Map<String, Collector.MetricFamilySamples> metrics, String cluster,
                                                String namespace, String topic, String name, String remoteCluster,
                                                double value, boolean splitTopicAndPartitionIndexLabel) {
        Map<String, String> labels = buildLabels(cluster, namespace, topic, splitTopicAndPartitionIndexLabel);
        labels.put("remote_cluster", remoteCluster);
        addMetric(metrics, labels, name, value);
    }

    private static Map<String, String> buildLabels(String cluster, String namespace, String topic,
                                                   boolean splitTopicAndPartitionIndexLabel) {
        Map<String, String> labels = new HashMap<>();
        labels.put("cluster", cluster);
        labels.put("namespace", namespace);
        if (splitTopicAndPartitionIndexLabel) {
            int index = topic.indexOf(PARTITIONED_TOPIC_SUFFIX);
            if (index > 0) {
                labels.put("topic", topic.substring(0, index));
                labels.put("partition", topic.substring(index + PARTITIONED_TOPIC_SUFFIX.length()));
            } else {
                labels.put("topic", topic);
                labels.put("partition", "-1");
            }
        } else {
            labels.put("topic", topic);
        }
        return labels;
    }

    static void addMetric(Map<String, Collector.MetricFamilySamples> metrics, Map<String, String> labels,
                          String name, double value) {
        Collector.MetricFamilySamples familySamples = metrics.getOrDefault(name,
                new Collector.MetricFamilySamples(name, Collector.Type.GAUGE, null, new ArrayList<>()));
        familySamples.samples.add(new Collector.MetricFamilySamples.Sample(name,
                labels.keySet().stream().toList(),
                labels.values().stream().toList(),
                value,
                System.currentTimeMillis()));
        metrics.put(name, familySamples);
    }
}
