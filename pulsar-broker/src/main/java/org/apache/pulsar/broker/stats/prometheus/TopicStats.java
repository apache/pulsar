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
import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
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

    static void printTopicStats(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                                TopicStats stats, Optional<CompactorMXBean> compactorMXBean,
                                boolean splitTopicAndPartitionIndexLabel) {
        metric(stream, cluster, namespace, topic, "pulsar_subscriptions_count", stats.subscriptionsCount,
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_producers_count", stats.producersCount,
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_consumers_count", stats.consumersCount,
                splitTopicAndPartitionIndexLabel);

        metric(stream, cluster, namespace, topic, "pulsar_rate_in", stats.rateIn,
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_rate_out", stats.rateOut,
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_throughput_in", stats.throughputIn,
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_throughput_out", stats.throughputOut,
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_average_msg_size", stats.averageMsgSize,
                splitTopicAndPartitionIndexLabel);

        metric(stream, cluster, namespace, topic, "pulsar_storage_size", stats.managedLedgerStats.storageSize,
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_logical_size",
                stats.managedLedgerStats.storageLogicalSize, splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_msg_backlog", stats.msgBacklog,
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_backlog_size",
                stats.managedLedgerStats.backlogSize, splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_offloaded_size", stats.managedLedgerStats
                .offloadedStorageUsed, splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_backlog_quota_limit", stats.backlogQuotaLimit,
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_backlog_quota_limit_time",
                stats.backlogQuotaLimitTime, splitTopicAndPartitionIndexLabel);

        long[] latencyBuckets = stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_0_5", latencyBuckets[0],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_1", latencyBuckets[1],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_5", latencyBuckets[2],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_10", latencyBuckets[3],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_20", latencyBuckets[4],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_50", latencyBuckets[5],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_100", latencyBuckets[6],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_200", latencyBuckets[7],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_1000", latencyBuckets[8],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_overflow", latencyBuckets[9],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_count",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getCount(), splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_sum",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getSum(), splitTopicAndPartitionIndexLabel);

        long[] ledgerWriteLatencyBuckets = stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_0_5",
                ledgerWriteLatencyBuckets[0], splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_1",
                ledgerWriteLatencyBuckets[1], splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_5",
                ledgerWriteLatencyBuckets[2], splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_10",
                ledgerWriteLatencyBuckets[3], splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_20",
                ledgerWriteLatencyBuckets[4], splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_50",
                ledgerWriteLatencyBuckets[5], splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_100",
                ledgerWriteLatencyBuckets[6], splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_200",
                ledgerWriteLatencyBuckets[7], splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_1000",
                ledgerWriteLatencyBuckets[8], splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_overflow",
                ledgerWriteLatencyBuckets[9], splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_count",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount(),
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_sum",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum(),
                splitTopicAndPartitionIndexLabel);

        long[] entrySizeBuckets = stats.managedLedgerStats.entrySizeBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_128", entrySizeBuckets[0],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_512", entrySizeBuckets[1],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_overflow", entrySizeBuckets[8],
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_count",
                stats.managedLedgerStats.entrySizeBuckets.getCount(), splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_sum",
                stats.managedLedgerStats.entrySizeBuckets.getSum(), splitTopicAndPartitionIndexLabel);

        stats.producerStats.forEach((p, producerStats) -> {
            metric(stream, cluster, namespace, topic, p, producerStats.producerId, "pulsar_producer_msg_rate_in",
                    producerStats.msgRateIn, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, p, producerStats.producerId, "pulsar_producer_msg_throughput_in",
                    producerStats.msgThroughputIn, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, p, producerStats.producerId, "pulsar_producer_msg_average_Size",
                    producerStats.averageMsgSize, splitTopicAndPartitionIndexLabel);
        });

        stats.subscriptionStats.forEach((n, subsStats) -> {
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_back_log",
                    subsStats.msgBacklog, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_back_log_no_delayed",
                    subsStats.msgBacklogNoDelayed, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_delayed",
                    subsStats.msgDelayed, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_redeliver",
                    subsStats.msgRateRedeliver, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_unacked_messages",
                    subsStats.unackedMessages, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_blocked_on_unacked_messages",
                    subsStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_out",
                    subsStats.msgRateOut, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_msg_throughput_out",
                    subsStats.msgThroughputOut, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_out_bytes_total",
                    subsStats.bytesOutCounter, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_out_messages_total",
                    subsStats.msgOutCounter, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_last_expire_timestamp",
                    subsStats.lastExpireTimestamp, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_last_acked_timestamp",
                subsStats.lastAckedTimestamp, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_last_consumed_flow_timestamp",
                subsStats.lastConsumedFlowTimestamp, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_last_consumed_timestamp",
                subsStats.lastConsumedTimestamp, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_last_mark_delete_advanced_timestamp",
                subsStats.lastMarkDeleteAdvancedTimestamp, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_expired",
                    subsStats.msgRateExpired, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_total_msg_expired",
                    subsStats.totalMsgExpired, splitTopicAndPartitionIndexLabel);
            subsStats.consumerStat.forEach((c, consumerStats) -> {
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_rate_redeliver", consumerStats.msgRateRedeliver,
                        splitTopicAndPartitionIndexLabel);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_unacked_messages", consumerStats.unackedMessages,
                        splitTopicAndPartitionIndexLabel);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_blocked_on_unacked_messages",
                        consumerStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0,
                        splitTopicAndPartitionIndexLabel);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_rate_out", consumerStats.msgRateOut,
                        splitTopicAndPartitionIndexLabel);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_throughput_out", consumerStats.msgThroughputOut,
                        splitTopicAndPartitionIndexLabel);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_available_permits", consumerStats.availablePermits,
                        splitTopicAndPartitionIndexLabel);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_out_bytes_total", consumerStats.bytesOutCounter,
                        splitTopicAndPartitionIndexLabel);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_out_messages_total", consumerStats.msgOutCounter,
                        splitTopicAndPartitionIndexLabel);
            });
        });

        if (!stats.replicationStats.isEmpty()) {
            stats.replicationStats.forEach((remoteCluster, replStats) -> {
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_rate_in", remoteCluster,
                        replStats.msgRateIn, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_rate_out", remoteCluster,
                        replStats.msgRateOut, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_throughput_in",
                        remoteCluster, replStats.msgThroughputIn, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_throughput_out",
                        remoteCluster, replStats.msgThroughputOut, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_backlog", remoteCluster,
                        replStats.replicationBacklog, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_connected_count",
                        remoteCluster, replStats.connectedCount, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_rate_expired",
                        remoteCluster, replStats.msgRateExpired, splitTopicAndPartitionIndexLabel);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_delay_in_seconds",
                        remoteCluster, replStats.replicationDelayInSeconds, splitTopicAndPartitionIndexLabel);
            });
        }

        metric(stream, cluster, namespace, topic, "pulsar_in_bytes_total", stats.bytesInCounter,
                splitTopicAndPartitionIndexLabel);
        metric(stream, cluster, namespace, topic, "pulsar_in_messages_total", stats.msgInCounter,
                splitTopicAndPartitionIndexLabel);

        // Compaction
        boolean hasCompaction = compactorMXBean.flatMap(mxBean -> mxBean.getCompactionRecordForTopic(topic))
                .map(__ -> true).orElse(false);
        if (hasCompaction) {
            metric(stream, cluster, namespace, topic, "pulsar_compaction_removed_event_count",
                    stats.compactionRemovedEventCount, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_succeed_count",
                    stats.compactionSucceedCount, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_failed_count",
                    stats.compactionFailedCount, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_duration_time_in_mills",
                    stats.compactionDurationTimeInMills, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_read_throughput",
                    stats.compactionReadThroughput, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_write_throughput",
                    stats.compactionWriteThroughput, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_compacted_entries_count",
                    stats.compactionCompactedEntriesCount, splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_compacted_entries_size",
                    stats.compactionCompactedEntriesSize, splitTopicAndPartitionIndexLabel);
            long[] compactionLatencyBuckets = stats.compactionLatencyBuckets.getBuckets();
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_0_5",
                    compactionLatencyBuckets[0], splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_1",
                    compactionLatencyBuckets[1], splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_5",
                    compactionLatencyBuckets[2], splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_10",
                    compactionLatencyBuckets[3], splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_20",
                    compactionLatencyBuckets[4], splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_50",
                    compactionLatencyBuckets[5], splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_100",
                    compactionLatencyBuckets[6], splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_200",
                    compactionLatencyBuckets[7], splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_1000",
                    compactionLatencyBuckets[8], splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_overflow",
                    compactionLatencyBuckets[9], splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_sum",
                    stats.compactionLatencyBuckets.getSum(), splitTopicAndPartitionIndexLabel);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_count",
                    stats.compactionLatencyBuckets.getCount(), splitTopicAndPartitionIndexLabel);
        }
    }

    static void metricType(SimpleTextOutputStream stream, String name) {

        if (!metricWithTypeDefinition.containsKey(name)) {
            metricWithTypeDefinition.put(name, "gauge");
            stream.write("# TYPE ").write(name).write(" gauge\n");
        }

    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
            String name, double value, boolean splitTopicAndPartitionIndexLabel) {
        metricType(stream, name);
        appendRequiredLabels(stream, cluster, namespace, topic, name, splitTopicAndPartitionIndexLabel).write("\"} ");
        stream.write(value);
        appendEndings(stream);
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
           String subscription, String name, long value, boolean splitTopicAndPartitionIndexLabel) {
        metricType(stream, name);
        appendRequiredLabels(stream, cluster, namespace, topic, name, splitTopicAndPartitionIndexLabel)
                .write("\",subscription=\"").write(subscription).write("\"} ");
        stream.write(value);
        appendEndings(stream);
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
            String producerName, long produceId, String name, double value, boolean splitTopicAndPartitionIndexLabel) {
        metricType(stream, name);
        appendRequiredLabels(stream, cluster, namespace, topic, name, splitTopicAndPartitionIndexLabel)
                .write("\",producer_name=\"").write(producerName)
                .write("\",producer_id=\"").write(produceId).write("\"} ");
        stream.write(value);
        appendEndings(stream);
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
            String subscription, String name, double value, boolean splitTopicAndPartitionIndexLabel) {
        metricType(stream, name);
        appendRequiredLabels(stream, cluster, namespace, topic, name, splitTopicAndPartitionIndexLabel)
                .write("\",subscription=\"").write(subscription).write("\"} ");
        stream.write(value);
        appendEndings(stream);
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
            String subscription, String consumerName, long consumerId, String name, long value,
            boolean splitTopicAndPartitionIndexLabel) {
        metricType(stream, name);
        appendRequiredLabels(stream, cluster, namespace, topic, name, splitTopicAndPartitionIndexLabel)
                .write("\",subscription=\"").write(subscription)
                .write("\",consumer_name=\"").write(consumerName).write("\",consumer_id=\"").write(consumerId)
                .write("\"} ");
        stream.write(value);
        appendEndings(stream);
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
            String subscription, String consumerName, long consumerId, String name, double value,
            boolean splitTopicAndPartitionIndexLabel) {
        metricType(stream, name);
        appendRequiredLabels(stream, cluster, namespace, topic, name, splitTopicAndPartitionIndexLabel)
                .write("\",subscription=\"").write(subscription)
                .write("\",consumer_name=\"").write(consumerName).write("\",consumer_id=\"")
                .write(consumerId).write("\"} ");
        stream.write(value);
        appendEndings(stream);
    }

    private static void metricWithRemoteCluster(SimpleTextOutputStream stream, String cluster, String namespace,
            String topic, String name, String remoteCluster, double value, boolean splitTopicAndPartitionIndexLabel) {
        metricType(stream, name);
        appendRequiredLabels(stream, cluster, namespace, topic, name, splitTopicAndPartitionIndexLabel)
                .write("\",remote_cluster=\"").write(remoteCluster).write("\"} ");
        stream.write(value);
        appendEndings(stream);
    }

    private static SimpleTextOutputStream appendRequiredLabels(SimpleTextOutputStream stream, String cluster,
            String namespace, String topic, String name, boolean splitTopicAndPartitionIndexLabel) {
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace);
        if (splitTopicAndPartitionIndexLabel) {
            int index = topic.indexOf(PARTITIONED_TOPIC_SUFFIX);
            if (index > 0) {
                stream.write("\",topic=\"").write(topic.substring(0, index)).write("\",partition=\"")
                        .write(topic.substring(index + PARTITIONED_TOPIC_SUFFIX.length()));
            } else {
                stream.write("\",topic=\"").write(topic).write("\",partition=\"").write("-1");
            }
        } else {
            stream.write("\",topic=\"").write(topic);
        }
        return stream;
    }

    private static void appendEndings(SimpleTextOutputStream stream) {
        stream.write(' ').write(System.currentTimeMillis()).write('\n');
    }
}
