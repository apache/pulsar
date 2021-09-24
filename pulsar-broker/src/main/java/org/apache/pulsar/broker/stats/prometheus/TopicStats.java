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
                                TopicStats stats, Optional<CompactorMXBean> compactorMXBean) {
        metric(stream, cluster, namespace, topic, "pulsar_subscriptions_count", stats.subscriptionsCount);
        metric(stream, cluster, namespace, topic, "pulsar_producers_count", stats.producersCount);
        metric(stream, cluster, namespace, topic, "pulsar_consumers_count", stats.consumersCount);

        metric(stream, cluster, namespace, topic, "pulsar_rate_in", stats.rateIn);
        metric(stream, cluster, namespace, topic, "pulsar_rate_out", stats.rateOut);
        metric(stream, cluster, namespace, topic, "pulsar_throughput_in", stats.throughputIn);
        metric(stream, cluster, namespace, topic, "pulsar_throughput_out", stats.throughputOut);
        metric(stream, cluster, namespace, topic, "pulsar_average_msg_size", stats.averageMsgSize);

        metric(stream, cluster, namespace, topic, "pulsar_storage_size", stats.managedLedgerStats.storageSize);
        metric(stream, cluster, namespace, topic, "pulsar_storage_logical_size",
                stats.managedLedgerStats.storageLogicalSize);
        metric(stream, cluster, namespace, topic, "pulsar_msg_backlog", stats.msgBacklog);
        metric(stream, cluster, namespace, topic, "pulsar_storage_backlog_size",
                stats.managedLedgerStats.backlogSize);
        metric(stream, cluster, namespace, topic, "pulsar_storage_offloaded_size", stats.managedLedgerStats
                .offloadedStorageUsed);
        metric(stream, cluster, namespace, topic, "pulsar_storage_backlog_quota_limit", stats.backlogQuotaLimit);
        metric(stream, cluster, namespace, topic, "pulsar_storage_backlog_quota_limit_time",
                stats.backlogQuotaLimitTime);

        long[] latencyBuckets = stats.managedLedgerStats.storageWriteLatencyBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_0_5", latencyBuckets[0]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_1", latencyBuckets[1]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_5", latencyBuckets[2]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_10", latencyBuckets[3]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_20", latencyBuckets[4]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_50", latencyBuckets[5]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_100", latencyBuckets[6]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_200", latencyBuckets[7]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_le_1000", latencyBuckets[8]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_overflow", latencyBuckets[9]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_count",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getCount());
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_sum",
                stats.managedLedgerStats.storageWriteLatencyBuckets.getSum());

        long[] ledgerWriteLatencyBuckets = stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_0_5",
                ledgerWriteLatencyBuckets[0]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_1",
                ledgerWriteLatencyBuckets[1]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_5",
                ledgerWriteLatencyBuckets[2]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_10",
                ledgerWriteLatencyBuckets[3]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_20",
                ledgerWriteLatencyBuckets[4]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_50",
                ledgerWriteLatencyBuckets[5]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_100",
                ledgerWriteLatencyBuckets[6]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_200",
                ledgerWriteLatencyBuckets[7]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_1000",
                ledgerWriteLatencyBuckets[8]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_overflow",
                ledgerWriteLatencyBuckets[9]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_count",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getCount());
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_sum",
                stats.managedLedgerStats.storageLedgerWriteLatencyBuckets.getSum());

        long[] entrySizeBuckets = stats.managedLedgerStats.entrySizeBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_128", entrySizeBuckets[0]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_512", entrySizeBuckets[1]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_overflow", entrySizeBuckets[8]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_count",
                stats.managedLedgerStats.entrySizeBuckets.getCount());
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_sum",
                stats.managedLedgerStats.entrySizeBuckets.getSum());

        stats.producerStats.forEach((p, producerStats) -> {
            metric(stream, cluster, namespace, topic, p, producerStats.producerId, "pulsar_producer_msg_rate_in",
                    producerStats.msgRateIn);
            metric(stream, cluster, namespace, topic, p, producerStats.producerId, "pulsar_producer_msg_throughput_in",
                    producerStats.msgThroughputIn);
            metric(stream, cluster, namespace, topic, p, producerStats.producerId, "pulsar_producer_msg_average_Size",
                    producerStats.averageMsgSize);
        });

        stats.subscriptionStats.forEach((n, subsStats) -> {
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_back_log",
                    subsStats.msgBacklog);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_back_log_no_delayed",
                    subsStats.msgBacklogNoDelayed);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_delayed",
                    subsStats.msgDelayed);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_redeliver",
                    subsStats.msgRateRedeliver);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_unacked_messages",
                    subsStats.unackedMessages);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_blocked_on_unacked_messages",
                    subsStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_out",
                    subsStats.msgRateOut);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_msg_throughput_out",
                    subsStats.msgThroughputOut);
            metric(stream, cluster, namespace, topic, n, "pulsar_out_bytes_total",
                    subsStats.bytesOutCounter);
            metric(stream, cluster, namespace, topic, n, "pulsar_out_messages_total",
                    subsStats.msgOutCounter);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_last_expire_timestamp",
                    subsStats.lastExpireTimestamp);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_last_acked_timestamp",
                subsStats.lastAckedTimestamp);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_last_consumed_flow_timestamp",
                subsStats.lastConsumedFlowTimestamp);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_last_consumed_timestamp",
                subsStats.lastConsumedTimestamp);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_last_mark_delete_advanced_timestamp",
                subsStats.lastMarkDeleteAdvancedTimestamp);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_expired",
                    subsStats.msgRateExpired);
            metric(stream, cluster, namespace, topic, n, "pulsar_subscription_total_msg_expired",
                    subsStats.totalMsgExpired);
            subsStats.consumerStat.forEach((c, consumerStats) -> {
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_rate_redeliver", consumerStats.msgRateRedeliver);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_unacked_messages", consumerStats.unackedMessages);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_blocked_on_unacked_messages",
                        consumerStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_rate_out", consumerStats.msgRateOut);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_throughput_out", consumerStats.msgThroughputOut);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_available_permits", consumerStats.availablePermits);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_out_bytes_total", consumerStats.bytesOutCounter);
                metric(stream, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_out_messages_total", consumerStats.msgOutCounter);
            });
        });

        if (!stats.replicationStats.isEmpty()) {
            stats.replicationStats.forEach((remoteCluster, replStats) -> {
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_rate_in", remoteCluster,
                        replStats.msgRateIn);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_rate_out", remoteCluster,
                        replStats.msgRateOut);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_throughput_in",
                        remoteCluster,
                        replStats.msgThroughputIn);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_throughput_out",
                        remoteCluster,
                        replStats.msgThroughputOut);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_backlog", remoteCluster,
                        replStats.replicationBacklog);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_connected_count",
                        remoteCluster, replStats.connectedCount);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_rate_expired",
                        remoteCluster, replStats.msgRateExpired);
                metricWithRemoteCluster(stream, cluster, namespace, topic, "pulsar_replication_delay_in_seconds",
                        remoteCluster, replStats.replicationDelayInSeconds);
            });
        }

        metric(stream, cluster, namespace, topic, "pulsar_in_bytes_total", stats.bytesInCounter);
        metric(stream, cluster, namespace, topic, "pulsar_in_messages_total", stats.msgInCounter);

        // Compaction
        boolean hasCompaction = compactorMXBean.flatMap(mxBean -> mxBean.getCompactionRecordForTopic(topic))
                .map(__ -> true).orElse(false);
        if (hasCompaction) {
            metric(stream, cluster, namespace, topic, "pulsar_compaction_removed_event_count",
                    stats.compactionRemovedEventCount);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_succeed_count",
                    stats.compactionSucceedCount);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_failed_count",
                    stats.compactionFailedCount);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_duration_time_in_mills",
                    stats.compactionDurationTimeInMills);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_read_throughput",
                    stats.compactionReadThroughput);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_write_throughput",
                    stats.compactionWriteThroughput);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_compacted_entries_count",
                    stats.compactionCompactedEntriesCount);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_compacted_entries_size",
                    stats.compactionCompactedEntriesSize);
            long[] compactionLatencyBuckets = stats.compactionLatencyBuckets.getBuckets();
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_0_5",
                    compactionLatencyBuckets[0]);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_1",
                    compactionLatencyBuckets[1]);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_5",
                    compactionLatencyBuckets[2]);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_10",
                    compactionLatencyBuckets[3]);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_20",
                    compactionLatencyBuckets[4]);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_50",
                    compactionLatencyBuckets[5]);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_100",
                    compactionLatencyBuckets[6]);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_200",
                    compactionLatencyBuckets[7]);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_le_1000",
                    compactionLatencyBuckets[8]);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_overflow",
                    compactionLatencyBuckets[9]);
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_sum",
                    stats.compactionLatencyBuckets.getSum());
            metric(stream, cluster, namespace, topic, "pulsar_compaction_latency_count",
                    stats.compactionLatencyBuckets.getCount());
        }
    }

    static void metricType(SimpleTextOutputStream stream, String name) {

        if (!metricWithTypeDefinition.containsKey(name)) {
            metricWithTypeDefinition.put(name, "gauge");
            stream.write("# TYPE ").write(name).write(" gauge\n");
        }

    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                               String name, double value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                               String subscription, String name, long value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\",subscription=\"").write(subscription).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                               String producerName, long produceId, String name, double value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\",producer_name=\"").write(producerName)
                .write("\",producer_id=\"").write(produceId).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                               String subscription, String name, double value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\",subscription=\"").write(subscription).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                               String subscription, String consumerName, long consumerId, String name, long value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\", namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\",subscription=\"").write(subscription)
                .write("\",consumer_name=\"").write(consumerName).write("\",consumer_id=\"").write(consumerId)
                .write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                               String subscription, String consumerName, long consumerId, String name, double value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\",subscription=\"").write(subscription)
                .write("\",consumer_name=\"").write(consumerName).write("\",consumer_id=\"")
                .write(consumerId).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metricWithRemoteCluster(SimpleTextOutputStream stream, String cluster, String namespace,
                                                String topic,
            String name, String remoteCluster, double value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace);
        stream.write("\",topic=\"").write(topic).write("\",remote_cluster=\"").write(remoteCluster).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }
}
