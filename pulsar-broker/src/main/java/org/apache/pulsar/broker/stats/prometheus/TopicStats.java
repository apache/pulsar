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
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.stats.sender.MetricsSender;
import org.apache.pulsar.broker.stats.sender.PulsarMetrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

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

    long storageSize;
    public long msgBacklog;

    long backlogSize;
    long offloadedStorageUsed;

    long backlogQuotaLimit;

    StatsBuckets storageWriteLatencyBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC);
    StatsBuckets storageLedgerWriteLatencyBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC);
    StatsBuckets entrySizeBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES);
    double storageWriteRate;
    double storageReadRate;

    Map<String, AggregatedReplicationStats> replicationStats = new HashMap<>();
    Map<String, AggregatedSubscriptionStats> subscriptionStats = new HashMap<>();

    // Used for tracking duplicate TYPE definitions
    static Map<String, String> metricWithTypeDefinition = new HashMap<>();


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

        storageSize = 0;
        msgBacklog = 0;
        storageWriteRate = 0;
        storageReadRate = 0;
        backlogSize = 0;
        offloadedStorageUsed = 0;
        backlogQuotaLimit = 0;

        replicationStats.clear();
        subscriptionStats.clear();
        storageWriteLatencyBuckets.reset();
        storageLedgerWriteLatencyBuckets.reset();
        entrySizeBuckets.reset();
    }

    static void resetTypes() {
        metricWithTypeDefinition.clear();
    }

    static void printTopicStats(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                                TopicStats stats) {
        metric(stream, cluster, namespace, topic, "pulsar_subscriptions_count", stats.subscriptionsCount);
        metric(stream, cluster, namespace, topic, "pulsar_producers_count", stats.producersCount);
        metric(stream, cluster, namespace, topic, "pulsar_consumers_count", stats.consumersCount);

        metric(stream, cluster, namespace, topic, "pulsar_rate_in", stats.rateIn);
        metric(stream, cluster, namespace, topic, "pulsar_rate_out", stats.rateOut);
        metric(stream, cluster, namespace, topic, "pulsar_throughput_in", stats.throughputIn);
        metric(stream, cluster, namespace, topic, "pulsar_throughput_out", stats.throughputOut);

        metric(stream, cluster, namespace, topic, "pulsar_storage_size", stats.storageSize);
        metric(stream, cluster, namespace, topic, "pulsar_msg_backlog", stats.msgBacklog);
        metric(stream, cluster, namespace, topic, "pulsar_storage_backlog_size", stats.backlogSize);
        metric(stream, cluster, namespace, topic, "pulsar_storage_offloaded_size", stats.offloadedStorageUsed);
        metric(stream, cluster, namespace, topic, "pulsar_storage_backlog_quota_limit", stats.backlogQuotaLimit);

        long[] latencyBuckets = stats.storageWriteLatencyBuckets.getBuckets();
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
                stats.storageWriteLatencyBuckets.getCount());
        metric(stream, cluster, namespace, topic, "pulsar_storage_write_latency_sum",
                stats.storageWriteLatencyBuckets.getSum());

        long[] ledgerWritelatencyBuckets = stats.storageLedgerWriteLatencyBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_0_5",
                ledgerWritelatencyBuckets[0]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_1",
                ledgerWritelatencyBuckets[1]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_5",
                ledgerWritelatencyBuckets[2]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_10",
                ledgerWritelatencyBuckets[3]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_20",
                ledgerWritelatencyBuckets[4]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_50",
                ledgerWritelatencyBuckets[5]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_100",
                ledgerWritelatencyBuckets[6]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_200",
                ledgerWritelatencyBuckets[7]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_le_1000",
                ledgerWritelatencyBuckets[8]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_overflow",
                ledgerWritelatencyBuckets[9]);
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_count",
                stats.storageLedgerWriteLatencyBuckets.getCount());
        metric(stream, cluster, namespace, topic, "pulsar_storage_ledger_write_latency_sum",
                stats.storageLedgerWriteLatencyBuckets.getSum());

        long[] entrySizeBuckets = stats.entrySizeBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_128", entrySizeBuckets[0]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_512", entrySizeBuckets[1]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_le_overflow", entrySizeBuckets[8]);
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_count", stats.entrySizeBuckets.getCount());
        metric(stream, cluster, namespace, topic, "pulsar_entry_size_sum", stats.entrySizeBuckets.getSum());

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
            });
        }

        metric(stream, cluster, namespace, topic, "pulsar_in_bytes_total", stats.bytesInCounter);
        metric(stream, cluster, namespace, topic, "pulsar_in_messages_total", stats.msgInCounter);
    }

    static void printTopicStats(MetricsSender metricsSender, String cluster, String namespace, String topic,
                                TopicStats stats) {
        metric(metricsSender, cluster, namespace, topic, "pulsar_subscriptions_count", stats.subscriptionsCount);
        metric(metricsSender, cluster, namespace, topic, "pulsar_producers_count", stats.producersCount);
        metric(metricsSender, cluster, namespace, topic, "pulsar_consumers_count", stats.consumersCount);

        metric(metricsSender, cluster, namespace, topic, "pulsar_rate_in", stats.rateIn);
        metric(metricsSender, cluster, namespace, topic, "pulsar_rate_out", stats.rateOut);
        metric(metricsSender, cluster, namespace, topic, "pulsar_throughput_in", stats.throughputIn);
        metric(metricsSender, cluster, namespace, topic, "pulsar_throughput_out", stats.throughputOut);

        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_size", stats.storageSize);
        metric(metricsSender, cluster, namespace, topic, "pulsar_msg_backlog", stats.msgBacklog);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_backlog_size", stats.backlogSize);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_offloaded_size", stats.offloadedStorageUsed);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_backlog_quota_limit", stats.backlogQuotaLimit);

        long[] latencyBuckets = stats.storageWriteLatencyBuckets.getBuckets();
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_le_0_5", latencyBuckets[0]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_le_1", latencyBuckets[1]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_le_5", latencyBuckets[2]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_le_10", latencyBuckets[3]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_le_20", latencyBuckets[4]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_le_50", latencyBuckets[5]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_le_100", latencyBuckets[6]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_le_200", latencyBuckets[7]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_le_1000", latencyBuckets[8]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_overflow", latencyBuckets[9]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_count",
                stats.storageWriteLatencyBuckets.getCount());
        metric(metricsSender, cluster, namespace, topic, "pulsar_storage_write_latency_sum",
                stats.storageWriteLatencyBuckets.getSum());

        long[] entrySizeBuckets = stats.entrySizeBuckets.getBuckets();
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_le_128", entrySizeBuckets[0]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_le_512", entrySizeBuckets[1]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_le_overflow", entrySizeBuckets[8]);
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_count", stats.entrySizeBuckets.getCount());
        metric(metricsSender, cluster, namespace, topic, "pulsar_entry_size_sum", stats.entrySizeBuckets.getSum());

        stats.subscriptionStats.forEach((n, subsStats) -> {
            metric(metricsSender, cluster, namespace, topic, n, "pulsar_subscription_back_log", subsStats.msgBacklog);
            metric(metricsSender, cluster, namespace, topic, n, "pulsar_subscription_back_log_no_delayed",
                    subsStats.msgBacklogNoDelayed);
            metric(metricsSender, cluster, namespace, topic, n, "pulsar_subscription_delayed", subsStats.msgDelayed);
            metric(metricsSender, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_redeliver",
                    subsStats.msgRateRedeliver);
            metric(metricsSender, cluster, namespace, topic, n, "pulsar_subscription_unacked_messages",
                    subsStats.unackedMessages);
            metric(metricsSender, cluster, namespace, topic, n, "pulsar_subscription_blocked_on_unacked_messages",
                    subsStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0);
            metric(metricsSender, cluster, namespace, topic, n, "pulsar_subscription_msg_rate_out",
                    subsStats.msgRateOut);
            metric(metricsSender, cluster, namespace, topic, n, "pulsar_subscription_msg_throughput_out",
                    subsStats.msgThroughputOut);
            metric(metricsSender, cluster, namespace, topic, n, "pulsar_out_bytes_total", subsStats.bytesOutCounter);
            metric(metricsSender, cluster, namespace, topic, n, "pulsar_out_messages_total", subsStats.msgOutCounter);
            subsStats.consumerStat.forEach((c, consumerStats) -> {
                metric(metricsSender, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_rate_redeliver", consumerStats.msgRateRedeliver);
                metric(metricsSender, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_unacked_messages", consumerStats.unackedMessages);
                metric(metricsSender, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_blocked_on_unacked_messages",
                        consumerStats.blockedSubscriptionOnUnackedMsgs ? 1 : 0);
                metric(metricsSender, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_rate_out", consumerStats.msgRateOut);
                metric(metricsSender, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_msg_throughput_out", consumerStats.msgThroughputOut);
                metric(metricsSender, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_consumer_available_permits", consumerStats.availablePermits);
                metric(metricsSender, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_out_bytes_total", consumerStats.bytesOutCounter);
                metric(metricsSender, cluster, namespace, topic, n, c.consumerName(), c.consumerId(),
                        "pulsar_out_messages_total", consumerStats.msgOutCounter);
            });
        });

        if (!stats.replicationStats.isEmpty()) {
            stats.replicationStats.forEach((remoteCluster, replStats) -> {
                metricWithRemoteCluster(metricsSender, cluster, namespace, topic, "pulsar_replication_rate_in",
                        remoteCluster, replStats.msgRateIn);
                metricWithRemoteCluster(metricsSender, cluster, namespace, topic, "pulsar_replication_rate_out",
                        remoteCluster, replStats.msgRateOut);
                metricWithRemoteCluster(metricsSender, cluster, namespace, topic, "pulsar_replication_throughput_in",
                        remoteCluster, replStats.msgThroughputIn);
                metricWithRemoteCluster(metricsSender, cluster, namespace, topic, "pulsar_replication_throughput_out",
                        remoteCluster,
                        replStats.msgThroughputOut);
                metricWithRemoteCluster(metricsSender, cluster, namespace, topic, "pulsar_replication_backlog",
                        remoteCluster, replStats.replicationBacklog);
            });
        }

        metric(metricsSender, cluster, namespace, topic, "pulsar_in_bytes_total", stats.bytesInCounter);
        metric(metricsSender, cluster, namespace, topic, "pulsar_in_messages_total", stats.msgInCounter);
    }

    static void metricType(SimpleTextOutputStream stream, String name) {

        if (!metricWithTypeDefinition.containsKey(name)) {
            metricWithTypeDefinition.put(name, "gauge");
            stream.write("# TYPE ").write(name).write(" gauge\n");
        }

    }

    static String metricType(String name) {
        if (!metricWithTypeDefinition.containsKey(name)) {
            metricWithTypeDefinition.put(name, "gauge");
            return "# TYPE " + name + " gauge\n";
        }
        return "";
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                               String name, double value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(MetricsSender metricsSender, String cluster, String namespace, String topic,
                               String name, double value) {
        String head = metricType(name);
        String body = name + "{cluster=\"" + cluster + "\",namespace=\"" + namespace + "\",topic=\"" + topic + "\"} "
                + value + " " + System.currentTimeMillis();

        metricsSender.send(new PulsarMetrics(head, body));
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                               String subscription, String name, long value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\",subscription=\"").write(subscription).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(MetricsSender metricsSender, String cluster, String namespace, String topic,
                               String subscription, String name, long value) {
        String head = metricType(name);
        String body = name + "{cluster=\"" + cluster + "\",namespace=\"" + namespace
                + "\",topic=\"" + topic + "\",subscription=\"" + subscription + "\"} "
                + value + " " + System.currentTimeMillis();
        metricsSender.send(new PulsarMetrics(head, body));
    }


    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String topic,
                               String subscription, String name, double value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\",subscription=\"").write(subscription).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(MetricsSender metricsSender, String cluster, String namespace, String topic,
                               String subscription, String name, double value) {
        String head = metricType(name);
        String body = name + "{cluster=\"" + cluster + "\",namespace=\"" + namespace
                + "\",topic=\"" + topic + "\",subscription=\"" + subscription + "\"} "
                + value + " " + System.currentTimeMillis();
        metricsSender.send(new PulsarMetrics(head, body));
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

    private static void metric(MetricsSender metricsSender, String cluster, String namespace, String topic,
                               String subscription, String consumerName, long consumerId, String name, long value) {
        String head = metricType(name);
        String body = name + "{cluster=\"" + cluster + "\", namespace=\"" + namespace
                + "\",topic=\"" + topic + "\",subscription=\"" + subscription
                + "\",consumer_name=\"" + consumerName + "\",consumer_id=\"" + consumerId + "\"} "
                + value + " " + System.currentTimeMillis();
        metricsSender.send(new PulsarMetrics(head, body));
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

    private static void metric(MetricsSender metricsSender, String cluster, String namespace, String topic,
                               String subscription, String consumerName, long consumerId, String name, double value) {
        String head = metricType(name);
        String body = name + "{cluster=\"" + cluster + "\",namespace=\"" + namespace
                + "\",topic=\"" + topic + "\",subscription=\"" + subscription
                + "\",consumer_name=\"" + consumerName + "\",consumer_id=\"" + consumerId + "\"} "
                + value + " " + System.currentTimeMillis();
        metricsSender.send(new PulsarMetrics(head, body));
    }

    private static void metricWithRemoteCluster(SimpleTextOutputStream stream, String cluster, String namespace,
                                                String topic,
            String name, String remoteCluster, double value) {
        metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace);
        stream.write("\",topic=\"").write(topic).write("\",remote_cluster=\"").write(remoteCluster).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metricWithRemoteCluster(MetricsSender metricsSender, String cluster, String namespace,
                                                String topic,
                                                String name, String remoteCluster, double value) {
        String head = metricType(name);
        String body = name + "{cluster=\"" + cluster + "\",namespace=\"" + namespace
                + "\",topic=\"" + topic + "\",remote_cluster=\"" + remoteCluster + "\"} "
                + value + " " + System.currentTimeMillis();
        metricsSender.send(new PulsarMetrics(head, body));
    }
}
