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
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.sender.MetricsSender;
import org.apache.pulsar.broker.stats.sender.PulsarMetrics;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

public class NamespaceStatsAggregator {

    private static FastThreadLocal<AggregatedNamespaceStats> localNamespaceStats =
            new FastThreadLocal<AggregatedNamespaceStats>() {
                @Override
                protected AggregatedNamespaceStats initialValue() throws Exception {
                    return new AggregatedNamespaceStats();
                }
            };

    private static FastThreadLocal<TopicStats> localTopicStats = new FastThreadLocal<TopicStats>() {
        @Override
        protected TopicStats initialValue() throws Exception {
            return new TopicStats();
        }
    };

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                SimpleTextOutputStream stream) {
        String cluster = pulsar.getConfiguration().getClusterName();
        AggregatedNamespaceStats namespaceStats = localNamespaceStats.get();
        TopicStats.resetTypes();
        TopicStats topicStats = localTopicStats.get();

        printDefaultBrokerStats(stream, cluster);

        LongAdder topicsCount = new LongAdder();
        pulsar.getBrokerService().getMultiLayerTopicMap().forEach((namespace, bundlesMap) -> {
            namespaceStats.reset();
            topicsCount.reset();

            bundlesMap.forEach((bundle, topicsMap) -> {
                topicsMap.forEach((name, topic) -> {
                    getTopicStats(topic, topicStats, includeConsumerMetrics,
                            pulsar.getConfiguration().isExposePreciseBacklogInPrometheus());

                    if (includeTopicMetrics) {
                        topicsCount.add(1);
                        TopicStats.printTopicStats(stream, cluster, namespace, name, topicStats);
                    } else {
                        namespaceStats.updateStats(topicStats);
                    }
                });
            });

            if (!includeTopicMetrics) {
                // Only include namespace level stats if we don't have the per-topic, otherwise we're going to report
                // the same data twice, and it will make the aggregation difficult
                printNamespaceStats(stream, cluster, namespace, namespaceStats);
            } else {
                printTopicsCountStats(stream, cluster, namespace, topicsCount);
            }
        });
    }

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics, MetricsSender metricsSender) {
        String cluster = pulsar.getConfiguration().getClusterName();
        AggregatedNamespaceStats namespaceStats = localNamespaceStats.get();
        TopicStats.resetTypes();
        TopicStats topicStats = localTopicStats.get();

        printDefaultBrokerStats(metricsSender, cluster);

        LongAdder topicsCount = new LongAdder();

        pulsar.getBrokerService().getMultiLayerTopicMap().forEach((namespace, bundlesMap) -> {
            namespaceStats.reset();

            bundlesMap.forEach((bundle, topicsMap) -> {
                topicsMap.forEach((name, topic) -> {
                    getTopicStats(topic, topicStats, includeConsumerMetrics, pulsar.getConfiguration().isExposePreciseBacklogInPrometheus());

                    if (includeTopicMetrics) {
                        topicsCount.add(1);
                        TopicStats.printTopicStats(metricsSender, cluster, namespace, name, topicStats);
                    } else {
                        namespaceStats.updateStats(topicStats);
                    }
                });
            });

            if (!includeTopicMetrics) {
                // Only include namespace level stats if we don't have the per-topic, otherwise we're going to report
                // the same data twice, and it will make the aggregation difficult
                printNamespaceStats(metricsSender, cluster, namespace, namespaceStats);
            } else {
                printTopicsCountStats(metricsSender, cluster, namespace, topicsCount);
            }
        });
    }

    private static void getTopicStats(Topic topic, TopicStats stats, boolean includeConsumerMetrics,
                                      boolean getPreciseBacklog) {
        stats.reset();

        if (topic instanceof PersistentTopic) {
            // Managed Ledger stats
            ManagedLedger ml = ((PersistentTopic) topic).getManagedLedger();
            ManagedLedgerMBeanImpl mlStats = (ManagedLedgerMBeanImpl) ml.getStats();

            stats.storageSize = mlStats.getStoredMessagesSize();
            stats.backlogSize = ml.getEstimatedBacklogSize();
            stats.offloadedStorageUsed = ml.getOffloadedSize();
            stats.backlogQuotaLimit = topic.getBacklogQuota().getLimit();

            stats.storageWriteLatencyBuckets.addAll(mlStats.getInternalAddEntryLatencyBuckets());
            stats.storageWriteLatencyBuckets.refresh();
            stats.storageLedgerWriteLatencyBuckets.addAll(mlStats.getInternalLedgerAddEntryLatencyBuckets());
            stats.storageLedgerWriteLatencyBuckets.refresh();

            stats.entrySizeBuckets.addAll(mlStats.getInternalEntrySizeBuckets());
            stats.entrySizeBuckets.refresh();

            stats.storageWriteRate = mlStats.getAddEntryMessagesRate();
            stats.storageReadRate = mlStats.getReadEntriesRate();
        }

        org.apache.pulsar.common.policies.data.TopicStats tStatus = topic.getStats(getPreciseBacklog);
        stats.msgInCounter = tStatus.msgInCounter;
        stats.bytesInCounter = tStatus.bytesInCounter;
        stats.msgOutCounter = tStatus.msgOutCounter;
        stats.bytesOutCounter = tStatus.bytesOutCounter;

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
            }
        });

        tStatus.subscriptions.forEach((subName, subscriptionStats) -> {
            stats.subscriptionsCount++;
            stats.msgBacklog += subscriptionStats.msgBacklog;

            AggregatedSubscriptionStats subsStats = stats.subscriptionStats
                    .computeIfAbsent(subName, k -> new AggregatedSubscriptionStats());
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
            subscriptionStats.consumers.forEach(cStats -> {
                stats.consumersCount++;
                subsStats.unackedMessages += cStats.unackedMessages;
                subsStats.msgRateRedeliver += cStats.msgRateRedeliver;
                subsStats.msgRateOut += cStats.msgRateOut;
                subsStats.msgThroughputOut += cStats.msgThroughputOut;
                subsStats.bytesOutCounter += cStats.bytesOutCounter;
                subsStats.msgOutCounter += cStats.msgOutCounter;
                if (!subsStats.blockedSubscriptionOnUnackedMsgs && cStats.blockedConsumerOnUnackedMsgs) {
                    subsStats.blockedSubscriptionOnUnackedMsgs = true;
                }
            });
            stats.rateOut += subsStats.msgRateOut;
            stats.throughputOut += subsStats.msgThroughputOut;
        });

        // Consumer stats can be a lot if a subscription has many consumers
        if (includeConsumerMetrics) {
            topic.getSubscriptions().forEach((name, subscription) -> {
                AggregatedSubscriptionStats subsStats = stats.subscriptionStats
                        .computeIfAbsent(name, k -> new AggregatedSubscriptionStats());
                subscription.getConsumers().forEach(consumer -> {
                    ConsumerStats conStats = consumer.getStats();

                    AggregatedConsumerStats consumerStats = subsStats.consumerStat
                            .computeIfAbsent(consumer, k -> new AggregatedConsumerStats());

                    consumerStats.unackedMessages = conStats.unackedMessages;
                    consumerStats.msgRateRedeliver = conStats.msgRateRedeliver;
                    consumerStats.msgRateOut = conStats.msgRateOut;
                    consumerStats.msgThroughputOut = conStats.msgThroughputOut;
                    consumerStats.bytesOutCounter = conStats.bytesOutCounter;
                    consumerStats.msgOutCounter = conStats.msgOutCounter;
                    consumerStats.availablePermits = conStats.availablePermits;
                    consumerStats.blockedSubscriptionOnUnackedMsgs = conStats.blockedConsumerOnUnackedMsgs;
                });
            });
        }

        topic.getReplicators().forEach((cluster, replicator) -> {
            AggregatedReplicationStats aggReplStats = stats.replicationStats.computeIfAbsent(cluster,
                    k -> new AggregatedReplicationStats());

            ReplicatorStats replStats = replicator.getStats();
            aggReplStats.msgRateOut += replStats.msgRateOut;
            aggReplStats.msgThroughputOut += replStats.msgThroughputOut;
            aggReplStats.replicationBacklog += replStats.replicationBacklog;
        });
    }

    private static void printDefaultBrokerStats(SimpleTextOutputStream stream, String cluster) {
        // Print metrics with 0 values. This is necessary to have the available brokers being
        // reported in the brokers dashboard even if they don't have any topic or traffic
        metric(stream, cluster, "pulsar_topics_count", 0);
        metric(stream, cluster, "pulsar_subscriptions_count", 0);
        metric(stream, cluster, "pulsar_producers_count", 0);
        metric(stream, cluster, "pulsar_consumers_count", 0);
        metric(stream, cluster, "pulsar_rate_in", 0);
        metric(stream, cluster, "pulsar_rate_out", 0);
        metric(stream, cluster, "pulsar_throughput_in", 0);
        metric(stream, cluster, "pulsar_throughput_out", 0);
        metric(stream, cluster, "pulsar_storage_size", 0);
        metric(stream, cluster, "pulsar_storage_write_rate", 0);
        metric(stream, cluster, "pulsar_storage_read_rate", 0);
        metric(stream, cluster, "pulsar_msg_backlog", 0);
    }

    private static void printDefaultBrokerStats(MetricsSender metricsSender, String cluster) {
        // Print metrics with 0 values. This is necessary to have the available brokers being
        // reported in the brokers dashboard even if they don't have any topic or traffic
        metric(metricsSender, cluster, "pulsar_topics_count", 0);
        metric(metricsSender, cluster, "pulsar_subscriptions_count", 0);
        metric(metricsSender, cluster, "pulsar_producers_count", 0);
        metric(metricsSender, cluster, "pulsar_consumers_count", 0);
        metric(metricsSender, cluster, "pulsar_rate_in", 0);
        metric(metricsSender, cluster, "pulsar_rate_out", 0);
        metric(metricsSender, cluster, "pulsar_throughput_in", 0);
        metric(metricsSender, cluster, "pulsar_throughput_out", 0);
        metric(metricsSender, cluster, "pulsar_storage_size", 0);
        metric(metricsSender, cluster, "pulsar_storage_write_rate", 0);
        metric(metricsSender, cluster, "pulsar_storage_read_rate", 0);
        metric(metricsSender, cluster, "pulsar_msg_backlog", 0);
    }

    private static void printTopicsCountStats(SimpleTextOutputStream stream, String cluster, String namespace,
                                              LongAdder topicsCount) {
        metric(stream, cluster, namespace, "pulsar_topics_count", topicsCount.sum());
    }

    private static void printTopicsCountStats(MetricsSender metricsSender, String cluster, String namespace,
                                              LongAdder topicsCount) {
        metric(metricsSender, cluster, namespace, "pulsar_topics_count", topicsCount.sum());
    }

    private static void printNamespaceStats(SimpleTextOutputStream stream, String cluster, String namespace,
                                            AggregatedNamespaceStats stats) {
        metric(stream, cluster, namespace, "pulsar_topics_count", stats.topicsCount);
        metric(stream, cluster, namespace, "pulsar_subscriptions_count", stats.subscriptionsCount);
        metric(stream, cluster, namespace, "pulsar_producers_count", stats.producersCount);
        metric(stream, cluster, namespace, "pulsar_consumers_count", stats.consumersCount);

        metric(stream, cluster, namespace, "pulsar_rate_in", stats.rateIn);
        metric(stream, cluster, namespace, "pulsar_rate_out", stats.rateOut);
        metric(stream, cluster, namespace, "pulsar_throughput_in", stats.throughputIn);
        metric(stream, cluster, namespace, "pulsar_throughput_out", stats.throughputOut);

        metric(stream, cluster, namespace, "pulsar_in_bytes_total", stats.bytesInCounter);
        metric(stream, cluster, namespace, "pulsar_in_messages_total", stats.msgInCounter);
        metric(stream, cluster, namespace, "pulsar_out_bytes_total", stats.bytesOutCounter);
        metric(stream, cluster, namespace, "pulsar_out_messages_total", stats.msgOutCounter);

        metric(stream, cluster, namespace, "pulsar_storage_size", stats.storageSize);
        metric(stream, cluster, namespace, "pulsar_storage_backlog_size", stats.backlogSize);
        metric(stream, cluster, namespace, "pulsar_storage_offloaded_size", stats.offloadedStorageUsed);

        metric(stream, cluster, namespace, "pulsar_storage_write_rate", stats.storageWriteRate);
        metric(stream, cluster, namespace, "pulsar_storage_read_rate", stats.storageReadRate);

        metric(stream, cluster, namespace, "pulsar_subscription_delayed", stats.msgDelayed);

        metricWithRemoteCluster(stream, cluster, namespace, "pulsar_msg_backlog", "local", stats.msgBacklog);

        stats.storageWriteLatencyBuckets.refresh();
        long[] latencyBuckets = stats.storageWriteLatencyBuckets.getBuckets();
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_le_0_5", latencyBuckets[0]);
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_le_1", latencyBuckets[1]);
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_le_5", latencyBuckets[2]);
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_le_10", latencyBuckets[3]);
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_le_20", latencyBuckets[4]);
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_le_50", latencyBuckets[5]);
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_le_100", latencyBuckets[6]);
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_le_200", latencyBuckets[7]);
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_le_1000", latencyBuckets[8]);
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_overflow", latencyBuckets[9]);
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_count",
                stats.storageWriteLatencyBuckets.getCount());
        metric(stream, cluster, namespace, "pulsar_storage_write_latency_sum",
                stats.storageWriteLatencyBuckets.getSum());

        stats.storageLedgerWriteLatencyBuckets.refresh();
        long[] ledgerWritelatencyBuckets = stats.storageLedgerWriteLatencyBuckets.getBuckets();
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_le_0_5", ledgerWritelatencyBuckets[0]);
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_le_1", ledgerWritelatencyBuckets[1]);
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_le_5", ledgerWritelatencyBuckets[2]);
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_le_10", ledgerWritelatencyBuckets[3]);
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_le_20", ledgerWritelatencyBuckets[4]);
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_le_50", ledgerWritelatencyBuckets[5]);
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_le_100", ledgerWritelatencyBuckets[6]);
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_le_200", ledgerWritelatencyBuckets[7]);
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_le_1000", ledgerWritelatencyBuckets[8]);
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_overflow",
                ledgerWritelatencyBuckets[9]);
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_count",
                stats.storageLedgerWriteLatencyBuckets.getCount());
        metric(stream, cluster, namespace, "pulsar_storage_ledger_write_latency_sum",
                stats.storageLedgerWriteLatencyBuckets.getSum());

        stats.entrySizeBuckets.refresh();
        long[] entrySizeBuckets = stats.entrySizeBuckets.getBuckets();
        metric(stream, cluster, namespace, "pulsar_entry_size_le_128", entrySizeBuckets[0]);
        metric(stream, cluster, namespace, "pulsar_entry_size_le_512", entrySizeBuckets[1]);
        metric(stream, cluster, namespace, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2]);
        metric(stream, cluster, namespace, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3]);
        metric(stream, cluster, namespace, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4]);
        metric(stream, cluster, namespace, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5]);
        metric(stream, cluster, namespace, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6]);
        metric(stream, cluster, namespace, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7]);
        metric(stream, cluster, namespace, "pulsar_entry_size_le_overflow", entrySizeBuckets[8]);
        metric(stream, cluster, namespace, "pulsar_entry_size_count", stats.entrySizeBuckets.getCount());
        metric(stream, cluster, namespace, "pulsar_entry_size_sum", stats.entrySizeBuckets.getSum());

        if (!stats.replicationStats.isEmpty()) {
            stats.replicationStats.forEach((remoteCluster, replStats) -> {
                metricWithRemoteCluster(stream, cluster, namespace, "pulsar_replication_rate_in", remoteCluster,
                        replStats.msgRateIn);
                metricWithRemoteCluster(stream, cluster, namespace, "pulsar_replication_rate_out", remoteCluster,
                        replStats.msgRateOut);
                metricWithRemoteCluster(stream, cluster, namespace, "pulsar_replication_throughput_in", remoteCluster,
                        replStats.msgThroughputIn);
                metricWithRemoteCluster(stream, cluster, namespace, "pulsar_replication_throughput_out", remoteCluster,
                        replStats.msgThroughputOut);
                metricWithRemoteCluster(stream, cluster, namespace, "pulsar_replication_backlog", remoteCluster,
                        replStats.replicationBacklog);
            });
        }
    }

    private static void printNamespaceStats(MetricsSender metricsSender, String cluster, String namespace,
                                            AggregatedNamespaceStats stats) {
        metric(metricsSender, cluster, namespace, "pulsar_topics_count", stats.topicsCount);
        metric(metricsSender, cluster, namespace, "pulsar_subscriptions_count", stats.subscriptionsCount);
        metric(metricsSender, cluster, namespace, "pulsar_producers_count", stats.producersCount);
        metric(metricsSender, cluster, namespace, "pulsar_consumers_count", stats.consumersCount);

        metric(metricsSender, cluster, namespace, "pulsar_rate_in", stats.rateIn);
        metric(metricsSender, cluster, namespace, "pulsar_rate_out", stats.rateOut);
        metric(metricsSender, cluster, namespace, "pulsar_throughput_in", stats.throughputIn);
        metric(metricsSender, cluster, namespace, "pulsar_throughput_out", stats.throughputOut);

        metric(metricsSender, cluster, namespace, "pulsar_in_bytes_total", stats.bytesInCounter);
        metric(metricsSender, cluster, namespace, "pulsar_in_messages_total", stats.msgInCounter);
        metric(metricsSender, cluster, namespace, "pulsar_out_bytes_total", stats.bytesOutCounter);
        metric(metricsSender, cluster, namespace, "pulsar_out_messages_total", stats.msgOutCounter);

        metric(metricsSender, cluster, namespace, "pulsar_storage_size", stats.storageSize);
        metric(metricsSender, cluster, namespace, "pulsar_storage_backlog_size", stats.backlogSize);
        metric(metricsSender, cluster, namespace, "pulsar_storage_offloaded_size", stats.offloadedStorageUsed);

        metric(metricsSender, cluster, namespace, "pulsar_storage_write_rate", stats.storageWriteRate);
        metric(metricsSender, cluster, namespace, "pulsar_storage_read_rate", stats.storageReadRate);

        metric(metricsSender, cluster, namespace, "pulsar_subscription_delayed", stats.msgDelayed);

        metricWithRemoteCluster(metricsSender, cluster, namespace, "pulsar_msg_backlog", "local", stats.msgBacklog);

        stats.storageWriteLatencyBuckets.refresh();
        long[] latencyBuckets = stats.storageWriteLatencyBuckets.getBuckets();
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_le_0_5", latencyBuckets[0]);
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_le_1", latencyBuckets[1]);
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_le_5", latencyBuckets[2]);
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_le_10", latencyBuckets[3]);
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_le_20", latencyBuckets[4]);
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_le_50", latencyBuckets[5]);
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_le_100", latencyBuckets[6]);
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_le_200", latencyBuckets[7]);
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_le_1000", latencyBuckets[8]);
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_overflow", latencyBuckets[9]);
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_count",
                stats.storageWriteLatencyBuckets.getCount());
        metric(metricsSender, cluster, namespace, "pulsar_storage_write_latency_sum",
                stats.storageWriteLatencyBuckets.getSum());

        stats.entrySizeBuckets.refresh();
        long[] entrySizeBuckets = stats.entrySizeBuckets.getBuckets();
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_le_128", entrySizeBuckets[0]);
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_le_512", entrySizeBuckets[1]);
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2]);
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3]);
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4]);
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5]);
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6]);
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7]);
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_le_overflow", entrySizeBuckets[8]);
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_count", stats.entrySizeBuckets.getCount());
        metric(metricsSender, cluster, namespace, "pulsar_entry_size_sum", stats.entrySizeBuckets.getSum());

        if (!stats.replicationStats.isEmpty()) {
            stats.replicationStats.forEach((remoteCluster, replStats) -> {
                metricWithRemoteCluster(metricsSender, cluster, namespace, "pulsar_replication_rate_in", remoteCluster,
                        replStats.msgRateIn);
                metricWithRemoteCluster(metricsSender, cluster, namespace, "pulsar_replication_rate_out", remoteCluster,
                        replStats.msgRateOut);
                metricWithRemoteCluster(metricsSender, cluster, namespace, "pulsar_replication_throughput_in", remoteCluster,
                        replStats.msgThroughputIn);
                metricWithRemoteCluster(metricsSender, cluster, namespace, "pulsar_replication_throughput_out", remoteCluster,
                        replStats.msgThroughputOut);
                metricWithRemoteCluster(metricsSender, cluster, namespace, "pulsar_replication_backlog", remoteCluster,
                        replStats.replicationBacklog);
            });
        }
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String name,
            long value) {
        TopicStats.metricType(stream, name);
        stream.write(name)
                .write("{cluster=\"").write(cluster).write("\"} ")
                .write(value).write(' ').write(System.currentTimeMillis())
                .write('\n');
    }

    private static void metric(MetricsSender metricsSender, String cluster, String name, long value) {
        String head = TopicStats.metricType(name);
        String body = name + "{cluster=\"" + cluster + "\"} " + value + " " + System.currentTimeMillis();
        metricsSender.send(new PulsarMetrics(head, body));
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String name,
                               long value) {
        TopicStats.metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(MetricsSender metricsSender, String cluster, String namespace, String name,
                               long value) {
        String head = TopicStats.metricType(name);
        String body = name + "{cluster=\"" + cluster + "\",namespace=\"" + namespace + "\"} " +
                value + " " + System.currentTimeMillis();
        metricsSender.send(new PulsarMetrics(head, body));
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String name,
                               double value) {
        TopicStats.metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(MetricsSender metricsSender, String cluster, String namespace, String name,
                               double value) {
        String head = TopicStats.metricType(name);
        String body = name + "{cluster=\"" + cluster + "\",namespace=\"" + namespace + "\"} " +
                value + " " + System.currentTimeMillis();
        metricsSender.send(new PulsarMetrics(head, body));
    }

    private static void metricWithRemoteCluster(SimpleTextOutputStream stream, String cluster, String namespace,
                                                String name, String remoteCluster, double value) {
        TopicStats.metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace);
        stream.write("\",remote_cluster=\"").write(remoteCluster).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metricWithRemoteCluster(MetricsSender metricsSender, String cluster, String namespace,
                                                String name, String remoteCluster, double value) {
        String head = TopicStats.metricType(name);
        String body = name + "{cluster=\"" + cluster + "\",namespace=\"" + namespace +
                "\",remote_cluster=\"" + remoteCluster + "\"} " +
                value + " " + System.currentTimeMillis();
        metricsSender.send(new PulsarMetrics(head, body));
    }
}
