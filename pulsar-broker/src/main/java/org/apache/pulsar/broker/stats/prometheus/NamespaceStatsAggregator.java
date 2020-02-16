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
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

public class NamespaceStatsAggregator {

    private static FastThreadLocal<AggregatedNamespaceStats> localNamespaceStats = new FastThreadLocal<AggregatedNamespaceStats>() {
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

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics, SimpleTextOutputStream stream) {
        String cluster = pulsar.getConfiguration().getClusterName();
        AggregatedNamespaceStats namespaceStats = localNamespaceStats.get();
        TopicStats.resetTypes();
        TopicStats topicStats = localTopicStats.get();

        printDefaultBrokerStats(stream, cluster);

        LongAdder topicsCount = new LongAdder();

        pulsar.getBrokerService().getMultiLayerTopicMap().forEach((namespace, bundlesMap) -> {
            namespaceStats.reset();

            bundlesMap.forEach((bundle, topicsMap) -> {
                topicsMap.forEach((name, topic) -> {
                    getTopicStats(topic, topicStats, includeConsumerMetrics, pulsar.getConfiguration().isExposePreciseBacklogInPrometheus());

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

    private static void getTopicStats(Topic topic, TopicStats stats, boolean includeConsumerMetrics, boolean getPreciseBacklog) {
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
            stats.entrySizeBuckets.addAll(mlStats.getInternalEntrySizeBuckets());
            stats.entrySizeBuckets.refresh();

            stats.storageWriteRate = mlStats.getAddEntryMessagesRate();
            stats.storageReadRate = mlStats.getReadEntriesRate();
        }

        stats.msgInCounter = topic.getStats(getPreciseBacklog).msgInCounter;
        stats.bytesInCounter = topic.getStats(getPreciseBacklog).bytesInCounter;

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

        topic.getSubscriptions().forEach((name, subscription) -> {
            stats.subscriptionsCount++;
            stats.msgBacklog += subscription.getNumberOfEntriesInBacklog(getPreciseBacklog);

            AggregatedSubscriptionStats subsStats = stats.subscriptionStats
                    .computeIfAbsent(name, k -> new AggregatedSubscriptionStats());
            subsStats.msgBacklog = subscription.getNumberOfEntriesInBacklog(getPreciseBacklog);
            subsStats.msgDelayed = subscription.getNumberOfEntriesDelayed();
            subsStats.msgBacklogNoDelayed = subsStats.msgBacklog - subsStats.msgDelayed;

            subscription.getConsumers().forEach(consumer -> {

                // Consumer stats can be a lot if a subscription has many consumers
                if (includeConsumerMetrics) {
                    AggregatedConsumerStats consumerStats = subsStats.consumerStat
                            .computeIfAbsent(consumer, k -> new AggregatedConsumerStats());
                    consumerStats.unackedMessages = consumer.getStats().unackedMessages;
                    consumerStats.msgRateRedeliver = consumer.getStats().msgRateRedeliver;
                    consumerStats.msgRateOut = consumer.getStats().msgRateOut;
                    consumerStats.msgThroughputOut = consumer.getStats().msgThroughputOut;
                    consumerStats.availablePermits = consumer.getStats().availablePermits;
                    consumerStats.blockedSubscriptionOnUnackedMsgs = consumer.getStats().blockedConsumerOnUnackedMsgs;
                }

                subsStats.unackedMessages += consumer.getStats().unackedMessages;
                subsStats.msgRateRedeliver += consumer.getStats().msgRateRedeliver;
                subsStats.msgRateOut += consumer.getStats().msgRateOut;
                subsStats.msgThroughputOut += consumer.getStats().msgThroughputOut;
                if (!subsStats.blockedSubscriptionOnUnackedMsgs && consumer.getStats().blockedConsumerOnUnackedMsgs) {
                    subsStats.blockedSubscriptionOnUnackedMsgs = true;
                }

                stats.consumersCount++;
                stats.rateOut += consumer.getStats().msgRateOut;
                stats.throughputOut += consumer.getStats().msgThroughputOut;
            });
        });

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

    private static void printTopicsCountStats(SimpleTextOutputStream stream, String cluster, String namespace,
                                              LongAdder topicsCount) {
        metric(stream, cluster, namespace, "pulsar_topics_count", topicsCount.sum());
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

    private static void metric(SimpleTextOutputStream stream, String cluster, String name,
            long value) {
        TopicStats.metricType(stream, name);
        stream.write(name)
                .write("{cluster=\"").write(cluster).write("\"} ")
                .write(value).write(' ').write(System.currentTimeMillis())
                .write('\n');
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String name,
                               long value) {
        TopicStats.metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String name,
                               double value) {
        TopicStats.metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metricWithRemoteCluster(SimpleTextOutputStream stream, String cluster, String namespace,
                                                String name, String remoteCluster, double value) {
        TopicStats.metricType(stream, name);
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace);
        stream.write("\",remote_cluster=\"").write(remoteCluster).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }
}
