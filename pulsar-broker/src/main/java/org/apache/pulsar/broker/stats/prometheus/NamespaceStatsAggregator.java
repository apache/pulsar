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

import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.utils.SimpleTextOutputStream;

import io.netty.util.concurrent.FastThreadLocal;

public class NamespaceStatsAggregator {

    private static FastThreadLocal<AggregatedNamespaceStats> localNamespaceStats = new FastThreadLocal<AggregatedNamespaceStats>() {
        @Override
        protected AggregatedNamespaceStats initialValue() throws Exception {
            return new AggregatedNamespaceStats();
        }
    };

    public static void generate(PulsarService pulsar, SimpleTextOutputStream stream) {
        String cluster = pulsar.getConfiguration().getClusterName();
        AggregatedNamespaceStats namespaceStats = localNamespaceStats.get();

        pulsar.getBrokerService().getMultiLayerTopicMap().forEach((namespace, bundlesMap) -> {
            namespaceStats.reset();

            bundlesMap.forEach((bundle, topicsMap) -> {
                topicsMap.forEach((name, topic) -> {
                    updateNamespaceStats(namespaceStats, topic);
                });
            });

            printNamespaceStats(stream, cluster, namespace, namespaceStats);
        });
    }

    private static void updateNamespaceStats(AggregatedNamespaceStats stats, Topic topic) {
        
        if(topic instanceof PersistentTopic) {
         // Managed Ledger stats
            ManagedLedgerMBeanImpl mlStats = (ManagedLedgerMBeanImpl) ((PersistentTopic)topic).getManagedLedger().getStats();

            stats.storageSize += mlStats.getStoredMessagesSize();
            stats.storageWriteLatencyBuckets.addAll(mlStats.getInternalAddEntryLatencyBuckets());
            stats.entrySizeBuckets.addAll(mlStats.getInternalEntrySizeBuckets());

            stats.storageWriteRate = mlStats.getAddEntryMessagesRate();
            stats.storageReadRate = mlStats.getReadEntriesRate();    
        }
        
        stats.topicsCount++;

        topic.getProducers().forEach(producer -> {
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
            stats.msgBacklog += subscription.getNumberOfEntriesInBacklog();

            subscription.getConsumers().forEach(consumer -> {
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
        metric(stream, cluster, namespace, "pulsar_storage_write_rate", stats.storageWriteRate);
        metric(stream, cluster, namespace, "pulsar_storage_read_rate", stats.storageReadRate);

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

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String name,
            long value) {
        stream.write(name).write("{cluster=\"").write(cluster).write("\", namespace=\"").write(namespace).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String namespace, String name,
            double value) {
        stream.write(name).write("{cluster=\"").write(cluster).write("\", namespace=\"").write(namespace).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metricWithRemoteCluster(SimpleTextOutputStream stream, String cluster, String namespace,
            String name, String remoteCluster, double value) {
        stream.write(name).write("{cluster=\"").write(cluster).write("\", namespace=\"").write(namespace);
        stream.write("\", remote_cluster=\"").write(remoteCluster).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }
}
