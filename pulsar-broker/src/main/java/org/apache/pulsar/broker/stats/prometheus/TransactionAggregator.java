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

import static org.apache.pulsar.common.naming.SystemTopicNames.isEventSystemTopic;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.impl.TransactionMetadataStoreStats;

@Slf4j
public class TransactionAggregator {

    private static final FastThreadLocal<AggregatedTransactionCoordinatorStats> localTransactionCoordinatorStats =
            new FastThreadLocal<>() {
                @Override
                protected AggregatedTransactionCoordinatorStats initialValue() {
                    return new AggregatedTransactionCoordinatorStats();
                }
            };

    private static final FastThreadLocal<ManagedLedgerStats> localManageLedgerStats =
            new FastThreadLocal<>() {
                @Override
                protected ManagedLedgerStats initialValue() {
                    return new ManagedLedgerStats();
                }
            };

    public static void generate(PulsarService pulsar, PrometheusMetricStreams stream, boolean includeTopicMetrics) {
        String cluster = pulsar.getConfiguration().getClusterName();

        if (includeTopicMetrics) {

            pulsar.getBrokerService().getMultiLayerTopicMap().forEach((namespace, bundlesMap) ->
                    bundlesMap.forEach((bundle, topicsMap) -> topicsMap.forEach((name, topic) -> {
                        if (topic instanceof PersistentTopic) {
                            topic.getSubscriptions().values().forEach(subscription -> {
                                try {
                                    localManageLedgerStats.get().reset();
                                    if (!isEventSystemTopic(TopicName.get(subscription.getTopic().getName()))
                                            && subscription instanceof PersistentSubscription
                                            && ((PersistentSubscription) subscription).checkIfPendingAckStoreInit()) {
                                        ManagedLedger managedLedger = ((PersistentSubscription) subscription)
                                                .getPendingAckManageLedger().get();
                                        generateManageLedgerStats(managedLedger,
                                                stream, cluster, namespace, name, subscription.getName());
                                    }
                                } catch (Exception e) {
                                    log.warn("Transaction pending ack generate managedLedgerStats fail!", e);
                                }
                            });
                        }
                    })));
        }
        AggregatedTransactionCoordinatorStats transactionCoordinatorStats = localTransactionCoordinatorStats.get();

        pulsar.getTransactionMetadataStoreService().getStores()
                .forEach((transactionCoordinatorID, transactionMetadataStore) -> {
                    transactionCoordinatorStats.reset();
                    TransactionMetadataStoreStats transactionMetadataStoreStats =
                            transactionMetadataStore.getMetadataStoreStats();
                    transactionCoordinatorStats.actives =
                            transactionMetadataStoreStats.getActives();
                    transactionCoordinatorStats.committedCount =
                            transactionMetadataStoreStats.getCommittedCount();
                    transactionCoordinatorStats.abortedCount =
                            transactionMetadataStoreStats.getAbortedCount();
                    transactionCoordinatorStats.createdCount =
                            transactionMetadataStoreStats.getCreatedCount();
                    transactionCoordinatorStats.timeoutCount =
                            transactionMetadataStoreStats.getTimeoutCount();
                    transactionCoordinatorStats.appendLogCount =
                            transactionMetadataStoreStats.getAppendLogCount();
                    transactionMetadataStoreStats.executionLatencyBuckets.refresh();
                    transactionCoordinatorStats.executionLatency =
                            transactionMetadataStoreStats.executionLatencyBuckets.getBuckets();
                    printTransactionCoordinatorStats(stream, cluster, transactionCoordinatorStats,
                            transactionMetadataStoreStats.getCoordinatorId());

                    localManageLedgerStats.get().reset();
                    if (transactionMetadataStore instanceof MLTransactionMetadataStore) {
                        ManagedLedger managedLedger =
                                ((MLTransactionMetadataStore) transactionMetadataStore).getManagedLedger();
                        generateManageLedgerStats(managedLedger,
                                stream, cluster, NamespaceName.SYSTEM_NAMESPACE.toString(),
                                MLTransactionLogImpl.TRANSACTION_LOG_PREFIX + transactionCoordinatorID.getId(),
                                MLTransactionLogImpl.TRANSACTION_SUBSCRIPTION_NAME);
                    }

                });
    }

    private static void generateManageLedgerStats(ManagedLedger managedLedger, PrometheusMetricStreams stream,
                                                  String cluster, String namespace, String topic, String subscription) {
        ManagedLedgerStats managedLedgerStats = localManageLedgerStats.get();
        ManagedLedgerMBeanImpl mlStats = (ManagedLedgerMBeanImpl) managedLedger.getStats();

        managedLedgerStats.storageSize = mlStats.getStoredMessagesSize();
        managedLedgerStats.storageLogicalSize = mlStats.getStoredMessagesLogicalSize();
        managedLedgerStats.backlogSize = managedLedger.getEstimatedBacklogSize();
        managedLedgerStats.offloadedStorageUsed = managedLedger.getOffloadedSize();

        managedLedgerStats.storageWriteLatencyBuckets
                .addAll(mlStats.getInternalAddEntryLatencyBuckets());
        managedLedgerStats.storageWriteLatencyBuckets.refresh();
        managedLedgerStats.storageLedgerWriteLatencyBuckets
                .addAll(mlStats.getInternalLedgerAddEntryLatencyBuckets());
        managedLedgerStats.storageLedgerWriteLatencyBuckets.refresh();

        managedLedgerStats.entrySizeBuckets.addAll(mlStats.getInternalEntrySizeBuckets());
        managedLedgerStats.entrySizeBuckets.refresh();

        managedLedgerStats.storageWriteRate = mlStats.getAddEntryMessagesRate();
        managedLedgerStats.storageReadRate = mlStats.getReadEntriesRate();
        printManageLedgerStats(stream, cluster, namespace, topic, subscription, managedLedgerStats);
    }

    private static void printManageLedgerStats(PrometheusMetricStreams stream, String cluster, String namespace,
                                               String topic, String subscription, ManagedLedgerStats stats) {

        writeMetric(stream, "pulsar_storage_size", stats.storageSize, cluster, namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_logical_size", stats.storageLogicalSize, cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_backlog_size", stats.backlogSize, cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_offloaded_size", stats.offloadedStorageUsed, cluster, namespace, topic,
                subscription);

        writeMetric(stream, "pulsar_storage_write_rate", stats.storageWriteRate, cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_read_rate", stats.storageReadRate, cluster, namespace, topic,
                subscription);

        stats.storageWriteLatencyBuckets.refresh();
        long[] latencyBuckets = stats.storageWriteLatencyBuckets.getBuckets();
        writeMetric(stream, "pulsar_storage_write_latency_le_0_5", latencyBuckets[0], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_write_latency_le_1", latencyBuckets[1], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_write_latency_le_5", latencyBuckets[2], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_write_latency_le_10", latencyBuckets[3], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_write_latency_le_20", latencyBuckets[4], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_write_latency_le_50", latencyBuckets[5], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_write_latency_le_100", latencyBuckets[6], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_write_latency_le_200", latencyBuckets[7], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_write_latency_le_1000", latencyBuckets[8], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_write_latency_overflow", latencyBuckets[9], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_storage_write_latency_count", stats.storageWriteLatencyBuckets.getCount(),
                cluster, namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_write_latency_sum", stats.storageWriteLatencyBuckets.getSum(), cluster,
                namespace, topic, subscription);

        stats.storageLedgerWriteLatencyBuckets.refresh();
        long[] ledgerWriteLatencyBuckets = stats.storageLedgerWriteLatencyBuckets.getBuckets();
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_0_5", ledgerWriteLatencyBuckets[0], cluster,
                namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_1", ledgerWriteLatencyBuckets[1], cluster,
                namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_5", ledgerWriteLatencyBuckets[2], cluster,
                namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_10", ledgerWriteLatencyBuckets[3], cluster,
                namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_20", ledgerWriteLatencyBuckets[4], cluster,
                namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_50", ledgerWriteLatencyBuckets[5], cluster,
                namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_100", ledgerWriteLatencyBuckets[6], cluster,
                namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_200", ledgerWriteLatencyBuckets[7], cluster,
                namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_le_1000", ledgerWriteLatencyBuckets[8], cluster,
                namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_overflow", ledgerWriteLatencyBuckets[9], cluster,
                namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_count",
                stats.storageLedgerWriteLatencyBuckets.getCount(), cluster, namespace, topic, subscription);
        writeMetric(stream, "pulsar_storage_ledger_write_latency_sum",
                stats.storageLedgerWriteLatencyBuckets.getSum(), cluster, namespace, topic, subscription);

        stats.entrySizeBuckets.refresh();
        long[] entrySizeBuckets = stats.entrySizeBuckets.getBuckets();
        writeMetric(stream, "pulsar_entry_size_le_128", entrySizeBuckets[0], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_entry_size_le_512", entrySizeBuckets[1], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_entry_size_le_100_kb", entrySizeBuckets[6], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_entry_size_le_overflow", entrySizeBuckets[8], cluster, namespace, topic,
                subscription);
        writeMetric(stream, "pulsar_entry_size_count", stats.entrySizeBuckets.getCount(), cluster, namespace,
                topic, subscription);
        writeMetric(stream, "pulsar_entry_size_sum", stats.entrySizeBuckets.getSum(), cluster, namespace, topic,
                subscription);
    }

    static void printTransactionCoordinatorStats(PrometheusMetricStreams stream, String cluster,
                                                 AggregatedTransactionCoordinatorStats stats,
                                                 long coordinatorId) {
        writeMetric(stream, "pulsar_txn_active_count", stats.actives, cluster,
                coordinatorId);
        writeMetric(stream, "pulsar_txn_committed_total", stats.committedCount, cluster,
                coordinatorId);
        writeMetric(stream, "pulsar_txn_aborted_total", stats.abortedCount, cluster,
                coordinatorId);
        writeMetric(stream, "pulsar_txn_created_total", stats.createdCount, cluster,
                coordinatorId);
        writeMetric(stream, "pulsar_txn_timeout_total", stats.timeoutCount, cluster,
                coordinatorId);
        writeMetric(stream, "pulsar_txn_append_log_total", stats.appendLogCount, cluster,
                coordinatorId);
        long[] latencyBuckets = stats.executionLatency;
        writeMetric(stream, "pulsar_txn_execution_latency_le_10", latencyBuckets[0], cluster, coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_20", latencyBuckets[1], cluster, coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_50", latencyBuckets[2], cluster, coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_100", latencyBuckets[3], cluster, coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_500", latencyBuckets[4], cluster, coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_1000", latencyBuckets[5], cluster, coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_5000", latencyBuckets[6], cluster, coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_15000", latencyBuckets[7], cluster, coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_30000", latencyBuckets[8], cluster, coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_60000", latencyBuckets[9], cluster, coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_300000", latencyBuckets[10], cluster,
                coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_1500000", latencyBuckets[11], cluster,
                coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_3000000", latencyBuckets[12], cluster,
                coordinatorId);
        writeMetric(stream, "pulsar_txn_execution_latency_le_overflow", latencyBuckets[13], cluster,
                coordinatorId);
    }

    private static void writeMetric(PrometheusMetricStreams stream, String metricName, double value, String cluster,
                                    long coordinatorId) {
        stream.writeSample(metricName, value, "cluster", cluster, "coordinator_id", String.valueOf(coordinatorId));
    }

    private static void writeMetric(PrometheusMetricStreams stream, String metricName, Number value, String cluster,
                                    String namespace, String topic, String subscription) {
        stream.writeSample(metricName, value, "cluster", cluster, "namespace", namespace, "topic", topic,
                "subscription", subscription);
    }
}
