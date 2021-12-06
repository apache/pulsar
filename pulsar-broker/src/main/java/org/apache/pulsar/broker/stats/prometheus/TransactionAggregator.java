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

import static org.apache.pulsar.common.events.EventsTopicNames.checkTopicIsEventsNames;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.impl.TransactionMetadataStoreStats;

@Slf4j
public class TransactionAggregator {

    private static final FastThreadLocal<AggregatedTransactionCoordinatorStats> localTransactionCoordinatorStats =
            new FastThreadLocal<AggregatedTransactionCoordinatorStats>() {
                @Override
                protected AggregatedTransactionCoordinatorStats initialValue() throws Exception {
                    return new AggregatedTransactionCoordinatorStats();
                }
            };

    private static final FastThreadLocal<ManagedLedgerStats> localManageLedgerStats =
            new FastThreadLocal<ManagedLedgerStats>() {
                @Override
                protected ManagedLedgerStats initialValue() throws Exception {
                    return new ManagedLedgerStats();
                }
            };

    public static void generate(PulsarService pulsar, SimpleTextOutputStream stream, boolean includeTopicMetrics) {
        String cluster = pulsar.getConfiguration().getClusterName();

        if (includeTopicMetrics) {
            pulsar.getBrokerService().getMultiLayerTopicMap().forEach((namespace, bundlesMap) -> {

                bundlesMap.forEach((bundle, topicsMap) -> {
                    topicsMap.forEach((name, topic) -> {
                        if (topic instanceof PersistentTopic) {
                            topic.getSubscriptions().values().forEach(subscription -> {
                                try {
                                    localManageLedgerStats.get().reset();
                                    if (!checkTopicIsEventsNames(TopicName.get(subscription.getTopic().getName()))
                                            && subscription instanceof  PersistentSubscription
                                            && ((PersistentSubscription) subscription).checkIfPendingAckStoreInit()) {
                                        ManagedLedger managedLedger =
                                                ((PersistentSubscription) subscription)
                                                        .getPendingAckManageLedger().get();
                                        generateManageLedgerStats(managedLedger,
                                                stream, cluster, namespace, name, subscription.getName());
                                    }
                                } catch (Exception e) {
                                    log.warn("Transaction pending ack generate managedLedgerStats fail!", e);
                                }
                            });
                        }
                    });
                });
            });
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

    private static void generateManageLedgerStats(ManagedLedger managedLedger, SimpleTextOutputStream stream,
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
        printManageLedgerStats(stream, cluster, namespace, topic,
                subscription, managedLedgerStats);
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String name,
                               double value, long coordinatorId) {
        stream.write("# TYPE ").write(name).write(" gauge\n")
                .write(name)
                .write("{cluster=\"").write(cluster)
                .write("\",coordinator_id=\"").write(coordinatorId).write("\"} ")
                .write(value).write(' ').write(System.currentTimeMillis())
                .write('\n');
    }

    private static void metrics(SimpleTextOutputStream stream, String cluster, String namespace,
                                String topic, String subscription, String name, long value) {
        stream.write(name).write("{cluster=\"").write(cluster).write("\", namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\",subscription=\"").write(subscription).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void metrics(SimpleTextOutputStream stream, String cluster, String namespace,
                                String topic, String subscription, String name, double value) {
        stream.write(name).write("{cluster=\"").write(cluster).write("\", namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\",subscription=\"").write(subscription).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    private static void printManageLedgerStats(SimpleTextOutputStream stream, String cluster, String namespace,
                                               String topic, String subscription, ManagedLedgerStats stats) {

        metrics(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_size", stats.storageSize);
        metrics(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_logical_size", stats.storageLogicalSize);
        metrics(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_backlog_size", stats.backlogSize);
        metrics(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_offloaded_size", stats.offloadedStorageUsed);

        metrics(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_rate", stats.storageWriteRate);
        metrics(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_read_rate", stats.storageReadRate);

        stats.storageWriteLatencyBuckets.refresh();
        long[] latencyBuckets = stats.storageWriteLatencyBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_latency_le_0_5", latencyBuckets[0]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_latency_le_1", latencyBuckets[1]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_latency_le_5", latencyBuckets[2]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_latency_le_10", latencyBuckets[3]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_latency_le_20", latencyBuckets[4]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_latency_le_50", latencyBuckets[5]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_latency_le_100", latencyBuckets[6]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_latency_le_200", latencyBuckets[7]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_latency_le_1000", latencyBuckets[8]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_write_latency_overflow", latencyBuckets[9]);
        metric(stream, cluster, namespace, topic, subscription, "pulsar_storage_write_latency_count",
                stats.storageWriteLatencyBuckets.getCount());
        metric(stream, cluster, namespace, topic, subscription, "pulsar_storage_write_latency_sum",
                stats.storageWriteLatencyBuckets.getSum());

        stats.storageLedgerWriteLatencyBuckets.refresh();
        long[] ledgerWritelatencyBuckets = stats.storageLedgerWriteLatencyBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_ledger_write_latency_le_0_5", ledgerWritelatencyBuckets[0]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_ledger_write_latency_le_1", ledgerWritelatencyBuckets[1]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_ledger_write_latency_le_5", ledgerWritelatencyBuckets[2]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_ledger_write_latency_le_10", ledgerWritelatencyBuckets[3]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_ledger_write_latency_le_20", ledgerWritelatencyBuckets[4]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_ledger_write_latency_le_50", ledgerWritelatencyBuckets[5]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_ledger_write_latency_le_100", ledgerWritelatencyBuckets[6]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_ledger_write_latency_le_200", ledgerWritelatencyBuckets[7]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_storage_ledger_write_latency_le_1000", ledgerWritelatencyBuckets[8]);
        metric(stream, cluster, namespace, topic, subscription, "pulsar_storage_ledger_write_latency_overflow",
                ledgerWritelatencyBuckets[9]);
        metric(stream, cluster, namespace, topic, subscription, "pulsar_storage_ledger_write_latency_count",
                stats.storageLedgerWriteLatencyBuckets.getCount());
        metric(stream, cluster, namespace, topic, subscription, "pulsar_storage_ledger_write_latency_sum",
                stats.storageLedgerWriteLatencyBuckets.getSum());

        stats.entrySizeBuckets.refresh();
        long[] entrySizeBuckets = stats.entrySizeBuckets.getBuckets();
        metric(stream, cluster, namespace, topic, subscription, "pulsar_entry_size_le_128", entrySizeBuckets[0]);
        metric(stream, cluster, namespace, topic, subscription, "pulsar_entry_size_le_512", entrySizeBuckets[1]);
        metric(stream, cluster, namespace, topic, subscription, "pulsar_entry_size_le_1_kb", entrySizeBuckets[2]);
        metric(stream, cluster, namespace, topic, subscription, "pulsar_entry_size_le_2_kb", entrySizeBuckets[3]);
        metric(stream, cluster, namespace, topic, subscription, "pulsar_entry_size_le_4_kb", entrySizeBuckets[4]);
        metric(stream, cluster, namespace, topic, subscription, "pulsar_entry_size_le_16_kb", entrySizeBuckets[5]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_entry_size_le_100_kb", entrySizeBuckets[6]);
        metric(stream, cluster, namespace, topic, subscription, "pulsar_entry_size_le_1_mb", entrySizeBuckets[7]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_entry_size_le_overflow", entrySizeBuckets[8]);
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_entry_size_count", stats.entrySizeBuckets.getCount());
        metric(stream, cluster, namespace, topic, subscription,
                "pulsar_entry_size_sum", stats.entrySizeBuckets.getSum());
    }

    private static void metric(SimpleTextOutputStream stream, String cluster,
                               String namespace, String topic, String subscription,
                               String name, long value) {
        stream.write(name).write("{cluster=\"").write(cluster).write("\",namespace=\"").write(namespace)
                .write("\",topic=\"").write(topic).write("\",subscription=\"").write(subscription).write("\"} ");
        stream.write(value).write(' ').write(System.currentTimeMillis()).write('\n');
    }

    static void printTransactionCoordinatorStats(SimpleTextOutputStream stream, String cluster,
                                                 AggregatedTransactionCoordinatorStats stats,
                                                 long coordinatorId) {
        metric(stream, cluster, "pulsar_txn_active_count",
                stats.actives, coordinatorId);
        metric(stream, cluster, "pulsar_txn_committed_count",
                stats.committedCount, coordinatorId);
        metric(stream, cluster, "pulsar_txn_aborted_count",
                stats.abortedCount, coordinatorId);
        metric(stream, cluster, "pulsar_txn_created_count",
                stats.createdCount, coordinatorId);
        metric(stream, cluster, "pulsar_txn_timeout_count",
                stats.timeoutCount, coordinatorId);
        metric(stream, cluster, "pulsar_txn_append_log_count",
                stats.appendLogCount, coordinatorId);
        long[] latencyBuckets = stats.executionLatency;
        metric(stream, cluster, "pulsar_txn_execution_latency_le_10", latencyBuckets[0], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_20", latencyBuckets[1], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_50", latencyBuckets[2], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_100", latencyBuckets[3], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_500", latencyBuckets[4], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_1000", latencyBuckets[5], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_5000", latencyBuckets[6], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_15000", latencyBuckets[7], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_30000", latencyBuckets[8], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_60000", latencyBuckets[9], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_300000",
                latencyBuckets[10], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_1500000",
                latencyBuckets[11], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_3000000",
                latencyBuckets[12], coordinatorId);
        metric(stream, cluster, "pulsar_txn_execution_latency_le_overflow",
                latencyBuckets[13], coordinatorId);
    }
}
