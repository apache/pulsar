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
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.transaction.coordinator.impl.TransactionMetadataStoreStats;

public class TransactionCoordinatorAggregator {

    private final static FastThreadLocal<AggregatedTransactionCoordinatorStats> localTransactionCoordinatorStats =
            new FastThreadLocal<AggregatedTransactionCoordinatorStats>() {
                @Override
                protected AggregatedTransactionCoordinatorStats initialValue() throws Exception {
                    return new AggregatedTransactionCoordinatorStats();
                }
            };

    public static void generate(PulsarService pulsar, SimpleTextOutputStream stream) {
        String cluster = pulsar.getConfiguration().getClusterName();
        AggregatedTransactionCoordinatorStats transactionCoordinatorStats = localTransactionCoordinatorStats.get();

        pulsar.getTransactionMetadataStoreService().getStores()
                .forEach((transactionCoordinatorID, transactionMetadataStore) -> {
                    transactionCoordinatorStats.reset();
                    TransactionMetadataStoreStats transactionMetadataStoreStats = transactionMetadataStore.getStats();
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

        });
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
