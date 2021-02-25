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
                    transactionCoordinatorStats.lowWaterMark = transactionMetadataStoreStats.getLowWaterMark();
                    transactionCoordinatorStats.ongoingTransactionCount =
                            transactionMetadataStoreStats.getOngoingTransactionCount();
                    transactionCoordinatorStats.transactionSequenceId =
                            transactionMetadataStoreStats.getTransactionSequenceId();
                    transactionCoordinatorStats.commitTransactionRate =
                            transactionMetadataStoreStats.getCommitTransactionRate();
                    transactionCoordinatorStats.abortTransactionRate =
                            transactionMetadataStoreStats.getAbortTransactionRate();
                    transactionCoordinatorStats.createTransactionRate =
                            transactionMetadataStoreStats.getCreateTransactionRate();
                    printTransactionCoordinatorStats(stream, cluster, transactionCoordinatorStats,
                            transactionMetadataStoreStats.getTransactionCoordinatorId());

        });
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String name,
                               long value, long transactionCoordinatorId) {
        stream.write(name)
                .write("{cluster=\"").write(cluster)
                .write("\",transaction_coordinator_id=\"").write(transactionCoordinatorId).write("\"} ")
                .write(value).write(' ').write(System.currentTimeMillis())
                .write('\n');
    }

    private static void metric(SimpleTextOutputStream stream, String cluster, String name,
                               double value, long transactionCoordinatorId) {
        stream.write(name)
                .write("{cluster=\"").write(cluster)
                .write("\",transaction_coordinator_id=\"").write(transactionCoordinatorId).write("\"} ")
                .write(value).write(' ').write(System.currentTimeMillis())
                .write('\n');
    }

    static void printTransactionCoordinatorStats(SimpleTextOutputStream stream, String cluster,
                                                 AggregatedTransactionCoordinatorStats stats,
                                                 long transactionCoordinatorId) {
        metric(stream, cluster, "pulsar_transaction_ongoing_count",
                stats.ongoingTransactionCount, transactionCoordinatorId);
        metric(stream, cluster, "pulsar_transaction_sequence_id",
                stats.transactionSequenceId, transactionCoordinatorId);
        metric(stream, cluster, "pulsar_transaction_low_water_mark",
                stats.lowWaterMark, transactionCoordinatorId);
        metric(stream, cluster, "pulsar_transaction_commit_rate",
                stats.commitTransactionRate, transactionCoordinatorId);
        metric(stream, cluster, "pulsar_transaction_abort_rate",
                stats.abortTransactionRate, transactionCoordinatorId);
        metric(stream, cluster, "pulsar_transaction_create_rate",
                stats.createTransactionRate, transactionCoordinatorId);
    }
}
