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
package org.apache.pulsar.transaction.coordinator.impl;

import io.netty.util.Timer;
import io.prometheus.client.CollectorRegistry;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;

/**
 * The provider that offers managed ledger implementation of {@link TransactionMetadataStore}.
 */
public class MLTransactionMetadataStoreProvider implements TransactionMetadataStoreProvider {


    private static volatile TxnLogBufferedWriterMetricsStats bufferedWriterMetrics =
            DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS;

    public static void initBufferedWriterMetrics(String brokerAdvertisedAddress){
        if (bufferedWriterMetrics != DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS) {
            return;
        }
        synchronized (MLTransactionMetadataStoreProvider.class){
            if (bufferedWriterMetrics != DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS) {
                return;
            }
            bufferedWriterMetrics = new MLTransactionMetadataStoreBufferedWriterMetrics(brokerAdvertisedAddress);
        }
    }

    public static void closeBufferedWriterMetrics() {
        synchronized (MLTransactionMetadataStoreProvider.class){
            if (bufferedWriterMetrics == DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS) {
                return;
            }
            bufferedWriterMetrics.close();
            bufferedWriterMetrics = DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS;
        }
    }

    @Override
    public CompletableFuture<TransactionMetadataStore> openStore(TransactionCoordinatorID transactionCoordinatorId,
                                                                 ManagedLedgerFactory managedLedgerFactory,
                                                                 ManagedLedgerConfig managedLedgerConfig,
                                                                 TransactionTimeoutTracker timeoutTracker,
                                                                 TransactionRecoverTracker recoverTracker,
                                                                 long maxActiveTransactionsPerCoordinator,
                                                                 TxnLogBufferedWriterConfig txnLogBufferedWriterConfig,
                                                                 Timer timer) {
        MLTransactionSequenceIdGenerator mlTransactionSequenceIdGenerator = new MLTransactionSequenceIdGenerator();
        managedLedgerConfig.setManagedLedgerInterceptor(mlTransactionSequenceIdGenerator);
        MLTransactionLogImpl txnLog = new MLTransactionLogImpl(transactionCoordinatorId,
                managedLedgerFactory, managedLedgerConfig, txnLogBufferedWriterConfig, timer, bufferedWriterMetrics);

        // MLTransactionLogInterceptor will init sequenceId and update the sequenceId to managedLedger properties.
        return txnLog.initialize().thenCompose(__ ->
                new MLTransactionMetadataStore(transactionCoordinatorId, txnLog, timeoutTracker,
                        mlTransactionSequenceIdGenerator, maxActiveTransactionsPerCoordinator).init(recoverTracker));
    }

    private static class MLTransactionMetadataStoreBufferedWriterMetrics extends TxnLogBufferedWriterMetricsStats {

        private MLTransactionMetadataStoreBufferedWriterMetrics(String brokerAdvertisedAddress) {
            super("pulsar_txn_tc",
                    new String[]{"broker"},
                    new String[]{brokerAdvertisedAddress},
                    CollectorRegistry.defaultRegistry);
        }
    }
}