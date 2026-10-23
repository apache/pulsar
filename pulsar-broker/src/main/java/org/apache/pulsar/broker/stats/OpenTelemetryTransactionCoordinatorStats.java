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
package org.apache.pulsar.broker.stats;

import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;

public class OpenTelemetryTransactionCoordinatorStats implements AutoCloseable {

    // Replaces ['pulsar_txn_aborted_total',
    //           'pulsar_txn_committed_total',
    //           'pulsar_txn_created_total',
    //           'pulsar_txn_timeout_total',
    //           'pulsar_txn_active_count']
    public static final String TRANSACTION_COUNTER = "pulsar.broker.transaction.coordinator.transaction.count";
    private final ObservableLongMeasurement transactionCounter;

    // Replaces pulsar_txn_append_log_total
    public static final String APPEND_LOG_COUNTER = "pulsar.broker.transaction.coordinator.append.log.count";
    private final ObservableLongMeasurement appendLogCounter;

    private final BatchCallback batchCallback;

    public OpenTelemetryTransactionCoordinatorStats(PulsarService pulsar) {
        var meter = pulsar.getOpenTelemetry().getMeter();

        transactionCounter = meter
                .upDownCounterBuilder(TRANSACTION_COUNTER)
                .setUnit("{transaction}")
                .setDescription("The number of transactions handled by the coordinator.")
                .buildObserver();

        appendLogCounter = meter
                .counterBuilder(APPEND_LOG_COUNTER)
                .setUnit("{entry}")
                .setDescription("The number of transaction metadata entries appended by the coordinator.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> {
                    var transactionMetadataStoreService = pulsar.getTransactionMetadataStoreService();
                    // Avoid NPE during Pulsar shutdown.
                    if (transactionMetadataStoreService != null) {
                        transactionMetadataStoreService.getStores()
                                .values()
                                .forEach(this::recordMetricsForTransactionMetadataStore);
                    }
                },
                transactionCounter,
                appendLogCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForTransactionMetadataStore(TransactionMetadataStore transactionMetadataStore) {
        var attributes = transactionMetadataStore.getAttributes();
        var stats = transactionMetadataStore.getMetadataStoreStats();

        transactionCounter.record(stats.getAbortedCount(), attributes.getTxnAbortedAttributes());
        transactionCounter.record(stats.getActives(), attributes.getTxnActiveAttributes());
        transactionCounter.record(stats.getCommittedCount(), attributes.getTxnCommittedAttributes());
        transactionCounter.record(stats.getCreatedCount(), attributes.getTxnCreatedAttributes());
        transactionCounter.record(stats.getTimeoutCount(), attributes.getTxnTimeoutAttributes());

        appendLogCounter.record(stats.getAppendLogCount(), attributes.getCommonAttributes());
    }
}
