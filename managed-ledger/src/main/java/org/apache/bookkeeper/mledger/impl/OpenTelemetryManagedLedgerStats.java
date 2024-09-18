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
package org.apache.bookkeeper.mledger.impl;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.opentelemetry.Constants;

public class OpenTelemetryManagedLedgerStats implements AutoCloseable {

    // Replaces pulsar_ml_AddEntryMessagesRate
    public static final String ADD_ENTRY_COUNTER = "pulsar.broker.managed_ledger.message.outgoing.count";
    private final ObservableLongMeasurement addEntryCounter;

    // Replaces pulsar_ml_AddEntryBytesRate
    public static final String BYTES_OUT_COUNTER = "pulsar.broker.managed_ledger.message.outgoing.logical.size";
    private final ObservableLongMeasurement bytesOutCounter;

    // Replaces pulsar_ml_AddEntryWithReplicasBytesRate
    public static final String BYTES_OUT_WITH_REPLICAS_COUNTER =
            "pulsar.broker.managed_ledger.message.outgoing.replicated.size";
    private final ObservableLongMeasurement bytesOutWithReplicasCounter;

    // Replaces pulsar_ml_NumberOfMessagesInBacklog
    public static final String BACKLOG_COUNTER = "pulsar.broker.managed_ledger.backlog.count";
    private final ObservableLongMeasurement backlogCounter;

    // Replaces pulsar_ml_ReadEntriesRate
    public static final String READ_ENTRY_COUNTER = "pulsar.broker.managed_ledger.message.incoming.count";
    private final ObservableLongMeasurement readEntryCounter;

    // Replaces pulsar_ml_ReadEntriesBytesRate
    public static final String BYTES_IN_COUNTER = "pulsar.broker.managed_ledger.message.incoming.size";
    private final ObservableLongMeasurement bytesInCounter;

    // Replaces brk_ml_ReadEntriesOpsCacheMissesRate
    public static final String READ_ENTRY_CACHE_MISS_COUNTER =
            "pulsar.broker.managed_ledger.message.incoming.cache.miss.count";
    private final ObservableLongMeasurement readEntryCacheMissCounter;

    // Replaces pulsar_ml_MarkDeleteRate
    public static final String MARK_DELETE_COUNTER = "pulsar.broker.managed_ledger.mark_delete.count";
    private final ObservableLongMeasurement markDeleteCounter;

    private final BatchCallback batchCallback;

    public OpenTelemetryManagedLedgerStats(OpenTelemetry openTelemetry, ManagedLedgerFactoryImpl factory) {
        var meter = openTelemetry.getMeter(Constants.BROKER_INSTRUMENTATION_SCOPE_NAME);

        addEntryCounter = meter
                .upDownCounterBuilder(ADD_ENTRY_COUNTER)
                .setUnit("{operation}")
                .setDescription("The number of write operations to this ledger.")
                .buildObserver();

        bytesOutCounter = meter
                .counterBuilder(BYTES_OUT_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes written to this ledger, excluding replicas.")
                .buildObserver();

        bytesOutWithReplicasCounter = meter
                .counterBuilder(BYTES_OUT_WITH_REPLICAS_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes written to this ledger, including replicas.")
                .buildObserver();

        backlogCounter = meter
                .upDownCounterBuilder(BACKLOG_COUNTER)
                .setUnit("{message}")
                .setDescription("The number of messages in backlog for all consumers from this ledger.")
                .buildObserver();

        readEntryCounter = meter
                .upDownCounterBuilder(READ_ENTRY_COUNTER)
                .setUnit("{operation}")
                .setDescription("The number of read operations from this ledger.")
                .buildObserver();

        bytesInCounter = meter
                .counterBuilder(BYTES_IN_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes read from this ledger.")
                .buildObserver();

        readEntryCacheMissCounter = meter
                .upDownCounterBuilder(READ_ENTRY_CACHE_MISS_COUNTER)
                .setUnit("{operation}")
                .setDescription("The number of cache misses during read operations from this ledger.")
                .buildObserver();

        markDeleteCounter = meter
                .counterBuilder(MARK_DELETE_COUNTER)
                .setUnit("{operation}")
                .setDescription("The total number of mark delete operations for this ledger.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> factory.getManagedLedgers()
                        .values()
                        .forEach(this::recordMetrics),
                addEntryCounter,
                bytesOutCounter,
                bytesOutWithReplicasCounter,
                backlogCounter,
                readEntryCounter,
                bytesInCounter,
                readEntryCacheMissCounter,
                markDeleteCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetrics(ManagedLedger ml) {
        var stats = ml.getStats();
        var ledgerAttributeSet = ml.getManagedLedgerAttributes();
        var attributes = ledgerAttributeSet.getAttributes();
        var attributesSucceed = ledgerAttributeSet.getAttributesOperationSucceed();
        var attributesFailure = ledgerAttributeSet.getAttributesOperationFailure();

        addEntryCounter.record(stats.getAddEntrySucceedTotal(), attributesSucceed);
        addEntryCounter.record(stats.getAddEntryErrorsTotal(), attributesFailure);
        bytesOutCounter.record(stats.getAddEntryBytesTotal(), attributes);
        bytesOutWithReplicasCounter.record(stats.getAddEntryWithReplicasBytesTotal(), attributes);

        readEntryCounter.record(stats.getReadEntriesSucceededTotal(), attributesSucceed);
        readEntryCounter.record(stats.getReadEntriesErrorsTotal(), attributesFailure);
        bytesInCounter.record(stats.getReadEntriesBytesTotal(), attributes);

        backlogCounter.record(stats.getNumberOfMessagesInBacklog(), attributes);
        markDeleteCounter.record(stats.getMarkDeleteTotal(), attributes);
        readEntryCacheMissCounter.record(stats.getReadEntriesOpsCacheMissesTotal(), attributes);
    }
}
