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
package org.apache.pulsar.transaction.coordinator.impl;

import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

/***
 * Describes the working status of the {@link TxnLogBufferedWriter}, helps users tune the thresholds of
 * {@link TxnLogBufferedWriter} for best performance.
 * Note-1: When batch feature is turned off, no data is logged at this. In this scenario，users can see the
 *    {@link org.apache.bookkeeper.mledger.ManagedLedgerMXBean}.
 * Note-2: Even if enable batch feature, if a single record is too big, it still directly write to Bookie without batch,
 *    property {@link #pulsarBatchedLogTriggeringCountByForce} can indicate this case. But this case will not affect
 *    other metrics, because it would obscure the real situation. E.g. there has two record:
 *    [{recordsCount=512, triggerByMaxRecordCount}, {recordCount=1, triggerByTooLarge}], we should not tell the users
 *    that the average batch records is 256, if the users knows that there are only 256 records per batch, then users
 *    will try to increase {@link TxnLogBufferedWriter#batchedWriteMaxDelayInMillis} so that there is more data per
 *    batch to improve throughput, but that does not work.
 */
@Data
public class TxnLogBufferedWriterMetricsStats {

    /**
     * Key is the name we used to create {@link TxnLogBufferedWriterMetricsStats}, and now there are two kinds:
     * ["pulsar_txn_tc_log", "pulsar_txn_tc_batched_log"]. There can be multiple labels in each
     * {@link TxnLogBufferedWriterMetricsStats}, such as The Transaction Coordinator using coordinatorId as label and
     * The Transaction Pending Ack Store using subscriptionName as label.
     */
    @VisibleForTesting
    static final HashMap<String, TxnLogBufferedWriterMetricsStats> METRICS_REGISTRY = new HashMap<>();
    /**
     * Marks all references to instance {@link TxnLogBufferedWriterMetricsStats}, after all objects that depend on the
     * {@link TxnLogBufferedWriterMetricsStats} are closed, the {@link TxnLogBufferedWriterMetricsStats} will call
     * {@link CollectorRegistry#unregister(Collector)} for release.
     */
    @VisibleForTesting
    static final HashMap<String, List<TxnLogBufferedWriterMetricsDefinition>> METRICS_INSTANCE_REFERENCE =
            new HashMap<>();

    static final double[] RECORD_COUNT_PER_ENTRY_BUCKETS = { 10, 50, 100, 200, 500, 1_000};

    static final double[] BYTES_SIZE_PER_ENTRY_BUCKETS = { 128, 512, 1_024, 2_048, 4_096, 16_384,
            102_400, 1_232_896 };

    static final double[] MAX_DELAY_TIME_BUCKETS = { 1, 5, 10 };

    /** Count of records in per transaction log batch. **/
    Histogram pulsarBatchedLogRecordsCountPerEntry;

    /** Bytes size per transaction log batch. **/
    Histogram pulsarBatchedLogEntrySizeBytes;

    /** The time of the oldest transaction log spent in the buffer before being sent. **/
    Histogram pulsarBatchedLogOldestRecordDelayTimeSeconds;

    /**
     * The count of the triggering transaction log batch flush actions by
     * {@link TxnLogBufferedWriter#batchedWriteMaxRecords}.
     */
    Counter pulsarBatchedLogTriggeringCountByRecords;

    /**
     * The count of the triggering transaction log batch flush actions by
     * {@link TxnLogBufferedWriter#batchedWriteMaxSize}.
     */
    Counter pulsarBatchedLogTriggeringCountBySize;

    /**
     * The count of the triggering transaction log batch flush actions by
     * {@link TxnLogBufferedWriter#batchedWriteMaxDelayInMillis}.
     */
    Counter pulsarBatchedLogTriggeringCountByDelayTime;

    /**
     * The count of the triggering transaction log batch flush actions by force-flush. In addition to manual flush,
     * force flush is triggered only if the log record bytes size reaches the TxnLogBufferedWriter#batchedWriteMaxSize}
     * limit.
     */
    Counter pulsarBatchedLogTriggeringCountByForce;

    public static synchronized TxnLogBufferedWriterMetricsStats getInstance(
                                                            TxnLogBufferedWriterMetricsDefinition metricsDefinition){
        // Mark who references stats.
        METRICS_INSTANCE_REFERENCE.computeIfAbsent(metricsDefinition.getComponent(), n -> new ArrayList<>());
        METRICS_INSTANCE_REFERENCE.get(metricsDefinition.getComponent()).add(metricsDefinition);
        // Get or create stats.
        return METRICS_REGISTRY.computeIfAbsent(
                metricsDefinition.getComponent(),
                n -> new TxnLogBufferedWriterMetricsStats(n, metricsDefinition.labelNames,
                        CollectorRegistry.defaultRegistry)
        );
    }

    public static synchronized void releaseReference(TxnLogBufferedWriterMetricsDefinition metricsDefinition){
        // Remove reference.
        List<TxnLogBufferedWriterMetricsDefinition> list =
                METRICS_INSTANCE_REFERENCE.get(metricsDefinition.getComponent());
        if (CollectionUtils.isEmpty(list)){
            return;
        }
        int removeCount = 0;
        while (list.contains(metricsDefinition)) {
            list.remove(metricsDefinition);
            removeCount++;
        }
        // Remove label values.
        TxnLogBufferedWriterMetricsStats stats = METRICS_REGISTRY.get(metricsDefinition.getComponent());
        if (stats == null){
            return;
        }
        if (removeCount > 0){
            stats.pulsarBatchedLogRecordsCountPerEntry.remove(metricsDefinition.labelValues);
            stats.pulsarBatchedLogEntrySizeBytes.remove(metricsDefinition.labelValues);
            stats.pulsarBatchedLogOldestRecordDelayTimeSeconds.remove(metricsDefinition.labelValues);
            stats.pulsarBatchedLogTriggeringCountByRecords.remove(metricsDefinition.labelValues);
            stats.pulsarBatchedLogTriggeringCountBySize.remove(metricsDefinition.labelValues);
            stats.pulsarBatchedLogTriggeringCountByDelayTime.remove(metricsDefinition.labelValues);
            stats.pulsarBatchedLogTriggeringCountByForce.remove(metricsDefinition.labelValues);
        }
        // Release metrics.
        if (CollectionUtils.isEmpty(list)){
            CollectorRegistry.defaultRegistry.unregister(stats.pulsarBatchedLogRecordsCountPerEntry);
            CollectorRegistry.defaultRegistry.unregister(stats.pulsarBatchedLogEntrySizeBytes);
            CollectorRegistry.defaultRegistry.unregister(stats.pulsarBatchedLogOldestRecordDelayTimeSeconds);
            CollectorRegistry.defaultRegistry.unregister(stats.pulsarBatchedLogTriggeringCountByRecords);
            CollectorRegistry.defaultRegistry.unregister(stats.pulsarBatchedLogTriggeringCountBySize);
            CollectorRegistry.defaultRegistry.unregister(stats.pulsarBatchedLogTriggeringCountByDelayTime);
            CollectorRegistry.defaultRegistry.unregister(stats.pulsarBatchedLogTriggeringCountByForce);
        }
    }

    private TxnLogBufferedWriterMetricsStats(String component, String[] labelNames, CollectorRegistry registry){
        pulsarBatchedLogRecordsCountPerEntry = new Histogram.Builder()
                .name(String.format("%s_batched_log_records_count_per_entry", component))
                .labelNames(labelNames)
                .help(String.format("%s_batched_log_records_count_per_entry A metrics for how many records in per batch"
                        + " written by the component[%s] per batch", component, component))
                .buckets(RECORD_COUNT_PER_ENTRY_BUCKETS)
                .register(registry);
        pulsarBatchedLogEntrySizeBytes = new Histogram.Builder()
                .name(String.format("%s_batched_log_entry_size_bytes", component))
                .labelNames(labelNames)
                .help(String.format("%s_batched_log_records_count_per_entry A metrics for how many bytes in per batch"
                        + " written by the component[%s] per batch", component, component))
                .buckets(BYTES_SIZE_PER_ENTRY_BUCKETS)
                .register(registry);
        pulsarBatchedLogOldestRecordDelayTimeSeconds = new Histogram.Builder()
                .name(String.format("%s_batched_log_oldest_record_delay_time_seconds", component))
                .labelNames(labelNames)
                .help(String.format("%s_batched_log_records_count_per_entry A metrics for the max latency in per batch"
                        + " written by the component[%s] per batch", component, component))
                .buckets(MAX_DELAY_TIME_BUCKETS)
                .register(registry);
        pulsarBatchedLogTriggeringCountByRecords = new Counter.Builder()
                .name(String.format("%s_batched_log_triggering_count_by_records", component))
                .labelNames(labelNames)
                .help(String.format("%s_batched_log_records_count_per_entry A metrics for how many batches of component"
                        + " %s were triggered due to threshold \"batchedWriteMaxRecords\"", component, component))
                .register(registry);
        pulsarBatchedLogTriggeringCountBySize = new Counter.Builder()
                .name(String.format("%s_batched_log_triggering_count_by_size", component))
                .labelNames(labelNames)
                .help(String.format("%s_batched_log_records_count_per_entry A metrics for how many batches of component"
                        + " %s were triggered due to threshold \"batchedWriteMaxSize\"", component, component))
                .register(registry);
        pulsarBatchedLogTriggeringCountByDelayTime = new Counter.Builder()
                .name(String.format("%s_batched_log_triggering_count_by_delay_time", component))
                .labelNames(labelNames)
                .help(String.format("%s_batched_log_records_count_per_entry A metrics for how many batches of component"
                        + " %s were triggered due to threshold \"batchedWriteMaxDelayInMillis\"", component, component))
                .register(registry);
        pulsarBatchedLogTriggeringCountByForce = new Counter.Builder()
                .name(String.format("%s_batched_log_triggering_count_by_force", component))
                .labelNames(labelNames)
                .help(String.format("%s_batched_log_records_count_per_entry A metrics for how many batches of component"
                        + " %s were triggered due to threshold \"forceFlush\"", component, component))
                .register(registry);
    }
}
