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

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.io.Closeable;
import java.util.HashMap;
import lombok.Data;

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
public class TxnLogBufferedWriterMetricsStats implements Closeable {

    /**
     * Key is the name we used to create {@link TxnLogBufferedWriterMetricsStats}, and now there are two kinds:
     * ["pulsar_txn_tc_log", "pulsar_txn_tc_batched_log"]. There can be multiple labels in each
     * {@link TxnLogBufferedWriterMetricsStats}, such as The Transaction Coordinator using coordinatorId as label and
     * The Transaction Pending Ack Store using subscriptionName as label.
     */
    private static final HashMap<String, Collector> COLLECTOR_CACHE = new HashMap<>();

    static final double[] RECORD_COUNT_PER_ENTRY_BUCKETS = { 10, 50, 100, 200, 500, 1_000};

    static final double[] BYTES_SIZE_PER_ENTRY_BUCKETS = { 128, 512, 1_024, 2_048, 4_096, 16_384,
            102_400, 1_232_896 };

    static final double[] MAX_DELAY_TIME_BUCKETS = { 1, 5, 10 };

    private final String metricsPrefix;

    private final String[] labelNames;

    private final String[] labelValues;

    /** Count of records in per transaction log batch. **/
    private final Histogram pulsarBatchedLogRecordsCountPerEntry;
    private final Histogram.Child pulsarBatchedLogRecordsCountPerEntryChild;

    /** Bytes size per transaction log batch. **/
    private final Histogram pulsarBatchedLogEntrySizeBytes;
    private final Histogram.Child pulsarBatchedLogEntrySizeBytesChild;

    /** The time of the oldest transaction log spent in the buffer before being sent. **/
    private final  Histogram pulsarBatchedLogOldestRecordDelayTimeSeconds;
    private final Histogram.Child pulsarBatchedLogOldestRecordDelayTimeSecondsChild;

    /**
     * The count of the triggering transaction log batch flush actions by
     * {@link TxnLogBufferedWriter#batchedWriteMaxRecords}.
     */
    private final Counter pulsarBatchedLogTriggeringCountByRecords;
    private final Counter.Child pulsarBatchedLogTriggeringCountByRecordsChild;

    /**
     * The count of the triggering transaction log batch flush actions by
     * {@link TxnLogBufferedWriter#batchedWriteMaxSize}.
     */
    private final Counter pulsarBatchedLogTriggeringCountBySize;
    private final Counter.Child pulsarBatchedLogTriggeringCountBySizeChild;

    /**
     * The count of the triggering transaction log batch flush actions by
     * {@link TxnLogBufferedWriter#batchedWriteMaxDelayInMillis}.
     */
    private final Counter pulsarBatchedLogTriggeringCountByDelayTime;
    private final Counter.Child pulsarBatchedLogTriggeringCountByDelayTimeChild;

    /**
     * The count of the triggering transaction log batch flush actions by force-flush. In addition to manual flush,
     * force flush is triggered only if the log record bytes size reaches the TxnLogBufferedWriter#batchedWriteMaxSize}
     * limit.
     */
    private final  Counter pulsarBatchedLogTriggeringCountByForce;
    private final Counter.Child pulsarBatchedLogTriggeringCountByForceChild;

    public void close(){
        pulsarBatchedLogRecordsCountPerEntry.remove(labelValues);
        pulsarBatchedLogEntrySizeBytes.remove(labelValues);
        pulsarBatchedLogOldestRecordDelayTimeSeconds.remove(labelValues);
        pulsarBatchedLogTriggeringCountByRecords.remove(labelValues);
        pulsarBatchedLogTriggeringCountBySize.remove(labelValues);
        pulsarBatchedLogTriggeringCountByDelayTime.remove(labelValues);
        pulsarBatchedLogTriggeringCountByForce.remove(labelValues);
    }

    public TxnLogBufferedWriterMetricsStats(String metricsPrefix, String[] labelNames, String[] labelValues,
                                              CollectorRegistry registry){
        this.metricsPrefix = metricsPrefix;
        this.labelNames = labelNames;
        this.labelValues = labelValues;

        String pulsarBatchedLogRecordsCountPerEntryName =
                String.format("%s_batched_log_records_count_per_entry", metricsPrefix);
        pulsarBatchedLogRecordsCountPerEntry = (Histogram) COLLECTOR_CACHE.computeIfAbsent(
                pulsarBatchedLogRecordsCountPerEntryName,
                k -> new Histogram.Builder()
                            .name(pulsarBatchedLogRecordsCountPerEntryName)
                            .labelNames(labelNames)
                            .help("A metrics for how many records in per batch")
                            .buckets(RECORD_COUNT_PER_ENTRY_BUCKETS)
                            .register(registry));

        pulsarBatchedLogRecordsCountPerEntryChild = pulsarBatchedLogRecordsCountPerEntry.labels(labelValues);

        String pulsarBatchedLogEntrySizeBytesName = String.format("%s_batched_log_entry_size_bytes", metricsPrefix);
        pulsarBatchedLogEntrySizeBytes = (Histogram) COLLECTOR_CACHE.computeIfAbsent(
                pulsarBatchedLogEntrySizeBytesName,
                k -> new Histogram.Builder()
                        .name(pulsarBatchedLogEntrySizeBytesName)
                        .labelNames(labelNames)
                        .help("A metrics for how many bytes in per batch")
                        .buckets(RECORD_COUNT_PER_ENTRY_BUCKETS)
                        .register(registry));
        pulsarBatchedLogEntrySizeBytesChild = pulsarBatchedLogEntrySizeBytes.labels(labelValues);

        String pulsarBatchedLogOldestRecordDelayTimeSecondsName =
                String.format("%s_batched_log_oldest_record_delay_time_seconds", metricsPrefix);
        pulsarBatchedLogOldestRecordDelayTimeSeconds = (Histogram) COLLECTOR_CACHE.computeIfAbsent(
                pulsarBatchedLogOldestRecordDelayTimeSecondsName,
                k -> new Histogram.Builder()
                        .name(pulsarBatchedLogOldestRecordDelayTimeSecondsName)
                        .labelNames(labelNames)
                        .help("A metrics for the max latency in per batch")
                        .buckets(RECORD_COUNT_PER_ENTRY_BUCKETS)
                        .register(registry));
        pulsarBatchedLogOldestRecordDelayTimeSecondsChild =
                pulsarBatchedLogOldestRecordDelayTimeSeconds.labels(labelValues);

        String pulsarBatchedLogTriggeringCountByRecordsName =
                String.format("%s_batched_log_triggering_count_by_records", metricsPrefix);
        pulsarBatchedLogTriggeringCountByRecords = (Counter) COLLECTOR_CACHE.computeIfAbsent(
                pulsarBatchedLogTriggeringCountByRecordsName,
                k -> new Counter.Builder()
                        .name(pulsarBatchedLogTriggeringCountByRecordsName)
                        .labelNames(labelNames)
                        .help("A metrics for how many batches were triggered due to threshold"
                                + " \"batchedWriteMaxRecords\"")
                        .register(registry));
        pulsarBatchedLogTriggeringCountByRecordsChild = pulsarBatchedLogTriggeringCountByRecords.labels(labelValues);

        String pulsarBatchedLogTriggeringCountBySizeName =
                String.format("%s_batched_log_triggering_count_by_size", metricsPrefix);
        pulsarBatchedLogTriggeringCountBySize = (Counter) COLLECTOR_CACHE.computeIfAbsent(
                pulsarBatchedLogTriggeringCountBySizeName,
                k -> new Counter.Builder()
                        .name(pulsarBatchedLogTriggeringCountBySizeName)
                        .labelNames(labelNames)
                        .help("A metrics for how many batches were triggered due to threshold \"batchedWriteMaxSize\"")
                        .register(registry));
        pulsarBatchedLogTriggeringCountBySizeChild = pulsarBatchedLogTriggeringCountBySize.labels(labelValues);

        String pulsarBatchedLogTriggeringCountByDelayTimeName =
                String.format("%s_batched_log_triggering_count_by_delay_time", metricsPrefix);
        pulsarBatchedLogTriggeringCountByDelayTime = (Counter) COLLECTOR_CACHE.computeIfAbsent(
                pulsarBatchedLogTriggeringCountByDelayTimeName,
                k -> new Counter.Builder()
                        .name(pulsarBatchedLogTriggeringCountByDelayTimeName)
                        .labelNames(labelNames)
                        .help("A metrics for how many batches were triggered due to threshold"
                                + " \"batchedWriteMaxDelayInMillis\"")
                        .register(registry));
        pulsarBatchedLogTriggeringCountByDelayTimeChild =
                pulsarBatchedLogTriggeringCountByDelayTime.labels(labelValues);

        String pulsarBatchedLogTriggeringCountByForcename =
                String.format("%s_batched_log_triggering_count_by_force", metricsPrefix);
        pulsarBatchedLogTriggeringCountByForce = (Counter) COLLECTOR_CACHE.computeIfAbsent(
                pulsarBatchedLogTriggeringCountByForcename,
                k -> new Counter.Builder()
                        .name(pulsarBatchedLogTriggeringCountByForcename)
                        .labelNames(labelNames)
                        .help("A metrics for how many batches were triggered due to threshold \"forceFlush\"")
                        .register(registry));
        pulsarBatchedLogTriggeringCountByForceChild = pulsarBatchedLogTriggeringCountByForce.labels(labelValues);
    }

    public void triggerFlushByRecordsCount(int recordCount, long bytesSize, long delayMillis){
        pulsarBatchedLogTriggeringCountByRecordsChild.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    public void triggerFlushByBytesSize(int recordCount, long bytesSize, long delayMillis){
        pulsarBatchedLogTriggeringCountBySizeChild.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    public void triggerFlushByByMaxDelay(int recordCount, long bytesSize, long delayMillis){
        pulsarBatchedLogTriggeringCountByDelayTimeChild.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    public void triggerFlushByForce(int recordCount, long bytesSize, long delayMillis){
        pulsarBatchedLogTriggeringCountByForceChild.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    /**
     * Append the metrics which is type of histogram.
     */
    private void observeHistogram(int recordCount, long bytesSize, long delayMillis){
        pulsarBatchedLogRecordsCountPerEntryChild.observe(recordCount);
        pulsarBatchedLogEntrySizeBytesChild.observe(bytesSize);
        pulsarBatchedLogOldestRecordDelayTimeSecondsChild.observe(delayMillis);
    }
}
