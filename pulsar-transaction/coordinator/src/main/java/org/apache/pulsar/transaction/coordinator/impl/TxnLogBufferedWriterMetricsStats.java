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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;

/***
 * Describes the working status of the {@link TxnLogBufferedWriter}, helps users tune the thresholds of
 * {@link TxnLogBufferedWriter} for best performance.
 * Note-1: When batch feature is turned off, no data is logged at this. In this scenario，users can see the
 *   {@link org.apache.bookkeeper.mledger.ManagedLedgerMXBean}.
 * Note-2: Even if enable batch feature. A batch has numerous triggers. The metrics in this class count each type of
 *   trigger to allow you to diagnose what mostly causing a batch flush. The metric also includes a histogram for delay
 *   of batch since 1st record entered, the size of batch in bytes number of records in batch. This will help you to
 *   tune the parameters that control some of the batch flush triggers: maxDelay, maxRecords, maxSize.
 *   Note that the 4th trigger - a single record larger than batch size - triggers a flush of the current batch, but
 *   the big record itself is not written in batch hence is not included in the batch metrics written above (batch
 *   size, batch delay, etc). The trigger is of course counted as other trigger types.
 */
public class TxnLogBufferedWriterMetricsStats implements Closeable {

    static final double[] RECORD_COUNT_PER_ENTRY_BUCKETS = {10, 50, 100, 200, 500, 1_000};

    static final double[] BYTES_SIZE_PER_ENTRY_BUCKETS = {128, 512, 1_024, 2_048, 4_096, 16_384,
            102_400, 1_232_896};

    static final double[] MAX_DELAY_TIME_BUCKETS = {1, 5, 10};

    private final CollectorRegistry collectorRegistry;
    @Getter
    private final String metricsPrefix;

    private final String[] labelNames;

    private final String[] labelValues;

    /** Count of records in per transaction log batch. **/
    private final Histogram recordsPerBatchMetric;
    private final Histogram.Child recordsPerBatchHistogram;

    /** Bytes size per transaction log batch. **/
    private final Histogram batchSizeBytesMetric;
    private final Histogram.Child batchSizeBytesHistogram;

    /** The time of the oldest transaction log spent in the buffer before being sent. **/
    private final Histogram oldestRecordInBatchDelayTimeSecondsMetric;
    private final Histogram.Child oldestRecordInBatchDelayTimeSecondsHistogram;

    /** The count of the triggering transaction log batch flush actions by "batchedWriteMaxRecords". **/
    private final Counter batchFlushTriggeredByMaxRecordsMetric;
    private final Counter.Child batchFlushTriggeredByMaxRecordsCounter;

    /** The count of the triggering transaction log batch flush actions by "batchedWriteMaxSize". **/
    private final Counter batchFlushTriggeredByMaxSizeMetric;
    private final Counter.Child batchFlushTriggeredByMaxSizeCounter;

    /** The count of the triggering transaction log batch flush actions by "batchedWriteMaxDelayInMillis". **/
    private final Counter batchFlushTriggeredByMaxDelayMetric;
    private final Counter.Child batchFlushTriggeredByMaxDelayCounter;

    /**
     * If {@link TxnLogBufferedWriter#asyncAddData(Object, TxnLogBufferedWriter.AddDataCallback, Object)} accept a
     * request that param-data is too large (larger than "batchedWriteMaxSize"), then two flushes are executed:
     *    1. Write the data cached in the queue to BK.
     *    2. Direct write the large data to BK.
     * This ensures the sequential nature of multiple writes to BK.
     */
    private final Counter batchFlushTriggeredByLargeSingleDataMetric;
    private final Counter.Child batchFlushTriggeredByLargeSingleDataCounter;

    private final AtomicBoolean closed;

    /**
     * Users needs to ensure that the {@link TxnLogBufferedWriterMetricsStats} of the same {@param metricsPrefix} can
     * only create once, otherwise an IllegalArgumentException will be thrown.
     */
    public TxnLogBufferedWriterMetricsStats(String metricsPrefix, String[] labelNames, String[] labelValues,
                                            CollectorRegistry registry) {
        this.collectorRegistry = registry;
        this.metricsPrefix = metricsPrefix;
        this.labelNames = labelNames.clone();
        this.labelValues = labelValues.clone();

        String recordsPerBatchMetricName =
                String.format("%s_bufferedwriter_batch_records", metricsPrefix);
        recordsPerBatchMetric = new Histogram.Builder()
                        .name(recordsPerBatchMetricName)
                        .labelNames(this.labelNames)
                        .help("Records per batch histogram")
                        .buckets(RECORD_COUNT_PER_ENTRY_BUCKETS)
                        .register(collectorRegistry);
        recordsPerBatchHistogram = recordsPerBatchMetric.labels(this.labelValues);

        String batchSizeBytesMetricName = String.format("%s_bufferedwriter_batch_size_bytes", metricsPrefix);
        batchSizeBytesMetric = new Histogram.Builder()
                        .name(batchSizeBytesMetricName)
                        .labelNames(this.labelNames)
                        .help("Batch size in bytes histogram")
                        .buckets(BYTES_SIZE_PER_ENTRY_BUCKETS)
                        .register(collectorRegistry);
        batchSizeBytesHistogram = batchSizeBytesMetric.labels(this.labelValues);

        String oldestRecordInBatchDelayTimeSecondsMetricName =
                String.format("%s_bufferedwriter_batch_oldest_record_delay_seconds", metricsPrefix);
        oldestRecordInBatchDelayTimeSecondsMetric = new Histogram.Builder()
                        .name(oldestRecordInBatchDelayTimeSecondsMetricName)
                        .labelNames(this.labelNames)
                        .help("Max record latency in batch histogram")
                        .buckets(MAX_DELAY_TIME_BUCKETS)
                        .register(collectorRegistry);
        oldestRecordInBatchDelayTimeSecondsHistogram =
                oldestRecordInBatchDelayTimeSecondsMetric.labels(this.labelValues);

        String batchFlushTriggeringByMaxRecordsMetricName =
                String.format("%s_bufferedwriter_flush_trigger_max_records", metricsPrefix);
        batchFlushTriggeredByMaxRecordsMetric = new Counter.Builder()
                        .name(batchFlushTriggeringByMaxRecordsMetricName)
                        .labelNames(this.labelNames)
                        .help("Event count of batch flush triggered by max records count")
                        .register(collectorRegistry);
        batchFlushTriggeredByMaxRecordsCounter = batchFlushTriggeredByMaxRecordsMetric.labels(this.labelValues);

        String batchFlushTriggeringByMaxSizeMetricName =
                String.format("%s_bufferedwriter_flush_trigger_max_size", metricsPrefix);
        batchFlushTriggeredByMaxSizeMetric = new Counter.Builder()
                        .name(batchFlushTriggeringByMaxSizeMetricName)
                        .labelNames(this.labelNames)
                        .help("Event count of batch flush triggered by max bytes size")
                        .register(collectorRegistry);
        batchFlushTriggeredByMaxSizeCounter = batchFlushTriggeredByMaxSizeMetric.labels(this.labelValues);

        String batchFlushTriggeringByMaxDelayMetricName =
                String.format("%s_bufferedwriter_flush_trigger_max_delay", metricsPrefix);
        batchFlushTriggeredByMaxDelayMetric = new Counter.Builder()
                        .name(batchFlushTriggeringByMaxDelayMetricName)
                        .labelNames(this.labelNames)
                        .help("Event count of batch flush triggered by max delay time")
                        .register(collectorRegistry);
        batchFlushTriggeredByMaxDelayCounter =
                batchFlushTriggeredByMaxDelayMetric.labels(this.labelValues);

        String batchFlushTriggeringByLargeSingleDataMetricName =
                String.format("%s_bufferedwriter_flush_trigger_large_data", metricsPrefix);
        batchFlushTriggeredByLargeSingleDataMetric = new Counter.Builder()
                        .name(batchFlushTriggeringByLargeSingleDataMetricName)
                        .labelNames(this.labelNames)
                        .help("Event count of batch flush triggered by the single large data write")
                        .register(collectorRegistry);
        batchFlushTriggeredByLargeSingleDataCounter =
                batchFlushTriggeredByLargeSingleDataMetric.labels(this.labelValues);

        this.closed = new AtomicBoolean(false);
    }

    public void close() {
        // Doing unregister more than once will throw exception, so avoid repeating close.
        if (!closed.compareAndSet(false, true)){
            return;
        }
        collectorRegistry.unregister(recordsPerBatchMetric);
        collectorRegistry.unregister(batchSizeBytesMetric);
        collectorRegistry.unregister(oldestRecordInBatchDelayTimeSecondsMetric);
        collectorRegistry.unregister(batchFlushTriggeredByMaxRecordsMetric);
        collectorRegistry.unregister(batchFlushTriggeredByMaxSizeMetric);
        collectorRegistry.unregister(batchFlushTriggeredByMaxDelayMetric);
        collectorRegistry.unregister(batchFlushTriggeredByLargeSingleDataMetric);
    }

    public void triggerFlushByRecordsCount(int recordCount, long bytesSize, long delayMillis) {
        batchFlushTriggeredByMaxRecordsCounter.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    public void triggerFlushByBytesSize(int recordCount, long bytesSize, long delayMillis) {
        batchFlushTriggeredByMaxSizeCounter.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    public void triggerFlushByByMaxDelay(int recordCount, long bytesSize, long delayMillis) {
        batchFlushTriggeredByMaxDelayCounter.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    public void triggerFlushByLargeSingleData(int recordCount, long bytesSize, long delayMillis) {
        batchFlushTriggeredByLargeSingleDataCounter.inc();
        observeHistogram(recordCount, bytesSize, delayMillis);
    }

    /**
     * Append the metrics which is type of histogram.
     */
    private void observeHistogram(int recordCount, long bytesSize, long delayMillis) {
        recordsPerBatchHistogram.observe(recordCount);
        batchSizeBytesHistogram.observe(bytesSize);
        // Unit conversion: milliSeconds -> seconds.
        oldestRecordInBatchDelayTimeSecondsHistogram.observe(delayMillis / 1000.0);
    }
}
