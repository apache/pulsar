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
package org.apache.pulsar.sql.presto;

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;

/**
 * This class helps to track metrics related to the connector.
 */
public class PulsarConnectorMetricsTracker implements AutoCloseable{

    private final StatsLogger statsLogger;

    private static final String SCOPE = "split";

    // metric names

    // time spend on waiting to get entry from entry queue because it is empty
    private static final String ENTRY_QUEUE_DEQUEUE_WAIT_TIME = "entry-queue-dequeue-wait-time";

    // total time spent on waiting to get entry from entry queue per query
    private static final String ENTRY_QUEUE_DEQUEUE_WAIT_TIME_PER_QUERY = "entry-queue-dequeue-wait-time-per-query";

    // number of bytes read from BookKeeper
    private static final String BYTES_READ = "bytes-read";

    // total number of bytes read per query
    private static final String BYTES_READ_PER_QUERY = "bytes-read-per-query";

    // time spent on derserializing entries
    private static final String ENTRY_DESERIALIZE_TIME = "entry-deserialize-time";

    // time spent on derserializing entries per query
    private static final String ENTRY_DESERIALIZE_TIME_PER_QUERY = "entry-deserialize-time_per_query";

    // time spent on waiting for message queue enqueue because the message queue is full
    private static final String MESSAGE_QUEUE_ENQUEUE_WAIT_TIME = "message-queue-enqueue-wait-time";

    // time spent on waiting for message queue enqueue because message queue is full per query
    private static final String MESSAGE_QUEUE_ENQUEUE_WAIT_TIME_PER_QUERY = "message-queue-enqueue-wait-time-per-query";

    private static final String NUM_MESSAGES_DERSERIALIZED = "num-messages-deserialized";

    // number of messages deserialized
    public static final String NUM_MESSAGES_DERSERIALIZED_PER_ENTRY = "num-messages-deserialized-per-entry";

    // number of messages deserialized per query
    public static final String NUM_MESSAGES_DERSERIALIZED_PER_QUERY = "num-messages-deserialized-per-query";

    // number of read attempts (fail if queues are full)
    public static final String READ_ATTEMPTS = "read-attempts";

    // number of read attempts per query
    public static final String READ_ATTEMTPS_PER_QUERY = "read-attempts-per-query";

    // latency of reads per batch
    public static final String READ_LATENCY_PER_BATCH = "read-latency-per-batch";

    // total read latency per query
    public static final String READ_LATENCY_PER_QUERY = "read-latency-per-query";

    // number of entries per batch
    public static final String NUM_ENTRIES_PER_BATCH = "num-entries-per-batch";

    // number of entries per query
    public static final String NUM_ENTRIES_PER_QUERY = "num-entries-per-query";

    // time spent on waiting to dequeue from message queue because it is empty per query
    public static final String MESSAGE_QUEUE_DEQUEUE_WAIT_TIME_PER_QUERY = "message-queue-dequeue-wait-time-per-query";

    // time spent on deserializing message to record. For example, Avro, JSON, and so on
    public static final String RECORD_DESERIALIZE_TIME = "record-deserialize-time";

    // time spent on deserializing message to record per query
    private static final String RECORD_DESERIALIZE_TIME_PER_QUERY = "record-deserialize-time-per-query";

    // Number of records deserialized
    private static final String NUM_RECORD_DESERIALIZED = "num-record-deserialized";

    private static final String TOTAL_EXECUTION_TIME = "total-execution-time";

    // stats loggers

    private final OpStatsLogger statsLoggerEntryQueueDequeueWaitTime;
    private final Counter statsLoggerBytesRead;
    private final OpStatsLogger statsLoggerEntryDeserializeTime;
    private final OpStatsLogger statsLoggerMessageQueueEnqueueWaitTime;
    private final Counter statsLoggerNumMessagesDeserialized;
    private final OpStatsLogger statsLoggerNumMessagesDeserializedPerEntry;
    private final OpStatsLogger statsLoggerReadAttempts;
    private final OpStatsLogger statsLoggerReadLatencyPerBatch;
    private final OpStatsLogger statsLoggerNumEntriesPerBatch;
    private final OpStatsLogger statsLoggerRecordDeserializeTime;
    private final Counter statsLoggerNumRecordDeserialized;
    private final OpStatsLogger statsLoggerTotalExecutionTime;

    // internal tracking variables
    private long entryQueueDequeueWaitTimeStartTime;
    private long entryQueueDequeueWaitTimeSum = 0L;
    private long bytesReadSum = 0L;
    private long entryDeserializeTimeStartTime;
    private long entryDeserializeTimeSum = 0L;
    private long messageQueueEnqueueWaitTimeStartTime;
    private long messageQueueEnqueueWaitTimeSum = 0L;
    private long numMessagesDerserializedSum = 0L;
    private long numMessagedDerserializedPerBatch = 0L;
    private long readAttemptsSuccessSum = 0L;
    private long readAttemptsFailSum = 0L;
    private long readLatencySuccessSum = 0L;
    private long readLatencyFailSum = 0L;
    private long numEntriesPerBatchSum = 0L;
    private long messageQueueDequeueWaitTimeSum = 0L;
    private long recordDeserializeTimeStartTime;
    private long recordDeserializeTimeSum = 0L;

    public PulsarConnectorMetricsTracker(StatsProvider statsProvider) {
        this.statsLogger = statsProvider instanceof NullStatsProvider
                ? null : statsProvider.getStatsLogger(SCOPE);

        if (this.statsLogger != null) {
            statsLoggerEntryQueueDequeueWaitTime = statsLogger.getOpStatsLogger(ENTRY_QUEUE_DEQUEUE_WAIT_TIME);
            statsLoggerBytesRead = statsLogger.getCounter(BYTES_READ);
            statsLoggerEntryDeserializeTime = statsLogger.getOpStatsLogger(ENTRY_DESERIALIZE_TIME);
            statsLoggerMessageQueueEnqueueWaitTime = statsLogger.getOpStatsLogger(MESSAGE_QUEUE_ENQUEUE_WAIT_TIME);
            statsLoggerNumMessagesDeserialized = statsLogger.getCounter(NUM_MESSAGES_DERSERIALIZED);
            statsLoggerNumMessagesDeserializedPerEntry = statsLogger
                .getOpStatsLogger(NUM_MESSAGES_DERSERIALIZED_PER_ENTRY);
            statsLoggerReadAttempts = statsLogger.getOpStatsLogger(READ_ATTEMPTS);
            statsLoggerReadLatencyPerBatch = statsLogger.getOpStatsLogger(READ_LATENCY_PER_BATCH);
            statsLoggerNumEntriesPerBatch = statsLogger.getOpStatsLogger(NUM_ENTRIES_PER_BATCH);
            statsLoggerRecordDeserializeTime = statsLogger.getOpStatsLogger(RECORD_DESERIALIZE_TIME);
            statsLoggerNumRecordDeserialized = statsLogger.getCounter(NUM_RECORD_DESERIALIZED);
            statsLoggerTotalExecutionTime = statsLogger.getOpStatsLogger(TOTAL_EXECUTION_TIME);
        } else {
            statsLoggerEntryQueueDequeueWaitTime = null;
            statsLoggerBytesRead = null;
            statsLoggerEntryDeserializeTime = null;
            statsLoggerMessageQueueEnqueueWaitTime = null;
            statsLoggerNumMessagesDeserialized = null;
            statsLoggerNumMessagesDeserializedPerEntry = null;
            statsLoggerReadAttempts = null;
            statsLoggerReadLatencyPerBatch = null;
            statsLoggerNumEntriesPerBatch = null;
            statsLoggerRecordDeserializeTime = null;
            statsLoggerNumRecordDeserialized = null;
            statsLoggerTotalExecutionTime = null;
        }
    }

    public void start_ENTRY_QUEUE_DEQUEUE_WAIT_TIME() {
        if (statsLogger != null) {
            entryQueueDequeueWaitTimeStartTime = System.nanoTime();
        }
    }

    public void end_ENTRY_QUEUE_DEQUEUE_WAIT_TIME() {
        if (statsLogger != null) {
            long time = System.nanoTime() - entryQueueDequeueWaitTimeStartTime;
            entryQueueDequeueWaitTimeSum += time;
            statsLoggerEntryQueueDequeueWaitTime.registerSuccessfulEvent(time, TimeUnit.NANOSECONDS);
        }
    }

    public void register_BYTES_READ(long bytes) {
        if (statsLogger != null) {
            bytesReadSum += bytes;
            statsLoggerBytesRead.add(bytes);
        }
    }

    public void start_ENTRY_DESERIALIZE_TIME() {
        if (statsLogger != null) {
            entryDeserializeTimeStartTime = System.nanoTime();
        }
    }

    public void end_ENTRY_DESERIALIZE_TIME() {
        if (statsLogger != null) {
            long time = System.nanoTime() - entryDeserializeTimeStartTime;
            entryDeserializeTimeSum += time;
            statsLoggerEntryDeserializeTime.registerSuccessfulEvent(time, TimeUnit.NANOSECONDS);
        }
    }

    public void start_MESSAGE_QUEUE_ENQUEUE_WAIT_TIME() {
        if (statsLogger != null) {
            messageQueueEnqueueWaitTimeStartTime = System.nanoTime();
        }
    }

    public void end_MESSAGE_QUEUE_ENQUEUE_WAIT_TIME() {
        if (statsLogger != null) {
            long time = System.nanoTime() - messageQueueEnqueueWaitTimeStartTime;
            messageQueueEnqueueWaitTimeSum += time;
            statsLoggerMessageQueueEnqueueWaitTime.registerSuccessfulEvent(time, TimeUnit.NANOSECONDS);
        }
    }

    public void incr_NUM_MESSAGES_DESERIALIZED_PER_ENTRY() {
        if (statsLogger != null) {
            numMessagedDerserializedPerBatch++;
            statsLoggerNumMessagesDeserialized.add(1);
        }
    }

    public void end_NUM_MESSAGES_DESERIALIZED_PER_ENTRY() {
        if (statsLogger != null) {
            numMessagesDerserializedSum += numMessagedDerserializedPerBatch;
            statsLoggerNumMessagesDeserializedPerEntry.registerSuccessfulValue(numMessagedDerserializedPerBatch);
            numMessagedDerserializedPerBatch = 0L;
        }
    }

    public void incr_READ_ATTEMPTS_SUCCESS() {
        if (statsLogger != null) {
            readAttemptsSuccessSum++;
            statsLoggerReadAttempts.registerSuccessfulValue(1L);
        }
    }

    public void incr_READ_ATTEMPTS_FAIL() {
        if (statsLogger != null) {
            readAttemptsFailSum++;
            statsLoggerReadAttempts.registerFailedValue(1L);
        }
    }

    public void register_READ_LATENCY_PER_BATCH_SUCCESS(long latency) {
        if (statsLogger != null) {
            readLatencySuccessSum += latency;
            statsLoggerReadLatencyPerBatch.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
        }
    }

    public void register_READ_LATENCY_PER_BATCH_FAIL(long latency) {
        if (statsLogger != null) {
            readLatencyFailSum += latency;
            statsLoggerReadLatencyPerBatch.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
        }
    }

    public void incr_NUM_ENTRIES_PER_BATCH_SUCCESS(long delta) {
        if (statsLogger != null) {
            numEntriesPerBatchSum += delta;
            statsLoggerNumEntriesPerBatch.registerSuccessfulValue(delta);
        }
    }

    public void incr_NUM_ENTRIES_PER_BATCH_FAIL(long delta) {
        if (statsLogger != null) {
            statsLoggerNumEntriesPerBatch.registerFailedValue(delta);
        }
    }

    public void register_MESSAGE_QUEUE_DEQUEUE_WAIT_TIME(long latency) {
        if (statsLogger != null) {
            messageQueueDequeueWaitTimeSum += latency;
        }
    }

    public void start_RECORD_DESERIALIZE_TIME() {
        if (statsLogger != null) {
            recordDeserializeTimeStartTime = System.nanoTime();
        }
    }

    public void end_RECORD_DESERIALIZE_TIME() {
        if (statsLogger != null) {
            long time = System.nanoTime() - recordDeserializeTimeStartTime;
            recordDeserializeTimeSum += time;
            statsLoggerRecordDeserializeTime.registerSuccessfulEvent(time, TimeUnit.NANOSECONDS);
        }
    }

    public void incr_NUM_RECORD_DESERIALIZED() {
        if (statsLogger != null) {
            statsLoggerNumRecordDeserialized.add(1);
        }
    }

    public void register_TOTAL_EXECUTION_TIME(long latency) {
        if (statsLogger != null) {
            statsLoggerTotalExecutionTime.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void close() {
        if (statsLogger != null) {
            // register total entry dequeue wait time for query
            statsLogger.getOpStatsLogger(ENTRY_QUEUE_DEQUEUE_WAIT_TIME_PER_QUERY)
                    .registerSuccessfulEvent(entryQueueDequeueWaitTimeSum, TimeUnit.NANOSECONDS);

            //register bytes read per query
            statsLogger.getOpStatsLogger(BYTES_READ_PER_QUERY)
                    .registerSuccessfulValue(bytesReadSum);

            // register total time spent deserializing entries for query
            statsLogger.getOpStatsLogger(ENTRY_DESERIALIZE_TIME_PER_QUERY)
                    .registerSuccessfulEvent(entryDeserializeTimeSum, TimeUnit.NANOSECONDS);

            // register time spent waiting for message queue enqueue because message queue is full per query
            statsLogger.getOpStatsLogger(MESSAGE_QUEUE_ENQUEUE_WAIT_TIME_PER_QUERY)
                    .registerSuccessfulEvent(messageQueueEnqueueWaitTimeSum, TimeUnit.NANOSECONDS);

            // register number of messages deserialized per query
            statsLogger.getOpStatsLogger(NUM_MESSAGES_DERSERIALIZED_PER_QUERY)
                    .registerSuccessfulValue(numMessagesDerserializedSum);

            // register number of read attempts per query
            statsLogger.getOpStatsLogger(READ_ATTEMTPS_PER_QUERY)
                    .registerSuccessfulValue(readAttemptsSuccessSum);
            statsLogger.getOpStatsLogger(READ_ATTEMTPS_PER_QUERY)
                    .registerFailedValue(readAttemptsFailSum);

            // register total read latency for query
            statsLogger.getOpStatsLogger(READ_LATENCY_PER_QUERY)
                    .registerSuccessfulEvent(readLatencySuccessSum, TimeUnit.NANOSECONDS);
            statsLogger.getOpStatsLogger(READ_LATENCY_PER_QUERY)
                    .registerFailedEvent(readLatencyFailSum, TimeUnit.NANOSECONDS);

            // register number of entries per query
            statsLogger.getOpStatsLogger(NUM_ENTRIES_PER_QUERY)
                    .registerSuccessfulValue(numEntriesPerBatchSum);

            // register time spent waiting to read for message queue per query
            statsLogger.getOpStatsLogger(MESSAGE_QUEUE_DEQUEUE_WAIT_TIME_PER_QUERY)
                    .registerSuccessfulEvent(messageQueueDequeueWaitTimeSum, TimeUnit.MILLISECONDS);

            // register time spent deserializing records per query
            statsLogger.getOpStatsLogger(RECORD_DESERIALIZE_TIME_PER_QUERY)
                    .registerSuccessfulEvent(recordDeserializeTimeSum, TimeUnit.NANOSECONDS);
        }
    }
}
