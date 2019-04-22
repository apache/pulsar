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

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;

import java.util.concurrent.TimeUnit;

public class PulsarConnectorMetricsTracker implements AutoCloseable{

    private final StatsLogger statsLogger;

    private static final String SCOPE = "split";

    /** metric names **/

    // time spend waiting to get entry from entry queue because it is empty
    private static final String ENTRY_QUEUE_DEQUEUE_WAIT_TIME = "entry-queue-dequeue-wait-time";

    // total time spend waiting to get entry from entry queue per query
    private static final String ENTRY_QUEUE_DEQUEUE_WAIT_TIME_PER_QUERY = "entry-queue-dequeue-wait-time-per-query";

    // number of bytes read from bookkeeper
    private static final String BYTES_READ = "bytes-read";

    // total number of bytes read per query
    private static final String BYTES_READ_PER_QUERY = "bytes-read-per-query";

    // time spent derserializing entries
    private static final String ENTRY_DESERIALIZE_TIME = "entry-deserialize-time";

    // time spent derserializing entries per query
    private static final String ENTRY_DESERIALIZE_TIME_PER_QUERY = "entry-deserialize-time_per_query";

    // time spent waiting for message queue enqueue because message queue is full
    private static final String MESSAGE_QUEUE_ENQUEUE_WAIT_TIME = "message-queue-enqueue-wait-time";

    // time spent waiting for message queue enqueue because message queue is full per query
    private static final String MESSAGE_QUEUE_ENQUEUE_WAIT_TIME_PER_QUERY = "message-queue-enqueue-wait-time-per-query";

    private static final String NUM_MESSAGES_DERSERIALIZED = "num-messages-deserialized";

    // number of messages deserialized
    public static final String NUM_MESSAGES_DERSERIALIZED_PER_ENTRY = "num-messages-deserialized-per-entry";

    // number of messages deserialized per query
    public static final String NUM_MESSAGES_DERSERIALIZED_PER_QUERY = "num-messages-deserialized-per-query";

    // number of read attempts.  Will fail if queues are full
    public static final String READ_ATTEMPTS = "read-attempts";

    // number of read attempts per query
    public static final String READ_ATTEMTPS_PER_QUERY= "read-attempts-per-query";

    // latency of reads per batch
    public static final String READ_LATENCY_PER_BATCH = "read-latency-per-batch";

    // total read latency per query
    public static final String READ_LATENCY_PER_QUERY = "read-latency-per-query";

    // number of entries per batch
    public static final String NUM_ENTRIES_PER_BATCH = "num-entries-per-batch";

    // number of entries per query
    public static final String NUM_ENTRIES_PER_QUERY = "num-entries-per-query";

    // time spent waiting to dequeue from message queue because its empty per query
    public static final String MESSAGE_QUEUE_DEQUEUE_WAIT_TIME_PER_QUERY = "message-queue-dequeue-wait-time-per-query";

    // time spent deserializing message to record e.g. avro, json, etc
    public static final String RECORD_DESERIALIZE_TIME = "record-deserialize-time";

    // time spent deserializing message to record per query
    private static final String RECORD_DESERIALIZE_TIME_PER_QUERY = "record-deserialize-time-per-query";

    private static final String NUM_RECORD_DESERIALIZED = "num-record-deserialized";

    private static final String TOTAL_EXECUTION_TIME = "total-execution-time";

    /** stats loggers **/

    private final OpStatsLogger statsLogger_entryQueueDequeueWaitTime;
    private final Counter statsLogger_bytesRead;
    private final OpStatsLogger statsLogger_entryDeserializetime;
    private final OpStatsLogger statsLogger_messageQueueEnqueueWaitTime;
    private final Counter statsLogger_numMessagesDeserialized;
    private final OpStatsLogger statsLogger_numMessagesDeserializedPerEntry;
    private final OpStatsLogger statsLogger_readAttempts;
    private final OpStatsLogger statsLogger_readLatencyPerBatch;
    private final OpStatsLogger statsLogger_numEntriesPerBatch;
    private final OpStatsLogger statsLogger_recordDeserializeTime;
    private final Counter statsLogger_numRecordDeserialized;
    private final OpStatsLogger statsLogger_totalExecutionTime;

    /** internal tracking variables **/
    private long ENTRY_QUEUE_DEQUEUE_WAIT_TIME_startTime;
    private long ENTRY_QUEUE_DEQUEUE_WAIT_TIME_sum = 0L;
    private long BYTES_READ_sum = 0L;
    private long ENTRY_DESERIALIZE_TIME_startTime;
    private long ENTRY_DESERIALIZE_TIME_sum = 0L;
    private long MESSAGE_QUEUE_ENQUEUE_WAIT_TIME_startTime;
    private long MESSAGE_QUEUE_ENQUEUE_WAIT_TIME_sum = 0L;
    private long NUM_MESSAGES_DERSERIALIZED_sum = 0L;
    private long NUM_MESSAGED_DERSERIALIZED_PER_BATCH = 0L;
    private long READ_ATTEMTPS_SUCCESS_sum = 0L;
    private long READ_ATTEMTPS_FAIL_sum = 0L;
    private long READ_LATENCY_SUCCESS_sum = 0L;
    private long READ_LATENCY_FAIL_sum = 0L;
    private long NUM_ENTRIES_PER_BATCH_sum = 0L;
    private long MESSAGE_QUEUE_DEQUEUE_WAIT_TIME_sum = 0L;
    private long RECORD_DESERIALIZE_TIME_startTime;
    private long RECORD_DESERIALIZE_TIME_sum = 0L;

    public PulsarConnectorMetricsTracker(StatsProvider statsProvider) {
        this.statsLogger = statsProvider instanceof NullStatsProvider
                ? null : statsProvider.getStatsLogger(SCOPE);

        if (this.statsLogger != null) {
            statsLogger_entryQueueDequeueWaitTime = statsLogger.getOpStatsLogger(ENTRY_QUEUE_DEQUEUE_WAIT_TIME);
            statsLogger_bytesRead = statsLogger.getCounter(BYTES_READ);
            statsLogger_entryDeserializetime = statsLogger.getOpStatsLogger(ENTRY_DESERIALIZE_TIME);
            statsLogger_messageQueueEnqueueWaitTime = statsLogger.getOpStatsLogger(MESSAGE_QUEUE_ENQUEUE_WAIT_TIME);
            statsLogger_numMessagesDeserialized = statsLogger.getCounter(NUM_MESSAGES_DERSERIALIZED);
            statsLogger_numMessagesDeserializedPerEntry = statsLogger.getOpStatsLogger(NUM_MESSAGES_DERSERIALIZED_PER_ENTRY);
            statsLogger_readAttempts = statsLogger.getOpStatsLogger(READ_ATTEMPTS);
            statsLogger_readLatencyPerBatch = statsLogger.getOpStatsLogger(READ_LATENCY_PER_BATCH);
            statsLogger_numEntriesPerBatch = statsLogger.getOpStatsLogger(NUM_ENTRIES_PER_BATCH);
            statsLogger_recordDeserializeTime = statsLogger.getOpStatsLogger(RECORD_DESERIALIZE_TIME);
            statsLogger_numRecordDeserialized = statsLogger.getCounter(NUM_RECORD_DESERIALIZED);
            statsLogger_totalExecutionTime = statsLogger.getOpStatsLogger(TOTAL_EXECUTION_TIME);
        } else {
            statsLogger_entryQueueDequeueWaitTime = null;
            statsLogger_bytesRead = null;
            statsLogger_entryDeserializetime = null;
            statsLogger_messageQueueEnqueueWaitTime = null;
            statsLogger_numMessagesDeserialized = null;
            statsLogger_numMessagesDeserializedPerEntry = null;
            statsLogger_readAttempts = null;
            statsLogger_readLatencyPerBatch = null;
            statsLogger_numEntriesPerBatch = null;
            statsLogger_recordDeserializeTime = null;
            statsLogger_numRecordDeserialized = null;
            statsLogger_totalExecutionTime = null;
        }
    }

    public void start_ENTRY_QUEUE_DEQUEUE_WAIT_TIME() {
        if (statsLogger != null) {
            ENTRY_QUEUE_DEQUEUE_WAIT_TIME_startTime = System.nanoTime();
        }
    }

    public void end_ENTRY_QUEUE_DEQUEUE_WAIT_TIME() {
        if (statsLogger != null) {
            long time = System.nanoTime() - ENTRY_QUEUE_DEQUEUE_WAIT_TIME_startTime;
            ENTRY_QUEUE_DEQUEUE_WAIT_TIME_sum += time;
            statsLogger_entryQueueDequeueWaitTime.registerSuccessfulEvent(time, TimeUnit.NANOSECONDS);
        }
    }

    public void register_BYTES_READ(long bytes) {
        if (statsLogger != null) {
            BYTES_READ_sum += bytes;
            statsLogger_bytesRead.add(bytes);
        }
    }

    public void start_ENTRY_DESERIALIZE_TIME() {
        if (statsLogger != null) {
            ENTRY_DESERIALIZE_TIME_startTime = System.nanoTime();
        }
    }

    public void end_ENTRY_DESERIALIZE_TIME() {
        if (statsLogger != null) {
            long time = System.nanoTime() - ENTRY_DESERIALIZE_TIME_startTime;
            ENTRY_DESERIALIZE_TIME_sum += time;
            statsLogger_entryDeserializetime.registerSuccessfulEvent(time, TimeUnit.NANOSECONDS);
        }
    }

    public void start_MESSAGE_QUEUE_ENQUEUE_WAIT_TIME() {
        if (statsLogger != null) {
            MESSAGE_QUEUE_ENQUEUE_WAIT_TIME_startTime = System.nanoTime();
        }
    }

    public void end_MESSAGE_QUEUE_ENQUEUE_WAIT_TIME() {
        if (statsLogger != null) {
            long time = System.nanoTime() - MESSAGE_QUEUE_ENQUEUE_WAIT_TIME_startTime;
            MESSAGE_QUEUE_ENQUEUE_WAIT_TIME_sum += time;
            statsLogger_messageQueueEnqueueWaitTime.registerSuccessfulEvent(time, TimeUnit.NANOSECONDS);
        }
    }

    public void incr_NUM_MESSAGES_DESERIALIZED_PER_ENTRY() {
        if (statsLogger != null) {
            NUM_MESSAGED_DERSERIALIZED_PER_BATCH++;
            statsLogger_numMessagesDeserialized.add(1);
        }
    }

    public void end_NUM_MESSAGES_DESERIALIZED_PER_ENTRY() {
        if (statsLogger != null) {
            NUM_MESSAGES_DERSERIALIZED_sum += NUM_MESSAGED_DERSERIALIZED_PER_BATCH;

            statsLogger_numMessagesDeserializedPerEntry.registerSuccessfulValue(NUM_MESSAGED_DERSERIALIZED_PER_BATCH);

            NUM_MESSAGED_DERSERIALIZED_PER_BATCH = 0L;
        }
    }

    public void incr_READ_ATTEMPTS_SUCCESS() {
        if (statsLogger != null) {
            READ_ATTEMTPS_SUCCESS_sum++;
            statsLogger_readAttempts.registerSuccessfulValue(1L);
        }
    }

    public void incr_READ_ATTEMPTS_FAIL() {
        if (statsLogger != null) {
            READ_ATTEMTPS_FAIL_sum++;
            statsLogger_readAttempts.registerFailedValue(1L);
        }
    }

    public void register_READ_LATENCY_PER_BATCH_SUCCESS(long latency) {
        if (statsLogger != null) {
            READ_LATENCY_SUCCESS_sum += latency;
            statsLogger_readLatencyPerBatch.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
        }
    }

    public void register_READ_LATENCY_PER_BATCH_FAIL(long latency) {
        if (statsLogger != null) {
            READ_LATENCY_FAIL_sum += latency;
            statsLogger_readLatencyPerBatch.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
        }
    }

    public void incr_NUM_ENTRIES_PER_BATCH_SUCCESS(long delta) {
        if (statsLogger != null) {
            NUM_ENTRIES_PER_BATCH_sum += delta;
            statsLogger_numEntriesPerBatch.registerSuccessfulValue(delta);
        }
    }

    public void incr_NUM_ENTRIES_PER_BATCH_FAIL(long delta) {
        if (statsLogger != null) {
            statsLogger_numEntriesPerBatch.registerFailedValue(delta);
        }
    }

    public void register_MESSAGE_QUEUE_DEQUEUE_WAIT_TIME(long latency) {
        if (statsLogger != null) {
            MESSAGE_QUEUE_DEQUEUE_WAIT_TIME_sum += latency;
        }
    }

    public void start_RECORD_DESERIALIZE_TIME() {
        if (statsLogger != null) {
            RECORD_DESERIALIZE_TIME_startTime = System.nanoTime();
        }
    }

    public void end_RECORD_DESERIALIZE_TIME() {
        if (statsLogger != null) {
            long time = System.nanoTime() - RECORD_DESERIALIZE_TIME_startTime;
            RECORD_DESERIALIZE_TIME_sum += time;
            statsLogger_recordDeserializeTime.registerSuccessfulEvent(time, TimeUnit.NANOSECONDS);
        }
    }

    public void incr_NUM_RECORD_DESERIALIZED() {
        if (statsLogger != null) {
            statsLogger_numRecordDeserialized.add(1);
        }
    }

    public void register_TOTAL_EXECUTION_TIME(long latency) {
        if (statsLogger != null) {
            statsLogger_totalExecutionTime.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void close() {
        if (statsLogger != null) {
            // register total entry dequeue wait time for query
            statsLogger.getOpStatsLogger(ENTRY_QUEUE_DEQUEUE_WAIT_TIME_PER_QUERY)
                    .registerSuccessfulEvent(ENTRY_QUEUE_DEQUEUE_WAIT_TIME_sum, TimeUnit.NANOSECONDS);

            //register bytes read per query
            statsLogger.getOpStatsLogger(BYTES_READ_PER_QUERY)
                    .registerSuccessfulValue(BYTES_READ_sum);

            // register total time spent deserializing entries for query
            statsLogger.getOpStatsLogger(ENTRY_DESERIALIZE_TIME_PER_QUERY)
                    .registerSuccessfulEvent(ENTRY_DESERIALIZE_TIME_sum, TimeUnit.NANOSECONDS);

            // register time spent waiting for message queue enqueue because message queue is full per query
            statsLogger.getOpStatsLogger(MESSAGE_QUEUE_ENQUEUE_WAIT_TIME_PER_QUERY)
                    .registerSuccessfulEvent(MESSAGE_QUEUE_ENQUEUE_WAIT_TIME_sum, TimeUnit.NANOSECONDS);

            // register number of messages deserialized per query
            statsLogger.getOpStatsLogger(NUM_MESSAGES_DERSERIALIZED_PER_QUERY)
                    .registerSuccessfulValue(NUM_MESSAGES_DERSERIALIZED_sum);

            // register number of read attempts per query
            statsLogger.getOpStatsLogger(READ_ATTEMTPS_PER_QUERY)
                    .registerSuccessfulValue(READ_ATTEMTPS_SUCCESS_sum);
            statsLogger.getOpStatsLogger(READ_ATTEMTPS_PER_QUERY)
                    .registerFailedValue(READ_ATTEMTPS_FAIL_sum);

            // register total read latency for query
            statsLogger.getOpStatsLogger(READ_LATENCY_PER_QUERY)
                    .registerSuccessfulEvent(READ_LATENCY_SUCCESS_sum, TimeUnit.NANOSECONDS);
            statsLogger.getOpStatsLogger(READ_LATENCY_PER_QUERY)
                    .registerFailedEvent(READ_LATENCY_FAIL_sum, TimeUnit.NANOSECONDS);

            // register number of entries per query
            statsLogger.getOpStatsLogger(NUM_ENTRIES_PER_QUERY)
                    .registerSuccessfulValue(NUM_ENTRIES_PER_BATCH_sum);

            // register time spent waiting to read for message queue per query
            statsLogger.getOpStatsLogger(MESSAGE_QUEUE_DEQUEUE_WAIT_TIME_PER_QUERY)
                    .registerSuccessfulEvent(MESSAGE_QUEUE_DEQUEUE_WAIT_TIME_sum, TimeUnit.MILLISECONDS);

            // register time spent deserializing records per query
            statsLogger.getOpStatsLogger(RECORD_DESERIALIZE_TIME_PER_QUERY)
                    .registerSuccessfulEvent(RECORD_DESERIALIZE_TIME_sum, TimeUnit.NANOSECONDS);
        }
    }
}
