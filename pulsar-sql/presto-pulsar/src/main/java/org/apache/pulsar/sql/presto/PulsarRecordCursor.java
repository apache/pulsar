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

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.google.common.base.Preconditions.checkArgument;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.io.netty.util.ReferenceCountUtil;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;


public class PulsarRecordCursor implements RecordCursor {

    private List<PulsarColumnHandle> columnHandles;
    private PulsarSplit pulsarSplit;
    private PulsarConnectorConfig pulsarConnectorConfig;
    private ReadOnlyCursor cursor;

    private ArrayDeque<RawMessage> messageQueue;
    private SpscArrayQueue<Entry> entryQueue;
    private Object currentRecord;
    private RawMessage currentMessage;
    private Map<String, PulsarInternalColumn> internalColumnMap = PulsarInternalColumn.getInternalFieldsMap();
    private SchemaHandler schemaHandler;
    private int maxBatchSize;
    private long completedBytes = 0;
    private ReadEntries readEntries;
    private TopicName topicName;
    private PulsarConnectorMetricsTracker metricsTracker;

    // Stats total execution time of split
    private long startTime;

    // Used to make sure we don't finish before all entries are processed since entries that have been dequeued
    // but not been deserialized and added messages to the message queue can be missed if we just check if the queues
    // are empty or not
    private final long splitSize;
    private long entriesProcessed = 0;

    private final AtomicBoolean readOperationPending = new AtomicBoolean();


    private static final Logger log = Logger.get(PulsarRecordCursor.class);

    public PulsarRecordCursor(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit,
                              PulsarConnectorConfig pulsarConnectorConfig) {
        this.splitSize = pulsarSplit.getSplitSize();
        // Set start time for split
        this.startTime = System.nanoTime();
        PulsarConnectorCache pulsarConnectorCache;
        try {
            pulsarConnectorCache = PulsarConnectorCache.getConnectorCache(pulsarConnectorConfig);
        } catch (Exception e) {
            log.error(e, "Failed to initialize Pulsar connector cache");
            close();
            throw new RuntimeException(e);
        }
        initialize(columnHandles, pulsarSplit, pulsarConnectorConfig,
                pulsarConnectorCache.getManagedLedgerFactory(),
                new PulsarConnectorMetricsTracker(pulsarConnectorCache.getStatsProvider()));
    }

    // Exposed for testing purposes
    PulsarRecordCursor(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
            pulsarConnectorConfig, ManagedLedgerFactory managedLedgerFactory, PulsarConnectorMetricsTracker pulsarConnectorMetricsTracker) {
        this.splitSize = pulsarSplit.getSplitSize();
        initialize(columnHandles, pulsarSplit, pulsarConnectorConfig, managedLedgerFactory, pulsarConnectorMetricsTracker);
    }

    private void initialize(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
            pulsarConnectorConfig, ManagedLedgerFactory managedLedgerFactory,
                            PulsarConnectorMetricsTracker pulsarConnectorMetricsTracker) {
        log.info("Initializing PulsarRecordCursor");
        this.columnHandles = columnHandles;
        this.pulsarSplit = pulsarSplit;
        this.pulsarConnectorConfig = pulsarConnectorConfig;
        this.maxBatchSize = pulsarConnectorConfig.getMaxEntryReadBatchSize();
        this.messageQueue = new ArrayDeque<>();
        this.entryQueue = new SpscArrayQueue<>(pulsarConnectorConfig.getMaxSplitEntryQueueSize());
        this.topicName = TopicName.get("persistent",
                NamespaceName.get(pulsarSplit.getSchemaName()),
                pulsarSplit.getTableName());
        this.metricsTracker = pulsarConnectorMetricsTracker;
        this.readEntries = new ReadEntries();

        Schema schema = PulsarConnectorUtils.parseSchema(pulsarSplit.getSchema());

        this.schemaHandler = getSchemaHandler(schema, pulsarSplit.getSchemaType(), columnHandles);

        log.info("Initializing split with parameters: %s", pulsarSplit);

        try {
            this.cursor = getCursor(TopicName.get("persistent", NamespaceName.get(pulsarSplit.getSchemaName()),
                    pulsarSplit.getTableName()), pulsarSplit.getStartPosition(), managedLedgerFactory);
        } catch (ManagedLedgerException | InterruptedException e) {
            log.error(e, "Failed to get read only cursor");
            close();
            throw new RuntimeException(e);
        }
    }

    private SchemaHandler getSchemaHandler(Schema schema, SchemaType schemaType,
                                           List<PulsarColumnHandle> columnHandles) {
        SchemaHandler schemaHandler;
        switch (schemaType) {
            case JSON:
                schemaHandler = new JSONSchemaHandler(columnHandles);
                break;
            case AVRO:
                schemaHandler = new AvroSchemaHandler(schema, columnHandles);
                break;
            default:
                throw new PrestoException(NOT_SUPPORTED, "Not supported schema type: " + schemaType);
        }
        return schemaHandler;
    }

    private ReadOnlyCursor getCursor(TopicName topicName, Position startPosition, ManagedLedgerFactory
            managedLedgerFactory)
            throws ManagedLedgerException, InterruptedException {

        ReadOnlyCursor cursor = managedLedgerFactory.openReadOnlyCursor(topicName.getPersistenceNamingEncoding(),
                startPosition, new ManagedLedgerConfig());

        return cursor;
    }

    @Override
    public long getCompletedBytes() {
        return this.completedBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @VisibleForTesting
    class ReadEntries implements AsyncCallbacks.ReadEntriesCallback {

        // indicate whether there are any additional entries left to read
        private boolean isDone = false;

        public void readNextBatch() {
            if (!cursor.hasMoreEntries() || ((PositionImpl) cursor.getReadPosition())
                    .compareTo(pulsarSplit.getEndPosition()) >= 0) {
                isDone = true;
                return;
            }

            int batchSize = Math.min(maxBatchSize, entryQueue.capacity() - entryQueue.size());

            if (batchSize > 0) {
                // If there's already a pending read, wait until that is completed
                if (readOperationPending.compareAndSet(false, true)) {
                    cursor.asyncReadEntries(batchSize, this, System.nanoTime());

                    // stats for successful read request
                    metricsTracker.incr_READ_ATTEMPTS_SUCCESS();
                }
            } else {
                // stats for failed read request because entry queue is full
                metricsTracker.incr_READ_ATTEMPTS_FAIL();
            }
        }

        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
            entryQueue.fill(new MessagePassingQueue.Supplier<Entry>() {
                private int i = 0;
                @Override
                public Entry get() {
                    Entry entry = entries.get(i);
                    i++;
                    return entry;
                }
            }, entries.size());

            readOperationPending.set(false);


            //set read latency stats for success
            metricsTracker.register_READ_LATENCY_PER_BATCH_SUCCESS(System.nanoTime() - (long)ctx);
            //stats for number of entries read
            metricsTracker.incr_NUM_ENTRIES_PER_BATCH_SUCCESS(entries.size());

            // Trigger the read of next batch, so that we can make sure to always
            // keep the entries queue with entries avaialble to be processed.
            readNextBatch();
        }

        public boolean hasFinished() {
            return isDone && messageQueue.isEmpty() && !readOperationPending.get() && splitSize <= entriesProcessed;
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            log.debug(exception, "Failed to read entries from topic %s", topicName.toString());
            readOperationPending.set(false);

            //set read latency stats for failed
            metricsTracker.register_READ_LATENCY_PER_BATCH_FAIL(System.nanoTime() - (long)ctx);
            //stats for number of entries read failed
            metricsTracker.incr_NUM_ENTRIES_PER_BATCH_FAIL((long) maxBatchSize);
        }
    }

    private boolean parseNextEntry() {
        Entry entry = entryQueue.poll();

        if (entry != null) {
            try {
                long bytes = entry.getDataBuffer().readableBytes();
                completedBytes += bytes;
                // register stats for bytes read
                metricsTracker.register_BYTES_READ(bytes);

                // check if we have processed all entries in this split
                if (((PositionImpl) entry.getPosition()).compareTo(pulsarSplit.getEndPosition()) >= 0) {
                    return false;
                }

                // set start time for time deserializing entries for stats
                metricsTracker.start_ENTRY_DESERIALIZE_TIME();

                try {
                    MessageParser.parseMessage(topicName, entry.getLedgerId(), entry.getEntryId(),
                            entry.getDataBuffer(), (message) -> {

                                // enqueue deserialize message from this entry
                                messageQueue.add(message);

                                // stats for number of messages read
                                metricsTracker.incr_NUM_MESSAGES_DESERIALIZED_PER_ENTRY();
                            });
                } catch (IOException e) {
                    log.error(e, "Failed to parse message from pulsar topic %s", topicName.toString());
                    throw new RuntimeException(e);
                }
                // stats for time spend deserializing entries
                metricsTracker.end_ENTRY_DESERIALIZE_TIME();

                // stats for num messages per entry
                metricsTracker.end_NUM_MESSAGES_DESERIALIZED_PER_ENTRY();

            } finally {
                entriesProcessed++;
                entry.release();
            }
        }

        if (entryQueue.size() < (entryQueue.capacity() / 2)) {
            // Refill the entries queue once we have dequeued half of it
            readEntries.readNextBatch();
        }

        return entry != null;
    }

    /**
     * Get next message or wait until it's available
     */
    private RawMessage getNextMessage() {
        do {
            RawMessage msg = messageQueue.poll();
            if (msg != null) {
                // There was already a message parsed and available
                return msg;
            }

            // Message queue is empty, try to get items from entry queue
            if (parseNextEntry()) {
                // We have broken down at least one batch, `messageQueue`
                // will now have some messages
                continue;
            }

            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                return null;
            }

        } while (!readEntries.hasFinished());

        // No more available messages
        return null;
    }

    @Override
    public boolean advanceNextPosition() {
        if (currentMessage != null) {
            currentMessage.release();
        }

        currentMessage = getNextMessage();

        if (currentMessage == null) {
            // No more messages to process
            return false;
        }

        //start time for deseralizing record
        metricsTracker.start_RECORD_DESERIALIZE_TIME();

        currentRecord = this.schemaHandler.deserialize(this.currentMessage.getData());
        metricsTracker.incr_NUM_RECORD_DESERIALIZED();

        // stats for time spend deserializing
        metricsTracker.end_RECORD_DESERIALIZE_TIME();

        return true;
    }


    @VisibleForTesting
    Object getRecord(int fieldIndex) {
        if (this.currentRecord == null) {
            return null;
        }

        Object data;
        PulsarColumnHandle pulsarColumnHandle = this.columnHandles.get(fieldIndex);

        if (pulsarColumnHandle.isInternal()) {
            String fieldName = this.columnHandles.get(fieldIndex).getName();
            PulsarInternalColumn pulsarInternalColumn = this.internalColumnMap.get(fieldName);
            data = pulsarInternalColumn.getData(this.currentMessage);
        } else {
            data = this.schemaHandler.extractField(fieldIndex, this.currentRecord);
        }

        return data;
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, boolean.class);
        return (boolean) getRecord(field);
    }

    @Override
    public long getLong(int field) {
        checkFieldType(field, long.class);

        Object record = getRecord(field);
        Type type = getType(field);

        if (type.equals(BIGINT)) {
            return ((Number) record).longValue();
        } else if (type.equals(DATE)) {
            return ((Number) record).longValue();
        } else if (type.equals(INTEGER)) {
            return ((Number) record).intValue();
        } else if (type.equals(REAL)) {
            return Float.floatToIntBits(((Number) record).floatValue());
        } else if (type.equals(SMALLINT)) {
            return ((Number) record).shortValue();
        } else if (type.equals(TIME)) {
            return ((Number) record).longValue();
        } else if (type.equals(TIMESTAMP)) {
            return ((Number) record).longValue();
        } else if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return packDateTimeWithZone(((Number) record).longValue(), 0);
        } else if (type.equals(TINYINT)) {
            return Byte.parseByte(record.toString());
        } else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + getType(field));
        }
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, double.class);
        Object record = getRecord(field);
        return (double) record;
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, Slice.class);

        Object record = getRecord(field);
        Type type = getType(field);
        if (type == VarcharType.VARCHAR) {
            return Slices.utf8Slice(record.toString());
        } else if (type == VarbinaryType.VARBINARY) {
            return Slices.wrappedBuffer((byte[]) record);
        } else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + type);
        }
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        Object record = getRecord(field);
        return record == null;
    }

    @Override
    public void close() {
        log.info("Closing cursor record");

        while (readOperationPending.get() == true) {
            // Wait for last pending read operation to complete so that we can
            // properly release all the entries
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                break;
            }
        }

        if (currentMessage != null) {
            currentMessage.release();
            currentMessage = null;
        }

        messageQueue.forEach(RawMessage::release);
        entryQueue.drain(Entry::release);

        if (this.cursor != null) {
            try {
                this.cursor.close();
            } catch (Exception e) {
                log.error(e);
            }
        }

        // set stat for total execution time of split
        if (this.metricsTracker != null) {
            this.metricsTracker.register_TOTAL_EXECUTION_TIME(System.nanoTime() - startTime);
            this.metricsTracker.close();
        }
    }

    private void checkFieldType(int field, Class<?> expected) {
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }
}
