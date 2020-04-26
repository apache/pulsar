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
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.impl.ReadOnlyCursorImpl;
import org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.sql.presto.PulsarInternalColumn.PartitionColumn;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;

/**
 * Implementation of a cursor to read records.
 */
public class PulsarRecordCursor implements RecordCursor {

    private List<PulsarColumnHandle> columnHandles;
    private PulsarSplit pulsarSplit;
    private PulsarConnectorConfig pulsarConnectorConfig;
    private ReadOnlyCursor cursor;
    private SpscArrayQueue<RawMessage> messageQueue;
    private SpscArrayQueue<Entry> entryQueue;
    private Object currentRecord;
    private RawMessage currentMessage;
    private Map<String, PulsarInternalColumn> internalColumnMap = PulsarInternalColumn.getInternalFieldsMap();
    private SchemaHandler schemaHandler;
    private int maxBatchSize;
    private long completedBytes = 0;
    private ReadEntries readEntries;
    private DeserializeEntries deserializeEntries;
    private TopicName topicName;
    private PulsarConnectorMetricsTracker metricsTracker;
    private boolean readOffloaded;

    // Stats total execution time of split
    private long startTime;

    // Used to make sure we don't finish before all entries are processed since entries that have been dequeued
    // but not been deserialized and added messages to the message queue can be missed if we just check if the queues
    // are empty or not
    private final long splitSize;
    private long entriesProcessed = 0;
    private int partition = -1;


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

        OffloadPolicies offloadPolicies = pulsarSplit.getOffloadPolicies();
        if (offloadPolicies != null) {
            offloadPolicies.setOffloadersDirectory(pulsarConnectorConfig.getOffloadersDirectory());
            offloadPolicies.setManagedLedgerOffloadMaxThreads(
                    pulsarConnectorConfig.getManagedLedgerOffloadMaxThreads());
        }
        initialize(columnHandles, pulsarSplit, pulsarConnectorConfig,
                pulsarConnectorCache.getManagedLedgerFactory(),
                pulsarConnectorCache.getManagedLedgerConfig(
                        TopicName.get("persistent", NamespaceName.get(pulsarSplit.getSchemaName()),
                                pulsarSplit.getTableName()).getNamespaceObject(), offloadPolicies),
                new PulsarConnectorMetricsTracker(pulsarConnectorCache.getStatsProvider()));
    }

    // Exposed for testing purposes
    PulsarRecordCursor(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
        pulsarConnectorConfig, ManagedLedgerFactory managedLedgerFactory, ManagedLedgerConfig managedLedgerConfig,
        PulsarConnectorMetricsTracker pulsarConnectorMetricsTracker) {
        this.splitSize = pulsarSplit.getSplitSize();
        initialize(columnHandles, pulsarSplit, pulsarConnectorConfig, managedLedgerFactory, managedLedgerConfig,
            pulsarConnectorMetricsTracker);
    }

    private void initialize(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
        pulsarConnectorConfig, ManagedLedgerFactory managedLedgerFactory, ManagedLedgerConfig managedLedgerConfig,
        PulsarConnectorMetricsTracker pulsarConnectorMetricsTracker) {
        this.columnHandles = columnHandles;
        this.pulsarSplit = pulsarSplit;
        this.partition = TopicName.getPartitionIndex(pulsarSplit.getTableName());
        this.pulsarConnectorConfig = pulsarConnectorConfig;
        this.maxBatchSize = pulsarConnectorConfig.getMaxEntryReadBatchSize();
        this.messageQueue = new SpscArrayQueue<>(pulsarConnectorConfig.getMaxSplitMessageQueueSize());
        this.entryQueue = new SpscArrayQueue<>(pulsarConnectorConfig.getMaxSplitEntryQueueSize());
        this.topicName = TopicName.get("persistent",
                NamespaceName.get(pulsarSplit.getSchemaName()),
                pulsarSplit.getTableName());
        this.metricsTracker = pulsarConnectorMetricsTracker;
        this.readOffloaded = pulsarConnectorConfig.getManagedLedgerOffloadDriver() != null;
        this.pulsarConnectorConfig = pulsarConnectorConfig;

        this.schemaHandler = PulsarSchemaHandlers.newPulsarSchemaHandler(
                pulsarSplit.getSchemaInfo(),
                columnHandles
        );

        log.info("Initializing split with parameters: %s", pulsarSplit);

        try {
            this.cursor = getCursor(TopicName.get("persistent", NamespaceName.get(pulsarSplit.getSchemaName()),
                pulsarSplit.getTableName()), pulsarSplit.getStartPosition(), managedLedgerFactory, managedLedgerConfig);
        } catch (ManagedLedgerException | InterruptedException e) {
            log.error(e, "Failed to get read only cursor");
            close();
            throw new RuntimeException(e);
        }
    }

    private ReadOnlyCursor getCursor(TopicName topicName, Position startPosition, ManagedLedgerFactory
            managedLedgerFactory, ManagedLedgerConfig managedLedgerConfig)
            throws ManagedLedgerException, InterruptedException {

        ReadOnlyCursor cursor = managedLedgerFactory.openReadOnlyCursor(topicName.getPersistenceNamingEncoding(),
                startPosition, managedLedgerConfig);

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
    class DeserializeEntries implements Runnable {

    protected boolean isRunning = false;

        private final Thread thread;

        public DeserializeEntries() {
            this.thread = new Thread(this, "derserialize-thread-split-" + pulsarSplit.getSplitId());
        }

        public void interrupt() {
            isRunning = false;
            thread.interrupt();
        }

        public void start() {
            this.thread.start();
        }

        @Override
        public void run() {
            isRunning = true;
            while (isRunning) {

                 int read = entryQueue.drain(new MessagePassingQueue.Consumer<Entry>() {
                    @Override
                    public void accept(Entry entry) {

                        try {
                            long bytes = entry.getDataBuffer().readableBytes();
                            completedBytes += bytes;
                            // register stats for bytes read
                            metricsTracker.register_BYTES_READ(bytes);

                            // check if we have processed all entries in this split
                            if (((PositionImpl) entry.getPosition()).compareTo(pulsarSplit.getEndPosition()) >= 0) {
                                return;
                            }

                            // set start time for time deserializing entries for stats
                            metricsTracker.start_ENTRY_DESERIALIZE_TIME();

                            try {
                                MessageParser.parseMessage(topicName, entry.getLedgerId(), entry.getEntryId(),
                                        entry.getDataBuffer(), (message) -> {
                                            try {
                                                // start time for message queue read
                                                metricsTracker.start_MESSAGE_QUEUE_ENQUEUE_WAIT_TIME();

                                                // enqueue deserialize message from this entry
                                                while (!messageQueue.offer(message)) {
                                                    Thread.sleep(1);
                                                }

                                                // stats for how long a read from message queue took
                                                metricsTracker.end_MESSAGE_QUEUE_ENQUEUE_WAIT_TIME();
                                                // stats for number of messages read
                                                metricsTracker.incr_NUM_MESSAGES_DESERIALIZED_PER_ENTRY();

                                            } catch (InterruptedException e) {
                                                //no-op
                                            }
                                        }, pulsarConnectorConfig.getMaxMessageSize());
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
                });

                if (read <= 0) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
    }

    @VisibleForTesting
    class ReadEntries implements AsyncCallbacks.ReadEntriesCallback {

        // indicate whether there are any additional entries left to read
        private boolean isDone = false;

        //num of outstanding read requests
        // set to 1 because we can only read one batch a time
        private final AtomicLong outstandingReadsRequests = new AtomicLong(1);

        public void run() {

            if (outstandingReadsRequests.get() > 0) {
                if (!cursor.hasMoreEntries() || ((PositionImpl) cursor.getReadPosition())
                        .compareTo(pulsarSplit.getEndPosition()) >= 0) {
                    isDone = true;

                } else {
                    int batchSize = Math.min(maxBatchSize, entryQueue.capacity() - entryQueue.size());

                    if (batchSize > 0) {

                        ReadOnlyCursorImpl readOnlyCursorImpl = ((ReadOnlyCursorImpl) cursor);
                        // check if ledger is offloaded
                        if (!readOffloaded  && readOnlyCursorImpl.getCurrentLedgerInfo().hasOffloadContext()) {
                            log.warn(
                                "Ledger %s is offloaded for topic %s. Ignoring it because offloader is not configured",
                                readOnlyCursorImpl.getCurrentLedgerInfo().getLedgerId(), pulsarSplit.getTableName());

                            long numEntries = readOnlyCursorImpl.getCurrentLedgerInfo().getEntries();
                            long entriesToSkip =
                                (numEntries - ((PositionImpl) cursor.getReadPosition()).getEntryId()) + 1;
                            cursor.skipEntries(Math.toIntExact((entriesToSkip)));

                            entriesProcessed += entriesToSkip;
                        } else {
                            outstandingReadsRequests.decrementAndGet();
                            cursor.asyncReadEntries(batchSize, this, System.nanoTime());
                        }

                        // stats for successful read request
                        metricsTracker.incr_READ_ATTEMPTS_SUCCESS();
                    } else {
                        // stats for failed read request because entry queue is full
                        metricsTracker.incr_READ_ATTEMPTS_FAIL();
                    }
                }
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

            outstandingReadsRequests.incrementAndGet();

            //set read latency stats for success
            metricsTracker.register_READ_LATENCY_PER_BATCH_SUCCESS(System.nanoTime() - (long) ctx);
            //stats for number of entries read
            metricsTracker.incr_NUM_ENTRIES_PER_BATCH_SUCCESS(entries.size());
        }

        public boolean hasFinished() {
            return messageQueue.isEmpty() && isDone && outstandingReadsRequests.get() >= 1
                && splitSize <= entriesProcessed;
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            log.debug(exception, "Failed to read entries from topic %s", topicName.toString());
            outstandingReadsRequests.incrementAndGet();

            //set read latency stats for failed
            metricsTracker.register_READ_LATENCY_PER_BATCH_FAIL(System.nanoTime() - (long) ctx);
            //stats for number of entries read failed
            metricsTracker.incr_NUM_ENTRIES_PER_BATCH_FAIL(maxBatchSize);
        }
    }

    @Override
    public boolean advanceNextPosition() {

        if (readEntries == null) {
            // start deserialize thread
            deserializeEntries = new DeserializeEntries();
            deserializeEntries.start();

            readEntries = new ReadEntries();
            readEntries.run();
        }

        if (currentMessage != null) {
            currentMessage.release();
            currentMessage = null;
        }

        while (true) {
            if (readEntries.hasFinished()) {
                return false;
            }

            if ((messageQueue.capacity() - messageQueue.size()) > 0) {
                readEntries.run();
            }

            currentMessage = messageQueue.poll();
            if (currentMessage != null) {
                break;
            } else {
                try {
                    Thread.sleep(1);
                    // stats for time spent wait to read from message queue because its empty
                    metricsTracker.register_MESSAGE_QUEUE_DEQUEUE_WAIT_TIME(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        //start time for deseralizing record
        metricsTracker.start_RECORD_DESERIALIZE_TIME();

        if (this.schemaHandler instanceof KeyValueSchemaHandler) {
            ByteBuf keyByteBuf = null;
            if (this.currentMessage.getKeyBytes().isPresent()) {
                keyByteBuf = this.currentMessage.getKeyBytes().get();
            }
            currentRecord = this.schemaHandler.deserialize(keyByteBuf, this.currentMessage.getData());
        } else {
            currentRecord = this.schemaHandler.deserialize(this.currentMessage.getData());
        }
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
            if (pulsarInternalColumn instanceof PartitionColumn) {
                data = this.partition;
            } else {
                data = pulsarInternalColumn.getData(this.currentMessage);
            }
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

        if (currentMessage != null) {
            currentMessage.release();
        }

        if (messageQueue != null) {
            messageQueue.drain(RawMessage::release);
        }

        if (entryQueue != null) {
            entryQueue.drain(Entry::release);
        }

        if (deserializeEntries != null) {
            deserializeEntries.interrupt();
        }

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
