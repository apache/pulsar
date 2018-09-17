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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.avro.Schema;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageParser;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.org.apache.bookkeeper.conf.ClientConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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


public class PulsarRecordCursor implements RecordCursor {

    private List<PulsarColumnHandle> columnHandles;
    private PulsarSplit pulsarSplit;
    private PulsarConnectorConfig pulsarConnectorConfig;
    private ReadOnlyCursor cursor;
    private ArrayBlockingQueue<Message> messageQueue;
    private ArrayBlockingQueue<Entry> entryQueue;
    private Object currentRecord;
    private Message currentMessage;
    private Map<String, PulsarInternalColumn> internalColumnMap = PulsarInternalColumn.getInternalFieldsMap();
    private SchemaHandler schemaHandler;
    private int batchSize;
    private AtomicLong completedBytes = new AtomicLong(0L);
    private ReadEntries readEntries;
    private DeserializeEntries deserializeEntries;
    private TopicName topicName;

    private static final Logger log = Logger.get(PulsarRecordCursor.class);

    private static ManagedLedgerFactory initManagedLedgerFactory(PulsarConnectorConfig pulsarConnectorConfig) throws Exception {
        ClientConfiguration bkClientConfiguration = new ClientConfiguration()
                .setZkServers(pulsarConnectorConfig.getZookeeperUri())
                .setAllowShadedLedgerManagerFactoryClass(true)
                .setShadedLedgerManagerFactoryClassPrefix("org.apache.pulsar.shade.");
        return new ManagedLedgerFactoryImpl(bkClientConfiguration);
    }

    public PulsarRecordCursor(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit,
                              PulsarConnectorConfig pulsarConnectorConfig) {
        PulsarConnectorCache pulsarConnectorCache;
        try {
            pulsarConnectorCache = PulsarConnectorCache.getConnectorCache(pulsarConnectorConfig);
        } catch (Exception e) {
            log.error(e, "Failed to initialize Pulsar connector cache");
            close();
            throw new RuntimeException(e);
        }
        initialize(columnHandles, pulsarSplit, pulsarConnectorConfig,
                pulsarConnectorCache.getManagedLedgerFactory());
    }

    // Exposed for testing purposes
    PulsarRecordCursor(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
            pulsarConnectorConfig, ManagedLedgerFactory managedLedgerFactory) {
        initialize(columnHandles, pulsarSplit, pulsarConnectorConfig, managedLedgerFactory);
    }

    private void initialize(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
            pulsarConnectorConfig, ManagedLedgerFactory managedLedgerFactory) {
        this.columnHandles = columnHandles;
        this.pulsarSplit = pulsarSplit;
        this.pulsarConnectorConfig = pulsarConnectorConfig;
        this.batchSize = pulsarConnectorConfig.getEntryReadBatchSize();
        this.messageQueue = new ArrayBlockingQueue<>(pulsarConnectorConfig.getMaxSplitMessageQueueSize());
        this.entryQueue = new ArrayBlockingQueue<>(pulsarConnectorConfig.getMaxSplitEntryQueueSize());
        this.topicName = TopicName.get("persistent",
                NamespaceName.get(pulsarSplit.getSchemaName()),
                pulsarSplit.getTableName());

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
        return this.completedBytes.get();
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

    protected AtomicBoolean isRunning = new AtomicBoolean(false);

        private final Thread thread;

        public DeserializeEntries() {
            this.thread = new Thread(this);
        }

        public void interrupt() {
            isRunning.set(false);
            thread.interrupt();
        }

        public void start() {
            this.thread.start();
        }

        @Override
        public void run() {
            isRunning.set(true);
            while (isRunning.get()) {
                Entry entry;
                try {
                    entry = entryQueue.take();
                } catch (InterruptedException e) {
                    break;
                }
                try {
                    completedBytes.addAndGet(entry.getDataBuffer().readableBytes());
                    // filter entries that is not part of my split
                    if (((PositionImpl) entry.getPosition()).compareTo(pulsarSplit.getEndPosition()) < 0) {
                        try {
                            MessageParser.parseMessage(topicName, entry.getLedgerId(), entry.getEntryId(),
                                    entry.getDataBuffer(), (messageId, message, byteBuf) -> {
                                        try {
                                            messageQueue.put(message);
                                        } catch (InterruptedException e) {
                                            //no-op
                                        }
                                    });
                        } catch (IOException e) {
                            log.error(e, "Failed to parse message from pulsar topic %s", topicName.toString());
                            throw new RuntimeException(e);
                        }
                    }
                } finally {
                    entry.release();
                }
            }
        }
    }

    @VisibleForTesting
    class ReadEntries implements AsyncCallbacks.ReadEntriesCallback {

        // indicate whether there are any additional entries left to read
        private final AtomicBoolean isDone = new AtomicBoolean(false);

        //num of outstanding read requests
        // set to 1 because we can only read one batch a time
        private final AtomicLong outstandingReadsRequests = new AtomicLong(1);

        public void run() {

            if (outstandingReadsRequests.get() > 0) {

                if (!cursor.hasMoreEntries() || ((PositionImpl) cursor.getReadPosition())
                        .compareTo(pulsarSplit.getEndPosition()) >= 0) {
                    isDone.set(true);

                } else if (entryQueue.remainingCapacity() > batchSize) {
                    outstandingReadsRequests.decrementAndGet();
                    cursor.asyncReadEntries(batchSize, this, System.currentTimeMillis());
                }
            }
        }

        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
            entryQueue.addAll(entries);
            outstandingReadsRequests.incrementAndGet();
        }

        public boolean hashFinished() {
            return messageQueue.isEmpty() && entryQueue.isEmpty() && isDone.get() && outstandingReadsRequests.get() >=1;
        }


        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            log.debug(exception, "Failed to read entries from topic %s", topicName.toString());
            outstandingReadsRequests.incrementAndGet();
        }
    }


    @Override
    public boolean advanceNextPosition() {

        if (readEntries == null) {
            readEntries = new ReadEntries();
            readEntries.run();

            // start deserialize thread
            deserializeEntries = new DeserializeEntries();
            deserializeEntries.start();
        }

        while(true) {
            if (readEntries.hashFinished()) {
                return false;
            }

            if (messageQueue.remainingCapacity() > 0) {
                readEntries.run();
            }

            currentMessage = messageQueue.poll();
            if (currentMessage != null) {
                break;
            } else {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        currentRecord = this.schemaHandler.deserialize(this.currentMessage.getData());
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
            return (int) record;
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
    }

    private void checkFieldType(int field, Class<?> expected) {
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }
}
