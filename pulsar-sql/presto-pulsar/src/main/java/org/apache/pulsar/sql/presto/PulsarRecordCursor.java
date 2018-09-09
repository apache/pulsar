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
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageParser;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    private ScheduledExecutorService scheduledExecutorService;
    private ReadOnlyCursor cursor;
    private Queue<Message> messageQueue = new ConcurrentLinkedQueue<>();
    private Object currentRecord;
    private Message currentMessage;
    private Map<String, PulsarInternalColumn> internalColumnMap = PulsarInternalColumn.getInternalFieldsMap();
    private SchemaHandler schemaHandler;
    private int batchSize;
    private AtomicLong completedBytes = new AtomicLong(0L);
    private ReadEntries readEntries;

    private static final Logger log = Logger.get(PulsarRecordCursor.class);

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
                pulsarConnectorCache.getManagedLedgerFactory(),
                pulsarConnectorCache.getScheduledExecutorService());
    }

    // Exposed for testing purposes
    PulsarRecordCursor(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
            pulsarConnectorConfig, ManagedLedgerFactory managedLedgerFactory, ScheduledExecutorService scheduledExecutorService) {
        initialize(columnHandles, pulsarSplit, pulsarConnectorConfig, managedLedgerFactory, scheduledExecutorService);
    }

    private void initialize(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
            pulsarConnectorConfig, ManagedLedgerFactory managedLedgerFactory, ScheduledExecutorService scheduledExecutorService) {
        this.columnHandles = columnHandles;
        this.pulsarSplit = pulsarSplit;
        this.pulsarConnectorConfig = pulsarConnectorConfig;
        this.batchSize = pulsarConnectorConfig.getEntryReadBatchSize();
        this.scheduledExecutorService = scheduledExecutorService;

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

    public class ReadEntries extends TimerTask implements AsyncCallbacks.ReadEntriesCallback {

        private final TopicName topicName = TopicName.get("persistent",
                NamespaceName.get(pulsarSplit.getSchemaName()),
                pulsarSplit.getTableName());

        private AtomicBoolean isDone = new AtomicBoolean(false);

        @Override
        public void run() {
            if (!cursor.hasMoreEntries() && ((PositionImpl) cursor.getReadPosition())
                    .compareTo(pulsarSplit.getEndPosition()) >= 0) {
                isDone.set(true);
            } else if (messageQueue.size() < pulsarConnectorConfig.getTargetSplitMessageQueueSize()){
                cursor.asyncReadEntries(batchSize, this, null);
            } else {
                scheduledExecutorService.schedule(this, 50, TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
            entries.forEach(entry -> {
                try {
                    completedBytes.addAndGet(entry.getDataBuffer().readableBytes());
                    // filter entries that is not part of my split
                    if (((PositionImpl) entry.getPosition()).compareTo(pulsarSplit.getEndPosition()) < 0) {
                        try {
                            MessageParser.parseMessage(topicName, entry.getLedgerId(), entry.getEntryId(),
                                    entry.getDataBuffer(), (messageId, message, byteBuf) -> {
                                        messageQueue.add(message);
                                    });
                        } catch (IOException e) {
                            log.error(e, "Failed to parse message from pulsar topic %s", topicName.toString());
                            throw new RuntimeException(e);
                        }
                    }
                } finally {
                    entry.release();
                }
            });

            // loop back
            run();
        }

        public boolean isDone() {
            return isDone.get();
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            log.error(exception, "Failed to read entries from topic %s", topicName.toString());
        }

    }
    @Override
    public boolean advanceNextPosition() {

        if (readEntries == null) {
            readEntries = new ReadEntries();
            readEntries.run();
        }

        while(true) {
            if (messageQueue.isEmpty() && readEntries.isDone()) {
                return false;
            }
            this.currentMessage = this.messageQueue.poll();
            if (this.currentMessage == null) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                break;
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
