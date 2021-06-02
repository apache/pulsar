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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.decoder.FieldValueProviders.bytesValueProvider;
import static io.prestosql.decoder.FieldValueProviders.longValueProvider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.netty.buffer.ByteBuf;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.Type;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.sql.presto.util.CacheSizeAllocator;
import org.apache.pulsar.sql.presto.util.NoStrictCacheSizeAllocator;
import org.apache.pulsar.sql.presto.util.NullCacheSizeAllocator;
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
    private CacheSizeAllocator messageQueueCacheSizeAllocator;
    private SpscArrayQueue<Entry> entryQueue;
    private CacheSizeAllocator entryQueueCacheSizeAllocator;
    private RawMessage currentMessage;
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


    private PulsarSqlSchemaInfoProvider schemaInfoProvider;

    private FieldValueProvider[] currentRowValues = null;

    PulsarDispatchingRowDecoderFactory decoderFactory;

    private static final Logger log = Logger.get(PulsarRecordCursor.class);

    public PulsarRecordCursor(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit,
                              PulsarConnectorConfig pulsarConnectorConfig,
                              PulsarDispatchingRowDecoderFactory decoderFactory) {
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

        OffloadPoliciesImpl offloadPolicies = pulsarSplit.getOffloadPolicies();
        if (offloadPolicies != null) {
            offloadPolicies.setOffloadersDirectory(pulsarConnectorConfig.getOffloadersDirectory());
            offloadPolicies.setManagedLedgerOffloadMaxThreads(
                    pulsarConnectorConfig.getManagedLedgerOffloadMaxThreads());
        }
        initialize(columnHandles, pulsarSplit, pulsarConnectorConfig,
                pulsarConnectorCache.getManagedLedgerFactory(),
                pulsarConnectorCache.getManagedLedgerConfig(
                        TopicName.get("persistent", NamespaceName.get(pulsarSplit.getSchemaName()),
                                pulsarSplit.getTableName()).getNamespaceObject(), offloadPolicies,
                        pulsarConnectorConfig),
                new PulsarConnectorMetricsTracker(pulsarConnectorCache.getStatsProvider()));
        this.decoderFactory = decoderFactory;
        initEntryCacheSizeAllocator(pulsarConnectorConfig);
    }

    // Exposed for testing purposes
    PulsarRecordCursor(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
            pulsarConnectorConfig, ManagedLedgerFactory managedLedgerFactory, ManagedLedgerConfig managedLedgerConfig,
                       PulsarConnectorMetricsTracker pulsarConnectorMetricsTracker,
                       PulsarDispatchingRowDecoderFactory decoderFactory) {
        this.splitSize = pulsarSplit.getSplitSize();
        initialize(columnHandles, pulsarSplit, pulsarConnectorConfig, managedLedgerFactory, managedLedgerConfig,
            pulsarConnectorMetricsTracker);
        this.decoderFactory = decoderFactory;
    }

    private void initialize(List<PulsarColumnHandle> columnHandles, PulsarSplit pulsarSplit, PulsarConnectorConfig
        pulsarConnectorConfig, ManagedLedgerFactory managedLedgerFactory, ManagedLedgerConfig managedLedgerConfig,
                            PulsarConnectorMetricsTracker pulsarConnectorMetricsTracker) {
        this.columnHandles = columnHandles;
        this.currentRowValues = new FieldValueProvider[columnHandles.size()];
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
        initEntryCacheSizeAllocator(pulsarConnectorConfig);

        try {
            this.schemaInfoProvider = new PulsarSqlSchemaInfoProvider(this.topicName,
                    pulsarConnectorConfig.getPulsarAdmin());
        } catch (PulsarClientException e) {
            log.error(e, "Failed to init  Pulsar SchemaInfo Provider");
            throw new RuntimeException(e);

        }
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
    public void setPulsarSqlSchemaInfoProvider(PulsarSqlSchemaInfoProvider schemaInfoProvider) {
        this.schemaInfoProvider = schemaInfoProvider;
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
                            entryQueueCacheSizeAllocator.release(entry.getLength());

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

                                                while (true) {
                                                    if (!haveAvailableCacheSize(
                                                            messageQueueCacheSizeAllocator, messageQueue)
                                                            || !messageQueue.offer(message)) {
                                                        Thread.sleep(1);
                                                    } else {
                                                        messageQueueCacheSizeAllocator.allocate(
                                                                message.getData().readableBytes());
                                                        break;
                                                    }
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
                        if (!readOffloaded && readOnlyCursorImpl.getCurrentLedgerInfo().hasOffloadContext()) {
                            log.warn(
                                "Ledger %s is offloaded for topic %s. Ignoring it because offloader is not configured",
                                readOnlyCursorImpl.getCurrentLedgerInfo().getLedgerId(), pulsarSplit.getTableName());

                            long numEntries = readOnlyCursorImpl.getCurrentLedgerInfo().getEntries();
                            long entriesToSkip =
                                (numEntries - ((PositionImpl) cursor.getReadPosition()).getEntryId()) + 1;
                            cursor.skipEntries(Math.toIntExact((entriesToSkip)));

                            entriesProcessed += entriesToSkip;
                        } else {
                            if (!haveAvailableCacheSize(entryQueueCacheSizeAllocator, entryQueue)) {
                                metricsTracker.incr_READ_ATTEMPTS_FAIL();
                                return;
                            }
                            // if the available size is invalid and the entry queue size is 0, read one entry
                            outstandingReadsRequests.decrementAndGet();
                            cursor.asyncReadEntries(batchSize, entryQueueCacheSizeAllocator.getAvailableCacheSize(),
                                    this, System.nanoTime(), PositionImpl.latest);
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
                    entryQueueCacheSizeAllocator.allocate(entry.getLength());
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

    /**
     * Check the queue has available cache size quota or not.
     * 1. If the CacheSizeAllocator is NullCacheSizeAllocator, return true.
     * 2. If the available cache size > 0, return true.
     * 3. If the available cache size is invalid and the queue size == 0, return true, ensure not block the query.
     */
    private boolean haveAvailableCacheSize(CacheSizeAllocator cacheSizeAllocator, SpscArrayQueue queue) {
        if (cacheSizeAllocator instanceof NullCacheSizeAllocator) {
            return true;
        }
        return cacheSizeAllocator.getAvailableCacheSize() > 0 || queue.size() == 0;
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
                messageQueueCacheSizeAllocator.release(currentMessage.getData().readableBytes());
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

        SchemaInfo schemaInfo = getBytesSchemaInfo(pulsarSplit.getSchemaType(), pulsarSplit.getSchemaName());
        try {
            if (schemaInfo == null) {
                schemaInfo =  schemaInfoProvider.getSchemaByVersion(this.currentMessage.getSchemaVersion()).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

        if (schemaInfo.getType().equals(SchemaType.KEY_VALUE)) {
            ByteBuf keyByteBuf;
            ByteBuf valueByteBuf;

            KeyValueEncodingType keyValueEncodingType = KeyValueSchemaInfo.decodeKeyValueEncodingType(schemaInfo);
            if (Objects.equals(keyValueEncodingType, KeyValueEncodingType.INLINE)) {
                ByteBuf dataPayload = this.currentMessage.getData();
                int keyLength = dataPayload.readInt();
                keyByteBuf = dataPayload.readSlice(keyLength);
                int valueLength = dataPayload.readInt();
                valueByteBuf = dataPayload.readSlice(valueLength);
            } else {
                keyByteBuf = this.currentMessage.getKeyBytes().get();
                valueByteBuf = this.currentMessage.getData();
            }

            KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
            Set<DecoderColumnHandle> keyColumnHandles = columnHandles.stream()
                    .filter(col -> !col.isInternal())
                    .filter(col -> PulsarColumnHandle.HandleKeyValueType.KEY
                            .equals(col.getHandleKeyValueType()))
                    .collect(toImmutableSet());
            PulsarRowDecoder keyDecoder = null;
            if (keyColumnHandles.size() > 0) {
                keyDecoder = decoderFactory.createRowDecoder(topicName,
                        kvSchemaInfo.getKey(), keyColumnHandles
                );
            }

            Set<DecoderColumnHandle> valueColumnHandles = columnHandles.stream()
                    .filter(col -> !col.isInternal())
                    .filter(col -> PulsarColumnHandle.HandleKeyValueType.VALUE
                            .equals(col.getHandleKeyValueType()))
                    .collect(toImmutableSet());
            PulsarRowDecoder valueDecoder = null;
            if (valueColumnHandles.size() > 0) {
                valueDecoder = decoderFactory.createRowDecoder(topicName,
                        kvSchemaInfo.getValue(),
                        valueColumnHandles);
            }

            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey;
            if (keyColumnHandles.size() > 0) {
                decodedKey = keyDecoder.decodeRow(keyByteBuf);
                decodedKey.ifPresent(currentRowValuesMap::putAll);
            }
            if (valueColumnHandles.size() > 0) {
                Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue =
                        valueDecoder.decodeRow(valueByteBuf);
                decodedValue.ifPresent(currentRowValuesMap::putAll);
            }
        } else {
            PulsarRowDecoder messageDecoder = decoderFactory.createRowDecoder(topicName,
                    schemaInfo,
                    columnHandles.stream()
                            .filter(col -> !col.isInternal())
                            .filter(col -> PulsarColumnHandle.HandleKeyValueType.NONE
                                    .equals(col.getHandleKeyValueType()))
                            .collect(toImmutableSet()));
            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue =
                    messageDecoder.decodeRow(this.currentMessage.getData());
            decodedValue.ifPresent(currentRowValuesMap::putAll);
        }

        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                if (PulsarInternalColumn.PARTITION.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle, longValueProvider(this.partition));
                } else if (PulsarInternalColumn.EVENT_TIME.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle, PulsarFieldValueProviders.timeValueProvider(
                            this.currentMessage.getEventTime(), this.currentMessage.getPublishTime() == 0));
                } else if (PulsarInternalColumn.PUBLISH_TIME.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle, PulsarFieldValueProviders.timeValueProvider(
                            this.currentMessage.getPublishTime(), this.currentMessage.getPublishTime() == 0));
                } else if (PulsarInternalColumn.MESSAGE_ID.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle, bytesValueProvider(
                            this.currentMessage.getMessageId().toString().getBytes()));
                } else if (PulsarInternalColumn.SEQUENCE_ID.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle, longValueProvider(this.currentMessage.getSequenceId()));
                } else if (PulsarInternalColumn.PRODUCER_NAME.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle,
                            bytesValueProvider(this.currentMessage.getProducerName().getBytes()));
                } else if (PulsarInternalColumn.KEY.getName().equals(columnHandle.getName())) {
                    String key = this.currentMessage.getKey().orElse(null);
                    currentRowValuesMap.put(columnHandle, bytesValueProvider(key == null ? null : key.getBytes()));
                } else if (PulsarInternalColumn.PROPERTIES.getName().equals(columnHandle.getName())) {
                    try {
                        currentRowValuesMap.put(columnHandle, bytesValueProvider(
                                new ObjectMapper().writeValueAsBytes(this.currentMessage.getProperties())));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new IllegalArgumentException("unknown internal field " + columnHandle.getName());
                }
            }
        }
        for (int i = 0; i < columnHandles.size(); i++) {
            ColumnHandle columnHandle = columnHandles.get(i);
            currentRowValues[i] = currentRowValuesMap.get(columnHandle);
        }

        metricsTracker.incr_NUM_RECORD_DESERIALIZED();

        // stats for time spend deserializing
        metricsTracker.end_RECORD_DESERIALIZE_TIME();

        return true;
    }

    private SchemaInfo getBytesSchemaInfo(SchemaType schemaType, String schemaName) {
        if (!schemaType.equals(SchemaType.BYTES) && !schemaType.equals(SchemaType.NONE)) {
            return null;
        }
        if (schemaName.equals(Schema.BYTES.getSchemaInfo().getName())) {
            return Schema.BYTES.getSchemaInfo();
        } else if (schemaName.equals(Schema.BYTEBUFFER.getSchemaInfo().getName())) {
            return Schema.BYTEBUFFER.getSchemaInfo();
        } else {
            return Schema.BYTES.getSchemaInfo();
        }
    }

    @Override
    public boolean getBoolean(int field) {
        return getFieldValueProvider(field, boolean.class).getBoolean();
    }

    @Override
    public long getLong(int field) {
        return getFieldValueProvider(field, long.class).getLong();
    }

    @Override
    public double getDouble(int field) {
        return getFieldValueProvider(field, double.class).getDouble();
    }

    @Override
    public Slice getSlice(int field) {
        return getFieldValueProvider(field, Slice.class).getSlice();
    }

    private FieldValueProvider getFieldValueProvider(int fieldIndex, Class<?> expectedType) {
        checkArgument(fieldIndex < columnHandles.size(), "Invalid field index");
        checkFieldType(fieldIndex, expectedType);
        return currentRowValues[fieldIndex];
    }

    @Override
    public Object getObject(int field) {
        return getFieldValueProvider(field, Block.class).getBlock();
    }

    @Override
    public boolean isNull(int field) {
        FieldValueProvider provider = currentRowValues[field];
        return provider == null || provider.isNull();
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

    private void initEntryCacheSizeAllocator(PulsarConnectorConfig connectorConfig) {
        if (connectorConfig.getMaxSplitQueueSizeBytes() >= 0) {
            this.entryQueueCacheSizeAllocator = new NoStrictCacheSizeAllocator(
                    connectorConfig.getMaxSplitQueueSizeBytes() / 2);
            this.messageQueueCacheSizeAllocator = new NoStrictCacheSizeAllocator(
                    connectorConfig.getMaxSplitQueueSizeBytes() / 2);
            log.info("Init cacheSizeAllocator with maxSplitEntryQueueSizeBytes {}.",
                    connectorConfig.getMaxSplitQueueSizeBytes());
        } else {
            this.entryQueueCacheSizeAllocator = new NullCacheSizeAllocator();
            this.messageQueueCacheSizeAllocator = new NullCacheSizeAllocator();
            log.info("Init cacheSizeAllocator with NullCacheSizeAllocator.");
        }
    }

}
