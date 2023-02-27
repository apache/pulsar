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

import static org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER;
import static org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER_LEN;
import static org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_VERSION_LEN;
import io.netty.buffer.ByteBuf;
import io.netty.util.Timer;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLog;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;
import org.apache.pulsar.transaction.coordinator.proto.BatchedTransactionMetadataEntry;
import org.apache.pulsar.transaction.coordinator.proto.TransactionMetadataEntry;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLTransactionLogImpl implements TransactionLog {

    private static final Logger log = LoggerFactory.getLogger(MLTransactionLogImpl.class);

    private final ManagedLedgerFactory managedLedgerFactory;
    private final ManagedLedgerConfig managedLedgerConfig;
    private ManagedLedger managedLedger;

    public static final String TRANSACTION_LOG_PREFIX = "__transaction_log_";

    private ManagedCursor cursor;

    public static final String TRANSACTION_SUBSCRIPTION_NAME = "transaction.subscription";

    private final SpscArrayQueue<Entry> entryQueue;

    private final long tcId;

    private final TopicName topicName;

    private TxnLogBufferedWriter<TransactionMetadataEntry> bufferedWriter;

    private final Timer timer;

    private final TxnLogBufferedWriterConfig txnLogBufferedWriterConfig;

    private final TxnLogBufferedWriterMetricsStats bufferedWriterMetrics;

    public MLTransactionLogImpl(TransactionCoordinatorID tcID,
                                ManagedLedgerFactory managedLedgerFactory,
                                ManagedLedgerConfig managedLedgerConfig,
                                TxnLogBufferedWriterConfig txnLogBufferedWriterConfig,
                                Timer timer,
                                TxnLogBufferedWriterMetricsStats bufferedWriterMetrics) {
        this.topicName = getMLTransactionLogName(tcID);
        this.tcId = tcID.getId();
        this.managedLedgerFactory = managedLedgerFactory;
        this.managedLedgerConfig = managedLedgerConfig;
        this.timer = timer;
        this.txnLogBufferedWriterConfig = txnLogBufferedWriterConfig;
        if (txnLogBufferedWriterConfig.isBatchEnabled()) {
            this.managedLedgerConfig.setDeletionAtBatchIndexLevelEnabled(true);
        }
        this.entryQueue = new SpscArrayQueue<>(2000);
        this.bufferedWriterMetrics = bufferedWriterMetrics;
    }

    public static TopicName getMLTransactionLogName(TransactionCoordinatorID tcID) {
        return TopicName.get(TopicDomain.persistent.value(),
                NamespaceName.SYSTEM_NAMESPACE, TRANSACTION_LOG_PREFIX + tcID.getId());
    }

    @Override
    public CompletableFuture<Void> initialize() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        managedLedgerFactory.asyncOpen(topicName.getPersistenceNamingEncoding(),
                managedLedgerConfig,
                new AsyncCallbacks.OpenLedgerCallback() {
                    @Override
                    public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                        MLTransactionLogImpl.this.managedLedger = ledger;
                        MLTransactionLogImpl.this.bufferedWriter = new TxnLogBufferedWriter<>(
                                managedLedger, ((ManagedLedgerImpl) managedLedger).getExecutor(),
                                timer, TransactionLogDataSerializer.INSTANCE,
                                txnLogBufferedWriterConfig.getBatchedWriteMaxRecords(),
                                txnLogBufferedWriterConfig.getBatchedWriteMaxSize(),
                                txnLogBufferedWriterConfig.getBatchedWriteMaxDelayInMillis(),
                                txnLogBufferedWriterConfig.isBatchEnabled(),
                                bufferedWriterMetrics);

                        managedLedger.asyncOpenCursor(TRANSACTION_SUBSCRIPTION_NAME,
                                CommandSubscribe.InitialPosition.Earliest, new AsyncCallbacks.OpenCursorCallback() {
                                    @Override
                                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                                        MLTransactionLogImpl.this.cursor = cursor;
                                        future.complete(null);
                                    }

                                    @Override
                                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                                        future.completeExceptionally(exception);
                                    }
                                }, null);
                    }

                    @Override
                    public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }
                }, null, null);
        return future;
    }

    @Override
    public void replayAsync(TransactionLogReplayCallback transactionLogReplayCallback) {
        new TransactionLogReplayer(transactionLogReplayCallback).start();
    }

    private void readAsync(int numberOfEntriesToRead,
                           AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        cursor.asyncReadEntries(numberOfEntriesToRead, readEntriesCallback, System.nanoTime(), PositionImpl.LATEST);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        managedLedger.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                log.info("Transaction log with tcId : {} close managedLedger successful!", tcId);
                completableFuture.complete(null);
                bufferedWriter.close();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                // If close managed ledger failure, should not close buffered writer.
                log.error("Transaction log with tcId : {} close managedLedger fail!", tcId);
                completableFuture.completeExceptionally(exception);
            }
        }, null);

        return completableFuture;
    }

    @Override
    public CompletableFuture<Position> append(TransactionMetadataEntry transactionMetadataEntry) {
        CompletableFuture<Position> completableFuture = new CompletableFuture<>();
        bufferedWriter.asyncAddData(transactionMetadataEntry, new TxnLogBufferedWriter.AddDataCallback() {
            @Override
            public void addComplete(Position position, Object context) {
                completableFuture.complete(position);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Transaction log write transaction operation error", exception);
                if (exception instanceof ManagedLedgerAlreadyClosedException) {
                    managedLedger.readyToCreateNewLedger();
                }
                completableFuture.completeExceptionally(exception);
            }
        }, null);

        return completableFuture;
    }

    public CompletableFuture<Void> deletePosition(List<Position> positions) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        // Change the flag in ackSet to deleted.
        for (Position position : positions) {
            if (position instanceof TxnBatchedPositionImpl batchedPosition){
                batchedPosition.setAckSetByIndex();
            }
        }
        this.cursor.asyncDelete(positions, new AsyncCallbacks.DeleteCallback() {
            @Override
            public void deleteComplete(Object position) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Deleted message at {}", topicName,
                            TRANSACTION_SUBSCRIPTION_NAME, position);
                }
                completableFuture.complete(null);
            }

            @Override
            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("[{}][{}] Failed to delete message at {}", topicName,
                        TRANSACTION_SUBSCRIPTION_NAME, ctx, exception);
                completableFuture.completeExceptionally(exception);
            }
        }, null);
        return completableFuture;
    }

    public ManagedLedger getManagedLedger() {
        return this.managedLedger;
    }

    class TransactionLogReplayer {

        private final FillEntryQueueCallback fillEntryQueueCallback;
        private final TransactionLogReplayCallback transactionLogReplayCallback;

        TransactionLogReplayer(TransactionLogReplayCallback transactionLogReplayCallback) {
            this.fillEntryQueueCallback = new FillEntryQueueCallback();
            this.transactionLogReplayCallback = transactionLogReplayCallback;
        }

        public void start() {
            while (fillEntryQueueCallback.fillQueue() || entryQueue.size() > 0) {
                Entry entry = entryQueue.poll();
                if (entry != null) {
                    try {
                        List<TransactionMetadataEntry> logs = deserializeEntry(entry);
                        if (logs.isEmpty()){
                            continue;
                        } else if (logs.size() == 1){
                            TransactionMetadataEntry log = logs.get(0);
                            transactionLogReplayCallback.handleMetadataEntry(entry.getPosition(), log);
                        } else {
                            /**
                             * 1. Query batch index of current entry from cursor.
                             * 2. Filter the data which has already ack.
                             * 3. Build batched position and handle valid data.
                             */
                            long[] ackSetAlreadyAck = cursor.getDeletedBatchIndexesAsLongArray(
                                    PositionImpl.get(entry.getLedgerId(), entry.getEntryId()));
                            BitSetRecyclable bitSetAlreadyAck = null;
                            if (ackSetAlreadyAck != null){
                                bitSetAlreadyAck = BitSetRecyclable.valueOf(ackSetAlreadyAck);
                            }
                            int batchSize = logs.size();
                            for (int batchIndex = 0; batchIndex < batchSize; batchIndex++){
                                if (bitSetAlreadyAck != null && !bitSetAlreadyAck.get(batchIndex)){
                                   continue;
                                }
                                TransactionMetadataEntry log = logs.get(batchIndex);
                                TxnBatchedPositionImpl batchedPosition = new TxnBatchedPositionImpl(entry.getLedgerId(),
                                        entry.getEntryId(), batchSize, batchIndex);
                                transactionLogReplayCallback.handleMetadataEntry(batchedPosition, log);
                            }
                            if (ackSetAlreadyAck != null){
                                bitSetAlreadyAck.recycle();
                            }
                        }
                    } finally {
                        entry.release();
                    }
                } else {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        //no-op
                    }
                }
            }
            transactionLogReplayCallback.replayComplete();
        }
    }

    public static List<TransactionMetadataEntry> deserializeEntry(ByteBuf buffer){
        // Check whether it is batched Entry.
        buffer.markReaderIndex();
        short magicNum = buffer.readShort();
        buffer.resetReaderIndex();
        if (magicNum == BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER){
            // skip magic num and version mark.
            buffer.skipBytes(BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER_LEN + BATCHED_ENTRY_DATA_PREFIX_VERSION_LEN);
            BatchedTransactionMetadataEntry batchedLog = new BatchedTransactionMetadataEntry();
            batchedLog.parseFrom(buffer, buffer.readableBytes());
            return batchedLog.getTransactionLogsList();
        } else {
            TransactionMetadataEntry log = new TransactionMetadataEntry();
            log.parseFrom(buffer, buffer.readableBytes());
            return Collections.singletonList(log);
        }
    }

    public static List<TransactionMetadataEntry> deserializeEntry(Entry entry){
        return deserializeEntry(entry.getDataBuffer());
    }

    class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);
        private volatile boolean isReadable = true;
        private static final int NUMBER_OF_PER_READ_ENTRY = 100;

        boolean fillQueue() {
            if (entryQueue.size() + NUMBER_OF_PER_READ_ENTRY < entryQueue.capacity()
                    && outstandingReadsRequests.get() == 0) {
                if (cursor.hasMoreEntries()) {
                    outstandingReadsRequests.incrementAndGet();
                    readAsync(NUMBER_OF_PER_READ_ENTRY, this);
                    return isReadable;
                } else {
                    return false;
                }
            } else {
                return isReadable;
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

            outstandingReadsRequests.decrementAndGet();
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            if (managedLedgerConfig.isAutoSkipNonRecoverableData()
                    && exception instanceof ManagedLedgerException.NonRecoverableLedgerException
                    || exception instanceof ManagedLedgerException.ManagedLedgerFencedException
                    || exception instanceof ManagedLedgerException.CursorAlreadyClosedException) {
                isReadable = false;
            } else {
                outstandingReadsRequests.decrementAndGet();
            }
            log.error("Transaction log init fail error!", exception);
        }

    }

    /**
     * Used only for buffered writer. Since all cmd-writes in buffered writer are in the same thread, so we can use
     * threadLocal variables here. Why need to be on the same thread ?
     * Because {@link BatchedTransactionMetadataEntry#clear()} will modifies the elements in the attribute
     * {@link BatchedTransactionMetadataEntry#getTransactionLogsList()} ()}, this will cause problems by multi-thread
     * write.
     */
    private static final FastThreadLocal<BatchedTransactionMetadataEntry> localBatchedTransactionLogCache =
            new FastThreadLocal<>() {
                @Override
                protected BatchedTransactionMetadataEntry initialValue() throws Exception {
                    return new BatchedTransactionMetadataEntry();
                }
            };

    private static class TransactionLogDataSerializer
            implements TxnLogBufferedWriter.DataSerializer<TransactionMetadataEntry>{

        private static final TransactionLogDataSerializer INSTANCE = new TransactionLogDataSerializer();

        @Override
        public int getSerializedSize(TransactionMetadataEntry data) {
            return data.getSerializedSize();
        }

        @Override
        public ByteBuf serialize(TransactionMetadataEntry data) {
            int transactionMetadataEntrySize = data.getSerializedSize();
            ByteBuf buf =
                    PulsarByteBufAllocator.DEFAULT.buffer(transactionMetadataEntrySize, transactionMetadataEntrySize);
            data.writeTo(buf);
            return buf;
        }

        @Override
        public ByteBuf serialize(ArrayList<TransactionMetadataEntry> transactionLogArray) {
            // Since all writes are in the same thread, so we can use threadLocal variables here.
            BatchedTransactionMetadataEntry data = localBatchedTransactionLogCache.get();
            data.clear();
            data.addAllTransactionLogs(transactionLogArray);
            int bytesSize = data.getSerializedSize();
            ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(bytesSize, bytesSize);
            data.writeTo(buf);
            return buf;
        }
    }
}