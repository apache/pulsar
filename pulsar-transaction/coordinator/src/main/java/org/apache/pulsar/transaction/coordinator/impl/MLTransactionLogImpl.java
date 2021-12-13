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

import io.netty.buffer.ByteBuf;
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
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.State;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLog;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;
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

    public MLTransactionLogImpl(TransactionCoordinatorID tcID,
                                ManagedLedgerFactory managedLedgerFactory,
                                ManagedLedgerConfig managedLedgerConfig) {
        this.topicName = getMLTransactionLogName(tcID);
        this.tcId = tcID.getId();
        this.managedLedgerFactory = managedLedgerFactory;
        this.managedLedgerConfig = managedLedgerConfig;
        this.entryQueue = new SpscArrayQueue<>(2000);
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
        cursor.asyncReadEntries(numberOfEntriesToRead, readEntriesCallback, System.nanoTime(), PositionImpl.latest);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        managedLedger.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                log.info("Transaction log with tcId : {} close managedLedger successful!", tcId);
                completableFuture.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Transaction log with tcId : {} close managedLedger fail!", tcId);
                completableFuture.completeExceptionally(exception);
            }
        }, null);

        return completableFuture;
    }

    @Override
    public CompletableFuture<Position> append(TransactionMetadataEntry transactionMetadataEntry) {
        int transactionMetadataEntrySize = transactionMetadataEntry.getSerializedSize();
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(transactionMetadataEntrySize, transactionMetadataEntrySize);
        CompletableFuture<Position> completableFuture = new CompletableFuture<>();
        transactionMetadataEntry.writeTo(buf);
        managedLedger.asyncAddEntry(buf, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                buf.release();
                completableFuture.complete(position);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Transaction log write transaction operation error", exception);
                if (exception instanceof ManagedLedgerAlreadyClosedException
                        && managedLedger instanceof ManagedLedgerImpl
                        && State.WriteFailed == ((ManagedLedgerImpl) managedLedger).getState()) {
                    managedLedger.readyToCreateNewLedger();
                }
                buf.release();
                completableFuture.completeExceptionally(exception);
            }
        }, null);

        return completableFuture;
    }

    public CompletableFuture<Void> deletePosition(List<Position> positions) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
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
            TransactionMetadataEntry transactionMetadataEntry = new TransactionMetadataEntry();

            while (fillEntryQueueCallback.fillQueue() || entryQueue.size() > 0) {
                Entry entry = entryQueue.poll();
                if (entry != null) {
                    try {
                        ByteBuf buffer = entry.getDataBuffer();
                        transactionMetadataEntry.parseFrom(buffer, buffer.readableBytes());
                        transactionLogReplayCallback.handleMetadataEntry(entry.getPosition(), transactionMetadataEntry);
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

    class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);
        private boolean isReadable = true;

        boolean fillQueue() {
            if (entryQueue.size() < entryQueue.capacity() && outstandingReadsRequests.get() == 0) {
                if (cursor.hasMoreEntries()) {
                    outstandingReadsRequests.incrementAndGet();
                    readAsync(100, this);
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
                    || exception instanceof ManagedLedgerException.ManagedLedgerFencedException) {
                isReadable = false;
            }
            log.error("Transaction log init fail error!", exception);
            outstandingReadsRequests.decrementAndGet();
        }

    }
}