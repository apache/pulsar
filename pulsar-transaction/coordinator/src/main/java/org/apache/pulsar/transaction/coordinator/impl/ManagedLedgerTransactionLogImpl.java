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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.pulsar.transaction.coordinator.TransactionLog;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ManagedLedgerTransactionLogImpl implements TransactionLog {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerTransactionLogImpl.class);

    private final ManagedLedger managedLedger;

    private final static String TRANSACTION_LOG_PREFIX = "transaction/log/";

    private final ReadOnlyCursor readOnlyCursor;

    private final SpscArrayQueue<Entry> entryQueue;

    private final PositionImpl lastConfirmedEntry;

    private final long tcId;

    ManagedLedgerTransactionLogImpl(long tcID,
                                    ManagedLedgerFactory managedLedgerFactory) throws Exception {
        this.tcId = tcID;
        this.managedLedger = managedLedgerFactory.open(TRANSACTION_LOG_PREFIX + tcID);
        this.readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(TRANSACTION_LOG_PREFIX + tcID,
                PositionImpl.earliest, new ManagedLedgerConfig());
        this.entryQueue = new SpscArrayQueue<>(2000);
        this.lastConfirmedEntry = (PositionImpl) managedLedger.getLastConfirmedEntry();
    }

    @Override
    public void replayAsync(TransactionLogReplayCallback transactionLogReplayCallback) {
        new TransactionLogReplayer(transactionLogReplayCallback).start();
    }

    private void readAsync(int numberOfEntriesToRead,
                           AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        readOnlyCursor.asyncReadEntries(numberOfEntriesToRead, readEntriesCallback, System.nanoTime());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        closeReadOnlyCursorAsync().thenRun(() -> {
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
        });

        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> append(TransactionMetadataEntry transactionMetadataEntry) {
        int transactionMetadataEntrySize = transactionMetadataEntry.getSerializedSize();
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(transactionMetadataEntrySize, transactionMetadataEntrySize);
        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(buf);
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        try {
            transactionMetadataEntry.writeTo(outStream);
            managedLedger.asyncAddEntry(buf, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, Object ctx) {
                    buf.release();
                    completableFuture.complete(null);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Transaction log write transaction operation error", exception);
                    buf.release();
                    completableFuture.completeExceptionally(exception);
                }
            } , null);
        } catch (IOException e) {
            log.error("Transaction log write transaction operation error", e);
            completableFuture.completeExceptionally(e);
        } finally {
            outStream.recycle();
        }
        return completableFuture;
    }

    class TransactionLogReplayer {

        private FillEntryQueueCallback fillEntryQueueCallback;
        private long currentLoadEntryId;
        private TransactionLogReplayCallback transactionLogReplayCallback;

        TransactionLogReplayer(TransactionLogReplayCallback transactionLogReplayCallback) {
            this.fillEntryQueueCallback = new FillEntryQueueCallback();
            this.transactionLogReplayCallback = transactionLogReplayCallback;
        }

        public void start() {
            while (currentLoadEntryId < lastConfirmedEntry.getEntryId()) {
                fillEntryQueueCallback.fillQueue();
                Entry entry = entryQueue.poll();
                if (entry != null) {
                    ByteBuf buffer = entry.getDataBuffer();
                    currentLoadEntryId = entry.getEntryId();
                    ByteBufCodedInputStream stream = ByteBufCodedInputStream.get(buffer);
                    TransactionMetadataEntry.Builder transactionMetadataEntryBuilder =
                            TransactionMetadataEntry.newBuilder();
                    TransactionMetadataEntry transactionMetadataEntry;
                    try {
                        transactionMetadataEntry =
                                transactionMetadataEntryBuilder.mergeFrom(stream, null).build();
                    } catch (IOException e) {
                        log.error(e.getMessage(), e);
                        throw new RuntimeException("TransactionLog convert entry error : ", e);
                    }
                    transactionLogReplayCallback.handleMetadataEntry(transactionMetadataEntry);
                    entry.release();
                    transactionMetadataEntry.recycle();
                    transactionMetadataEntryBuilder.recycle();
                    stream.recycle();
                } else {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        //no-op
                    }
                }
            }

            closeReadOnlyCursorAsync().thenRun(() -> transactionLogReplayCallback.replayComplete());
        }
    }

    private CompletableFuture<Void> closeReadOnlyCursorAsync() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        readOnlyCursor.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                log.info("Transaction log with tcId : {} close ReadOnlyCursor successful!", tcId);
                completableFuture.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Transaction log with tcId : " + tcId + " close ReadOnlyCursor fail", exception);
                completableFuture.completeExceptionally(exception);
            }
        }, null);
        return completableFuture;
    }

    class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private AtomicLong outstandingReadsRequests = new AtomicLong(0);

        void fillQueue() {
            if (entryQueue.size() < entryQueue.capacity() && outstandingReadsRequests.get() == 0) {
                if (readOnlyCursor.hasMoreEntries()) {
                    outstandingReadsRequests.incrementAndGet();
                    readAsync(100, this);
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

            outstandingReadsRequests.decrementAndGet();
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            log.error("Transaction log init fail error!", exception);
            outstandingReadsRequests.decrementAndGet();
        }

    }
}
