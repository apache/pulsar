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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckReplyCallBack;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.PendingAckMetadataEntry;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class MLPendingAckStore implements PendingAckStore {


    private ManagedLedger managedLedger;

    private final ManagedCursor cursor;

    private static final String PENDING_ACK_PERSISTENT_SUFFIX = "/transaction-pendingack";

    private static final String PENDING_ACK_CURSOR_NAME = "pendingack";

    private final PositionImpl lastConfirmedEntry;

    private final String ledgerName;

    private final String topicName;

    private final String subName;

    protected ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<Position>> pendingIndividualAckPersistentMap;
    protected KeyValue<TxnID, Position> pendingCumulativeAckPosition;

    public MLPendingAckStore(ManagedLedgerFactory managedLedgerFactory,
                             String topicName, String subName) throws Exception {
        this.topicName = topicName;
        this.subName = subName;
        this.ledgerName = topicName + "-" + subName + PENDING_ACK_PERSISTENT_SUFFIX;
        try {
            this.managedLedger = managedLedgerFactory.open(ledgerName);
            this.cursor = managedLedger.openCursor(PENDING_ACK_CURSOR_NAME, CommandSubscribe.InitialPosition.Earliest);
            this.lastConfirmedEntry = (PositionImpl) managedLedger.getLastConfirmedEntry();
        } catch (Exception e) {
            log.error("Pending ack persistent store init error!", e);
            throw new Exception(e.getCause());
        }
    }

    @Override
    public void replayAsync(PendingAckReplyCallBack pendingAckReplyCallBack) {
        new Thread(() -> new PendingAckReplay(pendingAckReplyCallBack).start()).start();
    }

    //TODO can control the number of entry to read
    private void readAsync(int numberOfEntriesToRead,
                           AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        cursor.asyncReadEntries(numberOfEntriesToRead, readEntriesCallback, System.nanoTime());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        pendingIndividualAckPersistentMap.clear();
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        cursor.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                try {
                    managedLedger.close();
                } catch (Exception e) {
                    completableFuture.completeExceptionally(e);
                }
                completableFuture.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                completableFuture.completeExceptionally(exception);
            }
        }, null);
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> append(TxnID txnID, PositionImpl position, AckType ackType) {
        if (ackType == AckType.Individual && pendingIndividualAckPersistentMap == null) {
            pendingIndividualAckPersistentMap = new ConcurrentOpenHashMap<>();
        }
        PendingAckMetadataEntry.Builder builder = PendingAckMetadataEntry.newBuilder();
        builder.setAckType(ackType);
        builder.setTxnidLeastBits(txnID.getLeastSigBits());
        builder.setTxnidMostBits(txnID.getMostSigBits());
        builder.setLedgerId(position.getLedgerId());
        builder.setEntryId(position.getEntryId());
        if (position.getAckSet() != null) {
            builder.addAllAckSet(SafeCollectionUtils.longArrayToList(position.getAckSet()));
        }
        PendingAckMetadataEntry pendingAckMetadataEntry = builder.build();
        int transactionMetadataEntrySize = pendingAckMetadataEntry.getSerializedSize();
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(transactionMetadataEntrySize, transactionMetadataEntrySize);
        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(buf);
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        try {
            pendingAckMetadataEntry.writeTo(outStream);
            managedLedger.asyncAddEntry(buf, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, Object ctx) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Pending ack message append success at {}", ledgerName, ctx, position);
                    }

                    if (ackType == AckType.Individual) {
                        ConcurrentOpenHashSet<Position> positions =
                                pendingIndividualAckPersistentMap
                                        .computeIfAbsent(txnID, v -> new ConcurrentOpenHashSet<>());
                        positions.add(position);
                    } else {
                        pendingCumulativeAckPosition = new KeyValue<>(txnID, position);
                    }
                    builder.recycle();
                    pendingAckMetadataEntry.recycle();
                    buf.release();
                    completableFuture.complete(null);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}][{}]Pending ack message append fail exception : {}", ledgerName, ctx, exception);
                    builder.recycle();
                    pendingAckMetadataEntry.recycle();
                    buf.release();
                    completableFuture.completeExceptionally(exception);
                }
            } , null);
        } catch (IOException e) {
            log.error("[{}] Pending ack message append fail exception : {}", ledgerName, e);
            buf.release();
            completableFuture.completeExceptionally(e);
        } finally {
            outStream.recycle();
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> deleteTxn(TxnID txnID, AckType ackType) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        if (ackType == AckType.Cumulative) {
            if (pendingCumulativeAckPosition != null && pendingCumulativeAckPosition.getKey() != null
                    && pendingCumulativeAckPosition.getKey().equals(txnID)) {
                this.cursor.asyncDelete(pendingCumulativeAckPosition.getValue(), new AsyncCallbacks.DeleteCallback() {
                    @Override
                    public void deleteComplete(Object position) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Pending ack delete message at {}", ledgerName, position);
                        }
                        completableFuture.complete(null);
                    }

                    @Override
                    public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}][{}] Pending ack failed to delete message at {}", ledgerName, ctx, exception);
                        completableFuture.completeExceptionally(exception);
                    }
                }, null);
            } else {
                log.error("Pending cumulative ack store delete txn " +
                        "is not current txn! topicName : [{}], txnID : [{}]", topicName, txnID);
                completableFuture.completeExceptionally(new TransactionConflictException("Pending cumulative ack " +
                        "store delete txn " + "is not current txn! topicName : " + topicName + ", txnID : " + txnID));
            }
        } else {
            if (pendingIndividualAckPersistentMap.containsKey(txnID)) {
                this.cursor.asyncDelete(pendingIndividualAckPersistentMap.get(txnID).values(),
                        new AsyncCallbacks.DeleteCallback() {
                            @Override
                            public void deleteComplete(Object position) {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Pending ack delete message at {}", ledgerName, position);
                                }
                                completableFuture.complete(null);
                            }

                            @Override
                            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                                log.error("[{}][{}] Pending ack failed to delete message at {}", ledgerName, ctx, exception);
                                completableFuture.completeExceptionally(exception);
                            }
                        }, null);
            } else {
                log.error("Pending ack store delete txnId : [{}] don't have position to delete!", txnID);
                completableFuture.completeExceptionally(new TransactionConflictException("Pending ack store delete " +
                        "txnId : " + txnID + " for TopicName : " + topicName + " don't have position to delete!"));
            }
        }
        return completableFuture;
    }

    class PendingAckReplay {

        private final FillEntryQueueCallback fillEntryQueueCallback;
        private long currentLoadEntryId = -1;
        private final PendingAckReplyCallBack pendingAckReplyCallBack;
        private final SpscArrayQueue<Entry> entryQueue;

        PendingAckReplay(PendingAckReplyCallBack pendingAckReplyCallBack) {
            this.entryQueue = new SpscArrayQueue<>(2000);
            this.fillEntryQueueCallback = new FillEntryQueueCallback(entryQueue);
            this.pendingAckReplyCallBack = pendingAckReplyCallBack;
        }

        public void start() {
            while (currentLoadEntryId < lastConfirmedEntry.getEntryId()) {
                fillEntryQueueCallback.fillQueue();
                Entry entry = entryQueue.poll();
                if (entry != null) {
                    ByteBuf buffer = entry.getDataBuffer();
                    currentLoadEntryId = entry.getEntryId();
                    ByteBufCodedInputStream stream = ByteBufCodedInputStream.get(buffer);
                    PendingAckMetadataEntry.Builder pendingAckMetadataEntryBuilder =
                            PendingAckMetadataEntry.newBuilder();
                    PendingAckMetadataEntry pendingAckMetadataEntry = null;
                    try {
                        pendingAckMetadataEntry =
                                pendingAckMetadataEntryBuilder.mergeFrom(stream, null).build();
                        pendingAckReplyCallBack.handleMetadataEntry(entry.getPosition(), pendingAckMetadataEntry);
                    } catch (Exception e) {
                        if (pendingAckMetadataEntry != null) {
                            log.error("TxnId : [{}:{}], Position : [{}:{}] Pending ack reply error!",
                                    pendingAckMetadataEntry.getTxnidMostBits(),
                                    pendingAckMetadataEntry.getTxnidLeastBits(),
                                    pendingAckMetadataEntry.getLedgerId(),
                                    pendingAckMetadataEntry.getEntryId(), e);
                        } else {
                            log.error("Pending ack reply error!", e);
                        }
                        throw new RuntimeException("Pending ack reply convert entry error : ", e);
                    }
                    entry.release();
                    pendingAckMetadataEntry.recycle();
                    pendingAckMetadataEntryBuilder.recycle();
                    stream.recycle();
                } else {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        //no-op
                    }
                }
            }
            log.info("MLPendingAckStore init success! TopicName : {}, SubName: {}", topicName, subName);
            pendingAckReplyCallBack.replayComplete();
        }
    }

    class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);
        private final SpscArrayQueue<Entry> entryQueue;

        public FillEntryQueueCallback(SpscArrayQueue<Entry> entryQueue) {
            this.entryQueue = entryQueue;
        }

        void fillQueue() {
            if (entryQueue.size() < entryQueue.capacity() && outstandingReadsRequests.get() == 0) {
                if (cursor.hasMoreEntries()) {
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
            log.error("Pending ack stat reply fail!", exception);
            outstandingReadsRequests.decrementAndGet();
        }

    }

    private static final Logger log = LoggerFactory.getLogger(MLPendingAckStore.class);
}