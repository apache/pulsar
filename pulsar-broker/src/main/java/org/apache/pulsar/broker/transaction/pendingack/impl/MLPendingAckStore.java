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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.util.Timer;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckReplyCallBack;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.proto.TransactionPendingAck.PendingAckMetadata;
import org.apache.pulsar.broker.transaction.proto.TransactionPendingAck.PendingAckMetadataEntry;
import org.apache.pulsar.broker.transaction.proto.TransactionPendingAck.PendingAckOp;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implement of the pending ack store by manageLedger.
 */
public class MLPendingAckStore implements PendingAckStore {


    private final ManagedLedger managedLedger;

    private final ManagedCursor cursor;

    private static final String PENDING_ACK_STORE_SUFFIX = "-transaction-pendingack";

    private static final String PENDING_ACK_STORE_CURSOR_NAME = "pendingack";

    private final SpscArrayQueue<Entry> entryQueue;

    //this is for replay
    private final PositionImpl lastConfirmedEntry;

    private PositionImpl currentLoadPosition;

    private final Timer timer;

    private final MLPendingAckStoreTimerTask mlPendingAckStoreTimerTask;

    private final int intervalTime;

    public MLPendingAckStore(ManagedLedger managedLedger, ManagedCursor cursor,
                             Timer timer, ManagedCursor subManagedCursor, int maxIntervalTime, int minIntervalTime) {
        this.managedLedger = managedLedger;
        this.cursor = cursor;
        this.currentLoadPosition = (PositionImpl) this.cursor.getMarkDeletedPosition();
        this.entryQueue = new SpscArrayQueue<>(2000);
        this.lastConfirmedEntry = (PositionImpl) managedLedger.getLastConfirmedEntry();
        this.timer = timer;
        this.intervalTime = minIntervalTime;
        this.mlPendingAckStoreTimerTask = new MLPendingAckStoreTimerTask(cursor, managedLedger,
                minIntervalTime, maxIntervalTime, subManagedCursor, this.timer);
    }

    @Override
    public void replayAsync(PendingAckHandleImpl pendingAckHandle, ScheduledExecutorService transactionReplayExecutor) {
        transactionReplayExecutor
                .execute(new PendingAckReplay(new MLPendingAckReplyCallBack(this, pendingAckHandle)));
    }

    //TODO can control the number of entry to read
    private void readAsync(int numberOfEntriesToRead,
                           AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        cursor.asyncReadEntries(numberOfEntriesToRead, readEntriesCallback, System.nanoTime());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
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
    public CompletableFuture<Void> appendIndividualAck(TxnID txnID, List<MutablePair<PositionImpl, Integer>> positions) {
        PendingAckMetadataEntry.Builder builder = PendingAckMetadataEntry.newBuilder();
        builder.setPendingAckOp(PendingAckOp.ACK);
        builder.setAckType(AckType.Individual);
        positions.forEach(positionIntegerMutablePair -> {
            PendingAckMetadata.Builder metadataBuilder = PendingAckMetadata.newBuilder();
            PositionImpl position = positionIntegerMutablePair.getLeft();
            int batchSize = positionIntegerMutablePair.getRight();
            if (positionIntegerMutablePair.getLeft().getAckSet() != null) {
                metadataBuilder.addAllAckSet(SafeCollectionUtils.longArrayToList(position.getAckSet()));
                metadataBuilder.setBatchSize(batchSize);
            }
            metadataBuilder.setLedgerId(position.getLedgerId());
            metadataBuilder.setEntryId(position.getEntryId());
            PendingAckMetadata pendingAckMetadata = metadataBuilder.build();
            metadataBuilder.recycle();
            builder.addPendingAckMetadata(pendingAckMetadata);
        });
        return appendCommon(builder, txnID);
    }

    @Override
    public CompletableFuture<Void> appendCumulativeAck(TxnID txnID, PositionImpl position) {
        PendingAckMetadataEntry.Builder builder = PendingAckMetadataEntry.newBuilder();
        builder.setPendingAckOp(PendingAckOp.ACK);
        builder.setAckType(AckType.Cumulative);
        PendingAckMetadata.Builder metadataBuilder = PendingAckMetadata.newBuilder();
        if (position.getAckSet() != null) {
            metadataBuilder.addAllAckSet(SafeCollectionUtils.longArrayToList(position.getAckSet()));
        }
        metadataBuilder.setLedgerId(position.getLedgerId());
        metadataBuilder.setEntryId(position.getEntryId());
        PendingAckMetadata pendingAckMetadata = metadataBuilder.build();
        metadataBuilder.recycle();
        builder.addPendingAckMetadata(pendingAckMetadata);
        return appendCommon(builder, txnID);
    }

    @Override
    public CompletableFuture<Void> appendCommitMark(TxnID txnID, AckType ackType) {
        PendingAckMetadataEntry.Builder builder = PendingAckMetadataEntry.newBuilder();
        builder.setPendingAckOp(PendingAckOp.COMMIT);
        builder.setAckType(ackType);
        return appendCommon(builder, txnID);
    }

    @Override
    public CompletableFuture<Void> appendAbortMark(TxnID txnID, AckType ackType) {
        PendingAckMetadataEntry.Builder builder = PendingAckMetadataEntry.newBuilder();
        builder.setPendingAckOp(PendingAckOp.ABORT);
        builder.setAckType(ackType);
        return appendCommon(builder, txnID);
    }

    private CompletableFuture<Void> appendCommon(PendingAckMetadataEntry.Builder builder, TxnID txnID) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        builder.setTxnidLeastBits(txnID.getLeastSigBits());
        builder.setTxnidMostBits(txnID.getMostSigBits());
        PendingAckMetadataEntry pendingAckMetadataEntry = builder.build();
        int transactionMetadataEntrySize = pendingAckMetadataEntry.getSerializedSize();
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(transactionMetadataEntrySize, transactionMetadataEntrySize);
        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(buf);
        try {
            pendingAckMetadataEntry.writeTo(outStream);
            managedLedger.asyncAddEntry(buf, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, Object ctx) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] MLPendingAckStore message append success at {}, operation : {}",
                                managedLedger.getName(), ctx, position, builder.getPendingAckOp());
                    }
                    builder.recycle();
                    pendingAckMetadataEntry.recycle();
                    buf.release();
                    completableFuture.complete(null);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}][{}] MLPendingAckStore message append fail exception : {}, operation : {}",
                            managedLedger.getName(), ctx, exception, builder.getPendingAckOp());
                    builder.recycle();
                    pendingAckMetadataEntry.recycle();
                    buf.release();
                    completableFuture.completeExceptionally(exception);
                }
            } , null);
        } catch (Exception e) {
            log.error("[{}] MLPendingAckStore message append fail exception : {}",
                    managedLedger.getName(), e);
            builder.recycle();
            pendingAckMetadataEntry.recycle();
            buf.release();
            completableFuture.completeExceptionally(e);
        } finally {
            outStream.recycle();
        }
        return completableFuture;
    }

    class PendingAckReplay implements Runnable {

        private final FillEntryQueueCallback fillEntryQueueCallback;
        private final PendingAckReplyCallBack pendingAckReplyCallBack;

        PendingAckReplay(PendingAckReplyCallBack pendingAckReplyCallBack) {
            this.fillEntryQueueCallback = new FillEntryQueueCallback();
            this.pendingAckReplyCallBack = pendingAckReplyCallBack;
        }

        @Override
        public void run() {
            while (lastConfirmedEntry.compareTo(currentLoadPosition) > 0) {
                fillEntryQueueCallback.fillQueue();
                Entry entry = entryQueue.poll();
                if (entry != null) {
                    ByteBuf buffer = entry.getDataBuffer();
                    currentLoadPosition = PositionImpl.get(entry.getLedgerId(), entry.getEntryId());
                    ByteBufCodedInputStream stream = ByteBufCodedInputStream.get(buffer);
                    PendingAckMetadataEntry.Builder pendingAckMetadataEntryBuilder =
                            PendingAckMetadataEntry.newBuilder();
                    PendingAckMetadataEntry pendingAckMetadataEntry = null;
                    try {
                        pendingAckMetadataEntry =
                                pendingAckMetadataEntryBuilder.mergeFrom(stream, null).build();
                        pendingAckReplyCallBack.handleMetadataEntry(pendingAckMetadataEntry);
                    } catch (Exception e) {
                        if (pendingAckMetadataEntry != null) {
                            log.error("TxnId : [{}:{}] MLPendingAckStore reply error!",
                                    pendingAckMetadataEntry.getTxnidMostBits(),
                                    pendingAckMetadataEntry.getTxnidLeastBits(), e);
                        } else {
                            log.error("MLPendingAckStore reply error!", e);
                        }
                    }
                    entry.release();
                    if (pendingAckMetadataEntry != null) {
                        pendingAckMetadataEntry.recycle();
                    }
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
            pendingAckReplyCallBack.replayComplete();
        }
    }

    class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);

        public FillEntryQueueCallback() {
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
            log.error("MLPendingAckStore stat reply fail!", exception);
            outstandingReadsRequests.decrementAndGet();
        }

    }

    //this task is for delete the pending ack store
    protected void startTimerTask() {
        this.timer.newTimeout(this.mlPendingAckStoreTimerTask, intervalTime, TimeUnit.MILLISECONDS);
    }

    public static String getTransactionPendingAckStoreSuffix(String originTopicName, String subName) {
        return TopicName.get(originTopicName) + "-" + subName + PENDING_ACK_STORE_SUFFIX;
    }

    public static String getTransactionPendingAckStoreCursorName() {
        return PENDING_ACK_STORE_CURSOR_NAME;
    }

    private static final Logger log = LoggerFactory.getLogger(MLPendingAckStore.class);
}