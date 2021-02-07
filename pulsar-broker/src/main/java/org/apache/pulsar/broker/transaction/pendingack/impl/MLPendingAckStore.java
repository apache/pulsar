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
import io.netty.util.Timer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckMetadata;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckMetadataEntry;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckOp;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.naming.TopicName;
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
        cursor.asyncReadEntries(numberOfEntriesToRead, readEntriesCallback, System.nanoTime(), PositionImpl.latest);
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
    public CompletableFuture<Void> appendIndividualAck(TxnID txnID,
                                                       List<MutablePair<PositionImpl, Integer>> positions) {
        PendingAckMetadataEntry pendingAckMetadataEntry = new PendingAckMetadataEntry();
        pendingAckMetadataEntry.setPendingAckOp(PendingAckOp.ACK);
        pendingAckMetadataEntry.setAckType(AckType.Individual);
        List<PendingAckMetadata> pendingAckMetadataList = new ArrayList<>();
        positions.forEach(positionIntegerMutablePair -> {
            PendingAckMetadata pendingAckMetadata = new PendingAckMetadata();
            PositionImpl position = positionIntegerMutablePair.getLeft();
            int batchSize = positionIntegerMutablePair.getRight();
            if (positionIntegerMutablePair.getLeft().getAckSet() != null) {
                for (long l : position.getAckSet()) {
                    pendingAckMetadata.addAckSet(l);
                }
            }
            pendingAckMetadata.setLedgerId(position.getLedgerId());
            pendingAckMetadata.setEntryId(position.getEntryId());
            pendingAckMetadata.setBatchSize(batchSize);
            pendingAckMetadataList.add(pendingAckMetadata);
        });
        pendingAckMetadataEntry.addAllPendingAckMetadatas(pendingAckMetadataList);
        return appendCommon(pendingAckMetadataEntry, txnID);
    }

    @Override
    public CompletableFuture<Void> appendCumulativeAck(TxnID txnID, PositionImpl position) {
        PendingAckMetadataEntry pendingAckMetadataEntry = new PendingAckMetadataEntry();
        pendingAckMetadataEntry.setPendingAckOp(PendingAckOp.ACK);
        pendingAckMetadataEntry.setAckType(AckType.Cumulative);
        PendingAckMetadata pendingAckMetadata = new PendingAckMetadata();
        if (position.getAckSet() != null) {
            for (long l : position.getAckSet()) {
                pendingAckMetadata.addAckSet(l);
            }
        }
        pendingAckMetadata.setLedgerId(position.getLedgerId());
        pendingAckMetadata.setEntryId(position.getEntryId());
        pendingAckMetadataEntry.addAllPendingAckMetadatas(Collections.singleton(pendingAckMetadata));
        return appendCommon(pendingAckMetadataEntry, txnID);
    }

    @Override
    public CompletableFuture<Void> appendCommitMark(TxnID txnID, AckType ackType) {
        PendingAckMetadataEntry pendingAckMetadataEntry = new PendingAckMetadataEntry();
        pendingAckMetadataEntry.setPendingAckOp(PendingAckOp.COMMIT);
        pendingAckMetadataEntry.setAckType(ackType);
        return appendCommon(pendingAckMetadataEntry, txnID);
    }

    @Override
    public CompletableFuture<Void> appendAbortMark(TxnID txnID, AckType ackType) {
        PendingAckMetadataEntry pendingAckMetadataEntry = new PendingAckMetadataEntry();
        pendingAckMetadataEntry.setPendingAckOp(PendingAckOp.ABORT);
        pendingAckMetadataEntry.setAckType(ackType);
        return appendCommon(pendingAckMetadataEntry, txnID);
    }

    private CompletableFuture<Void> appendCommon(PendingAckMetadataEntry pendingAckMetadataEntry, TxnID txnID) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        pendingAckMetadataEntry.setTxnidLeastBits(txnID.getLeastSigBits());
        pendingAckMetadataEntry.setTxnidMostBits(txnID.getMostSigBits());
        int transactionMetadataEntrySize = pendingAckMetadataEntry.getSerializedSize();
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(transactionMetadataEntrySize, transactionMetadataEntrySize);
        pendingAckMetadataEntry.writeTo(buf);
        try {
            managedLedger.asyncAddEntry(buf, new AsyncCallbacks.AddEntryCallback() {

                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] MLPendingAckStore message append success at {}, operation : {}",
                                managedLedger.getName(), ctx, position, pendingAckMetadataEntry.getPendingAckOp());
                    }
                    buf.release();
                    completableFuture.complete(null);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}][{}] MLPendingAckStore message append fail exception : {}, operation : {}",
                            managedLedger.getName(), ctx, exception, pendingAckMetadataEntry.getPendingAckOp());
                    buf.release();
                    completableFuture.completeExceptionally(exception);
                }
            } , null);
        } catch (Exception e) {
            log.error("[{}] MLPendingAckStore message append fail exception : {}",
                    managedLedger.getName(), e);
            buf.release();
            completableFuture.completeExceptionally(e);
        }
        return completableFuture;
    }

    class PendingAckReplay implements Runnable {

        private final FillEntryQueueCallback fillEntryQueueCallback;
        private final PendingAckReplyCallBack pendingAckReplyCallBack;
        private final AtomicLong exceptionNumber = new AtomicLong();
        // TODO: MAX_EXCEPTION_NUMBER can config
        private static final int MAX_EXCEPTION_NUMBER = 500;

        PendingAckReplay(PendingAckReplyCallBack pendingAckReplyCallBack) {
            this.fillEntryQueueCallback = new FillEntryQueueCallback(exceptionNumber);
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
                    PendingAckMetadataEntry pendingAckMetadataEntry = new PendingAckMetadataEntry();
                    pendingAckMetadataEntry.parseFrom(buffer, buffer.readableBytes());
                    pendingAckReplyCallBack.handleMetadataEntry(pendingAckMetadataEntry);
                    entry.release();
                } else {
                    if (exceptionNumber.get() > MAX_EXCEPTION_NUMBER) {
                        log.error("[{}]Transaction pending ack recover fail when "
                                + "replay message error number > {}!", managedLedger.getName(), MAX_EXCEPTION_NUMBER);
                        return;
                    }
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        if (Thread.interrupted()) {
                            log.error("[{}]Transaction pending "
                                    + "replay thread interrupt!", managedLedger.getName(), e);
                        }
                    }
                }
            }
            pendingAckReplyCallBack.replayComplete();
        }
    }

    class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);
        private final AtomicLong exceptionNumber;
        public FillEntryQueueCallback(AtomicLong exceptionNumber) {
            this.exceptionNumber = exceptionNumber;
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
            exceptionNumber.getAndIncrement();
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