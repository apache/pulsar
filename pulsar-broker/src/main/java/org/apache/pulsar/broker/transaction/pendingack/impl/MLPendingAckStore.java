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

import com.google.common.collect.ComparisonChain;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
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

    public static final String PENDING_ACK_STORE_SUFFIX = "__transaction_pending_ack";

    public static final String PENDING_ACK_STORE_CURSOR_NAME = "__pending_ack_state";

    private final SpscArrayQueue<Entry> entryQueue;

    //this is for replay
    private final PositionImpl lastConfirmedEntry;

    private PositionImpl currentLoadPosition;

    /**
     * The map is for pending ack store clear useless data.
     * <p>
     *     When ack message append to pending ack store, it will store the position which is persistent as key.
     * <p>
     *     When ack message append to pending ack store, it will store the position which is the max position of this
     *     ack by the original topic as value.
     * <p>
     *     It will judge the position with the max sub cursor position whether smaller than the subCursor mark
     *     delete position.
     *     <p>
     *         If the max position is smaller than the subCursor mark delete position, the log cursor will mark delete
     *         the position.
     */
    private final ConcurrentSkipListMap<PositionImpl, PositionImpl> metadataPositions;

    private final ManagedCursor subManagedCursor;

    public MLPendingAckStore(ManagedLedger managedLedger, ManagedCursor cursor,
                             ManagedCursor subManagedCursor) {
        this.managedLedger = managedLedger;
        this.cursor = cursor;
        this.currentLoadPosition = (PositionImpl) this.cursor.getMarkDeletedPosition();
        this.entryQueue = new SpscArrayQueue<>(2000);
        this.lastConfirmedEntry = (PositionImpl) managedLedger.getLastConfirmedEntry();
        this.metadataPositions = new ConcurrentSkipListMap<>();
        this.subManagedCursor = subManagedCursor;
    }

    @Override
    public void replayAsync(PendingAckHandleImpl pendingAckHandle, ScheduledExecutorService transactionReplayExecutor) {
        transactionReplayExecutor
                .execute(new PendingAckReplay(new MLPendingAckReplyCallBack(pendingAckHandle)));
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
                managedLedger.asyncClose(new AsyncCallbacks.CloseCallback() {

                    @Override
                    public void closeComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}][{}] MLPendingAckStore closed successfully！", managedLedger.getName(), ctx);
                        }
                        completableFuture.complete(null);
                    }

                    @Override
                    public void closeFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}][{}] MLPendingAckStore closed failed,exception={}", managedLedger.getName(),
                                ctx, exception);
                        completableFuture.completeExceptionally(exception);
                    }
                }, ctx);
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
        managedLedger.asyncAddEntry(buf, new AsyncCallbacks.AddEntryCallback() {

            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] MLPendingAckStore message append success at {} txnId: {}, operation : {}",
                            managedLedger.getName(), ctx, position, txnID, pendingAckMetadataEntry.getPendingAckOp());
                }
                // store the persistent position in to memory
                if (pendingAckMetadataEntry.getPendingAckOp() != PendingAckOp.ABORT
                        && pendingAckMetadataEntry.getPendingAckOp() != PendingAckOp.COMMIT) {
                    Optional<PendingAckMetadata> optional = pendingAckMetadataEntry.getPendingAckMetadatasList()
                            .stream().max((o1, o2) -> ComparisonChain.start().compare(o1.getLedgerId(),
                                    o2.getLedgerId()).compare(o1.getEntryId(), o2.getEntryId()).result());
                    optional.ifPresent(pendingAckMetadata ->
                            metadataPositions.compute((PositionImpl) position, (thisPosition, otherPosition) -> {
                                PositionImpl nowPosition = PositionImpl.get(pendingAckMetadata.getLedgerId(),
                                        pendingAckMetadata.getEntryId());
                                if (otherPosition == null) {
                                    return nowPosition;
                                } else {
                                    return nowPosition.compareTo(otherPosition) > 0 ? nowPosition : otherPosition;
                                }
                    }));
                }

                buf.release();
                completableFuture.complete(null);

                if (!metadataPositions.isEmpty()) {
                    PositionImpl firstPosition = metadataPositions.firstEntry().getKey();
                    PositionImpl deletePosition = metadataPositions.firstEntry().getKey();
                    while (!metadataPositions.isEmpty()
                            && metadataPositions.firstKey() != null
                            && subManagedCursor.getPersistentMarkDeletedPosition() != null
                            && metadataPositions.firstEntry().getValue()
                            .compareTo((PositionImpl) subManagedCursor.getPersistentMarkDeletedPosition()) <= 0) {
                        deletePosition = metadataPositions.firstKey();
                        metadataPositions.remove(metadataPositions.firstKey());
                    }

                    if (firstPosition != deletePosition) {
                        PositionImpl finalDeletePosition = deletePosition;
                        cursor.asyncMarkDelete(deletePosition,
                                new AsyncCallbacks.MarkDeleteCallback() {
                                    @Override
                                    public void markDeleteComplete(Object ctx) {
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Transaction pending ack store mark delete position : "
                                                            + "[{}] success", managedLedger.getName(),
                                                    finalDeletePosition);
                                        }
                                    }

                                    @Override
                                    public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                                        if (log.isDebugEnabled()) {
                                            log.error("[{}] Transaction pending ack store mark delete position : "
                                                            + "[{}] fail!", managedLedger.getName(),
                                                    finalDeletePosition, exception);
                                        }
                                    }
                                }, null);
                    }
                }
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                log.error("[{}][{}] MLPendingAckStore message append fail exception : {}, operation : {}",
                        managedLedger.getName(), ctx, exception, pendingAckMetadataEntry.getPendingAckOp());
                buf.release();
                completableFuture.completeExceptionally(new PersistenceException(exception));
            }
        } , null);
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
            try {
                if (cursor.isClosed()) {
                    log.warn("[{}] MLPendingAckStore cursor have been closed, close replay thread.",
                            cursor.getManagedLedger().getName());
                    return;
                }
                while (lastConfirmedEntry.compareTo(currentLoadPosition) > 0 && fillEntryQueueCallback.fillQueue()) {
                    Entry entry = entryQueue.poll();
                    if (entry != null) {
                        ByteBuf buffer = entry.getDataBuffer();
                        currentLoadPosition = PositionImpl.get(entry.getLedgerId(), entry.getEntryId());
                        PendingAckMetadataEntry pendingAckMetadataEntry = new PendingAckMetadataEntry();
                        pendingAckMetadataEntry.parseFrom(buffer, buffer.readableBytes());
                        // store the persistent position in to memory
                        // store the max position of this entry retain
                        if (pendingAckMetadataEntry.getPendingAckOp() != PendingAckOp.ABORT
                                && pendingAckMetadataEntry.getPendingAckOp() != PendingAckOp.COMMIT) {
                            Optional<PendingAckMetadata> optional = pendingAckMetadataEntry.getPendingAckMetadatasList()
                                    .stream().max((o1, o2) -> ComparisonChain.start().compare(o1.getLedgerId(),
                                            o2.getLedgerId()).compare(o1.getEntryId(), o2.getEntryId()).result());

                            optional.ifPresent(pendingAckMetadata ->
                                    metadataPositions.compute(PositionImpl.get(entry.getLedgerId(), entry.getEntryId()),
                                            (thisPosition, otherPosition) -> {
                                                PositionImpl nowPosition = PositionImpl
                                                        .get(pendingAckMetadata.getLedgerId(),
                                                                pendingAckMetadata.getEntryId());
                                                if (otherPosition == null) {
                                                    return nowPosition;
                                                } else {
                                                    return nowPosition.compareTo(otherPosition) > 0 ? nowPosition
                                                            : otherPosition;
                                                }
                                            }));
                        }
                        pendingAckReplyCallBack.handleMetadataEntry(pendingAckMetadataEntry);
                        entry.release();
                    } else {
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
            } catch (Exception e) {
                log.error("[{}] Pending ack recover fail!", subManagedCursor.getManagedLedger().getName(), e);
                return;
            }
            pendingAckReplyCallBack.replayComplete();
        }
    }

    class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private volatile boolean isReadable = true;
        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);

        boolean fillQueue() {
            if (entryQueue.size() < entryQueue.capacity() && outstandingReadsRequests.get() == 0) {
                if (cursor.hasMoreEntries()) {
                    outstandingReadsRequests.incrementAndGet();
                    readAsync(100, this);
                }
            }
            return isReadable;
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
            if (managedLedger.getConfig().isAutoSkipNonRecoverableData()
                    && exception instanceof ManagedLedgerException.NonRecoverableLedgerException
                    || exception instanceof ManagedLedgerException.ManagedLedgerFencedException) {
                isReadable = false;
            }
            log.error("MLPendingAckStore of topic [{}] stat reply fail!", managedLedger.getName(), exception);
            outstandingReadsRequests.decrementAndGet();
        }

    }

    public CompletableFuture<ManagedLedger> getManagedLedger() {
        return CompletableFuture.completedFuture(this.managedLedger);
    }

    public static String getTransactionPendingAckStoreSuffix(String originTopicName, String subName) {
        return TopicName.get(originTopicName) + "-" + subName + PENDING_ACK_STORE_SUFFIX;
    }

    public static String getTransactionPendingAckStoreCursorName() {
        return PENDING_ACK_STORE_CURSOR_NAME;
    }

    private static final Logger log = LoggerFactory.getLogger(MLPendingAckStore.class);
}