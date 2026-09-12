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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import static org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER;
import static org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER_LEN;
import static org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriter.BATCHED_ENTRY_DATA_PREFIX_VERSION_LEN;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import io.github.merlimat.slog.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.util.Timer;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.AckSetStateUtil;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckReplyCallBack;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.proto.BatchedPendingAckMetadataEntry;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckMetadata;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckMetadataEntry;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckOp;
import org.apache.pulsar.broker.transaction.util.LogIndexLagBackoff;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.transaction.coordinator.impl.TxnBatchedPositionImpl;
import org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriter;
import org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriterConfig;
import org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriterMetricsStats;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;

/**
 * The implement of the pending ack store by manageLedger.
 */
public class MLPendingAckStore implements PendingAckStore {

    private static final Logger LOG = Logger.get(MLPendingAckStore.class);
    private final Logger log;

    private final ManagedLedger managedLedger;

    private final ManagedCursor cursor;

    private final SpscArrayQueue<Entry> entryQueue;

    //this is for replay
    private final Position lastConfirmedEntry;

    private Position currentLoadPosition;

    private final AtomicLong currentIndexLag = new AtomicLong(0);
    private volatile long maxIndexLag;

    protected Position maxAckPosition = PositionFactory.EARLIEST;
    private final LogIndexLagBackoff logIndexBackoff;

    /**
     * If the Batch feature is enabled by {@link #bufferedWriter}, {@link #handleMetadataEntry(Position, List)} is
     * executed after all data in the batch is written, instead of
     * {@link #handleMetadataEntry(Position, PendingAckMetadataEntry)} after each data is written. This is because
     * method {@link #clearUselessLogData()} deletes the data in the unit of Entry.
     */
    private final ArrayList<PendingAckMetadataEntry> batchedPendingAckLogsWaitingForHandle;

    /**
     * The map is for pending ack store clear useless data.
     * <p>
     *     key:the largest ack position of origin topic, corresponds to the value position.
     * <p>
     *     value:the position persistent by pendingAck log.
     * <p>
     *     It will judge the position with the max sub cursor position (key) whether smaller than the subCursor mark
     *     delete position.
     *     <p>
     *         If the max position (key) is smaller than the subCursor mark delete position,
     *         the log cursor will mark delete the position before log position (value).
     */
    final ConcurrentSkipListMap<Position, Position> pendingAckLogIndex;

    private final ManagedCursor subManagedCursor;

    private TxnLogBufferedWriter<PendingAckMetadataEntry> bufferedWriter;

    @SuppressWarnings("unchecked")
    public MLPendingAckStore(ManagedLedger managedLedger, ManagedCursor cursor,
                             ManagedCursor subManagedCursor, long transactionPendingAckLogIndexMinLag,
                             TxnLogBufferedWriterConfig bufferedWriterConfig,
                             Timer timer, TxnLogBufferedWriterMetricsStats bufferedWriterMetrics, Executor executor) {
        this.managedLedger = managedLedger;
        this.cursor = cursor;
        this.log = LOG.with()
                .attr("managedLedger", managedLedger.getName())
                .attr("cursor", cursor.getName())
                .build();
        this.currentLoadPosition = this.cursor.getMarkDeletedPosition();
        this.entryQueue = new SpscArrayQueue<>(2000);
        this.lastConfirmedEntry = managedLedger.getLastConfirmedEntry();
        this.pendingAckLogIndex = new ConcurrentSkipListMap<>();
        this.subManagedCursor = subManagedCursor;
        this.logIndexBackoff = new LogIndexLagBackoff(transactionPendingAckLogIndexMinLag, Long.MAX_VALUE, 1);
        this.maxIndexLag = logIndexBackoff.next(0);
        this.bufferedWriter = new TxnLogBufferedWriter(managedLedger, executor,
                timer, PendingAckLogSerializer.INSTANCE,
                bufferedWriterConfig.getBatchedWriteMaxRecords(), bufferedWriterConfig.getBatchedWriteMaxSize(),
                bufferedWriterConfig.getBatchedWriteMaxDelayInMillis(), bufferedWriterConfig.isBatchEnabled(),
                bufferedWriterMetrics);
        this.batchedPendingAckLogsWaitingForHandle = new ArrayList<>();
    }

    @Override
    public void replayAsync(PendingAckHandleImpl pendingAckHandle, ExecutorService transactionReplayExecutor) {
        transactionReplayExecutor
                .execute(new PendingAckReplay(new MLPendingAckReplyCallBack(pendingAckHandle)));
    }

    //TODO can control the number of entry to read
    private void readAsync(int numberOfEntriesToRead,
                           AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        cursor.asyncReadEntries(numberOfEntriesToRead, readEntriesCallback, System.nanoTime(), PositionFactory.LATEST);
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
                        log.debug("MLPendingAckStore closed successfully");
                        bufferedWriter.close();
                        completableFuture.complete(null);
                    }

                    @Override
                    public void closeFailed(ManagedLedgerException exception, Object ctx) {
                        log.error()
                                .exceptionMessage(exception)
                                .log("MLPendingAckStore close failed");
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
                                                       List<MutablePair<Position, Integer>> positions) {
        PendingAckMetadataEntry pendingAckMetadataEntry = new PendingAckMetadataEntry();
        pendingAckMetadataEntry.setPendingAckOp(PendingAckOp.ACK);
        pendingAckMetadataEntry.setAckType(AckType.Individual);
        List<PendingAckMetadata> pendingAckMetadataList = new ArrayList<>();
        positions.forEach(positionIntegerMutablePair -> {
            PendingAckMetadata pendingAckMetadata = new PendingAckMetadata();
            Position position = positionIntegerMutablePair.getLeft();
            int batchSize = positionIntegerMutablePair.getRight();
            long[] positionAckSet = AckSetStateUtil.getAckSetArrayOrNull(position);
            if (positionAckSet != null) {
                for (long l : positionAckSet) {
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
    public CompletableFuture<Void> appendCumulativeAck(TxnID txnID, Position position) {
        PendingAckMetadataEntry pendingAckMetadataEntry = new PendingAckMetadataEntry();
        pendingAckMetadataEntry.setPendingAckOp(PendingAckOp.ACK);
        pendingAckMetadataEntry.setAckType(AckType.Cumulative);
        PendingAckMetadata pendingAckMetadata = new PendingAckMetadata();
        long[] positionAckSet = AckSetStateUtil.getAckSetArrayOrNull(position);
        if (positionAckSet != null) {
            for (long l : positionAckSet) {
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
        bufferedWriter.asyncAddData(pendingAckMetadataEntry, new TxnLogBufferedWriter.AddDataCallback() {

            @Override
            public void addComplete(Position position, Object ctx) {
                log.debug()
                        .attr("position", position)
                        .attr("txnId", txnID)
                        .attr("operation", pendingAckMetadataEntry.getPendingAckOp())
                        .log("MLPendingAckStore message append success");
                currentIndexLag.incrementAndGet();
                /**
                 * If the Batch feature is enabled by {@link #bufferedWriter},
                 * {@link #handleMetadataEntry(Position, List)} is executed after all data in the batch is written,
                 * instead of {@link #handleMetadataEntry(Position, PendingAckMetadataEntry)} after each data is
                 * written. This is because method {@link #clearUselessLogData()} deletes the data in the unit of Entry.
                 * {@link TxnLogBufferedWriter.AddDataCallback#addComplete} for elements in a batch is executed
                 * simultaneously and in strict order, so when the last element in a batch is complete, the whole
                 * batch is complete.
                 */
                if (position instanceof TxnBatchedPositionImpl batchedPosition){
                    batchedPendingAckLogsWaitingForHandle.add(pendingAckMetadataEntry);
                    if (batchedPosition.getBatchIndex() == batchedPosition.getBatchSize() - 1){
                        handleMetadataEntry(position, batchedPendingAckLogsWaitingForHandle);
                        batchedPendingAckLogsWaitingForHandle.clear();
                    }
                } else {
                    handleMetadataEntry(position, pendingAckMetadataEntry);
                }
                completableFuture.complete(null);
                clearUselessLogData();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                log.error()
                        .exceptionMessage(exception)
                        .attr("operation", pendingAckMetadataEntry.getPendingAckOp())
                        .log("MLPendingAckStore message append failed");

                if (exception instanceof ManagedLedgerException.ManagedLedgerAlreadyClosedException) {
                    managedLedger.readyToCreateNewLedger();
                }
                completableFuture.completeExceptionally(new PersistenceException(exception));
            }
        }, null);
        return completableFuture;
    }

    /**
     * Build the index mapping of Transaction pending ack log (aka t-log) and Topic message log (aka m-log).
     * When m-log has been ack, t-log which holds m-log is no longer useful, this method builder the mapping of them.
     *
     * If a Ledger Entry has many t-log, we only need to care about the record that carries the largest acknowledgement
     * info. Because all Commit/Abort log after this record describes behavior acknowledgement, if the behavior
     * acknowledgement has been handle correct, these Commit/Abort log is no longer useful.
     * @param logPosition The position of batch log Entry.
     * @param logList Pending ack log records in a batch log Entry.
     */
    private void handleMetadataEntry(Position logPosition,
                                     List<PendingAckMetadataEntry> logList) {
        Stream<PendingAckMetadata> pendingAckMetaStream = logList.stream()
                .filter(log -> bothNotAbortAndCommitPredicate.test(log))
                .flatMap(log -> log.getPendingAckMetadatasList().stream());
        handleMetadataEntry(logPosition, pendingAckMetaStream);
    }

    private final Predicate<PendingAckMetadataEntry> bothNotAbortAndCommitPredicate = (pendingAckLog) ->
            pendingAckLog.getPendingAckOp() != PendingAckOp.ABORT
            && pendingAckLog.getPendingAckOp() != PendingAckOp.COMMIT;

    private void handleMetadataEntry(Position logPosition,
                                     PendingAckMetadataEntry pendingAckMetadataEntry) {
        // store the persistent position in to memory
        // store the max position of this entry retain
        if (bothNotAbortAndCommitPredicate.test(pendingAckMetadataEntry)) {
            handleMetadataEntry(logPosition, pendingAckMetadataEntry.getPendingAckMetadatasList().stream());
        }
    }

    private void handleMetadataEntry(Position logPosition, Stream<PendingAckMetadata> pendingAckListStream) {
        // store the persistent position in to memory
        // store the max position of this entry retain
        Optional<PendingAckMetadata> optional = pendingAckListStream
                .max((o1, o2) -> ComparisonChain.start().compare(o1.getLedgerId(),
                        o2.getLedgerId()).compare(o1.getEntryId(), o2.getEntryId()).result());
        optional.ifPresent(pendingAckMetadata -> {
            Position nowPosition = PositionFactory.create(pendingAckMetadata.getLedgerId(),
                    pendingAckMetadata.getEntryId());
            if (nowPosition.compareTo(maxAckPosition) > 0) {
                maxAckPosition = nowPosition;
            }
            if (currentIndexLag.get() >= maxIndexLag) {
                pendingAckLogIndex.compute(maxAckPosition,
                        (thisPosition, otherPosition) -> logPosition);
                maxIndexLag = logIndexBackoff.next(pendingAckLogIndex.size());
                currentIndexLag.set(0);
            }
        });
    }

    @VisibleForTesting
    void clearUselessLogData() {
        if (!pendingAckLogIndex.isEmpty()) {
            Position deletePosition = null;
            while (!pendingAckLogIndex.isEmpty()
                    && pendingAckLogIndex.firstKey() != null
                    && subManagedCursor.getPersistentMarkDeletedPosition() != null
                    && pendingAckLogIndex.firstEntry().getKey()
                    .compareTo(subManagedCursor.getPersistentMarkDeletedPosition()) <= 0) {
                deletePosition = pendingAckLogIndex.remove(pendingAckLogIndex.firstKey());
            }

            if (deletePosition != null) {
                maxIndexLag = logIndexBackoff.next(pendingAckLogIndex.size());
                Position finalDeletePosition = deletePosition;
                cursor.asyncMarkDelete(deletePosition,
                        new AsyncCallbacks.MarkDeleteCallback() {
                            @Override
                            public void markDeleteComplete(Object ctx) {
                                log.debug()
                                        .attr("position", finalDeletePosition)
                                        .log("Transaction pending ack store mark delete position success");
                            }

                            @Override
                            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                                log.error()
                                        .attr("position", finalDeletePosition)
                                        .exception(exception)
                                        .log("Transaction pending ack store mark delete position fail");
                            }
                        }, null);
            }
        }
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
                    failReplay(new ManagedLedgerException
                            .CursorAlreadyClosedException("MLPendingAckStore cursor have been closed."));
                    log.warn("MLPendingAckStore cursor have been closed, close replay thread");
                    return;
                }
                while (lastConfirmedEntry.compareTo(currentLoadPosition) > 0 && fillEntryQueueCallback.fillQueue()) {
                    Entry entry = entryQueue.poll();
                    if (entry != null) {
                        currentLoadPosition = PositionFactory.create(entry.getLedgerId(), entry.getEntryId());
                        List<PendingAckMetadataEntry> logs = deserializeEntry(entry);
                        if (logs.isEmpty()){
                            continue;
                        } else if (logs.size() == 1){
                            currentIndexLag.incrementAndGet();
                            PendingAckMetadataEntry log = logs.get(0);
                            handleMetadataEntry(PositionFactory.create(entry.getLedgerId(), entry.getEntryId()), log);
                            pendingAckReplyCallBack.handleMetadataEntry(log);
                        } else {
                            int batchSize = logs.size();
                            for (int batchIndex = 0; batchIndex < batchSize; batchIndex++){
                                PendingAckMetadataEntry log = logs.get(batchIndex);
                                pendingAckReplyCallBack.handleMetadataEntry(log);
                            }
                            currentIndexLag.addAndGet(batchSize);
                            handleMetadataEntry(PositionFactory.create(entry.getLedgerId(), entry.getEntryId()), logs);
                        }
                        entry.release();
                        clearUselessLogData();
                    } else {
                        // Covers a read that was issued but whose callback never arrives: no failure is
                        // ever delivered, so readEntriesFailed cannot notice the close. A cursor closed
                        // between two reads takes a different path -- that read fails synchronously in
                        // ManagedCursorImpl#asyncReadEntriesWithSkip, which clears isReadable and ends
                        // the loop through the existing read failure handling.
                        if (cursor.isClosed()) {
                            failReplay(new ManagedLedgerException
                                    .CursorAlreadyClosedException("MLPendingAckStore cursor was closed "
                                    + "while replaying."));
                            log.warn("MLPendingAckStore cursor was closed while replaying, close replay thread");
                            return;
                        }
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            // Restore the interrupt flag and stop. The replay is incomplete, so it must not
                            // be reported as successful: replayFailed() lets PendingAckHandleImpl decide
                            // whether to retry instead of leaving the handle Ready with partial state.
                            Thread.currentThread().interrupt();
                            failReplay(e);
                            log.warn()
                                    .exception(e)
                                    .log("Transaction pending ack replay thread was interrupted");
                            return;
                        }
                    }
                }
            } catch (Exception e) {
                failReplay(e);
                log.error().exception(e).log("Pending ack recover fail");
                return;
            }
            stopReplay();
            // Written by readEntriesFailed before the volatile isReadable write that made fillQueue()
            // return false, so reading it here is safely ordered.
            ManagedLedgerException attemptFailure = fillEntryQueueCallback.replayAttemptFailure();
            if (attemptFailure != null && lastConfirmedEntry.compareTo(currentLoadPosition) > 0) {
                // A read failed before the replay reached the last confirmed entry, so the replayed
                // pending ack state is incomplete: report the attempt as failed and let
                // PendingAckHandleImpl#exceptionHandleFuture choose between a backoff-paced retry and
                // failing the subscription fast. If the loop had instead already caught up, the state
                // is complete, and a failure of a still-in-flight read beyond it must not fail the
                // attempt.
                //
                // Reads run ahead of processing: entries this attempt read but never processed have
                // already advanced the shared cursor's read position, and stopReplay() has just
                // released them. The cursor is cached by the managed ledger, so the next attempt would
                // resume at that advanced position and silently skip them, then report the incomplete
                // replay as successful. Rewinding puts the read position back to the first entry after
                // the mark-delete position, which is exactly where the next attempt's
                // currentLoadPosition starts. Re-reading entries this attempt already applied is safe:
                // the replay handlers deduplicate against the state they have already built.
                // No read is in flight here -- the recorded failure was the only outstanding one, and
                // fillQueue() has issued none since -- so nothing can move the read position after this.
                cursor.rewind();
                failReplay(attemptFailure);
                return;
            }
            pendingAckReplyCallBack.replayComplete();
        }

        /**
         * Ends the replay. Entries that were read but never processed are released, and any read that is
         * still in flight will release its entries itself instead of queueing them, so nothing is left
         * holding a buffer once the replay thread is gone. Called on every exit from {@link #run()}.
         */
        private void stopReplay() {
            for (Entry entry : fillEntryQueueCallback.stopAndDrain()) {
                entry.release();
            }
        }

        /**
         * Ends this replay attempt as failed. The store is abandoned afterwards: PendingAckHandleImpl
         * either retries with a brand new store or fails the handle, and in both cases
         * pendingAckStoreFuture stops referring to this one. Closing the buffered writer here cancels
         * its recurring flush timer, which nothing else would ever reach. That touches no shared state
         * -- the cursor and the managed ledger belong to the managed ledger factory cache and must stay
         * open for the next attempt -- and no append can be in flight, because acknowledgements are
         * queued until the handle is ready.
         */
        private void failReplay(Throwable t) {
            stopReplay();
            bufferedWriter.close();
            pendingAckReplyCallBack.replayFailed(t);
        }
    }

    private List<PendingAckMetadataEntry> deserializeEntry(Entry entry){
        ByteBuf buffer = entry.getDataBuffer();
        // Check whether it is batched Entry.
        buffer.markReaderIndex();
        short magicNum = buffer.readShort();
        buffer.resetReaderIndex();
        if (magicNum == BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER){
            // skip version
            buffer.skipBytes(BATCHED_ENTRY_DATA_PREFIX_MAGIC_NUMBER_LEN + BATCHED_ENTRY_DATA_PREFIX_VERSION_LEN);
            BatchedPendingAckMetadataEntry batchedPendingAckMetadataEntry = new BatchedPendingAckMetadataEntry();
            batchedPendingAckMetadataEntry.parseFrom(buffer, buffer.readableBytes());
            return batchedPendingAckMetadataEntry.getPendingAckLogsList();
        } else {
            PendingAckMetadataEntry pendingAckMetadataEntry = new PendingAckMetadataEntry();
            pendingAckMetadataEntry.parseFrom(buffer, buffer.readableBytes());
            return Collections.singletonList(pendingAckMetadataEntry);
        }
    }

    class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private volatile boolean isReadable = true;
        /**
         * The read failure that ended this replay attempt, or null if no read failed. Written on a
         * managed ledger thread by {@link #readEntriesFailed} before the volatile {@link #isReadable}
         * write that stops the replay loop, and consumed by the replay thread only after it has
         * observed {@code fillQueue() == false}, so the volatile hand-off publishes it. Never set by
         * the failures that complete the replay instead (see {@link #readEntriesFailed}).
         */
        private volatile ManagedLedgerException replayAttemptFailure;
        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);
        private static final int NUMBER_OF_PER_READ_ENTRY = 100;
        /**
         * Guards {@link #stopped} against {@link #readEntriesComplete}. A read can still be in flight when
         * the replay ends, and its completion runs on a managed ledger thread, so handing ownership of the
         * entries over has to be atomic with respect to enqueuing them.
         */
        private final Object stopLock = new Object();
        private boolean stopped;

        boolean fillQueue() {
            // isReadable is deliberately checked AFTER outstandingReadsRequests: readEntriesFailed
            // clears isReadable before its decrement, so a thread that observes the failed read's
            // decrement here is guaranteed to also observe isReadable == false. No read is therefore
            // ever issued after a read failure, which keeps the failure recorded by readEntriesFailed
            // the only one of the attempt. (The reverse order would allow one more doomed read.)
            if (entryQueue.size() + NUMBER_OF_PER_READ_ENTRY < entryQueue.capacity()
                    && outstandingReadsRequests.get() == 0
                    && isReadable) {
                if (cursor.hasMoreEntries()) {
                    outstandingReadsRequests.incrementAndGet();
                    readAsync(NUMBER_OF_PER_READ_ENTRY, this);
                } else if (entryQueue.size() == 0) {
                    // Nothing left to read and everything read so far has been processed: the replay is
                    // done. The loop condition in PendingAckReplay cannot detect this on its own because
                    // it compares lastConfirmedEntry -- a snapshot taken when this store was created --
                    // against currentLoadPosition, which starts at the cursor's mark-delete position,
                    // while whether anything can still be read is decided by the cursor's read position.
                    // Those two can disagree permanently, e.g. when the ledger holding the mark-delete
                    // position was trimmed and the cursor was recovered onto a later ledger. Entries
                    // below the mark-delete position have already been applied, so completing here is
                    // correct. TopicTransactionBuffer's equivalent loop was fixed the same way in
                    // https://github.com/apache/pulsar/pull/13739
                    log.debug()
                            .attr("lastConfirmedEntry", lastConfirmedEntry)
                            .attr("currentLoadPosition", currentLoadPosition)
                            .attr("markDeletePosition", cursor.getMarkDeletedPosition())
                            .attr("readPosition", cursor.getReadPosition())
                            .log("Pending ack replay stopped before reaching the last confirmed entry "
                                    + "because the cursor has nothing left to read");
                    isReadable = false;
                }
            }
            return isReadable;
        }

        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
            List<Entry> entriesToRelease = null;
            synchronized (stopLock) {
                if (stopped) {
                    // The replay already finished, so nothing will ever consume these.
                    entriesToRelease = entries;
                } else {
                    int filled = entryQueue.fill(new MessagePassingQueue.Supplier<Entry>() {
                        private int i = 0;
                        @Override
                        public Entry get() {
                            Entry entry = entries.get(i);
                            i++;
                            return entry;
                        }
                    }, entries.size());
                    if (filled < entries.size()) {
                        entriesToRelease = entries.subList(filled, entries.size());
                    }
                }
            }
            // Released outside the lock: releasing an entry can run a deallocation callback.
            if (entriesToRelease != null) {
                entriesToRelease.forEach(Entry::release);
            }
            outstandingReadsRequests.decrementAndGet();
        }

        /**
         * Stops accepting entries and returns everything still queued, so that the replay thread can
         * release them. Must only be called by the replay thread, which is the queue's single consumer.
         */
        List<Entry> stopAndDrain() {
            List<Entry> drained = new ArrayList<>();
            synchronized (stopLock) {
                stopped = true;
                Entry entry;
                while ((entry = entryQueue.poll()) != null) {
                    drained.add(entry);
                }
            }
            return drained;
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            // These three failures have always completed the replay as if it had reached the end of
            // the log: the unreadable data is skipped by explicit configuration, or the store is being
            // taken over or torn down and the handle must still reach Ready to release its callers.
            // TransactionTest#testEndTPRecoveringWhenManagerLedgerDisReadable pins Ready for each.
            boolean completesReplay = managedLedger.getConfig().isAutoSkipNonRecoverableData()
                    && exception instanceof ManagedLedgerException.NonRecoverableLedgerException
                    || exception instanceof ManagedLedgerException.ManagedLedgerFencedException
                    || exception instanceof ManagedLedgerException.CursorAlreadyClosedException;
            if (!completesReplay) {
                // Any other failure ends this replay attempt as failed instead of re-issuing the same
                // read in a hot loop that monopolises the shared replay thread (issue #26374).
                // PendingAckHandleImpl#exceptionHandleFuture classifies what happens next: transient
                // failures (a plain ManagedLedgerException, e.g. a BookKeeper outage, a read timeout
                // or read throttling) reschedule init() with backoff, which frees the replay thread
                // between attempts; permanent ones (NonRecoverableLedgerException with
                // autoSkipNonRecoverableData disabled) fail the subscription fast instead of retrying
                // a read that can never succeed. The volatile write below must stay before the
                // isReadable write: observing isReadable == false is what publishes it to the replay
                // thread.
                replayAttemptFailure = exception;
            }
            if (completesReplay) {
                // This path never reaches exceptionHandleFuture, and the handle goes on to log its
                // recovery as a success, so without this line a store fenced by a takeover, a cursor
                // closed by teardown, or data skipped because autoSkipNonRecoverableData is enabled
                // would leave no record at any default level -- a cursor closed between two reads
                // fails synchronously in ManagedCursorImpl without even a managed ledger layer log.
                // At most one line per replay attempt: only one read is ever outstanding.
                log.warn()
                        .exception(exception)
                        .attr("currentLoadPosition", currentLoadPosition)
                        .attr("lastConfirmedEntry", lastConfirmedEntry)
                        .log("Pending ack replay read failed; completing the replay anyway");
            } else {
                // The record operators act on is written downstream by
                // PendingAckHandleImpl#exceptionHandleFuture: one WARN per backoff paced retry, or an
                // ERROR when it gives up and fails the subscription.
                log.debug().exception(exception).log("Pending ack replay read failed");
            }
            // Written before the decrement so that fillQueue() can never issue another read after a
            // failure: it re-checks isReadable after observing outstandingReadsRequests == 0.
            isReadable = false;
            outstandingReadsRequests.decrementAndGet();
        }

        /**
         * The read failure that ended this replay attempt, or null if it ended without one. Only
         * meaningful once the replay loop has exited.
         */
        ManagedLedgerException replayAttemptFailure() {
            return replayAttemptFailure;
        }

    }

    public CompletableFuture<ManagedLedger> getManagedLedger() {
        return CompletableFuture.completedFuture(this.managedLedger);
    }

    public static String getTransactionPendingAckStoreSuffix(String originTopicName, String subName) {
        TopicName origin = TopicName.get(originTopicName);
        // URL-encode the subscription name so that any '/' characters it contains do not create
        // extra path segments when the resulting string is parsed as a topic name.  TopicName
        // always decodes the local-name component on parse (via Codec.decode) and re-encodes it
        // on output (via getEncodedLocalName / getPersistenceNamingEncoding), so encoding here
        // produces a valid round-trip with no double-encoding.
        String encodedSubName = Codec.encode(subName);
        // Segment topics ("segment://tenant/ns/topic/<hexStart>-<hexEnd>-<segmentId>") cannot
        // host a derived pending-ack topic in the segment domain — the descriptor parser would
        // reject any name with extra dashes appended. Map to a flat persistent topic in the same
        // namespace, encoding the segment descriptor into the local name.
        if (origin.isSegment()) {
            return String.format("persistent://%s/%s/%s-%s-%s%s",
                    origin.getTenant(), origin.getNamespacePortion(),
                    origin.getLocalName(), origin.getSegmentDescriptor(),
                    encodedSubName, SystemTopicNames.PENDING_ACK_STORE_SUFFIX);
        }
        return origin + "-" + encodedSubName + SystemTopicNames.PENDING_ACK_STORE_SUFFIX;
    }

    public static String getTransactionPendingAckStoreCursorName() {
        return SystemTopicNames.PENDING_ACK_STORE_CURSOR_NAME;
    }
    /**
     * Used only for buffered writer. Since all cmd-writes in buffered writer are in the same thread, so we can use
     * threadLocal variables here. Why need to be on the same thread ?
     * Because {@link BatchedPendingAckMetadataEntry#clear()} will modifies the elements in the attribute
     * {@link BatchedPendingAckMetadataEntry#getPendingAckLogsList()}, this will cause problems by multi-thread write.
     */
    private static final FastThreadLocal<BatchedPendingAckMetadataEntry> batchedMetaThreadLocalForBufferedWriter =
            new FastThreadLocal<>() {
                @Override
                protected BatchedPendingAckMetadataEntry initialValue() throws Exception {
                    return new BatchedPendingAckMetadataEntry();
                }
            };

    private static class PendingAckLogSerializer
            implements TxnLogBufferedWriter.DataSerializer<PendingAckMetadataEntry>{

        private static final PendingAckLogSerializer INSTANCE = new PendingAckLogSerializer();

        @Override
        public int getSerializedSize(PendingAckMetadataEntry data) {
            return data.getSerializedSize();
        }

        @Override
        public ByteBuf serialize(PendingAckMetadataEntry data) {
            int batchSize = data.getSerializedSize();
            ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(batchSize, batchSize);
            data.writeTo(buf);
            return buf;
        }

        @Override
        public ByteBuf serialize(ArrayList<PendingAckMetadataEntry> dataArray) {
            // Since all writes are in the same thread, so we can use threadLocal variables here.
            BatchedPendingAckMetadataEntry batch = batchedMetaThreadLocalForBufferedWriter.get();
            batch.clear();
            batch.addAllPendingAckLogs(dataArray);
            int batchSize = batch.getSerializedSize();
            ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(batchSize, batchSize);
            batch.writeTo(buf);
            return buf;
        }
    }
}