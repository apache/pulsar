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
package org.apache.pulsar.broker.transaction.buffer.impl;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.broker.transaction.buffer.metadata.TransactionBufferSnapshot;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.RecoverTimeRecord;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;

/**
 * Transaction buffer based on normal persistent topic.
 */
@Slf4j
public class TopicTransactionBuffer extends TopicTransactionBufferState implements TransactionBuffer, TimerTask {

    private final PersistentTopic topic;

    private volatile PositionImpl maxReadPosition;

    /**
     * Ongoing transaction, map for remove txn stable position, linked for find max read position.
     */
    private final LinkedMap<TxnID, PositionImpl> ongoingTxns = new LinkedMap<>();

    // when add abort or change max read position, the count will +1. Take snapshot will set 0 into it.
    private final AtomicLong changeMaxReadPositionAndAddAbortTimes = new AtomicLong();

    private final LongAdder txnCommittedCounter = new LongAdder();

    private final LongAdder txnAbortedCounter = new LongAdder();

    private final Timer timer;

    private final int takeSnapshotIntervalNumber;

    private final int takeSnapshotIntervalTime;

    private final CompletableFuture<Void> transactionBufferFuture = new CompletableFuture<>();

    /**
     * The map is used to store the lowWaterMarks which key is TC ID and value is lowWaterMark of the TC.
     */
    private final ConcurrentHashMap<Long, Long> lowWaterMarks = new ConcurrentHashMap<>();

    public final RecoverTimeRecord recoverTime = new RecoverTimeRecord();

    private final Semaphore handleLowWaterMark = new Semaphore(1);

    private final AbortedTxnProcessor snapshotAbortedTxnProcessor;

    public TopicTransactionBuffer(PersistentTopic topic) {
        super(State.None);
        this.topic = topic;
        this.timer = topic.getBrokerService().getPulsar().getTransactionTimer();
        this.takeSnapshotIntervalNumber = topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotMaxTransactionCount();
        this.takeSnapshotIntervalTime = topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotMinTimeInMillis();
        this.maxReadPosition = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
        if (topic.getBrokerService().getPulsar().getConfiguration().isTransactionBufferSegmentedSnapshotEnabled()) {
            snapshotAbortedTxnProcessor = new SnapshotSegmentAbortedTxnProcessorImpl(topic);
        } else {
            snapshotAbortedTxnProcessor = new SingleSnapshotAbortedTxnProcessorImpl(topic);
        }
        this.recover();
    }

    private void recover() {
        recoverTime.setRecoverStartTime(System.currentTimeMillis());
        this.topic.getBrokerService().getPulsar().getTransactionExecutorProvider().getExecutor(this)
                .execute(new TopicTransactionBufferRecover(new TopicTransactionBufferRecoverCallBack() {
                    @Override
                    public void recoverComplete() {
                        synchronized (TopicTransactionBuffer.this) {
                            if (ongoingTxns.isEmpty()) {
                                maxReadPosition = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
                            }
                            if (!changeToReadyState()) {
                                log.error("[{}]Transaction buffer recover fail, current state: {}",
                                        topic.getName(), getState());
                                transactionBufferFuture.completeExceptionally
                                        (new BrokerServiceException.ServiceUnitNotReadyException(
                                                "Transaction buffer recover failed to change the status to Ready,"
                                                        + "current state is: " + getState()));
                            } else {
                                timer.newTimeout(TopicTransactionBuffer.this,
                                        takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
                                transactionBufferFuture.complete(null);
                                recoverTime.setRecoverEndTime(System.currentTimeMillis());
                            }
                        }
                    }

                    @Override
                    public void noNeedToRecover() {
                        synchronized (TopicTransactionBuffer.this) {
                            maxReadPosition = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
                            if (!changeToNoSnapshotState()) {
                                log.error("[{}]Transaction buffer recover fail", topic.getName());
                            } else {
                                transactionBufferFuture.complete(null);
                                recoverTime.setRecoverEndTime(System.currentTimeMillis());
                            }
                        }
                    }
                    @Override
                    public void handleTxnEntry(Entry entry) {
                        ByteBuf metadataAndPayload = entry.getDataBuffer();

                        MessageMetadata msgMetadata = Commands.peekMessageMetadata(metadataAndPayload,
                                TopicTransactionBufferRecover.SUBSCRIPTION_NAME, -1);
                        if (msgMetadata != null && msgMetadata.hasTxnidMostBits() && msgMetadata.hasTxnidLeastBits()) {
                            TxnID txnID = new TxnID(msgMetadata.getTxnidMostBits(), msgMetadata.getTxnidLeastBits());
                            PositionImpl position = PositionImpl.get(entry.getLedgerId(), entry.getEntryId());
                            if (Markers.isTxnMarker(msgMetadata)) {
                                if (Markers.isTxnAbortMarker(msgMetadata)) {
                                    snapshotAbortedTxnProcessor.putAbortedTxnAndPosition(txnID, position);
                                }
                                updateMaxReadPosition(txnID);
                            } else {
                                handleTransactionMessage(txnID, position);
                            }
                        }
                    }

                    @Override
                    public void recoverExceptionally(Throwable e) {

                        log.warn("Closing topic {} due to read transaction buffer snapshot while recovering the "
                                + "transaction buffer throw exception", topic.getName(), e);
                        // when create reader or writer fail throw PulsarClientException,
                        // should close this topic and then reinit this topic
                        if (e instanceof PulsarClientException) {
                            // if transaction buffer recover fail throw PulsarClientException,
                            // we need to change the PulsarClientException to ServiceUnitNotReadyException,
                            // the tc do op will retry
                            transactionBufferFuture.completeExceptionally
                                    (new BrokerServiceException.ServiceUnitNotReadyException(e.getMessage(), e));
                        } else {
                            transactionBufferFuture.completeExceptionally(e);
                        }
                        recoverTime.setRecoverEndTime(System.currentTimeMillis());
                        topic.close(true);
                    }
                }, this.topic, this, snapshotAbortedTxnProcessor));
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> checkIfTBRecoverCompletely(boolean isTxnEnabled) {
        if (!isTxnEnabled) {
            return CompletableFuture.completedFuture(null);
        } else {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            transactionBufferFuture.thenRun(() -> {
                if (checkIfNoSnapshot()) {
                    snapshotAbortedTxnProcessor.takeAbortedTxnsSnapshot(maxReadPosition).thenRun(() -> {
                        if (changeToReadyStateFromNoSnapshot()) {
                            timer.newTimeout(TopicTransactionBuffer.this,
                                    takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
                        }
                        completableFuture.complete(null);
                    }).exceptionally(exception -> {
                        log.error("Topic {} failed to take snapshot", this.topic.getName());
                        completableFuture.completeExceptionally(exception);
                        return null;
                    });
                } else {
                    completableFuture.complete(null);
                }
            }).exceptionally(exception -> {
                log.error("Topic {}: TransactionBuffer recover failed", this.topic.getName(), exception.getCause());
                completableFuture.completeExceptionally(exception.getCause());
                return null;
            });
            return completableFuture;
        }
    }

    @Override
    public long getOngoingTxnCount() {
        return this.ongoingTxns.size();
    }

    @Override
    public long getAbortedTxnCount() {
        return this.txnAbortedCounter.sum();
    }

    @Override
    public long getCommittedTxnCount() {
        return this.txnCommittedCounter.sum();
    }

    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        CompletableFuture<Position> completableFuture = new CompletableFuture<>();
        Long lowWaterMark = lowWaterMarks.get(txnId.getMostSigBits());
        if (lowWaterMark != null && lowWaterMark >= txnId.getLeastSigBits()) {
            completableFuture.completeExceptionally(new BrokerServiceException
                    .NotAllowedException("Transaction [" + txnId + "] has been ended. "
                    + "Please use a new transaction to send message."));
            return completableFuture;
        }
        topic.getManagedLedger().asyncAddEntry(buffer, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                synchronized (TopicTransactionBuffer.this) {
                    handleTransactionMessage(txnId, position);
                }
                completableFuture.complete(position);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Failed to append buffer to txn {}", txnId, exception);
                completableFuture.completeExceptionally(exception);
            }
        }, null);
        return completableFuture;
    }

    private void handleTransactionMessage(TxnID txnId, Position position) {
        if (!ongoingTxns.containsKey(txnId) && !this.snapshotAbortedTxnProcessor
                .checkAbortedTransaction(txnId)) {
            ongoingTxns.put(txnId, (PositionImpl) position);
            PositionImpl firstPosition = ongoingTxns.get(ongoingTxns.firstKey());
            //max read position is less than first ongoing transaction message position, so entryId -1
            maxReadPosition = PositionImpl.get(firstPosition.getLedgerId(), firstPosition.getEntryId() - 1);
        }
    }

    @Override
    public CompletableFuture<TransactionBufferReader> openTransactionBufferReader(TxnID txnID, long startSequenceId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, long lowWaterMark) {
        if (log.isDebugEnabled()) {
            log.debug("Transaction {} commit on topic {}.", txnID.toString(), topic.getName());
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        //Wait TB recover completely.
        transactionBufferFuture.thenRun(() -> {
            ByteBuf commitMarker = Markers.newTxnCommitMarker(-1L, txnID.getMostSigBits(),
                    txnID.getLeastSigBits());
            try {
                topic.getManagedLedger().asyncAddEntry(commitMarker, new AsyncCallbacks.AddEntryCallback() {
                    @Override
                    public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                        synchronized (TopicTransactionBuffer.this) {
                            updateMaxReadPosition(txnID);
                            handleLowWaterMark(txnID, lowWaterMark);
                            snapshotAbortedTxnProcessor.trimExpiredAbortedTxns();
                            takeSnapshotByChangeTimes();
                        }
                        txnCommittedCounter.increment();
                        completableFuture.complete(null);
                    }

                    @Override
                    public void addFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("Failed to commit for txn {}", txnID, exception);
                        checkAppendMarkerException(exception);
                        completableFuture.completeExceptionally(new PersistenceException(exception));
                    }
                }, null);
            } finally {
                commitMarker.release();
            }
        }).exceptionally(exception -> {
            log.error("Transaction {} commit on topic {}.", txnID.toString(), topic.getName(), exception.getCause());
            completableFuture.completeExceptionally(exception.getCause());
            return null;
        });
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID, long lowWaterMark) {
        if (log.isDebugEnabled()) {
            log.debug("Transaction {} abort on topic {}.", txnID.toString(), topic.getName());
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        //Wait TB recover completely.
        transactionBufferFuture.thenRun(() -> {
            //no message sent, need not to add abort mark by txn timeout.
            if (!checkIfReady()) {
                completableFuture.complete(null);
                return;
            }
            ByteBuf abortMarker = Markers.newTxnAbortMarker(-1L, txnID.getMostSigBits(), txnID.getLeastSigBits());
            try {
                topic.getManagedLedger().asyncAddEntry(abortMarker, new AsyncCallbacks.AddEntryCallback() {
                    @Override
                    public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                        synchronized (TopicTransactionBuffer.this) {
                            snapshotAbortedTxnProcessor.putAbortedTxnAndPosition(txnID, (PositionImpl) position);
                            updateMaxReadPosition(txnID);
                            snapshotAbortedTxnProcessor.trimExpiredAbortedTxns();
                            takeSnapshotByChangeTimes();
                        }
                        txnAbortedCounter.increment();
                        completableFuture.complete(null);
                        handleLowWaterMark(txnID, lowWaterMark);
                    }

                    @Override
                    public void addFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("Failed to abort for txn {}", txnID, exception);
                        checkAppendMarkerException(exception);
                        completableFuture.completeExceptionally(new PersistenceException(exception));
                    }
                }, null);
            } finally {
                abortMarker.release();
            }
        }).exceptionally(exception -> {
            log.error("Transaction {} abort on topic {}.", txnID.toString(), topic.getName(), exception.getCause());
            completableFuture.completeExceptionally(exception.getCause());
            return null;
        });
        return completableFuture;
    }

    private void checkAppendMarkerException(ManagedLedgerException exception) {
        if (exception instanceof ManagedLedgerException.ManagedLedgerAlreadyClosedException) {
            topic.getManagedLedger().readyToCreateNewLedger();
        }
    }

    private void handleLowWaterMark(TxnID txnID, long lowWaterMark) {
        lowWaterMarks.compute(txnID.getMostSigBits(), (tcId, oldLowWaterMark) -> {
            if (oldLowWaterMark == null || oldLowWaterMark < lowWaterMark) {
                return lowWaterMark;
            } else {
                return oldLowWaterMark;
            }
        });
        if (handleLowWaterMark.tryAcquire()) {
            if (!ongoingTxns.isEmpty()) {
                TxnID firstTxn = ongoingTxns.firstKey();
                long tCId = firstTxn.getMostSigBits();
                Long lowWaterMarkOfFirstTxnId = lowWaterMarks.get(tCId);
                if (lowWaterMarkOfFirstTxnId != null && firstTxn.getLeastSigBits() <= lowWaterMarkOfFirstTxnId) {
                    abortTxn(firstTxn, lowWaterMarkOfFirstTxnId)
                            .thenRun(() -> {
                                log.warn("Successes to abort low water mark for txn [{}], topic [{}],"
                                        + " lowWaterMark [{}]", firstTxn, topic.getName(), lowWaterMarkOfFirstTxnId);
                                handleLowWaterMark.release();
                            })
                            .exceptionally(ex -> {
                                log.warn("Failed to abort low water mark for txn {}, topic [{}], "
                                        + "lowWaterMark [{}], ", firstTxn, topic.getName(), lowWaterMarkOfFirstTxnId,
                                        ex);
                                handleLowWaterMark.release();
                                return null;
                            });
                    return;
                }
            }
            handleLowWaterMark.release();
        }
    }

    private void takeSnapshotByChangeTimes() {
        if (changeMaxReadPositionAndAddAbortTimes.get() >= takeSnapshotIntervalNumber) {
            this.changeMaxReadPositionAndAddAbortTimes.set(0);
            this.snapshotAbortedTxnProcessor.takeAbortedTxnsSnapshot(this.maxReadPosition);
        }
    }

    private void takeSnapshotByTimeout() {
        if (changeMaxReadPositionAndAddAbortTimes.get() > 0) {
            this.changeMaxReadPositionAndAddAbortTimes.set(0);
            this.snapshotAbortedTxnProcessor.takeAbortedTxnsSnapshot(this.maxReadPosition);
        }
        this.timer.newTimeout(TopicTransactionBuffer.this,
                takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
    }

    void updateMaxReadPosition(TxnID txnID) {
        PositionImpl preMaxReadPosition = this.maxReadPosition;
        ongoingTxns.remove(txnID);
        if (!ongoingTxns.isEmpty()) {
            PositionImpl position = ongoingTxns.get(ongoingTxns.firstKey());
            //max read position is less than first ongoing transaction message position, so entryId -1
            maxReadPosition = PositionImpl.get(position.getLedgerId(), position.getEntryId() - 1);
        } else {
            maxReadPosition = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
        }
        if (preMaxReadPosition.compareTo(this.maxReadPosition) != 0) {
            this.changeMaxReadPositionAndAddAbortTimes.getAndIncrement();
        }
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        return null;
    }

    @Override
    public CompletableFuture<Void> clearSnapshot() {
        return snapshotAbortedTxnProcessor.clearAbortedTxnSnapshot();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        changeToCloseState();
        return this.snapshotAbortedTxnProcessor.closeAsync();
    }

    @Override
    public boolean isTxnAborted(TxnID txnID, PositionImpl readPosition) {
        return snapshotAbortedTxnProcessor.checkAbortedTransaction(txnID);
    }

    @Override
    public void syncMaxReadPositionForNormalPublish(PositionImpl position) {
        // when ongoing transaction is empty, proved that lastAddConfirm is can read max position, because callback
        // thread is the same tread, in this time the lastAddConfirm don't content transaction message.
        synchronized (TopicTransactionBuffer.this) {
            if (checkIfNoSnapshot()) {
                this.maxReadPosition = position;
            } else if (checkIfReady()) {
                if (ongoingTxns.isEmpty()) {
                    maxReadPosition = position;
                    changeMaxReadPositionAndAddAbortTimes.incrementAndGet();
                }
            }
        }
    }

    @Override
    public PositionImpl getMaxReadPosition() {
        if (checkIfReady() || checkIfNoSnapshot()) {
            return this.maxReadPosition;
        } else {
            return PositionImpl.EARLIEST;
        }
    }

    @Override
    public TransactionInBufferStats getTransactionInBufferStats(TxnID txnID) {
        TransactionInBufferStats transactionInBufferStats = new TransactionInBufferStats();
        transactionInBufferStats.aborted = isTxnAborted(txnID, null);
        if (ongoingTxns.containsKey(txnID)) {
            transactionInBufferStats.startPosition = ongoingTxns.get(txnID).toString();
        }
        return transactionInBufferStats;
    }

    @Override
    public TransactionBufferStats getStats(boolean lowWaterMarks) {
        TransactionBufferStats transactionBufferStats = new TransactionBufferStats();
        transactionBufferStats.lastSnapshotTimestamps = this.snapshotAbortedTxnProcessor.getLastSnapshotTimestamps();
        transactionBufferStats.state = this.getState().name();
        transactionBufferStats.maxReadPosition = this.maxReadPosition.toString();
        if (lowWaterMarks) {
            transactionBufferStats.lowWaterMarks = this.lowWaterMarks;
        }
        transactionBufferStats.ongoingTxnSize = ongoingTxns.size();

        transactionBufferStats.recoverStartTime = recoverTime.getRecoverStartTime();
        transactionBufferStats.recoverEndTime = recoverTime.getRecoverEndTime();
        return transactionBufferStats;
    }

    @Override
    public void run(Timeout timeout) {
        if (checkIfReady()) {
            synchronized (TopicTransactionBuffer.this) {
                takeSnapshotByTimeout();
            }
        }
    }

    // we store the maxReadPosition from snapshot then open the non-durable cursor by this topic's manageLedger.
    // the non-durable cursor will read to lastConfirmedEntry.
    @VisibleForTesting
    public static class TopicTransactionBufferRecover implements Runnable {

        private final PersistentTopic topic;

        private final TopicTransactionBufferRecoverCallBack callBack;

        private Position startReadCursorPosition = PositionImpl.EARLIEST;

        private final SpscArrayQueue<Entry> entryQueue;

        private final AtomicLong exceptionNumber = new AtomicLong();

        public static final String SUBSCRIPTION_NAME = "transaction-buffer-sub";

        private final TopicTransactionBuffer topicTransactionBuffer;

        private final AbortedTxnProcessor abortedTxnProcessor;

        private TopicTransactionBufferRecover(TopicTransactionBufferRecoverCallBack callBack, PersistentTopic topic,
                                              TopicTransactionBuffer transactionBuffer,
                                              AbortedTxnProcessor abortedTxnProcessor) {
            this.topic = topic;
            this.callBack = callBack;
            this.entryQueue = new SpscArrayQueue<>(2000);
            this.topicTransactionBuffer = transactionBuffer;
            this.abortedTxnProcessor = abortedTxnProcessor;
        }

        @SneakyThrows
        @Override
        public void run() {
            if (!this.topicTransactionBuffer.changeToInitializingState()) {
                log.warn("TransactionBuffer {} of topic {} can not change state to Initializing",
                        this, topic.getName());
                return;
            }
            abortedTxnProcessor.recoverFromSnapshot().thenAcceptAsync(startReadCursorPosition -> {
                //Transaction is not use for this topic, so just make maxReadPosition as LAC.
                if (startReadCursorPosition == null) {
                    callBack.noNeedToRecover();
                    return;
                } else {
                    this.startReadCursorPosition = startReadCursorPosition;
                }
                ManagedCursor managedCursor;
                try {
                    managedCursor = topic.getManagedLedger()
                            .newNonDurableCursor(this.startReadCursorPosition, SUBSCRIPTION_NAME);
                } catch (ManagedLedgerException e) {
                    callBack.recoverExceptionally(e);
                    log.error("[{}]Transaction buffer recover fail when open cursor!", topic.getName(), e);
                    return;
                }
                PositionImpl lastConfirmedEntry =
                        (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
                PositionImpl currentLoadPosition = (PositionImpl) this.startReadCursorPosition;
                FillEntryQueueCallback fillEntryQueueCallback = new FillEntryQueueCallback(entryQueue,
                        managedCursor, TopicTransactionBufferRecover.this);
                if (lastConfirmedEntry.getEntryId() != -1) {
                    while (lastConfirmedEntry.compareTo(currentLoadPosition) > 0
                            && fillEntryQueueCallback.fillQueue()) {
                        Entry entry = entryQueue.poll();
                        if (entry != null) {
                            try {
                                currentLoadPosition = PositionImpl.get(entry.getLedgerId(),
                                        entry.getEntryId());
                                callBack.handleTxnEntry(entry);
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
                }

                closeCursor(SUBSCRIPTION_NAME);
                callBack.recoverComplete();
            }, topic.getBrokerService().getPulsar().getTransactionExecutorProvider()
                    .getExecutor(this)).exceptionally(e -> {
                callBack.recoverExceptionally(e.getCause());
                log.error("[{}]Transaction buffer failed to recover snapshot!", topic.getName(), e);
                return null;
            });
        }

        private void closeCursor(String subscriptionName) {
            topic.getManagedLedger().asyncDeleteCursor(Codec.encode(subscriptionName),
                    new AsyncCallbacks.DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    log.info("[{}]Transaction buffer snapshot recover cursor close complete.", topic.getName());
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}]Transaction buffer snapshot recover cursor close fail.", topic.getName());
                }

            }, null);
        }

        private void callBackException(ManagedLedgerException e) {
            log.error("Transaction buffer recover fail when recover transaction entry!", e);
            this.exceptionNumber.getAndIncrement();
        }

        private void closeReader(SystemTopicClient.Reader<TransactionBufferSnapshot> reader) {
            reader.closeAsync().exceptionally(e -> {
                log.error("[{}]Transaction buffer reader close error!", topic.getName(), e);
                return null;
            });
        }
    }

    static class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);

        private final SpscArrayQueue<Entry> entryQueue;

        private final ManagedCursor cursor;

        private final TopicTransactionBufferRecover recover;

        private volatile boolean isReadable = true;

        private static final int NUMBER_OF_PER_READ_ENTRY = 100;

        private FillEntryQueueCallback(SpscArrayQueue<Entry> entryQueue, ManagedCursor cursor,
                                       TopicTransactionBufferRecover recover) {
            this.entryQueue = entryQueue;
            this.cursor = cursor;
            this.recover = recover;
        }
        boolean fillQueue() {
            if (entryQueue.size() + NUMBER_OF_PER_READ_ENTRY < entryQueue.capacity()
                    && outstandingReadsRequests.get() == 0) {
                if (cursor.hasMoreEntries()) {
                    outstandingReadsRequests.incrementAndGet();
                    cursor.asyncReadEntries(NUMBER_OF_PER_READ_ENTRY,
                            this, System.nanoTime(), PositionImpl.LATEST);
                } else {
                    if (entryQueue.size() == 0) {
                        isReadable = false;
                    }
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
            if (recover.topic.getManagedLedger().getConfig().isAutoSkipNonRecoverableData()
                    && exception instanceof ManagedLedgerException.NonRecoverableLedgerException
                    || exception instanceof ManagedLedgerException.ManagedLedgerFencedException
                    || exception instanceof ManagedLedgerException.CursorAlreadyClosedException) {
                isReadable = false;
            } else {
                outstandingReadsRequests.decrementAndGet();
            }
            recover.callBackException(exception);
        }
    }
}
