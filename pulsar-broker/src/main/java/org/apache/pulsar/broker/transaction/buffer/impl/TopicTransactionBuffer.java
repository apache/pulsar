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
package org.apache.pulsar.broker.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.broker.transaction.buffer.matadata.AbortTxnMetadata;
import org.apache.pulsar.broker.transaction.buffer.matadata.TransactionBufferSnapshot;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
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

    /**
     * Aborts, map for jude message is aborted, linked for remove abort txn in memory when this
     * position have been deleted.
     */
    private final LinkedMap<TxnID, PositionImpl> aborts = new LinkedMap<>();

    private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshot>> takeSnapshotWriter;

    // when add abort or change max read position, the count will +1. Take snapshot will set 0 into it.
    private final AtomicLong changeMaxReadPositionAndAddAbortTimes = new AtomicLong();

    private final Timer timer;

    private final int takeSnapshotIntervalNumber;

    private final int takeSnapshotIntervalTime;

    private volatile long lastSnapshotTimestamps;

    private final CompletableFuture<Void> transactionBufferFuture = new CompletableFuture<>();

    public TopicTransactionBuffer(PersistentTopic topic) {
        super(State.None);
        this.topic = topic;
        this.changeToInitializingState();
        this.takeSnapshotWriter = this.topic.getBrokerService().getPulsar()
                .getTransactionBufferSnapshotService().createWriter(TopicName.get(topic.getName()));
        this.timer = topic.getBrokerService().getPulsar().getTransactionTimer();
        this.takeSnapshotIntervalNumber = topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotMaxTransactionCount();
        this.takeSnapshotIntervalTime = topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotMinTimeInMillis();
        this.maxReadPosition = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
        this.topic.getBrokerService().getPulsar().getTransactionReplayExecutor()
                .execute(new TopicTransactionBufferRecover(new TopicTransactionBufferRecoverCallBack() {
                    @Override
                    public void recoverComplete() {
                        if (!changeToReadyState()) {
                            log.error("[{}]Transaction buffer recover fail", topic.getName());
                        } else {
                            timer.newTimeout(TopicTransactionBuffer.this,
                                    takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
                            transactionBufferFuture.complete(null);
                        }
                    }

                    @Override
                    public void noNeedToRecover() {
                        if (!changeToNoSnapshotState()) {
                            log.error("[{}]Transaction buffer recover fail", topic.getName());
                        } else {
                            transactionBufferFuture.complete(null);
                        }
                    }

                    @Override
                    public void handleSnapshot(TransactionBufferSnapshot snapshot) {
                        maxReadPosition = PositionImpl.get(snapshot.getMaxReadPositionLedgerId(),
                                snapshot.getMaxReadPositionEntryId());
                        if (snapshot.getAborts() != null) {
                            snapshot.getAborts().forEach(abortTxnMetadata ->
                                    aborts.put(new TxnID(abortTxnMetadata.getTxnIdMostBits(),
                                                    abortTxnMetadata.getTxnIdLeastBits()),
                                            PositionImpl.get(abortTxnMetadata.getLedgerId(),
                                                    abortTxnMetadata.getEntryId())));
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
                                    aborts.put(txnID, position);
                                }
                                updateMaxReadPosition(txnID);
                            } else {
                                handleTransactionMessage(txnID, position);
                            }
                        }
                    }

                    @Override
                    public void recoverExceptionally(Exception e) {
                        transactionBufferFuture.completeExceptionally(e);
                    }
                }, this.topic, this));
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return null;
    }

    @Override
    public CompletableFuture<Void> checkIfTBRecoverCompletely(boolean isTxnEnabled) {
        if (!isTxnEnabled) {
            return CompletableFuture.completedFuture(null);
        } else {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            transactionBufferFuture.thenRun(() -> {
                if (checkIfNoSnapshot()) {
                    takeSnapshot().thenRun(() -> {
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
                log.error("Topic {}: TransactionBuffer recover failed", this.topic.getName(), exception);
                completableFuture.completeExceptionally(exception);
                return null;
            });
            return completableFuture;
        }
    }


    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        CompletableFuture<Position> completableFuture = new CompletableFuture<>();
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
        if (!ongoingTxns.containsKey(txnId) && !aborts.containsKey(txnId)) {
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
                            clearAbortedTransactions();
                            takeSnapshotByChangeTimes();
                        }
                        completableFuture.complete(null);
                    }

                    @Override
                    public void addFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("Failed to commit for txn {}", txnID, exception);
                        completableFuture.completeExceptionally(new PersistenceException(exception));
                    }
                }, null);
            } finally {
                commitMarker.release();
            }
        }).exceptionally(exception -> {
            log.error("Transaction {} commit on topic {}.", txnID.toString(), topic.getName(), exception);
            completableFuture.completeExceptionally(exception);
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
                            aborts.put(txnID, (PositionImpl) position);
                            updateMaxReadPosition(txnID);
                            handleLowWaterMark(txnID, lowWaterMark);
                            changeMaxReadPositionAndAddAbortTimes.getAndIncrement();
                            clearAbortedTransactions();
                            takeSnapshotByChangeTimes();
                        }
                        completableFuture.complete(null);
                    }

                    @Override
                    public void addFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("Failed to abort for txn {}", txnID, exception);
                        completableFuture.completeExceptionally(new PersistenceException(exception));
                    }
                }, null);
            } finally {
                abortMarker.release();
            }
        }).exceptionally(exception -> {
            log.error("Transaction {} abort on topic {}.", txnID.toString(), topic.getName());
            completableFuture.completeExceptionally(exception);
            return null;
        });
        return completableFuture;
    }

    private void handleLowWaterMark(TxnID txnID, long lowWaterMark) {
        if (!ongoingTxns.isEmpty()) {
            TxnID firstTxn = ongoingTxns.firstKey();
            if (firstTxn.getMostSigBits() == txnID.getMostSigBits() && lowWaterMark >= firstTxn.getLeastSigBits()) {
                ByteBuf abortMarker = Markers.newTxnAbortMarker(-1L,
                        firstTxn.getMostSigBits(), firstTxn.getLeastSigBits());
                try {
                    topic.getManagedLedger().asyncAddEntry(abortMarker, new AsyncCallbacks.AddEntryCallback() {
                        @Override
                        public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                            synchronized (TopicTransactionBuffer.this) {
                                aborts.put(firstTxn, (PositionImpl) position);
                                updateMaxReadPosition(firstTxn);
                            }
                        }

                        @Override
                        public void addFailed(ManagedLedgerException exception, Object ctx) {
                            log.error("Failed to abort low water mark for txn {}", txnID, exception);
                        }
                    }, null);
                } finally {
                    abortMarker.release();
                }
            }
        }
    }

    private void takeSnapshotByChangeTimes() {
        if (changeMaxReadPositionAndAddAbortTimes.get() >= takeSnapshotIntervalNumber) {
            takeSnapshot();
        }
    }

    private void takeSnapshotByTimeout() {
        if (changeMaxReadPositionAndAddAbortTimes.get() > 0) {
            takeSnapshot();
        }
        this.timer.newTimeout(TopicTransactionBuffer.this,
                takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Void> takeSnapshot() {
        changeMaxReadPositionAndAddAbortTimes.set(0);
        return takeSnapshotWriter.thenCompose(writer -> {
            TransactionBufferSnapshot snapshot = new TransactionBufferSnapshot();
            synchronized (TopicTransactionBuffer.this) {
                snapshot.setTopicName(topic.getName());
                snapshot.setMaxReadPositionLedgerId(maxReadPosition.getLedgerId());
                snapshot.setMaxReadPositionEntryId(maxReadPosition.getEntryId());
                List<AbortTxnMetadata> list = new ArrayList<>();
                aborts.forEach((k, v) -> {
                    AbortTxnMetadata abortTxnMetadata = new AbortTxnMetadata();
                    abortTxnMetadata.setTxnIdMostBits(k.getMostSigBits());
                    abortTxnMetadata.setTxnIdLeastBits(k.getLeastSigBits());
                    abortTxnMetadata.setLedgerId(v.getLedgerId());
                    abortTxnMetadata.setEntryId(v.getEntryId());
                    list.add(abortTxnMetadata);
                });
                snapshot.setAborts(list);
            }
            return writer.writeAsync(snapshot).thenAccept(messageId-> {
                this.lastSnapshotTimestamps = System.currentTimeMillis();
                if (log.isDebugEnabled()) {
                    log.debug("[{}]Transaction buffer take snapshot success! "
                            + "messageId : {}", topic.getName(), messageId);
                }
            }).exceptionally(e -> {
                log.warn("[{}]Transaction buffer take snapshot fail! ", topic.getName(), e);
                return null;
            });
        });
    }
    private void clearAbortedTransactions() {
        while (!aborts.isEmpty() && !((ManagedLedgerImpl) topic.getManagedLedger())
                .ledgerExists(aborts.get(aborts.firstKey()).getLedgerId())) {
            if (log.isDebugEnabled()) {
                aborts.firstKey();
                log.debug("[{}] Topic transaction buffer clear aborted transaction, TxnId : {}, Position : {}",
                        topic.getName(), aborts.firstKey(), aborts.get(aborts.firstKey()));
            }
            aborts.remove(aborts.firstKey());
        }
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
        return this.takeSnapshotWriter.thenCompose(writer -> {
            TransactionBufferSnapshot snapshot = new TransactionBufferSnapshot();
            snapshot.setTopicName(topic.getName());
            return writer.deleteAsync(snapshot);
        }).thenCompose(__ -> CompletableFuture.completedFuture(null));
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        changeToCloseState();
        return this.takeSnapshotWriter.thenCompose(SystemTopicClient.Writer::closeAsync);
    }

    @Override
    public boolean isTxnAborted(TxnID txnID) {
        return aborts.containsKey(txnID);
    }

    @Override
    public void syncMaxReadPositionForNormalPublish(PositionImpl position) {
        // when ongoing transaction is empty, proved that lastAddConfirm is can read max position, because callback
        // thread is the same tread, in this time the lastAddConfirm don't content transaction message.
        synchronized (TopicTransactionBuffer.this) {
            if (checkIfNoSnapshot()) {
                maxReadPosition = position;
                changeMaxReadPositionAndAddAbortTimes.incrementAndGet();
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
            return PositionImpl.earliest;
        }
    }

    @Override
    public TransactionInBufferStats getTransactionInBufferStats(TxnID txnID) {
        TransactionInBufferStats transactionInBufferStats = new TransactionInBufferStats();
        transactionInBufferStats.aborted = isTxnAborted(txnID);
        if (ongoingTxns.containsKey(txnID)) {
            transactionInBufferStats.startPosition = ongoingTxns.get(txnID).toString();
        }
        return transactionInBufferStats;
    }

    @Override
    public TransactionBufferStats getStats() {
        TransactionBufferStats transactionBufferStats = new TransactionBufferStats();
        transactionBufferStats.lastSnapshotTimestamps = this.lastSnapshotTimestamps;
        transactionBufferStats.state = this.getState().name();
        transactionBufferStats.maxReadPosition = this.maxReadPosition.toString();
        return transactionBufferStats;
    }

    @Override
    public void run(Timeout timeout) {
        if (checkIfReady()) {
            takeSnapshotByTimeout();
        }
    }

    // we store the maxReadPosition from snapshot then open the non-durable cursor by this topic's manageLedger.
    // the non-durable cursor will read to lastConfirmedEntry.
    static class TopicTransactionBufferRecover implements Runnable {

        private final PersistentTopic topic;

        private final TopicTransactionBufferRecoverCallBack callBack;

        private Position startReadCursorPosition = PositionImpl.earliest;

        private final SpscArrayQueue<Entry> entryQueue;

        private final AtomicLong exceptionNumber = new AtomicLong();

        public static final String SUBSCRIPTION_NAME = "transaction-buffer-sub";

        private final TopicTransactionBuffer topicTransactionBuffer;

        private TopicTransactionBufferRecover(TopicTransactionBufferRecoverCallBack callBack, PersistentTopic topic,
                                              TopicTransactionBuffer transactionBuffer) {
            this.topic = topic;
            this.callBack = callBack;
            this.entryQueue = new SpscArrayQueue<>(2000);
            this.topicTransactionBuffer = transactionBuffer;
        }

        @SneakyThrows
        @Override
        public void run() {
            this.topicTransactionBuffer.changeToInitializingState();
            topic.getBrokerService().getPulsar().getTransactionBufferSnapshotService()
                    .createReader(TopicName.get(topic.getName())).thenAcceptAsync(reader -> {
                try {
                    boolean hasSnapshot = false;
                    while (reader.hasMoreEvents()) {
                        Message<TransactionBufferSnapshot> message = reader.readNext();
                        if (topic.getName().equals(message.getKey())) {
                            TransactionBufferSnapshot transactionBufferSnapshot = message.getValue();
                            if (transactionBufferSnapshot != null) {
                                hasSnapshot = true;
                                callBack.handleSnapshot(transactionBufferSnapshot);
                                this.startReadCursorPosition = PositionImpl.get(
                                        transactionBufferSnapshot.getMaxReadPositionLedgerId(),
                                        transactionBufferSnapshot.getMaxReadPositionEntryId());
                            }
                        }
                    }
                    if (!hasSnapshot) {
                        callBack.noNeedToRecover();
                        return;
                    }
                } catch (PulsarClientException pulsarClientException) {
                    log.error("[{}]Transaction buffer recover fail when read "
                            + "transactionBufferSnapshot!", topic.getName(), pulsarClientException);
                    callBack.recoverExceptionally(pulsarClientException);
                    reader.closeAsync().exceptionally(e -> {
                        log.error("[{}]Transaction buffer reader close error!", topic.getName(), e);
                        return null;
                    });
                    return;
                }
                reader.closeAsync().exceptionally(e -> {
                    log.error("[{}]Transaction buffer reader close error!", topic.getName(), e);
                    return null;
                });

                ManagedCursor managedCursor;
                try {
                    managedCursor = topic.getManagedLedger()
                            .newNonDurableCursor(this.startReadCursorPosition, SUBSCRIPTION_NAME);
                } catch (ManagedLedgerException e) {
                    callBack.recoverExceptionally(e);
                    log.error("[{}]Transaction buffer recover fail when open cursor!", topic.getName(), e);
                    return;
                }
                PositionImpl lastConfirmedEntry = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
                PositionImpl currentLoadPosition = (PositionImpl) this.startReadCursorPosition;
                FillEntryQueueCallback fillEntryQueueCallback = new FillEntryQueueCallback(entryQueue, managedCursor,
                        TopicTransactionBufferRecover.this);
                if (lastConfirmedEntry.getEntryId() != -1) {
                    while (lastConfirmedEntry.compareTo(currentLoadPosition) > 0
                            && fillEntryQueueCallback.fillQueue()) {
                        Entry entry = entryQueue.poll();
                        if (entry != null) {
                            try {
                                currentLoadPosition = PositionImpl.get(entry.getLedgerId(), entry.getEntryId());
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

                closeCursor(managedCursor);
                callBack.recoverComplete();
            }).exceptionally(e -> {
                callBack.recoverExceptionally(new Exception(e));
                log.error("[{}]Transaction buffer new snapshot reader fail!", topic.getName(), e);
                return null;
            });
        }

        private void closeCursor(ManagedCursor cursor) {
            cursor.asyncClose(new AsyncCallbacks.CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    log.info("[{}]Transaction buffer snapshot recover cursor close complete.", topic.getName());
                }

                @Override
                public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}]Transaction buffer snapshot recover cursor close fail.", topic.getName());
                }
            }, null);
        }

        private void callBackException(ManagedLedgerException e) {
            log.error("Transaction buffer recover fail when recover transaction entry!", e);
            this.exceptionNumber.getAndIncrement();
        }
    }

    static class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);

        private final SpscArrayQueue<Entry> entryQueue;

        private final ManagedCursor cursor;

        private final TopicTransactionBufferRecover recover;

        private volatile boolean isReadable = true;

        private FillEntryQueueCallback(SpscArrayQueue<Entry> entryQueue, ManagedCursor cursor,
                                       TopicTransactionBufferRecover recover) {
            this.entryQueue = entryQueue;
            this.cursor = cursor;
            this.recover = recover;
        }
        boolean fillQueue() {
            if (entryQueue.size() < entryQueue.capacity() && outstandingReadsRequests.get() == 0) {
                if (cursor.hasMoreEntries()) {
                    outstandingReadsRequests.incrementAndGet();
                    cursor.asyncReadEntries(100, this, System.nanoTime(), PositionImpl.latest);
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
                    || exception instanceof ManagedLedgerException.ManagedLedgerFencedException) {
                isReadable = false;
            }
            recover.callBackException(exception);
            outstandingReadsRequests.decrementAndGet();
        }
    }
}
