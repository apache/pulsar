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
 * Unless required by applicable law or agreed to in writing,2
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.impl.ReadOnlyManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndex;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexesMetadata;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotSegment;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TxnIDData;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class SnapshotSegmentAbortedTxnProcessorImpl implements AbortedTxnProcessor {

    //Store the latest aborted transaction IDs and the latest max read position.
    private PositionImpl maxReadPosition;
    private ArrayList<TxnIDData> unsealedAbortedTxnIdSegment = new ArrayList<>();

    //Store the fixed aborted transaction segment
    private final ConcurrentSkipListMap<PositionImpl, ArrayList<TxnIDData>> abortTxnSegments
            = new ConcurrentSkipListMap<>();

    private final ConcurrentSkipListMap<PositionImpl, TransactionBufferSnapshotIndex> indexes
            = new ConcurrentSkipListMap<>();
    //The latest persistent snapshot index. This is used to combine new segment indexes with the latest metadata and
    // indexes.
    private TransactionBufferSnapshotIndexes persistentSnapshotIndexes = new TransactionBufferSnapshotIndexes();

    private final Timer timer;

    private final PersistentTopic topic;

    //When add abort or change max read position, the count will +1. Take snapshot will set 0 into it.
    private final AtomicLong changeMaxReadPositionAndAddAbortTimes = new AtomicLong();

    private volatile long lastSnapshotTimestamps;

    //Configurations
    private final int takeSnapshotIntervalNumber;

    private final int takeSnapshotIntervalTime;

    private final int transactionBufferMaxAbortedTxnsOfSnapshotSegment;

    //Persistent snapshot segment and index at the single thread.
    private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotSegment>>
            snapshotSegmentsWriterFuture;
    private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotIndexes>>
            snapshotIndexWriterFuture;
    private final PersistentWorker persistentWorker;

    public SnapshotSegmentAbortedTxnProcessorImpl(PersistentTopic topic) {
        this.topic = topic;
        this.persistentWorker = new PersistentWorker(topic);
        this.maxReadPosition = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
        this.takeSnapshotIntervalNumber = topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotMaxTransactionCount();
        this.takeSnapshotIntervalTime = topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotMinTimeInMillis();
        this.transactionBufferMaxAbortedTxnsOfSnapshotSegment =  topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotSegmentSize();
        snapshotSegmentsWriterFuture =  this.topic.getBrokerService().getPulsar()
                .getTransactionBufferSnapshotServiceFactory()
                .getTxnBufferSnapshotSegmentService().createWriter(TopicName.get(topic.getName()));
        snapshotIndexWriterFuture =  this.topic.getBrokerService().getPulsar()
                .getTransactionBufferSnapshotServiceFactory()
                .getTxnBufferSnapshotIndexService().createWriter(TopicName.get(topic.getName()));

        this.timer = topic.getBrokerService().getPulsar().getTransactionTimer();
    }

    @Override
    public void appendAbortedTxn(TxnIDData abortedTxnId, PositionImpl position) {
        unsealedAbortedTxnIdSegment.add(abortedTxnId);
        //The size of lastAbortedTxns reaches the configuration of the size of snapshot segment.
        if (unsealedAbortedTxnIdSegment.size() == transactionBufferMaxAbortedTxnsOfSnapshotSegment) {
            changeMaxReadPositionAndAddAbortTimes.set(0);
            abortTxnSegments.put(position, unsealedAbortedTxnIdSegment);
            persistentWorker.appendTask(PersistentWorker.OperationType.WriteSegment, () ->
                    persistentWorker.takeSnapshotSegmentAsync(unsealedAbortedTxnIdSegment, position));
            unsealedAbortedTxnIdSegment = new ArrayList<>();
        }
    }

    @Override
    public void updateMaxReadPosition(Position position) {
        if (position != this.maxReadPosition) {
            this.maxReadPosition = (PositionImpl) position;
            updateSnapshotIndexMetadataByChangeTimes();
        }
    }

    @Override
    public void updateMaxReadPositionNotIncreaseChangeTimes(Position maxReadPosition) {
        this.maxReadPosition = (PositionImpl) maxReadPosition;
    }

    @Override
    public boolean checkAbortedTransaction(TxnIDData txnID, Position readPosition) {
        if (readPosition == null) {
            return abortTxnSegments.values().stream()
                    .anyMatch(list -> list.contains(txnID)) || unsealedAbortedTxnIdSegment.contains(txnID);
        }
        Map.Entry<PositionImpl, ArrayList<TxnIDData>> ceilingEntry = abortTxnSegments
                .ceilingEntry((PositionImpl) readPosition);
        if (ceilingEntry == null) {
            return unsealedAbortedTxnIdSegment.contains(txnID);
        } else {
            return ceilingEntry.getValue().contains(txnID);
        }
    }

    @Override
    public void trimExpiredTxnIDDataOrSnapshotSegments() {
        //Checking whether there are some segment expired.
        while (!abortTxnSegments.isEmpty() && !((ManagedLedgerImpl) topic.getManagedLedger())
                .ledgerExists(abortTxnSegments.firstKey().getLedgerId())) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Topic transaction buffer clear aborted transactions, maxReadPosition : {}",
                        topic.getName(), abortTxnSegments.firstKey());
            }
            PositionImpl positionNeedToDelete = abortTxnSegments.firstKey();
            persistentWorker.appendTask(PersistentWorker.OperationType.DeleteSegment,
                    () -> persistentWorker.deleteSnapshotSegment(positionNeedToDelete));
        }
    }

    private String buildKey(long sequenceId) {
        return "multiple-" + sequenceId + this.topic.getName();
    }

    private void updateSnapshotIndexMetadataByChangeTimes() {
        if (this.changeMaxReadPositionAndAddAbortTimes.incrementAndGet() == takeSnapshotIntervalNumber) {
            changeMaxReadPositionAndAddAbortTimes.set(0);
            persistentWorker.appendTask(PersistentWorker.OperationType.UpdateIndex,
                    persistentWorker::updateIndexMetadataForTheLastSnapshot);
        }
    }

    private void takeSnapshotByTimeout() {
        if (changeMaxReadPositionAndAddAbortTimes.get() > 0) {
            changeMaxReadPositionAndAddAbortTimes.set(0);
            persistentWorker.appendTask(PersistentWorker.OperationType.UpdateIndex,
                    persistentWorker::updateIndexMetadataForTheLastSnapshot);
        }
        timer.newTimeout(SnapshotSegmentAbortedTxnProcessorImpl.this,
                takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run(Timeout timeout) {
        takeSnapshotByTimeout();
    }


    @Override
    public CompletableFuture<Void> takesFirstSnapshot() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        persistentWorker.appendTask(PersistentWorker.OperationType.UpdateIndex, () -> {
            TransactionBufferSnapshotIndexes indexes = new TransactionBufferSnapshotIndexes();
            return snapshotIndexWriterFuture
                    .thenCompose((indexesWriter) -> {
                        TransactionBufferSnapshotIndexesMetadata transactionBufferSnapshotIndexesMetadata =
                                new TransactionBufferSnapshotIndexesMetadata();
                        transactionBufferSnapshotIndexesMetadata.setAborts(unsealedAbortedTxnIdSegment);
                        transactionBufferSnapshotIndexesMetadata.setMaxReadPositionEntryId(maxReadPosition.getEntryId());
                        transactionBufferSnapshotIndexesMetadata.setMaxReadPositionLedgerId(maxReadPosition.getLedgerId());
                        indexes.setSnapshot(transactionBufferSnapshotIndexesMetadata);
                        indexes.setIndexList(new ArrayList<>());
                        indexes.setTopicName(this.topic.getName());
                        return indexesWriter.writeAsync(topic.getName(), indexes);
                    })
                    .thenRun(() -> {
                        //TODO: check again
                        persistentSnapshotIndexes.setSnapshot(indexes.getSnapshot());
                        indexes.setIndexList(new ArrayList<>());
                        indexes.setTopicName(this.topic.getName());
                        this.lastSnapshotTimestamps = System.currentTimeMillis();
                        completableFuture.complete(null);
                    })
                    .exceptionally(e -> {
                        log.error("[{}] Failed to update snapshot segment index", indexes.getTopicName(), e);
                        completableFuture.completeExceptionally(e);
                        return null;
                    });
        });
        return completableFuture;
    }


    @Override
    public CompletableFuture<PositionImpl> recoverFromSnapshot(TopicTransactionBufferRecoverCallBack callBack) {
        return topic.getBrokerService().getPulsar().getTransactionBufferSnapshotServiceFactory()
                .getTxnBufferSnapshotIndexService()
                .createReader(TopicName.get(topic.getName())).thenComposeAsync(reader -> {
                    PositionImpl startReadCursorPosition = null;
                    try {
                        boolean hasIndex = false;
                        //Read Index to recover the sequenceID, indexes, lastAbortedTxns and maxReadPosition.
                        while (reader.hasMoreEvents()) {
                            Message<TransactionBufferSnapshotIndexes> message = reader.readNext();
                            if (topic.getName().equals(message.getKey())) {
                                TransactionBufferSnapshotIndexes transactionBufferSnapshotIndexes = message.getValue();
                                if (transactionBufferSnapshotIndexes != null) {
                                    hasIndex = true;
                                    this.persistentSnapshotIndexes = transactionBufferSnapshotIndexes;
                                    startReadCursorPosition = PositionImpl.get(
                                            transactionBufferSnapshotIndexes.getSnapshot().getMaxReadPositionLedgerId(),
                                            transactionBufferSnapshotIndexes.getSnapshot().getMaxReadPositionEntryId());
                                }
                            }
                        }
                        closeReader(reader);
                        if (!hasIndex) {
                            callBack.noNeedToRecover();
                            return CompletableFuture.completedFuture(null);
                        } else {
                            persistentSnapshotIndexes.getIndexList()
                                    .forEach(transactionBufferSnapshotIndex ->
                                            indexes.put(new PositionImpl(
                                                    transactionBufferSnapshotIndex.persistentPositionLedgerID,
                                                            transactionBufferSnapshotIndex.persistentPositionEntryID),
                                                    transactionBufferSnapshotIndex));
                            this.unsealedAbortedTxnIdSegment = (ArrayList<TxnIDData>) persistentSnapshotIndexes
                                    .getSnapshot().getAborts();
                            this.maxReadPosition = new PositionImpl(persistentSnapshotIndexes
                                    .getSnapshot().getMaxReadPositionLedgerId(),
                                    persistentSnapshotIndexes.getSnapshot().getMaxReadPositionEntryId());
                            if (indexes.size() != 0) {
                                persistentWorker.sequenceID.set(indexes.lastEntry().getValue().sequenceID + 1);
                            }
                        }
                        //Read snapshot segment to recover aborts.
                        ArrayList<CompletableFuture<Void>> completableFutures = new ArrayList<>();
                        AtomicLong invalidIndex = new AtomicLong(0);
                        AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback = new AsyncCallbacks
                                .OpenReadOnlyManagedLedgerCallback() {
                            @Override
                            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedgerImpl readOnlyManagedLedger, Object ctx) {
                                persistentSnapshotIndexes.getIndexList().forEach(index -> {
                                    CompletableFuture<Void> completableFuture1 = new CompletableFuture<>();
                                    completableFutures.add(completableFuture1);
                                    readOnlyManagedLedger.asyncReadEntry(
                                            new PositionImpl(index.getPersistentPositionLedgerID(),
                                                    index.getPersistentPositionEntryID()),
                                            new AsyncCallbacks.ReadEntryCallback() {
                                                @Override
                                                public void readEntryComplete(Entry entry, Object ctx) {
                                                    //Remove invalid index
                                                    if (entry == null) {
                                                        indexes.remove(new PositionImpl(
                                                                index.getMaxReadPositionLedgerID(),
                                                                index.getMaxReadPositionEntryID()));
                                                        completableFuture1.complete(null);
                                                        invalidIndex.getAndIncrement();
                                                        return;
                                                    }
                                                    handleSnapshotSegmentEntry(entry);
                                                    completableFuture1.complete(null);
                                                }

                                                @Override
                                                public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                                                    completableFuture1.completeExceptionally(exception);
                                                }
                                            }, null);
                                });
                            }

                            @Override
                            public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                                //
                            }
                        };

                        TopicName snapshotIndexTopicName = TopicName.get(TopicDomain.persistent.toString(),
                                TopicName.get(topic.getName()).getNamespaceObject(),
                                EventType.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS.toString());
                        this.topic.getBrokerService().getPulsar().getManagedLedgerFactory()
                                .asyncOpenReadOnlyManagedLedger(snapshotIndexTopicName
                                                .getPersistenceNamingEncoding(), callback,
                                        topic.getManagedLedger().getConfig(),
                                        null);
                        //Wait the processor recover completely and the allow TB to recover the messages
                        // after the startReadCursorPosition.
                        FutureUtil.waitForAll(completableFutures).get();
                        if (invalidIndex.get() != 0 ) {
                            persistentWorker.appendTask(PersistentWorker.OperationType.UpdateIndex, ()
                                    -> persistentWorker.updateSnapshotIndex(null));
                        }
                        return CompletableFuture.completedFuture(startReadCursorPosition);
                    } catch (Exception ex) {
                        log.error("[{}] Transaction buffer recover fail when read "
                                + "transactionBufferSnapshot!", topic.getName(), ex);
                        callBack.recoverExceptionally(ex);
                        closeReader(reader);
                        return null;
                    }

                },  topic.getBrokerService().getPulsar().getTransactionExecutorProvider()
                        .getExecutor(this));
    }

    @Override
    public CompletableFuture<Void> clearSnapshot() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        persistentWorker.appendTask(PersistentWorker.OperationType.Close, () -> {
            ArrayList<CompletableFuture<Void>> completableFutures = new ArrayList<>();
            //Delete all segment
            while (!abortTxnSegments.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Topic transaction buffer clear aborted transactions, maxReadPosition : {}",
                            topic.getName(), abortTxnSegments.firstKey());
                }
                PositionImpl positionNeedToDelete = abortTxnSegments.firstKey();
                completableFutures.add(persistentWorker.deleteSnapshotSegment(positionNeedToDelete));
            }
            //Delete index
            return FutureUtil.waitForAll(completableFutures)
                    .thenCompose((ignore) -> snapshotIndexWriterFuture
                            .thenCompose(indexesWriter -> indexesWriter.writeAsync(topic.getName(), null)))
                    .thenRun(() -> {
                        log.info("Successes to clear the snapshot segment and indexes for the topic [{}]",
                                topic.getName());
                        completableFuture.thenCompose(null);
                    })
                    .exceptionally(e -> {
                        log.error("Failed to clear the snapshot segment and indexes for the topic [{}]",
                                topic.getName(), e);
                        completableFuture.completeExceptionally(e);
                        return null;
                    });
        });
        return completableFuture;
    }


    @Override
    public PositionImpl getMaxReadPosition() {
        return this.maxReadPosition;
    }

    @Override
    public long getLastSnapshotTimestamps() {
        return this.lastSnapshotTimestamps;
    }

    private void handleSnapshotSegmentEntry(Entry entry) {
        //decode snapshot from entry
        ByteBuf headersAndPayload = entry.getDataBuffer();
        //skip metadata
        Commands.parseMessageMetadata(headersAndPayload);
        TransactionBufferSnapshotSegment snapshotSegment = Schema.AVRO(TransactionBufferSnapshotSegment.class)
                .decode(Unpooled.wrappedBuffer(headersAndPayload).nioBuffer());
        abortTxnSegments.put(new PositionImpl(snapshotSegment.getMaxReadPositionLedgerId(),
                snapshotSegment.getMaxReadPositionEntryId()), (ArrayList<TxnIDData>) snapshotSegment.getAborts());

    }

    private <T> void  closeReader(SystemTopicClient.Reader<T> reader) {
        reader.closeAsync().exceptionally(e -> {
            log.error("[{}]Transaction buffer snapshot reader close error!", topic.getName(), e);
            return null;
        });
    }

    private class PersistentWorker {
        protected final AtomicLong sequenceID = new AtomicLong(0);

        private final PersistentTopic topic;

        private enum OperationState {
            None,
            UpdatingIndex,
            WritingSegment,
            DeletingSegment,
            Closing,
            Closed
        }
        private static final AtomicReferenceFieldUpdater<PersistentWorker, PersistentWorker.OperationState>
                STATE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(PersistentWorker.class,
                        PersistentWorker.OperationState.class, "operationState");

        public enum OperationType {
            UpdateIndex,
            WriteSegment,
            DeleteSegment,
            Close
        }

        private volatile OperationState operationState = OperationState.None;

        ConcurrentSkipListMap<OperationType, Supplier<CompletableFuture<Void>>> taskQueue =
                new ConcurrentSkipListMap<>();
        private CompletableFuture<Void> lastOperationFuture;
        private final Timer timer;

        public PersistentWorker(PersistentTopic topic) {
            this.topic = topic;
            this.timer = topic.getBrokerService().getPulsar().getTransactionTimer();

        }

        public void appendTask(OperationType operationType, Supplier<CompletableFuture<Void>> task) {
            switch (operationType) {
                case UpdateIndex -> {
                    if (!taskQueue.isEmpty()) {
                        return;
                    } else if(STATE_UPDATER.compareAndSet(this, OperationState.None, OperationState.UpdatingIndex)) {
                        lastOperationFuture = task.get();
                        lastOperationFuture.whenComplete((ignore, throwable) -> {
                            if (throwable != null && log.isDebugEnabled()) {
                                log.debug("[{}] Failed to update index snapshot", topic.getName(), throwable);
                            }

                            STATE_UPDATER.compareAndSet(this, OperationState.UpdatingIndex, OperationState.None);
                        });
                    }
                }
                case WriteSegment, DeleteSegment -> {
                    taskQueue.put(operationType, task);
                    executeTask();
                }
                case Close -> {
                    STATE_UPDATER.set(this, OperationState.Closing);
                    taskQueue.clear();
                    lastOperationFuture.thenRun(() -> {
                        lastOperationFuture = task.get();
                        lastOperationFuture.thenRun(() ->
                                STATE_UPDATER.compareAndSet(this, OperationState.Closing, OperationState.Closed));
                    });
                }
            }
        }

        private void executeTask() {
            OperationType operationType = taskQueue.firstKey();
            switch (operationType) {
                case WriteSegment -> {
                    if (STATE_UPDATER.compareAndSet(this, OperationState.None, OperationState.WritingSegment)) {
                        if (taskQueue.firstKey() == OperationType.WriteSegment) {
                            lastOperationFuture = taskQueue.firstEntry().getValue().get();
                            lastOperationFuture.whenComplete((ignore, throwable) -> {
                                if (throwable != null) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("[{}] Failed to write snapshot segment", topic.getName(), throwable);
                                    }
                                    timer.newTimeout(timeout -> executeTask(),
                                            takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
                                } else {
                                    taskQueue.remove(taskQueue.firstKey());
                                }
                                STATE_UPDATER.compareAndSet(this, OperationState.WritingSegment, OperationState.None);
                            });
                        }
                    }
                }
                case DeleteSegment -> {
                    if (STATE_UPDATER.compareAndSet(this, OperationState.None, OperationState.DeletingSegment)) {
                        if (taskQueue.firstKey() == OperationType.DeleteSegment) {
                            lastOperationFuture = taskQueue.firstEntry().getValue().get();
                            lastOperationFuture.whenComplete((ignore, throwable) -> {
                                if (throwable != null) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("[{}] Failed to delete snapshot segment", topic.getName(), throwable);
                                    }
                                    timer.newTimeout(timeout -> executeTask(),
                                            takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
                                } else {
                                    taskQueue.remove(taskQueue.firstKey());
                                }

                                STATE_UPDATER.compareAndSet(this, OperationState.DeletingSegment, OperationState.None);
                            });
                        }
                    }
                }
            }
        }

        protected CompletableFuture<Void> takeSnapshotSegmentAsync(ArrayList<TxnIDData> sealedAbortedTxnIdSegment,
                                                                 PositionImpl maxReadPosition) {
            return writeSnapshotSegmentAsync(sealedAbortedTxnIdSegment, maxReadPosition).thenRun(() -> {
                if (log.isDebugEnabled()) {
                    log.debug("Successes to take snapshot segment [{}] at maxReadPosition [{}] "
                                    + "for the topic [{}], and the size of the segment is [{}]",
                            this.sequenceID, maxReadPosition, topic.getName(), sealedAbortedTxnIdSegment.size());
                }
                this.sequenceID.getAndIncrement();
            }).exceptionally(e -> {
                //Just log the error, and the processor will try to take snapshot again when the transactionBuffer
                //append aborted txn nex time.
                log.error("Failed to take snapshot segment [{}] at maxReadPosition [{}] "
                                + "for the topic [{}], and the size of the segment is [{}]",
                        this.sequenceID, maxReadPosition, topic.getName(), sealedAbortedTxnIdSegment.size(), e);
                return null;
            });
        }

        private CompletableFuture<Void> writeSnapshotSegmentAsync(List<TxnIDData> segment, PositionImpl maxReadPosition) {
            TransactionBufferSnapshotSegment transactionBufferSnapshotSegment = new TransactionBufferSnapshotSegment();
            transactionBufferSnapshotSegment.setAborts(segment);
            transactionBufferSnapshotSegment.setTopicName(this.topic.getName());
            transactionBufferSnapshotSegment.setMaxReadPositionEntryId(maxReadPosition.getEntryId());
            transactionBufferSnapshotSegment.setMaxReadPositionLedgerId(maxReadPosition.getLedgerId());

            return snapshotSegmentsWriterFuture.thenCompose(segmentWriter -> {
                transactionBufferSnapshotSegment.setSequenceId(this.sequenceID.get());
                return segmentWriter.writeAsync(buildKey(this.sequenceID.get()), transactionBufferSnapshotSegment);
            }).thenCompose((messageId) -> {
                //Build index for this segment
                TransactionBufferSnapshotIndex index = new TransactionBufferSnapshotIndex();
                index.setSequenceID(transactionBufferSnapshotSegment.getSequenceId());
                index.setMaxReadPositionLedgerID(maxReadPosition.getLedgerId());
                index.setMaxReadPositionEntryID(maxReadPosition.getEntryId());
                index.setPersistentPositionLedgerID(((MessageIdImpl) messageId).getLedgerId());
                index.setPersistentPositionEntryID(((MessageIdImpl) messageId).getEntryId());

                indexes.put(maxReadPosition, index);
                //update snapshot segment index.
                return updateSnapshotIndex(maxReadPosition);
            });
        }

        private CompletableFuture<Void> deleteSnapshotSegment(PositionImpl positionNeedToDelete) {
            long sequenceIdNeedToDelete = indexes.get(positionNeedToDelete).getSequenceID();
            return snapshotSegmentsWriterFuture.thenCompose(writer -> writer.deleteAsync(buildKey(sequenceIdNeedToDelete), null))
                    .thenRun(() -> {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Successes to delete the snapshot segment, "
                                            + "whose sequenceId is [{}] and maxReadPosition is [{}]",
                                    this.topic.getName(), this.sequenceID, positionNeedToDelete);
                        }
                        abortTxnSegments.remove(positionNeedToDelete);
                        //The process will check whether the snapshot segment is null, and update index when recovered.
                        indexes.remove(positionNeedToDelete);
                        updateSnapshotIndex(null);
                    }).exceptionally(e -> {
                        log.warn("[{}] Failed to delete the snapshot segment, "
                                        + "whose sequenceId is [{}] and maxReadPosition is [{}]",
                                this.topic.getName(), this.sequenceID, positionNeedToDelete, e);
                        return null;
                    });
        }

        //Update the indexes in the transactionBufferSnapshotIndexe.
        //Concurrency control is performed by snapshotIndexWriterFuture.
        private CompletableFuture<Void> updateSnapshotIndex(PositionImpl maxReadPosition) {
            TransactionBufferSnapshotIndexes snapshotIndexes = new TransactionBufferSnapshotIndexes();
            return snapshotIndexWriterFuture
                    .thenCompose((indexesWriter) -> {
                        snapshotIndexes.setIndexList(indexes.values().stream().toList());
                        //update the metadata in the indexes.
                        if (maxReadPosition != null) {
                            snapshotIndexes.setSnapshot(new TransactionBufferSnapshotIndexesMetadata(
                                    maxReadPosition.getLedgerId(), maxReadPosition.getEntryId(), new ArrayList<>()));
                        } else {
                            //metadata keep no change
                            snapshotIndexes.setSnapshot(persistentSnapshotIndexes.getSnapshot());
                        }
                        return indexesWriter.writeAsync(topic.getName(), snapshotIndexes);
                    })
                    .thenRun(() -> {
                        persistentSnapshotIndexes.setIndexList(snapshotIndexes.getIndexList());
                        if (maxReadPosition != null) {
                            persistentSnapshotIndexes.setSnapshot(new TransactionBufferSnapshotIndexesMetadata(
                                    maxReadPosition.getLedgerId(), maxReadPosition.getEntryId(), new ArrayList<>()));
                        }
                        lastSnapshotTimestamps = System.currentTimeMillis();
                    })
                    .exceptionally(e -> {
                        log.error("[{}] Failed to update snapshot segment index", snapshotIndexes.getTopicName(), e);
                        return null;
                    });
        }

        //Only update the metadata in the transactionBufferSnapshotIndexes.
        //Concurrency control is performed by snapshotIndexWriterFuture.
        private CompletableFuture<Void> updateIndexMetadataForTheLastSnapshot() {
            TransactionBufferSnapshotIndexes indexes = new TransactionBufferSnapshotIndexes();
            return snapshotIndexWriterFuture
                    .thenCompose((indexesWriter) -> {
                        //Store the latest metadata
                        TransactionBufferSnapshotIndexesMetadata transactionBufferSnapshotSegment =
                                new TransactionBufferSnapshotIndexesMetadata();
                        transactionBufferSnapshotSegment.setAborts(unsealedAbortedTxnIdSegment);
                        indexes.setSnapshot(transactionBufferSnapshotSegment);
                        //Only update the metadata in indexes and keep the index in indexes unchanged.
                        indexes.setIndexList(persistentSnapshotIndexes.getIndexList());
                        return indexesWriter.writeAsync(topic.getName(), indexes);
                    })
                    .thenRun(() -> {
                        persistentSnapshotIndexes.setSnapshot(indexes.getSnapshot());
                        lastSnapshotTimestamps = System.currentTimeMillis();
                    })
                    .exceptionally(e -> {
                        log.error("[{}] Failed to update snapshot segment index", indexes.getTopicName(), e);
                        return null;
                    });
        }

    }

}