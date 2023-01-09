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
import io.netty.util.Timer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException;
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
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class SnapshotSegmentAbortedTxnProcessorImpl implements AbortedTxnProcessor {

    //Stored the unsealed aborted txn id, it will be persistent as a snapshot segment and reinit
    // when its size reach the capital of a snapshot segment.
    private LinkedList<TxnID> unsealedAbortedTxnIdSegment;

    //A mapping form the latest txn mark persistent position in a segment to its latest txn ID.
    //This is mainly used to trim expired snapshot segment.
    private final LinkedMap<PositionImpl, TxnID> segmentIndex = new LinkedMap<>();

    //Store all aborted txn IDs check whether a txn is an aborted txn.
    private final LinkedMap<TxnID, TxnID> aborts = new LinkedMap<>();
    //The indexes of the snapshot segments whose key is the aborted mark persistent position.
    private final LinkedMap<PositionImpl, TransactionBufferSnapshotIndex> indexes = new LinkedMap<>();
    //The latest persistent snapshot index. This is used to combine new segment indexes with the latest metadata and
    // indexes.
    private TransactionBufferSnapshotIndexes persistentSnapshotIndexes = new TransactionBufferSnapshotIndexes();

    private final PersistentTopic topic;

    private volatile long lastSnapshotTimestamps;

    private final int transactionBufferMaxAbortedTxnsOfSnapshotSegment;
    private final PersistentWorker persistentWorker;

    private static final String SNAPSHOT_PREFIX = "multiple-";

    public SnapshotSegmentAbortedTxnProcessorImpl(PersistentTopic topic) {
        this.topic = topic;
        this.persistentWorker = new PersistentWorker(topic);
        this.transactionBufferMaxAbortedTxnsOfSnapshotSegment =  topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotSegmentSize();
        this.unsealedAbortedTxnIdSegment = new LinkedList<>();
    }

    @Override
    public void putAbortedTxnAndPosition(TxnID abortedTxnId, PositionImpl abortedMarkerPersistentPosition) {
        unsealedAbortedTxnIdSegment.add(abortedTxnId);
        aborts.put(abortedTxnId, abortedTxnId);
        //The size of lastAbortedTxns reaches the configuration of the size of snapshot segment.
        if (unsealedAbortedTxnIdSegment.size() == transactionBufferMaxAbortedTxnsOfSnapshotSegment) {
            LinkedList<TxnID> abortedSegment = unsealedAbortedTxnIdSegment;
            segmentIndex.put(abortedMarkerPersistentPosition, abortedTxnId);
            persistentWorker.appendTask(PersistentWorker.OperationType.WriteSegment, () ->
                    persistentWorker.takeSnapshotSegmentAsync(abortedSegment, abortedMarkerPersistentPosition));
            this.unsealedAbortedTxnIdSegment = new LinkedList<>();
        }
    }

    @Override
    public boolean checkAbortedTransaction(TxnID txnID, Position readPosition) {
        return aborts.containsKey(txnID);
    }

    //In this implementation, we adopt snapshot segments. And then we clear invalid segment by its max read position.
    @Override
    public void trimExpiredAbortedTxns() {
        //Checking whether there are some segment expired.
        List<PositionImpl> positionsNeedToDelete = new ArrayList<>();
        while (!segmentIndex.isEmpty() && !((ManagedLedgerImpl) topic.getManagedLedger())
                .ledgerExists(segmentIndex.firstKey().getLedgerId())) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Topic transaction buffer clear aborted transactions, maxReadPosition : {}",
                        topic.getName(), segmentIndex.firstKey());
            }
            PositionImpl positionNeedToDelete = segmentIndex.firstKey();
            positionsNeedToDelete.add(positionNeedToDelete);
        }
        //Batch delete the expired segment and then update segment index.
        if (!positionsNeedToDelete.isEmpty()) {
            persistentWorker.appendTask(PersistentWorker.OperationType.DeleteSegment,
                    () -> persistentWorker.deleteSnapshotSegment(positionsNeedToDelete)
                            .thenRun(() -> {
                                persistentWorker.updateSnapshotIndex(persistentSnapshotIndexes.getSnapshot(),
                                        indexes.values().stream().toList());
                            }));
        }
    }

    private String buildKey(long sequenceId) {
        return SNAPSHOT_PREFIX + sequenceId + "-" + this.topic.getName();
    }

    @Override
    public CompletableFuture<Void> takeAbortedTxnsSnapshot(PositionImpl maxReadPosition) {
        TransactionBufferSnapshotIndexesMetadata metadata = new TransactionBufferSnapshotIndexesMetadata(
                maxReadPosition.getLedgerId(), maxReadPosition.getEntryId(),
                convertTypeToTxnIDData(unsealedAbortedTxnIdSegment));
        return updateSnapshotIndexMetadata(metadata);
    }

    private CompletableFuture<Void> updateSnapshotIndexMetadata(TransactionBufferSnapshotIndexesMetadata metadata) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        persistentWorker.appendTask(PersistentWorker.OperationType.UpdateIndex,
                () -> persistentWorker
                        .updateSnapshotIndex(metadata, persistentSnapshotIndexes.getIndexList())
                        .thenRun(() -> completableFuture.complete(null))
                        .exceptionally(e -> {
                            completableFuture.completeExceptionally(e);
                            return null;
                        }));
        return completableFuture;
    }

    @Override
    public CompletableFuture<PositionImpl> recoverFromSnapshot() {
        return topic.getBrokerService().getPulsar().getTransactionBufferSnapshotServiceFactory()
                .getTxnBufferSnapshotIndexService()
                .createReader(TopicName.get(topic.getName())).thenComposeAsync(reader -> {
                    PositionImpl startReadCursorPosition = null;
                    boolean hasIndex = false;
                    try {
                        //Read Index to recover the sequenceID, indexes, lastAbortedTxns and maxReadPosition.
                        while (reader.hasMoreEvents()) {
                            Message<TransactionBufferSnapshotIndexes> message = reader.readNextAsync()
                                    .get(getSystemClientOperationTimeoutMs(), TimeUnit.MILLISECONDS);
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
                    } catch (TimeoutException ex) {
                        Throwable t = FutureUtil.unwrapCompletionException(ex);
                        String errorMessage = String.format("[%s] Transaction buffer recover fail by read "
                                + "transactionBufferSnapshot timeout!", topic.getName());
                        log.error(errorMessage, t);
                        return FutureUtil.failedFuture(
                                new BrokerServiceException.ServiceUnitNotReadyException(errorMessage, t));
                    } catch (Exception ex) {
                        log.error("[{}] Transaction buffer recover fail when read "
                                + "transactionBufferSnapshot!", topic.getName(), ex);
                        return FutureUtil.failedFuture(ex);
                    } finally {
                        closeReader(reader);
                    }
                    PositionImpl finalStartReadCursorPosition = startReadCursorPosition;
                    if (!hasIndex) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        persistentSnapshotIndexes.getIndexList()
                                .forEach(transactionBufferSnapshotIndex ->
                                        indexes.put(new PositionImpl(
                                                        transactionBufferSnapshotIndex.abortedMarkLedgerID,
                                                        transactionBufferSnapshotIndex.abortedMarkEntryID),
                                                transactionBufferSnapshotIndex));
                        this.unsealedAbortedTxnIdSegment = convertTypeToTxnID(persistentSnapshotIndexes
                                .getSnapshot().getAborts());
                        //If the size of indexes is 0, the sequence ID will be the init value 0.
                        if (indexes.size() != 0) {
                            persistentWorker.sequenceID.set(indexes.get(indexes.lastKey()).sequenceID + 1);
                        }
                    }
                    //Read snapshot segment to recover aborts.
                    ArrayList<CompletableFuture<Void>> completableFutures = new ArrayList<>();
                    CompletableFuture<Void> openManagedLedgerFuture = new CompletableFuture<>();
                    AtomicBoolean hasInvalidIndex = new AtomicBoolean(false);
                    AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback = new AsyncCallbacks
                            .OpenReadOnlyManagedLedgerCallback() {
                        @Override
                        public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedgerImpl readOnlyManagedLedger,
                                                                      Object ctx) {
                            persistentSnapshotIndexes.getIndexList().forEach(index -> {
                                CompletableFuture<Void> handleSegmentFuture = new CompletableFuture<>();
                                completableFutures.add(handleSegmentFuture);
                                readOnlyManagedLedger.asyncReadEntry(
                                        new PositionImpl(index.getSegmentLedgerID(),
                                                index.getSegmentEntryID()),
                                        new AsyncCallbacks.ReadEntryCallback() {
                                            @Override
                                            public void readEntryComplete(Entry entry, Object ctx) {
                                                handleSnapshotSegmentEntry(entry);
                                                entry.release();
                                                handleSegmentFuture.complete(null);
                                            }

                                            @Override
                                            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                                                if (exception instanceof ManagedLedgerException
                                                        .NonRecoverableLedgerException) {
                                                    if (((ManagedLedgerImpl)topic.getManagedLedger())
                                                            .ledgerExists(index.getAbortedMarkLedgerID())) {
                                                        log.error("[{}] Failed to read snapshot segment [{}:{}]",
                                                                topic.getName(), index.segmentLedgerID,
                                                                index.segmentEntryID, exception);
                                                        topic.close();
                                                        handleSegmentFuture.completeExceptionally(exception);
                                                    } else {
                                                        indexes.remove(new PositionImpl(
                                                                index.getAbortedMarkLedgerID(),
                                                                index.getAbortedMarkEntryID()));
                                                        hasInvalidIndex.set(true);
                                                    }
                                                }
                                            }
                                        }, null);
                            });
                            openManagedLedgerFuture.complete(null);
                        }

                        @Override
                        public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                            log.error("[{}] Failed to open readOnly managed ledger", topic, exception);
                            openManagedLedgerFuture.completeExceptionally(exception);
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

                    return openManagedLedgerFuture
                            .thenCompose((ignore) -> FutureUtil.waitForAll(completableFutures).thenCompose((i) -> {
                                if (hasInvalidIndex.get()) {
                                    persistentWorker.appendTask(PersistentWorker.OperationType.UpdateIndex, ()
                                            -> persistentWorker
                                            .updateSnapshotIndex(persistentSnapshotIndexes.getSnapshot(),
                                            indexes.values().stream().toList()));
                                }
                                return CompletableFuture.completedFuture(finalStartReadCursorPosition);
                            })).exceptionally(ex -> {
                                log.error("[{}] Failed to recover snapshot segment", this.topic.getName(), ex);
                                return null;
                            });

                    },  topic.getBrokerService().getPulsar().getTransactionExecutorProvider()
                        .getExecutor(this));
    }

    @Override
    public CompletableFuture<Void> deleteAbortedTxnSnapshot() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        persistentWorker.appendTask(PersistentWorker.OperationType.Clear,
                () -> persistentWorker.clearSnapshotSegmentAndIndexes()
                        .thenRun(() -> {
                            completableFuture.thenCompose(null);
                        }).exceptionally(e -> {
                            completableFuture.completeExceptionally(e);
                            return null;
                        }));
        return completableFuture;
    }

    @Override
    public long getLastSnapshotTimestamps() {
        return this.lastSnapshotTimestamps;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return persistentWorker.closeAsync();
    }

    private void handleSnapshotSegmentEntry(Entry entry) {
        //decode snapshot from entry
        ByteBuf headersAndPayload = entry.getDataBuffer();
        //skip metadata
        Commands.parseMessageMetadata(headersAndPayload);
        TransactionBufferSnapshotSegment snapshotSegment = Schema.AVRO(TransactionBufferSnapshotSegment.class)
                .decode(Unpooled.wrappedBuffer(headersAndPayload).nioBuffer());

        TxnIDData lastTxn = snapshotSegment.getAborts().get(snapshotSegment.getAborts().size() - 1);
        segmentIndex.put(new PositionImpl(snapshotSegment.getPersistentPositionLedgerId(),
                snapshotSegment.getPersistentPositionEntryId()),
                new TxnID(lastTxn.getMostSigBits(), lastTxn.getLeastSigBits()));
        convertTypeToTxnID(snapshotSegment.getAborts()).forEach(txnID -> aborts.put(txnID, txnID));
    }

    private long getSystemClientOperationTimeoutMs() throws Exception {
        PulsarClientImpl pulsarClient = (PulsarClientImpl) topic.getBrokerService().getPulsar().getClient();
        return pulsarClient.getConfiguration().getOperationTimeoutMs();
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

        //Persistent snapshot segment and index at the single thread.
        private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotSegment>>
                snapshotSegmentsWriterFuture;
        private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotIndexes>>
                snapshotIndexWriterFuture;

        private enum OperationState {
            None,
            Operating
        }
        private static final AtomicReferenceFieldUpdater<PersistentWorker, PersistentWorker.OperationState>
                STATE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(PersistentWorker.class,
                PersistentWorker.OperationState.class, "operationState");

        public enum OperationType {
            UpdateIndex,
            WriteSegment,
            DeleteSegment,
            Clear
        }

        private volatile OperationState operationState = OperationState.None;

        ConcurrentLinkedDeque<Pair<OperationType, Supplier<CompletableFuture<Void>>>> taskQueue =
                new ConcurrentLinkedDeque<>();
        private CompletableFuture<Void> lastOperationFuture;

        public PersistentWorker(PersistentTopic topic) {
            this.topic = topic;
            this.snapshotSegmentsWriterFuture =  this.topic.getBrokerService().getPulsar()
                    .getTransactionBufferSnapshotServiceFactory()
                    .getTxnBufferSnapshotSegmentService().createWriter(TopicName.get(topic.getName()));
            this.snapshotIndexWriterFuture =  this.topic.getBrokerService().getPulsar()
                    .getTransactionBufferSnapshotServiceFactory()
                    .getTxnBufferSnapshotIndexService().createWriter(TopicName.get(topic.getName()))
                    .exceptionally((ex) -> {
                        log.error("{} Failed to create snapshot writer", topic.getName());
                        topic.close();
                        return null;
                    });;
        }

        public void appendTask(OperationType operationType, Supplier<CompletableFuture<Void>> task) {
            switch (operationType) {
                //Update index is can be canceled when the task queue is not empty, so it should be executed immediately
                // instead of taking in queue. If the task queue is not empty, execute the task from the queue.
                case UpdateIndex -> {
                    if (!taskQueue.isEmpty()) {
                        topic.getBrokerService().getPulsar().getTransactionExecutorProvider()
                                .getExecutor(this).submit(this::executeTask);
                    } else if (STATE_UPDATER.compareAndSet(this, OperationState.None, OperationState.Operating)) {
                        lastOperationFuture = task.get();
                        lastOperationFuture.whenComplete((ignore, throwable) -> {
                            if (throwable != null && log.isDebugEnabled()) {
                                log.debug("[{}] Failed to update index snapshot", topic.getName(), throwable);
                            }

                            STATE_UPDATER.compareAndSet(this, OperationState.Operating, OperationState.None);
                        });
                    }
                }
                case WriteSegment, DeleteSegment -> {
                    taskQueue.add(new MutablePair<>(operationType, task));
                    executeTask();
                }
                //If the operation type is clear, all the operation in the queue is meaningless.
                case Clear -> {
                    STATE_UPDATER.set(this, OperationState.Operating);
                    taskQueue.clear();
                }
            }
        }

        private void executeTask() {
            if (taskQueue.isEmpty()) return;
            OperationType operationType = taskQueue.getFirst().getKey();
            switch (operationType) {
                case WriteSegment -> {
                    if (STATE_UPDATER.compareAndSet(this, OperationState.None, OperationState.Operating)) {
                        if (taskQueue.getFirst().getKey() == OperationType.WriteSegment) {
                            lastOperationFuture = taskQueue.getFirst().getValue().get();
                            lastOperationFuture.whenComplete((ignore, throwable) -> {
                                if (throwable != null) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("[{}] Failed to write snapshot segment", topic.getName(), throwable);
                                    }
                                } else {
                                    taskQueue.removeFirst();
                                }
                                STATE_UPDATER.compareAndSet(this, OperationState.Operating, OperationState.None);
                                topic.getBrokerService().getPulsar().getTransactionExecutorProvider()
                                        .getExecutor(this).submit(this::executeTask);
                            });
                        }
                    }
                }
                case DeleteSegment -> {
                    if (STATE_UPDATER.compareAndSet(this, OperationState.None, OperationState.Operating)) {
                        if (taskQueue.getFirst().getKey() == OperationType.DeleteSegment) {
                            lastOperationFuture = taskQueue.getFirst().getValue().get();
                            lastOperationFuture.whenComplete((ignore, throwable) -> {
                                if (throwable != null) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("[{}] Failed to delete snapshot segment", topic.getName(), throwable);
                                    }
                                } else {
                                    taskQueue.removeFirst();
                                }

                                STATE_UPDATER.compareAndSet(this, OperationState.Operating, OperationState.None);
                                topic.getBrokerService().getPulsar().getTransactionExecutorProvider()
                                        .getExecutor(this).submit(this::executeTask);
                            });
                        }
                    }
                }
            }
        }

        private CompletableFuture<Void> takeSnapshotSegmentAsync(LinkedList<TxnID> sealedAbortedTxnIdSegment,
                                                                 PositionImpl abortedMarkerPersistentPosition) {
            return writeSnapshotSegmentAsync(sealedAbortedTxnIdSegment, abortedMarkerPersistentPosition).thenRun(() -> {
                if (log.isDebugEnabled()) {
                    log.debug("Successes to take snapshot segment [{}] at maxReadPosition [{}] "
                                    + "for the topic [{}], and the size of the segment is [{}]",
                            this.sequenceID, abortedMarkerPersistentPosition, topic.getName(), sealedAbortedTxnIdSegment.size());
                }
                this.sequenceID.getAndIncrement();
            }).exceptionally(e -> {
                //Just log the error, and the processor will try to take snapshot again when the transactionBuffer
                //append aborted txn nex time.
                log.error("Failed to take snapshot segment [{}] at maxReadPosition [{}] "
                                + "for the topic [{}], and the size of the segment is [{}]",
                        this.sequenceID, abortedMarkerPersistentPosition, topic.getName(), sealedAbortedTxnIdSegment.size(), e);
                return null;
            });
        }

        private CompletableFuture<Void> writeSnapshotSegmentAsync(LinkedList<TxnID> segment,
                                                                  PositionImpl abortedMarkerPersistentPosition) {
            TransactionBufferSnapshotSegment transactionBufferSnapshotSegment = new TransactionBufferSnapshotSegment();
            transactionBufferSnapshotSegment.setAborts(convertTypeToTxnIDData(segment));
            transactionBufferSnapshotSegment.setTopicName(this.topic.getName());
            transactionBufferSnapshotSegment.setPersistentPositionEntryId(abortedMarkerPersistentPosition.getEntryId());
            transactionBufferSnapshotSegment.setPersistentPositionLedgerId(abortedMarkerPersistentPosition.getLedgerId());

            return snapshotSegmentsWriterFuture.thenCompose(segmentWriter -> {
                transactionBufferSnapshotSegment.setSequenceId(this.sequenceID.get());
                return segmentWriter.writeAsync(buildKey(this.sequenceID.get()), transactionBufferSnapshotSegment);
            }).thenCompose((messageId) -> {
                PositionImpl maxReadPosition = topic.getMaxReadPosition();
                //Build index for this segment
                TransactionBufferSnapshotIndex index = new TransactionBufferSnapshotIndex();
                index.setSequenceID(transactionBufferSnapshotSegment.getSequenceId());
                index.setAbortedMarkLedgerID(abortedMarkerPersistentPosition.getLedgerId());
                index.setAbortedMarkEntryID(abortedMarkerPersistentPosition.getEntryId());
                index.setSegmentLedgerID(((MessageIdImpl) messageId).getLedgerId());
                index.setSegmentEntryID(((MessageIdImpl) messageId).getEntryId());

                indexes.put(abortedMarkerPersistentPosition, index);
                //update snapshot segment index.
                return updateSnapshotIndex(new TransactionBufferSnapshotIndexesMetadata(
                                maxReadPosition.getLedgerId(), maxReadPosition.getEntryId(), new LinkedList<>()),
                        indexes.values().stream().toList());
            });
        }

        // update index after delete all segment.
        private CompletableFuture<Void> deleteSnapshotSegment(List<PositionImpl> positionNeedToDeletes) {
            List<CompletableFuture<Void>> results = new ArrayList<>();
            for (PositionImpl positionNeedToDelete : positionNeedToDeletes) {
                long sequenceIdNeedToDelete = indexes.get(positionNeedToDelete).getSequenceID();
                results.add(snapshotSegmentsWriterFuture
                        .thenCompose(writer -> writer.deleteAsync(buildKey(sequenceIdNeedToDelete), null))
                        .thenRun(() -> {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Successes to delete the snapshot segment, "
                                                + "whose sequenceId is [{}] and maxReadPosition is [{}]",
                                        this.topic.getName(), this.sequenceID, positionNeedToDelete);
                            }
                            TxnID theLatestDeletedTxnID = segmentIndex.remove(positionNeedToDelete);
                            //The position is already deleted.
                            if (theLatestDeletedTxnID == null) {
                                return;
                            }
                            while (!aborts.firstKey().equals(theLatestDeletedTxnID)) {
                                aborts.remove(aborts.firstKey());
                            }
                            aborts.remove(theLatestDeletedTxnID);
                            //The process will check whether the snapshot segment is null,
                            // and update index when recovered.
                            indexes.remove(positionNeedToDelete);
                        }).exceptionally(e -> {
                            log.warn("[{}] Failed to delete the snapshot segment, "
                                            + "whose sequenceId is [{}] and maxReadPosition is [{}]",
                                    this.topic.getName(), this.sequenceID, positionNeedToDelete, e);
                            return null;
                        }));
            }
            return FutureUtil.waitForAll(results);
        }

        //Update the indexes with the giving index snapshot and index list in the transactionBufferSnapshotIndexes.
        private CompletableFuture<Void> updateSnapshotIndex(TransactionBufferSnapshotIndexesMetadata snapshotSegment,
                                                            List<TransactionBufferSnapshotIndex> indexList) {
            TransactionBufferSnapshotIndexes snapshotIndexes = new TransactionBufferSnapshotIndexes();
            return snapshotIndexWriterFuture
                    .thenCompose((indexesWriter) -> {
                        snapshotIndexes.setIndexList(indexList);
                        snapshotIndexes.setSnapshot(snapshotSegment);
                        return indexesWriter.writeAsync(topic.getName(), snapshotIndexes);
                    })
                    .thenRun(() -> {
                        persistentSnapshotIndexes = snapshotIndexes;
                        lastSnapshotTimestamps = System.currentTimeMillis();
                    })
                    .exceptionally(e -> {
                        log.error("[{}] Failed to update snapshot segment index", snapshotIndexes.getTopicName(), e);
                        return null;
                    });
        }

        private CompletableFuture<Void> clearSnapshotSegmentAndIndexes() {
            ArrayList<CompletableFuture<Void>> completableFutures = new ArrayList<>();
            //Delete all segment
            completableFutures.add(persistentWorker.deleteSnapshotSegment(segmentIndex.keySet().stream().toList()));
            //Delete index
            return FutureUtil.waitForAll(completableFutures)
                    .thenCompose((ignore) -> snapshotIndexWriterFuture
                            .thenCompose(indexesWriter -> indexesWriter.writeAsync(topic.getName(), null)))
                    .thenRun(() -> {
                        log.info("Successes to clear the snapshot segment and indexes for the topic [{}]",
                                topic.getName());
                    })
                    .exceptionally(e -> {
                        log.error("Failed to clear the snapshot segment and indexes for the topic [{}]",
                                topic.getName(), e);

                        return null;
                    });
        }


        CompletableFuture<Void> closeAsync() {
            return CompletableFuture.allOf(
                    this.snapshotIndexWriterFuture.thenCompose(SystemTopicClient.Writer::closeAsync),
                    this.snapshotSegmentsWriterFuture.thenCompose(SystemTopicClient.Writer::closeAsync));
        }
    }

    private LinkedList<TxnID> convertTypeToTxnID(List<TxnIDData> snapshotSegment) {
        LinkedList<TxnID> abortedTxns = new LinkedList<>();
        snapshotSegment.forEach(txnIDData -> {
            abortedTxns.add(new TxnID(txnIDData.getMostSigBits(), txnIDData.getLeastSigBits()));
        });
        return abortedTxns;
    }

    private List<TxnIDData> convertTypeToTxnIDData(LinkedList<TxnID> abortedTxns) {
        List<TxnIDData> segment = new LinkedList<>();
        abortedTxns.forEach(txnID -> {
            segment.add(new TxnIDData(txnID.getMostSigBits(), txnID.getLeastSigBits()));
        });
        return segment;
    }

}