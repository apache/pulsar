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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.SystemTopicTxnBufferSnapshotService.ReferenceCountedWriter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.metadata.TransactionBufferSnapshot;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndex;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexesMetadata;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotSegment;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TxnIDData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SegmentStats;
import org.apache.pulsar.common.policies.data.SegmentsStats;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class SnapshotSegmentAbortedTxnProcessorImpl implements AbortedTxnProcessor {

    /**
     * Stored the unsealed aborted transaction IDs Whose size is always less than the snapshotSegmentCapacity.
     * It will be persistent as a snapshot segment when its size reach the configured capacity.
     */
    private LinkedList<TxnID> unsealedTxnIds;

    /**
     * The map is used to clear the aborted transaction IDs persistent in the expired ledger.
     * <p>
     *     The key Position {@link Position} is the persistent position of
     *     the latest transaction of a segment.
     *     The value TxnID {@link TxnID} is the latest Transaction ID in a segment.
     * </p>
     *
     * <p>
     *     If the position is expired, the processor can get the according latest
     *     transaction ID in this map. And then the processor can clear all the
     *     transaction IDs in the aborts {@link SnapshotSegmentAbortedTxnProcessorImpl#aborts}
     *     that lower than the transaction ID.
     *     And then the processor can delete the segments persistently according to
     *     the positions.
     * </p>
     */
    private final LinkedMap<Position, TxnID> segmentIndex = new LinkedMap<>();

    /**
     * This map is used to check whether a transaction is an aborted transaction.
     * <p>
     *     The transaction IDs is appended in order, so the processor can delete expired
     *     transaction IDs according to the latest expired transaction IDs in segmentIndex
     *     {@link SnapshotSegmentAbortedTxnProcessorImpl#segmentIndex}.
     * </p>
     */
    private final LinkedMap<TxnID, TxnID> aborts = new LinkedMap<>();
    /**
     * This map stores the indexes of the snapshot segment.
     * <p>
     *     The key is the persistent position of the marker of the last transaction in the segment.
     *     The value TransactionBufferSnapshotIndex {@link TransactionBufferSnapshotIndex} is the
     *     indexes of the snapshot segment.
     * </p>
     */
    private final LinkedMap<Position, TransactionBufferSnapshotIndex> indexes = new LinkedMap<>();

    private final PersistentTopic topic;

    private volatile long lastSnapshotTimestamps;

    private volatile long lastTakedSnapshotSegmentTimestamp;

    /**
     * The number of the aborted transaction IDs in a segment.
     * This is calculated according to the configured memory size.
     */
    private final int snapshotSegmentCapacity;
    /**
     * Responsible for executing the persistent tasks.
     * <p>Including:</p>
     * <p>    Update segment index.</p>
     * <p>    Write snapshot segment.</p>
     * <p>    Delete snapshot segment.</p>
     * <p>    Clear all snapshot segment. </p>
     */
    private final PersistentWorker persistentWorker;

    private static final String SNAPSHOT_PREFIX = "multiple-";

    public SnapshotSegmentAbortedTxnProcessorImpl(PersistentTopic topic) {
        this.topic = topic;
        this.persistentWorker = new PersistentWorker(topic);
        /*
           Calculate the segment capital according to its size configuration.
           <p>
               The empty transaction segment size is 5.
               Adding an empty linkedList, the size increase to 6.
               Add the topic name the size increase to the 7 + topic.getName().length().
               Add the aborted transaction IDs, the size increase to 8 +
               topic.getName().length() + 3 * aborted transaction ID size.
           </p>
         */
        this.snapshotSegmentCapacity = (topic.getBrokerService().getPulsar()
                .getConfiguration().getTransactionBufferSnapshotSegmentSize() - 8 - topic.getName().length()) / 3;
        this.unsealedTxnIds = new LinkedList<>();
    }

    @Override
    public void putAbortedTxnAndPosition(TxnID txnID, Position position) {
        unsealedTxnIds.add(txnID);
        aborts.put(txnID, txnID);
        /*
           The size of lastAbortedTxns reaches the configuration of the size of snapshot segment.
           Append a task to persistent the segment with the aborted transaction IDs and the latest
           transaction mark persistent position passed by param.
         */
        if (unsealedTxnIds.size() >= snapshotSegmentCapacity) {
            LinkedList<TxnID> abortedSegment = unsealedTxnIds;
            segmentIndex.put(position, txnID);
            persistentWorker.appendTask(PersistentWorker.OperationType.WriteSegment,
                    () -> persistentWorker.takeSnapshotSegmentAsync(abortedSegment, position));
            this.unsealedTxnIds = new LinkedList<>();
        }
    }

    @Override
    public boolean checkAbortedTransaction(TxnID txnID) {
        return aborts.containsKey(txnID);
    }

    /**
     * Check whether the position in segmentIndex {@link SnapshotSegmentAbortedTxnProcessorImpl#segmentIndex}
     * is expired. If the position is not exist in the original topic, the according transaction is an invalid
     * transaction. And the according segment is invalid, too. The transaction IDs before the transaction ID
     * in the aborts are invalid, too.
     */
    @Override
    public void trimExpiredAbortedTxns() {
        //Checking whether there are some segment expired.
        List<Position> positionsNeedToDelete = new ArrayList<>();
        while (!segmentIndex.isEmpty() && !topic.getManagedLedger().getLedgersInfo()
                .containsKey(segmentIndex.firstKey().getLedgerId())) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Topic transaction buffer clear aborted transactions, maxReadPosition : {}",
                        topic.getName(), segmentIndex.firstKey());
            }
            Position positionNeedToDelete = segmentIndex.firstKey();
            positionsNeedToDelete.add(positionNeedToDelete);

            TxnID theLatestDeletedTxnID = segmentIndex.remove(0);
            while (!aborts.firstKey().equals(theLatestDeletedTxnID)) {
                aborts.remove(0);
            }
            aborts.remove(0);
        }
        //Batch delete the expired segment
        if (!positionsNeedToDelete.isEmpty()) {
            persistentWorker.appendTask(PersistentWorker.OperationType.DeleteSegment,
                    () -> persistentWorker.deleteSnapshotSegment(positionsNeedToDelete));
        }
    }

    private String buildKey(long sequenceId) {
        return SNAPSHOT_PREFIX + sequenceId + "-" + this.topic.getName();
    }

    @Override
    public CompletableFuture<Void> takeAbortedTxnsSnapshot(Position maxReadPosition) {
        //Store the latest aborted transaction IDs in unsealedTxnIDs and the according the latest max read position.
        TransactionBufferSnapshotIndexesMetadata metadata = new TransactionBufferSnapshotIndexesMetadata(
                maxReadPosition.getLedgerId(), maxReadPosition.getEntryId(),
                convertTypeToTxnIDData(unsealedTxnIds));
        return persistentWorker.appendTask(PersistentWorker.OperationType.UpdateIndex,
                () -> persistentWorker.updateSnapshotIndex(metadata));
    }

    @Override
    public CompletableFuture<Position> recoverFromSnapshot() {
        final var pulsar = topic.getBrokerService().getPulsar();
        final var future = new CompletableFuture<Position>();
        pulsar.getTransactionExecutorProvider().getExecutor(this).execute(() -> {
            try {
                final var indexes = pulsar.getTransactionBufferSnapshotServiceFactory()
                        .getTxnBufferSnapshotIndexService().getTableView().readLatest(topic.getName());
                if (indexes == null) {
                    // Try recovering from the old format snapshot
                    future.complete(recoverOldSnapshot());
                    return;
                }
                final var snapshot = indexes.getSnapshot();
                final var startReadCursorPosition = PositionFactory.create(snapshot.getMaxReadPositionLedgerId(),
                        snapshot.getMaxReadPositionEntryId());
                this.unsealedTxnIds = convertTypeToTxnID(snapshot.getAborts());
                // Read snapshot segment to recover aborts
                final var snapshotSegmentTopicName = TopicName.get(TopicDomain.persistent.toString(),
                        TopicName.get(topic.getName()).getNamespaceObject(),
                        SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS);
                readSegmentEntries(snapshotSegmentTopicName, indexes);
                if (!this.indexes.isEmpty()) {
                    // If there is no segment index, the persistent worker will write segment begin from 0.
                    persistentWorker.sequenceID.set(this.indexes.get(this.indexes.lastKey()).sequenceID + 1);
                }
                unsealedTxnIds.forEach(txnID -> aborts.put(txnID, txnID));
                future.complete(startReadCursorPosition);
            } catch (Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    private void readSegmentEntries(TopicName topicName, TransactionBufferSnapshotIndexes indexes) throws Exception {
        final var managedLedger = openReadOnlyManagedLedger(topicName);
        boolean hasInvalidIndex = false;
        for (var index : indexes.getIndexList()) {
            final var position = PositionFactory.create(index.getSegmentLedgerID(), index.getSegmentEntryID());
            final var abortedPosition = PositionFactory.create(index.abortedMarkLedgerID, index.abortedMarkEntryID);
            try {
                final var entry = readEntry(managedLedger, position);
                try {
                    handleSnapshotSegmentEntry(entry);
                    this.indexes.put(abortedPosition, index);
                } finally {
                    entry.release();
                }
            } catch (Throwable throwable) {
                if (topic.getManagedLedger().getLedgersInfo()
                        .containsKey(index.getAbortedMarkLedgerID())) {
                    log.error("[{}] Failed to read snapshot segment [{}:{}]",
                            topic.getName(), index.segmentLedgerID,
                            index.segmentEntryID, throwable);
                    throw throwable;
                } else {
                    hasInvalidIndex = true;
                }
            }
        }
        if (hasInvalidIndex) {
            // Update the snapshot segment index if there exist invalid indexes.
            persistentWorker.appendTask(PersistentWorker.OperationType.UpdateIndex,
                    () -> persistentWorker.updateSnapshotIndex(indexes.getSnapshot()));
        }
    }

    private ReadOnlyManagedLedger openReadOnlyManagedLedger(TopicName topicName) throws Exception {
        final var future = new CompletableFuture<ReadOnlyManagedLedger>();
        final var callback = new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
            @Override
            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger, Object ctx) {
                future.complete(managedLedger);
            }

            @Override
            public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }

            @Override
            public String toString() {
                return String.format("Transaction buffer [%s] recover from snapshot",
                        SnapshotSegmentAbortedTxnProcessorImpl.this.topic.getName());
            }
        };
        topic.getBrokerService().getManagedLedgerFactoryForTopic(topicName).thenAccept(managedLedgerFactory ->
                        managedLedgerFactory.asyncOpenReadOnlyManagedLedger(topicName.getPersistenceNamingEncoding(),
                                callback, topic.getManagedLedger().getConfig(), null))
                .exceptionally(e -> {
                    future.completeExceptionally(e);
                    return null;
                });
        return wait(future, "open read only ml for " + topicName);
    }

    private Entry readEntry(ReadOnlyManagedLedger managedLedger, Position position) throws Exception {
        final var future = new CompletableFuture<Entry>();
        managedLedger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                future.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return wait(future, "read entry from " + position);
    }

    // This method will be deprecated and removed in version 4.x.0
    private Position recoverOldSnapshot() throws Exception {
        final var pulsar = topic.getBrokerService().getPulsar();
        final var topicName = TopicName.get(topic.getName());
        final var topics = wait(pulsar.getPulsarResources().getTopicResources().listPersistentTopicsAsync(
                NamespaceName.get(topicName.getNamespace())), "list persistent topics");
        if (!topics.contains(TopicDomain.persistent + "://" + topicName.getNamespace() + "/"
                + SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT)) {
            return null;
        }
        final var snapshot = pulsar.getTransactionBufferSnapshotServiceFactory().getTxnBufferSnapshotService()
                .getTableView().readLatest(topic.getName());
        if (snapshot == null) {
            return null;
        }
        handleOldSnapshot(snapshot);
        return PositionFactory.create(snapshot.getMaxReadPositionLedgerId(), snapshot.getMaxReadPositionEntryId());
    }

    // This method will be deprecated and removed in version 4.x.0
    private void handleOldSnapshot(TransactionBufferSnapshot snapshot) {
        if (snapshot.getAborts() != null) {
            snapshot.getAborts().forEach(abortTxnMetadata -> {
                TxnID txnID = new TxnID(abortTxnMetadata.getTxnIdMostBits(),
                        abortTxnMetadata.getTxnIdLeastBits());
                aborts.put(txnID, txnID);
                //The old data will be written into the first segment.
                unsealedTxnIds.add(txnID);
            });
        }
    }

    @Override
    public CompletableFuture<Void> clearAbortedTxnSnapshot() {
        return persistentWorker.appendTask(PersistentWorker.OperationType.Clear,
                persistentWorker::clearSnapshotSegmentAndIndexes);
    }

    public TransactionBufferStats generateSnapshotStats(boolean segmentStats) {
        TransactionBufferStats transactionBufferStats = new TransactionBufferStats();
        transactionBufferStats.totalAbortedTransactions = this.aborts.size();
        transactionBufferStats.lastSnapshotTimestamps = this.lastSnapshotTimestamps;
        SegmentsStats segmentsStats = new SegmentsStats();
        segmentsStats.currentSegmentCapacity = this.snapshotSegmentCapacity;
        segmentsStats.lastTookSnapshotSegmentTimestamp = this.lastTakedSnapshotSegmentTimestamp;
        segmentsStats.unsealedAbortTxnIDSize = this.unsealedTxnIds.size();
        segmentsStats.segmentsSize = indexes.size();
        if (segmentStats) {
            List<SegmentStats> statsList = new ArrayList<>();
            segmentIndex.forEach((position, txnID) -> {
                SegmentStats stats = new SegmentStats(txnID.toString(), position.toString());
                statsList.add(stats);
            });
            segmentsStats.segmentStats = statsList;
        }
        transactionBufferStats.segmentsStats = segmentsStats;
        return transactionBufferStats;
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
        segmentIndex.put(PositionFactory.create(snapshotSegment.getPersistentPositionLedgerId(),
                snapshotSegment.getPersistentPositionEntryId()),
                new TxnID(lastTxn.getMostSigBits(), lastTxn.getLeastSigBits()));
        convertTypeToTxnID(snapshotSegment.getAborts()).forEach(txnID -> aborts.put(txnID, txnID));
    }

    private long getSystemClientOperationTimeoutMs() throws Exception {
        PulsarClientImpl pulsarClient = (PulsarClientImpl) topic.getBrokerService().getPulsar().getClient();
        return pulsarClient.getConfiguration().getOperationTimeoutMs();
    }

    private <R> R wait(CompletableFuture<R> future, String msg) throws Exception {
        try {
            return future.get(getSystemClientOperationTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new CompletionException("Failed to " + msg, e.getCause());
        }
    }

    private <T> void closeReader(SystemTopicClient.Reader<T> reader) {
        reader.closeAsync().exceptionally(e -> {
            log.warn("[{}] Failed to close reader: {}", topic.getName(), e.getMessage());
            return null;
        });
    }

    /**
     * The PersistentWorker be responsible for executing the persistent tasks, including:
     * <p>
     *     1. Write snapshot segment --- Encapsulate a sealed snapshot segment and persistent it.
     *     2. Delete snapshot segment --- Evict expired snapshot segments.
     *     3. Update snapshot indexes --- Update snapshot indexes after writing or deleting snapshot segment
     *                                  or update snapshot indexes metadata regularly.
     *     4. Clear all snapshot segments and indexes.  --- Executed when deleting this topic.
     * </p>
     * * Task 1 and task 2 will be put into a task queue. The tasks in the queue will be executed in order.
     * * If the task queue is empty, task 3 will be executed immediately when it is appended to the worker.
     * Else, the worker will try to execute the tasks in the task queue.
     * * When task 4 was appended into worker, the worker will change the operation state to closed
     * and cancel all tasks in the task queue. finally, execute the task 4 (clear task).
     * If there are race conditions, throw an Exception to let users try again.
     */
    public class PersistentWorker {
        protected final AtomicLong sequenceID = new AtomicLong(0);

        private final PersistentTopic topic;

        //Persistent snapshot segment and index at the single thread.
        private final ReferenceCountedWriter<TransactionBufferSnapshotSegment> snapshotSegmentsWriter;
        private final ReferenceCountedWriter<TransactionBufferSnapshotIndexes> snapshotIndexWriter;

        private volatile boolean closed = false;

        private enum OperationState {
            None,
            Operating,
            Closed
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

        ConcurrentLinkedDeque<Pair<OperationType, Pair<CompletableFuture<Void>,
                Supplier<CompletableFuture<Void>>>>> taskQueue = new ConcurrentLinkedDeque<>();

        public PersistentWorker(PersistentTopic topic) {
            this.topic = topic;
            this.snapshotSegmentsWriter = this.topic.getBrokerService().getPulsar()
                    .getTransactionBufferSnapshotServiceFactory()
                    .getTxnBufferSnapshotSegmentService()
                    .getReferenceWriter(TopicName.get(topic.getName()).getNamespaceObject());
            this.snapshotSegmentsWriter.getFuture().exceptionally(ex -> {
                        log.error("{} Failed to create snapshot index writer", topic.getName());
                        topic.close();
                        return null;
                    });
            this.snapshotIndexWriter =  this.topic.getBrokerService().getPulsar()
                    .getTransactionBufferSnapshotServiceFactory()
                    .getTxnBufferSnapshotIndexService()
                    .getReferenceWriter(TopicName.get(topic.getName()).getNamespaceObject());
            this.snapshotIndexWriter.getFuture().exceptionally((ex) -> {
                        log.error("{} Failed to create snapshot writer", topic.getName());
                        topic.close();
                        return null;
                    });
        }

        public CompletableFuture<Void> appendTask(OperationType operationType,
                                                  Supplier<CompletableFuture<Void>> task) {
            CompletableFuture<Void> taskExecutedResult = new CompletableFuture<>();
            switch (operationType) {
                case UpdateIndex -> {
                    /*
                      The update index operation can be canceled when the task queue is not empty,
                      so it should be executed immediately instead of appending to the task queue.
                      If the taskQueue is not empty, the worker will execute the tasks in the queue.
                     */
                    if (!taskQueue.isEmpty()) {
                        executeTask();
                        return cancelUpdateIndexTask();
                    } else if (STATE_UPDATER.compareAndSet(this, OperationState.None, OperationState.Operating)) {
                        return task.get().whenComplete((ignore, throwable) -> {
                            if (throwable != null && log.isDebugEnabled()) {
                                log.debug("[{}] Failed to update index snapshot", topic.getName(), throwable);
                            }
                            STATE_UPDATER.compareAndSet(this, OperationState.Operating, OperationState.None);
                        });
                    } else {
                        return cancelUpdateIndexTask();
                    }
                }
                /*
                  Only the operations of WriteSegment and DeleteSegment will be appended into the taskQueue.
                  The operation will be canceled when the worker is close which means the topic is deleted.
                 */
                case WriteSegment, DeleteSegment -> {
                    if (!STATE_UPDATER.get(this).equals(OperationState.Closed)) {
                        taskQueue.add(new MutablePair<>(operationType, new MutablePair<>(taskExecutedResult, task)));
                        executeTask();
                        return taskExecutedResult;
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }
                case Clear -> {
                    /*
                      Do not clear the snapshots if the topic is used.
                      If the users want to delete a topic, they should stop the usage of the topic.
                     */
                    if (STATE_UPDATER.compareAndSet(this, OperationState.None, OperationState.Closed)) {
                        taskQueue.forEach(pair ->
                                pair.getRight().getRight().get().completeExceptionally(
                                        new BrokerServiceException.ServiceUnitNotReadyException(
                                                String.format("Cancel the operation [%s] due to the"
                                                                + " transaction buffer of the topic[%s] already closed",
                                                        pair.getLeft().name(), this.topic.getName()))));
                        taskQueue.clear();
                        /*
                          The task of clear all snapshot segments and indexes is executed immediately.
                         */
                        return task.get();
                    } else {
                        return FutureUtil.failedFuture(
                                new BrokerServiceException.NotAllowedException(
                                        String.format("Failed to clear the snapshot of topic [%s] due to "
                                                + "the topic is used. Please stop the using of the topic "
                                                + "and try it again", this.topic.getName())));
                    }
                }
                default -> {
                    return FutureUtil.failedFuture(new BrokerServiceException
                            .NotAllowedException(String.format("Th operation [%s] is unsupported",
                            operationType.name())));
                }
            }
        }

        private CompletableFuture<Void> cancelUpdateIndexTask() {
            if (log.isDebugEnabled()) {
                log.debug("The operation of updating index is canceled due there is other operation executing");
            }
            return FutureUtil.failedFuture(new BrokerServiceException
                    .ServiceUnitNotReadyException("The operation of updating index is canceled"));
        }

        private void executeTask() {
            if (taskQueue.isEmpty()) {
                return;
            }
            if (STATE_UPDATER.compareAndSet(this, OperationState.None, OperationState.Operating)) {
                //Double-check. Avoid NoSuchElementException due to the first task is completed by other thread.
                if (taskQueue.isEmpty()) {
                    return;
                }
                Pair<OperationType, Pair<CompletableFuture<Void>, Supplier<CompletableFuture<Void>>>> firstTask =
                        taskQueue.getFirst();
                firstTask.getValue().getRight().get().whenComplete((ignore, throwable) -> {
                    if (throwable != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Failed to do operation do operation of [{}]",
                                    topic.getName(), firstTask.getKey().name(), throwable);
                        }
                        //Do not execute the tasks in the task queue until the next task is appended to the task queue.
                        firstTask.getRight().getKey().completeExceptionally(throwable);
                    } else {
                        firstTask.getRight().getKey().complete(null);
                        taskQueue.removeFirst();
                        //Execute the next task in the other thread.
                        topic.getBrokerService().getPulsar().getTransactionExecutorProvider()
                                .getExecutor(this).submit(this::executeTask);
                    }
                    STATE_UPDATER.compareAndSet(this, OperationState.Operating,
                            OperationState.None);
                });
            }
        }

        private CompletableFuture<Void> takeSnapshotSegmentAsync(LinkedList<TxnID> sealedAbortedTxnIdSegment,
                                                                 Position abortedMarkerPersistentPosition) {
            CompletableFuture<Void> res =  writeSnapshotSegmentAsync(sealedAbortedTxnIdSegment,
                    abortedMarkerPersistentPosition).thenRun(() -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Successes to take snapshot segment [{}] at maxReadPosition [{}] "
                                            + "for the topic [{}], and the size of the segment is [{}]",
                                    this.sequenceID, abortedMarkerPersistentPosition, topic.getName(),
                                    sealedAbortedTxnIdSegment.size());
                        }
                        this.sequenceID.getAndIncrement();
                    });
            res.exceptionally(e -> {
                //Just log the error, and the processor will try to take snapshot again when the transactionBuffer
                //append aborted txn next time.
                log.error("Failed to take snapshot segment [{}] at maxReadPosition [{}] "
                                + "for the topic [{}], and the size of the segment is [{}]",
                        this.sequenceID, abortedMarkerPersistentPosition, topic.getName(),
                        sealedAbortedTxnIdSegment.size(), e);
                return null;
            });
            return res;
        }

        private CompletableFuture<Void> writeSnapshotSegmentAsync(LinkedList<TxnID> segment,
                                                                  Position abortedMarkerPersistentPosition) {
            TransactionBufferSnapshotSegment transactionBufferSnapshotSegment = new TransactionBufferSnapshotSegment();
            transactionBufferSnapshotSegment.setAborts(convertTypeToTxnIDData(segment));
            transactionBufferSnapshotSegment.setTopicName(this.topic.getName());
            transactionBufferSnapshotSegment.setPersistentPositionEntryId(abortedMarkerPersistentPosition.getEntryId());
            transactionBufferSnapshotSegment.setPersistentPositionLedgerId(
                    abortedMarkerPersistentPosition.getLedgerId());

            return snapshotSegmentsWriter.getFuture().thenCompose(segmentWriter -> {
                transactionBufferSnapshotSegment.setSequenceId(this.sequenceID.get());
                return segmentWriter.writeAsync(buildKey(this.sequenceID.get()), transactionBufferSnapshotSegment);
            }).thenCompose((messageId) -> {
                lastTakedSnapshotSegmentTimestamp = System.currentTimeMillis();
                //Build index for this segment
                TransactionBufferSnapshotIndex index = new TransactionBufferSnapshotIndex();
                index.setSequenceID(transactionBufferSnapshotSegment.getSequenceId());
                index.setAbortedMarkLedgerID(abortedMarkerPersistentPosition.getLedgerId());
                index.setAbortedMarkEntryID(abortedMarkerPersistentPosition.getEntryId());
                index.setSegmentLedgerID(((MessageIdImpl) messageId).getLedgerId());
                index.setSegmentEntryID(((MessageIdImpl) messageId).getEntryId());

                indexes.put(abortedMarkerPersistentPosition, index);
                //update snapshot segment index.
                //If the index can not be written successfully, the snapshot segment wil be overwritten
                //when the processor writes snapshot segment next time.
                //And if the task is not the newest in the queue, it is no need to update the index.
                return updateIndexWhenExecuteTheLatestTask();
            });
        }

        private CompletionStage<Void> updateIndexWhenExecuteTheLatestTask() {
            Position maxReadPosition = topic.getMaxReadPosition();
            List<TxnIDData> aborts = convertTypeToTxnIDData(unsealedTxnIds);
            if (taskQueue.size() != 1) {
                return CompletableFuture.completedFuture(null);
            } else {
                return updateSnapshotIndex(new TransactionBufferSnapshotIndexesMetadata(
                        maxReadPosition.getLedgerId(), maxReadPosition.getEntryId(), aborts));
            }
        }

        // update index after delete all segment.
        private CompletableFuture<Void> deleteSnapshotSegment(List<Position> positionNeedToDeletes) {
            List<CompletableFuture<Void>> results = new ArrayList<>();
            for (Position positionNeedToDelete : positionNeedToDeletes) {
                long sequenceIdNeedToDelete = indexes.get(positionNeedToDelete).getSequenceID();
                CompletableFuture<Void> res = snapshotSegmentsWriter.getFuture()
                        .thenCompose(writer -> writer.deleteAsync(buildKey(sequenceIdNeedToDelete), null))
                        .thenCompose(messageId -> {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Successes to delete the snapshot segment, "
                                                + "whose sequenceId is [{}] and maxReadPosition is [{}]",
                                        this.topic.getName(), this.sequenceID, positionNeedToDelete);
                            }
                            //The index may fail to update but the processor will check
                            //whether the snapshot segment is null, and update the index when recovering.
                            //And if the task is not the newest in the queue, it is no need to update the index.
                            indexes.remove(positionNeedToDelete);
                            return updateIndexWhenExecuteTheLatestTask();
                        });
                res.exceptionally(e -> {
                    log.warn("[{}] Failed to delete the snapshot segment, "
                                    + "whose sequenceId is [{}] and maxReadPosition is [{}]",
                            this.topic.getName(), this.sequenceID, positionNeedToDelete, e);
                    return null;
                });
                results.add(res);
            }
            return FutureUtil.waitForAll(results);
        }

        private CompletableFuture<Void> updateSnapshotIndex(TransactionBufferSnapshotIndexesMetadata snapshotSegment) {
            TransactionBufferSnapshotIndexes snapshotIndexes = new TransactionBufferSnapshotIndexes();
            CompletableFuture<Void> res = snapshotIndexWriter.getFuture()
                    .thenCompose((indexesWriter) -> {
                        snapshotIndexes.setIndexList(indexes.values().stream().toList());
                        snapshotIndexes.setSnapshot(snapshotSegment);
                        snapshotIndexes.setTopicName(topic.getName());
                        return indexesWriter.writeAsync(topic.getName(), snapshotIndexes)
                                .thenCompose(messageId -> CompletableFuture.completedFuture(null));
                    });
            res.thenRun(() -> lastSnapshotTimestamps = System.currentTimeMillis()).exceptionally(e -> {
                log.error("[{}] Failed to update snapshot segment index", snapshotIndexes.getTopicName(), e);
                return null;
            });
            return res;
        }

        private CompletableFuture<Void> clearSnapshotSegmentAndIndexes() {
            CompletableFuture<Void> res = persistentWorker.clearAllSnapshotSegments()
                    .thenCompose((ignore) -> snapshotIndexWriter.getFuture()
                            .thenCompose(indexesWriter -> indexesWriter.writeAsync(topic.getName(), null)))
                    .thenRun(() ->
                            log.debug("Successes to clear the snapshot segment and indexes for the topic [{}]",
                                    topic.getName()));
            res.exceptionally(e -> {
                log.error("Failed to clear the snapshot segment and indexes for the topic [{}]",
                        topic.getName(), e);
                return null;
            });
            return res;
        }

        /**
         * Because the operation of writing segment and index is not atomic,
         * we cannot use segment index to clear the snapshot segments.
         * If we use segment index to clear snapshot segments, there will case dirty data in the below case:
         * <p>
         *     1. Write snapshot segment 1, 2, 3, update index (1, 2, 3)
         *     2. Write snapshot 4, failing to update index
         *     3. Trim expired snapshot segment 1, 2, 3, update index (empty)
         *     4. Write snapshot segment 1, 2, update  index (1, 2)
         *     5. Delete topic, clear all snapshot segment (segment1. segment2).
         *     Segment 3 and segment 4 can not be cleared until this namespace being deleted.
         * </p>
         */
        private CompletableFuture<Void> clearAllSnapshotSegments() {
            final var future = new CompletableFuture<Void>();
            final var pulsar = topic.getBrokerService().getPulsar();
            pulsar.getTransactionExecutorProvider().getExecutor(this).execute(() -> {
                try {
                    final var reader = wait(pulsar.getTransactionBufferSnapshotServiceFactory()
                            .getTxnBufferSnapshotSegmentService().createReader(TopicName.get(topic.getName()))
                            , "create reader");
                    try {
                        while (wait(reader.hasMoreEventsAsync(), "has more events")) {
                            final var message = wait(reader.readNextAsync(), "read next");
                            if (topic.getName().equals(message.getValue().getTopicName())) {
                                snapshotSegmentsWriter.getFuture().get().write(message.getKey(), null);
                            }
                        }
                        future.complete(null);
                    } finally {
                        closeReader(reader);
                    }
                } catch (Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
            return future;
        }

        private <R> R wait(CompletableFuture<R> future, String msg) throws Exception {
            try {
                return future.get(getSystemClientOperationTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                throw new CompletionException("Failed to " + msg, e.getCause());
            }
        }

        synchronized CompletableFuture<Void> closeAsync() {
            if (!closed) {
                closed = true;
                snapshotSegmentsWriter.release();
                snapshotIndexWriter.release();
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private LinkedList<TxnID> convertTypeToTxnID(List<TxnIDData> snapshotSegment) {
        LinkedList<TxnID> abortedTxns = new LinkedList<>();
        snapshotSegment.forEach(txnIDData ->
                abortedTxns.add(new TxnID(txnIDData.getMostSigBits(), txnIDData.getLeastSigBits())));
        return abortedTxns;
    }

    private List<TxnIDData> convertTypeToTxnIDData(List<TxnID> abortedTxns) {
        List<TxnIDData> segment = new LinkedList<>();
        abortedTxns.forEach(txnID -> segment.add(new TxnIDData(txnID.getMostSigBits(), txnID.getLeastSigBits())));
        return segment;
    }

}
