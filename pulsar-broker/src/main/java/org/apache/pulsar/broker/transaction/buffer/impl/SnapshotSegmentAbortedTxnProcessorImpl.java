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
import java.util.concurrent.CompletionStage;
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
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
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
     *     The key PositionImpl {@link PositionImpl} is the persistent position of
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
    private final LinkedMap<PositionImpl, TxnID> segmentIndex = new LinkedMap<>();

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
    private final LinkedMap<PositionImpl, TransactionBufferSnapshotIndex> indexes = new LinkedMap<>();

    private final PersistentTopic topic;

    private volatile long lastSnapshotTimestamps;

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
    public void putAbortedTxnAndPosition(TxnID txnID, PositionImpl position) {
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
     * Check werther the position in segmentIndex {@link SnapshotSegmentAbortedTxnProcessorImpl#segmentIndex}
     * is expired. If the position is not exist in the original topic, the according transaction is an invalid
     * transaction. And the according segment is invalid, too. The transaction IDs before the transaction ID
     * in the aborts are invalid, too.
     */
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
    public CompletableFuture<Void> takeAbortedTxnsSnapshot(PositionImpl maxReadPosition) {
        //Store the latest aborted transaction IDs in unsealedTxnIDs and the according the latest max read position.
        TransactionBufferSnapshotIndexesMetadata metadata = new TransactionBufferSnapshotIndexesMetadata(
                maxReadPosition.getLedgerId(), maxReadPosition.getEntryId(),
                convertTypeToTxnIDData(unsealedTxnIds));
        return persistentWorker.appendTask(PersistentWorker.OperationType.UpdateIndex,
                () -> persistentWorker.updateSnapshotIndex(metadata));
    }

    @Override
    public CompletableFuture<PositionImpl> recoverFromSnapshot() {
        return topic.getBrokerService().getPulsar().getTransactionBufferSnapshotServiceFactory()
                .getTxnBufferSnapshotIndexService()
                .createReader(TopicName.get(topic.getName())).thenComposeAsync(reader -> {
                    PositionImpl startReadCursorPosition = null;
                    TransactionBufferSnapshotIndexes persistentSnapshotIndexes = null;
                    try {
                        /*
                          Read the transaction snapshot segment index.
                          <p>
                              The processor can get the sequence ID, unsealed transaction IDs,
                              segment index list and max read position in the snapshot segment index.
                              Then we can traverse the index list to read all aborted transaction IDs
                              in segments to aborts.
                          </p>
                         */
                        while (reader.hasMoreEvents()) {
                            Message<TransactionBufferSnapshotIndexes> message = reader.readNextAsync()
                                    .get(getSystemClientOperationTimeoutMs(), TimeUnit.MILLISECONDS);
                            if (topic.getName().equals(message.getKey())) {
                                TransactionBufferSnapshotIndexes transactionBufferSnapshotIndexes = message.getValue();
                                if (transactionBufferSnapshotIndexes != null) {
                                    persistentSnapshotIndexes = transactionBufferSnapshotIndexes;
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
                    TransactionBufferSnapshotIndexes finalPersistentSnapshotIndexes = persistentSnapshotIndexes;
                    if (persistentSnapshotIndexes == null) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        this.unsealedTxnIds = convertTypeToTxnID(persistentSnapshotIndexes
                                .getSnapshot().getAborts());
                    }
                    //Read snapshot segment to recover aborts.
                    ArrayList<CompletableFuture<Void>> completableFutures = new ArrayList<>();
                    CompletableFuture<Void> openManagedLedgerAndHandleSegmentsFuture = new CompletableFuture<>();
                    AtomicBoolean hasInvalidIndex = new AtomicBoolean(false);
                    AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback = new AsyncCallbacks
                            .OpenReadOnlyManagedLedgerCallback() {
                        @Override
                        public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedgerImpl readOnlyManagedLedger,
                                                                      Object ctx) {
                            finalPersistentSnapshotIndexes.getIndexList().forEach(index -> {
                                CompletableFuture<Void> handleSegmentFuture = new CompletableFuture<>();
                                completableFutures.add(handleSegmentFuture);
                                readOnlyManagedLedger.asyncReadEntry(
                                        new PositionImpl(index.getSegmentLedgerID(),
                                                index.getSegmentEntryID()),
                                        new AsyncCallbacks.ReadEntryCallback() {
                                            @Override
                                            public void readEntryComplete(Entry entry, Object ctx) {
                                                handleSnapshotSegmentEntry(entry);
                                                indexes.put(new PositionImpl(
                                                                index.abortedMarkLedgerID,
                                                                index.abortedMarkEntryID),
                                                        index);
                                                entry.release();
                                                handleSegmentFuture.complete(null);
                                            }

                                            @Override
                                            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                                                /*
                                                  The logic flow of deleting expired segment is:
                                                  <p>
                                                      1. delete segment
                                                      2. update segment index
                                                  </p>
                                                  If the worker delete segment successfully
                                                  but failed to update segment index,
                                                  the segment can not be read according to the index.
                                                  We update index again if there are invalid indexes.
                                                 */
                                                if (((ManagedLedgerImpl) topic.getManagedLedger())
                                                        .ledgerExists(index.getAbortedMarkLedgerID())) {
                                                    log.error("[{}] Failed to read snapshot segment [{}:{}]",
                                                            topic.getName(), index.segmentLedgerID,
                                                            index.segmentEntryID, exception);
                                                    handleSegmentFuture.completeExceptionally(exception);
                                                } else {
                                                    hasInvalidIndex.set(true);
                                                }
                                            }
                                        }, null);
                            });
                            openManagedLedgerAndHandleSegmentsFuture.complete(null);
                        }

                        @Override
                        public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                            log.error("[{}] Failed to open readOnly managed ledger", topic, exception);
                            openManagedLedgerAndHandleSegmentsFuture.completeExceptionally(exception);
                        }
                    };

                    TopicName snapshotSegmentTopicName = TopicName.get(TopicDomain.persistent.toString(),
                            TopicName.get(topic.getName()).getNamespaceObject(),
                            SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS);
                    this.topic.getBrokerService().getPulsar().getManagedLedgerFactory()
                            .asyncOpenReadOnlyManagedLedger(snapshotSegmentTopicName
                                            .getPersistenceNamingEncoding(), callback,
                                    topic.getManagedLedger().getConfig(),
                                    null);
                    /*
                       Wait the processor recover completely and then allow TB
                       to recover the messages after the startReadCursorPosition.
                     */
                    return openManagedLedgerAndHandleSegmentsFuture
                            .thenCompose((ignore) -> FutureUtil.waitForAll(completableFutures))
                            .thenCompose((i) -> {
                                /*
                                  Update the snapshot segment index if there exist invalid indexes.
                                 */
                                if (hasInvalidIndex.get()) {
                                    persistentWorker.appendTask(PersistentWorker.OperationType.UpdateIndex,
                                            () -> persistentWorker.updateSnapshotIndex(
                                                    finalPersistentSnapshotIndexes.getSnapshot()));
                                }
                                /*
                                   If there is no segment index, the persistent worker will write segment begin from 0.
                                 */
                                if (indexes.size() != 0) {
                                    persistentWorker.sequenceID.set(indexes.get(indexes.lastKey()).sequenceID + 1);
                                }
                                /*
                                  Append the aborted txn IDs in the index metadata
                                  can keep the order of the aborted txn in the aborts.
                                  So that we can trim the expired snapshot segment in aborts
                                  according to the latest transaction IDs in the segmentIndex.
                                 */
                                unsealedTxnIds.forEach(txnID -> aborts.put(txnID, txnID));
                                return CompletableFuture.completedFuture(finalStartReadCursorPosition);
                            }).exceptionally(ex -> {
                                log.error("[{}] Failed to recover snapshot segment", this.topic.getName(), ex);
                                return null;
                            });

                    },  topic.getBrokerService().getPulsar().getTransactionExecutorProvider()
                        .getExecutor(this));
    }

    @Override
    public CompletableFuture<Void> clearAbortedTxnSnapshot() {
        return persistentWorker.appendTask(PersistentWorker.OperationType.Clear,
                persistentWorker::clearSnapshotSegmentAndIndexes);
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
        private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotSegment>>
                snapshotSegmentsWriterFuture;
        private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotIndexes>>
                snapshotIndexWriterFuture;

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
            this.snapshotSegmentsWriterFuture =  this.topic.getBrokerService().getPulsar()
                    .getTransactionBufferSnapshotServiceFactory()
                    .getTxnBufferSnapshotSegmentService().createWriter(TopicName.get(topic.getName()));
            this.snapshotSegmentsWriterFuture.exceptionally(ex -> {
                        log.error("{} Failed to create snapshot index writer", topic.getName());
                        topic.close();
                        return null;
                    });
            this.snapshotIndexWriterFuture =  this.topic.getBrokerService().getPulsar()
                    .getTransactionBufferSnapshotServiceFactory()
                    .getTxnBufferSnapshotIndexService().createWriter(TopicName.get(topic.getName()));
            this.snapshotIndexWriterFuture.exceptionally((ex) -> {
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
                                                                 PositionImpl abortedMarkerPersistentPosition) {
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
                                                                  PositionImpl abortedMarkerPersistentPosition) {
            TransactionBufferSnapshotSegment transactionBufferSnapshotSegment = new TransactionBufferSnapshotSegment();
            transactionBufferSnapshotSegment.setAborts(convertTypeToTxnIDData(segment));
            transactionBufferSnapshotSegment.setTopicName(this.topic.getName());
            transactionBufferSnapshotSegment.setPersistentPositionEntryId(abortedMarkerPersistentPosition.getEntryId());
            transactionBufferSnapshotSegment.setPersistentPositionLedgerId(
                    abortedMarkerPersistentPosition.getLedgerId());

            return snapshotSegmentsWriterFuture.thenCompose(segmentWriter -> {
                transactionBufferSnapshotSegment.setSequenceId(this.sequenceID.get());
                return segmentWriter.writeAsync(buildKey(this.sequenceID.get()), transactionBufferSnapshotSegment);
            }).thenCompose((messageId) -> {
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
            PositionImpl maxReadPosition = topic.getMaxReadPosition();
            List<TxnIDData> aborts = convertTypeToTxnIDData(unsealedTxnIds);
            if (taskQueue.size() != 1) {
                return CompletableFuture.completedFuture(null);
            } else {
                return updateSnapshotIndex(new TransactionBufferSnapshotIndexesMetadata(
                        maxReadPosition.getLedgerId(), maxReadPosition.getEntryId(), aborts));
            }
        }

        // update index after delete all segment.
        private CompletableFuture<Void> deleteSnapshotSegment(List<PositionImpl> positionNeedToDeletes) {
            List<CompletableFuture<Void>> results = new ArrayList<>();
            for (PositionImpl positionNeedToDelete : positionNeedToDeletes) {
                long sequenceIdNeedToDelete = indexes.get(positionNeedToDelete).getSequenceID();
                CompletableFuture<Void> res = snapshotSegmentsWriterFuture
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
            CompletableFuture<Void> res = snapshotIndexWriterFuture
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
                    .thenCompose((ignore) -> snapshotIndexWriterFuture
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
            return topic.getBrokerService().getPulsar().getTransactionBufferSnapshotServiceFactory()
                    .getTxnBufferSnapshotSegmentService()
                    .createReader(TopicName.get(topic.getName())).thenComposeAsync(reader -> {
                        try {
                            while (reader.hasMoreEvents()) {
                                Message<TransactionBufferSnapshotSegment> message = reader.readNextAsync()
                                        .get(getSystemClientOperationTimeoutMs(), TimeUnit.MILLISECONDS);
                                if (topic.getName().equals(message.getValue().getTopicName())) {
                                   snapshotSegmentsWriterFuture.get().write(message.getKey(), null);
                                }
                            }
                            return CompletableFuture.completedFuture(null);
                        } catch (Exception ex) {
                            log.error("[{}] Transaction buffer clear snapshot segments fail!", topic.getName(), ex);
                            return FutureUtil.failedFuture(ex);
                        } finally {
                            closeReader(reader);
                        }
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