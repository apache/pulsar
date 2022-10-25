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
import io.netty.buffer.Unpooled;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
    private final AtomicLong sequenceID = new AtomicLong(0);

    private final ConcurrentSkipListMap<PositionImpl, List<TxnIDData>> aborts
            = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<PositionImpl, List<TxnIDData>> snapshotSegmentQueue
            = new ConcurrentSkipListMap<>();

    private final ConcurrentSkipListMap<PositionImpl, TransactionBufferSnapshotIndex> indexes
            = new ConcurrentSkipListMap<>();

    private TransactionBufferSnapshotIndexes theLatestSnapshotIndexes = new TransactionBufferSnapshotIndexes();
    private final Timer timer;
    private PositionImpl maxReadPosition;

    private final PersistentTopic topic;

    private CopyOnWriteArrayList<TxnIDData> lastAbortedTxnIDs = new CopyOnWriteArrayList<>();

    //When add abort or change max read position, the count will +1. Take snapshot will set 0 into it.
    private final AtomicLong changeMaxReadPositionAndAddAbortTimes = new AtomicLong();

    private volatile long lastSnapshotTimestamps;

    private final int takeSnapshotIntervalNumber;

    private final int takeSnapshotIntervalTime;

    private final int transactionBufferMaxAbortedTxnsOfSnapshotSegment;

    private final Semaphore segmentUpdateSemaphore = new Semaphore(1);

    private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotSegment>>
            snapshotSegmentsWriterFuture;
    private final CompletableFuture<SystemTopicClient.Writer<TransactionBufferSnapshotIndexes>>
            snapshotIndexWriterFuture;

    public SnapshotSegmentAbortedTxnProcessorImpl(PersistentTopic topic) {
        this.topic = topic;
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
        lastAbortedTxnIDs.add(abortedTxnId);
        //The size of lastAbortedTxns reaches the configuration of the size of snapshot segment.
        if (lastAbortedTxnIDs.size() == transactionBufferMaxAbortedTxnsOfSnapshotSegment) {
            changeMaxReadPositionAndAddAbortTimes.set(0);
            aborts.put(position, lastAbortedTxnIDs);
            //Guarantee the order of the segments.
            snapshotSegmentQueue.put(position, lastAbortedTxnIDs);
            takeSnapshotSegment();
            lastAbortedTxnIDs = new CopyOnWriteArrayList<>();
        }
    }

    private void takeSnapshotSegment() {
        //Only one segment can be written at the same time.
        if (segmentUpdateSemaphore.tryAcquire()) {
            CopyOnWriteArrayList<TxnIDData> abortedTxns =
                    (CopyOnWriteArrayList<TxnIDData>) snapshotSegmentQueue.firstEntry().getValue();
            PositionImpl maxReadPosition = snapshotSegmentQueue.firstKey();

            takeSnapshotSegmentAsync(abortedTxns, maxReadPosition).thenRun(() -> {
                if (log.isDebugEnabled()) {
                    log.debug("Successes to take snapshot segment [{}] at maxReadPosition [{}] "
                                    + "for the topic [{}], and the size of the segment is [{}]",
                            sequenceID, maxReadPosition, topic.getName(), abortedTxns.size());
                }
                sequenceID.getAndIncrement();
            }).exceptionally(e -> {
                //Just log the error, and the processor will try to take snapshot again when the transactionBuffer
                //append aborted txn nex time.
                log.error("Failed to take snapshot segment [{}] at maxReadPosition [{}] "
                                + "for the topic [{}], and the size of the segment is [{}]",
                        sequenceID, maxReadPosition, topic.getName(), abortedTxns.size(), e);
                //Try again
                timer.newTimeout((ignore) -> takeSnapshotSegment(), takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
                return null;
            });
        }
    }


    @Override
    public void updateMaxReadPosition(Position position) {
        if (position != this.maxReadPosition) {
            this.maxReadPosition = (PositionImpl) position;
            updateSnapshotMetadataByChangeTimes();
        }
    }

    @Override
    public boolean checkAbortedTransaction(TxnIDData txnID, Position readPosition) {
        if (readPosition == null) {
            return aborts.values().stream().anyMatch(list -> list.contains(txnID)) || lastAbortedTxnIDs.contains(txnID);
        }
        List<TxnIDData> txnIDSet = aborts.ceilingEntry((PositionImpl) readPosition).getValue();
        if (txnIDSet == null) {
            return lastAbortedTxnIDs.contains(txnID);
        } else {
            return txnIDSet.contains(txnID);
        }
    }

    @Override
    public void trimExpiredTxnIDDataOrSnapshotSegments() {
        //Checking whether there are some segment expired.
        while (!aborts.isEmpty() && !((ManagedLedgerImpl) topic.getManagedLedger())
                .ledgerExists(aborts.firstKey().getLedgerId())) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Topic transaction buffer clear aborted transactions, maxReadPosition : {}",
                        topic.getName(), aborts.firstKey());
            }
            PositionImpl positionNeedToDelete = aborts.firstKey();
            clearSnapshotSegment(positionNeedToDelete);
        }
    }

    private String buildKey(long sequenceId) {
        return "multiple-" + sequenceId + this.topic.getName();
    }

    private void updateSnapshotMetadataByChangeTimes() {
        if (this.changeMaxReadPositionAndAddAbortTimes.incrementAndGet() == takeSnapshotIntervalNumber) {
            changeMaxReadPositionAndAddAbortTimes.set(0);
            updateIndexMetadataForTheLastSnapshot();
        }
    }

    private void takeSnapshotByTimeout() {
        if (changeMaxReadPositionAndAddAbortTimes.get() > 0) {
            updateIndexMetadataForTheLastSnapshot();
        }
        timer.newTimeout(SnapshotSegmentAbortedTxnProcessorImpl.this,
                takeSnapshotIntervalTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run(Timeout timeout) {
        takeSnapshotByTimeout();
    }

    private CompletableFuture<Void> takeSnapshotSegmentAsync(List<TxnIDData> segment, PositionImpl maxReadPosition) {
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
            return updateSnapshotIndex();
        });
    }

    //Update the indexes in the transactionBufferSnapshotIndexe.
    //Concurrency control is performed by snapshotIndexWriterFuture.
    private CompletableFuture<Void> updateSnapshotIndex() {
        TransactionBufferSnapshotIndexes snapshotIndexes = new TransactionBufferSnapshotIndexes();
        return snapshotIndexWriterFuture
                .thenCompose((indexesWriter) -> {
                    snapshotIndexes.setIndexList(indexes.values().stream().toList());
                    //Only update the index in indexes and keep the metadata in indexes unchanged.
                    snapshotIndexes.setSnapshot(theLatestSnapshotIndexes.getSnapshot());
                    return indexesWriter.writeAsync(topic.getName(), snapshotIndexes);
                })
                .thenRun(() -> {
                    theLatestSnapshotIndexes.setIndexList(snapshotIndexes.getIndexList());
                    this.lastSnapshotTimestamps = System.currentTimeMillis();
                })
                .exceptionally(e -> {
                    log.error("[{}] Failed to update snapshot segment index", snapshotIndexes.getTopicName(), e);
                    return null;
                });
    }

    //Update the metadata in the transactionBufferSnapshotIndexes.
    //Concurrency control is performed by snapshotIndexWriterFuture.
    private CompletableFuture<Void> updateIndexMetadataForTheLastSnapshot() {
        TransactionBufferSnapshotIndexes indexes = new TransactionBufferSnapshotIndexes();
        return snapshotIndexWriterFuture
                .thenCompose((indexesWriter) -> {
                    //Store the latest metadata
                    TransactionBufferSnapshotSegment transactionBufferSnapshotSegment =
                            new TransactionBufferSnapshotSegment();
                    transactionBufferSnapshotSegment.setAborts(lastAbortedTxnIDs);
                    indexes.setSnapshot(transactionBufferSnapshotSegment);
                    //Only update the metadata in indexes and keep the index in indexes unchanged.
                    indexes.setIndexList(theLatestSnapshotIndexes.getIndexList());
                    return indexesWriter.writeAsync(topic.getName(), indexes);
                })
                .thenRun(() -> {
                    theLatestSnapshotIndexes.setSnapshot(indexes.getSnapshot());
                    this.lastSnapshotTimestamps = System.currentTimeMillis();
                })
                .exceptionally(e -> {
                    log.error("[{}] Failed to update snapshot segment index", indexes.getTopicName(), e);
                    return null;
                });
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
                                    this.theLatestSnapshotIndexes = transactionBufferSnapshotIndexes;
                                    startReadCursorPosition = PositionImpl.get(
                                            transactionBufferSnapshotIndexes.getSnapshot().getMaxReadPositionLedgerId(),
                                            transactionBufferSnapshotIndexes.getSnapshot().getMaxReadPositionEntryId());
                                }
                            }
                        }
                        closeReader(reader);
                        if (!hasIndex) {
                            callBack.noNeedToRecover();
                            return null;
                        } else {
                            theLatestSnapshotIndexes.getIndexList()
                                    .forEach(transactionBufferSnapshotIndex ->
                                            indexes.put(new PositionImpl(
                                                    transactionBufferSnapshotIndex.persistentPositionLedgerID,
                                                            transactionBufferSnapshotIndex.persistentPositionEntryID),
                                                    transactionBufferSnapshotIndex));
                            this.lastAbortedTxnIDs = (CopyOnWriteArrayList<TxnIDData>) theLatestSnapshotIndexes
                                    .getSnapshot().getAborts();
                            this.maxReadPosition = new PositionImpl(theLatestSnapshotIndexes
                                    .getSnapshot().getMaxReadPositionLedgerId(),
                                    theLatestSnapshotIndexes.getSnapshot().getMaxReadPositionEntryId());
                            sequenceID.set(indexes.lastEntry().getValue().sequenceID + 1);
                        }
                        //Read snapshot segment to recover aborts.
                        LinkedList<CompletableFuture<Void>> completableFutures = new LinkedList<>();
                        AtomicLong invalidIndex = new AtomicLong(0);
                        AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback = new AsyncCallbacks
                                .OpenReadOnlyManagedLedgerCallback() {
                            @Override
                            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedgerImpl readOnlyManagedLedger, Object ctx) {
                                theLatestSnapshotIndexes.getIndexList().forEach(index -> {
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
                        if (invalidIndex.get() != 0) {
                            updateSnapshotIndex();
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
        ArrayList<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        while (!aborts.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Topic transaction buffer clear aborted transactions, maxReadPosition : {}",
                        topic.getName(), aborts.firstKey());
            }
            PositionImpl positionNeedToDelete = aborts.firstKey();
            completableFutures.add(clearSnapshotSegment(positionNeedToDelete));
        }
        return FutureUtil.waitForAll(completableFutures)
                .thenCompose((ignore) -> snapshotIndexWriterFuture
                        .thenCompose(indexesWriter -> indexesWriter.writeAsync(topic.getName(), null)))
                .thenRun(() -> log.info("Successes to clear the snapshot segment and indexes for the topic [{}]",
                        topic.getName()))
                .exceptionally(e -> {
                    log.error("Failed to clear the snapshot segment and indexes for the topic [{}]",
                            topic.getName(), e);
                    return null;
                });
    }

    private CompletableFuture<Void> clearSnapshotSegment(PositionImpl positionNeedToDelete) {

        long sequenceIdNeedToDelete = indexes.get(positionNeedToDelete).getSequenceID();
        return snapshotSegmentsWriterFuture.thenCompose(writer -> writer.deleteAsync(buildKey(sequenceIdNeedToDelete), null))
                .thenRun(() -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Successes to delete the snapshot segment, "
                                        + "whose sequenceId is [{}] and maxReadPosition is [{}]",
                                this.topic.getName(), this.sequenceID, this.maxReadPosition);
                    }
                    aborts.remove(positionNeedToDelete);
                    indexes.remove(positionNeedToDelete);
                    //The process will check whether the snapshot segment is null, and update index when recovered.
                    updateSnapshotIndex();
                }).exceptionally(e -> {
                    log.error("[{}] Failed to delete the snapshot segment, "
                                    + "whose sequenceId is [{}] and maxReadPosition is [{}]",
                            this.topic.getName(), this.sequenceID, this.maxReadPosition);
                    return null;
                });
    }

    @Override
    public CompletableFuture<Void> takesFirstSnapshot() {
        return updateIndexMetadataForTheLastSnapshot();
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
        aborts.put(new PositionImpl(snapshotSegment.getMaxReadPositionLedgerId(),
                snapshotSegment.getMaxReadPositionEntryId()), snapshotSegment.getAborts());

    }

    private <T> void  closeReader(SystemTopicClient.Reader<T> reader) {
        reader.closeAsync().exceptionally(e -> {
            log.error("[{}]Transaction buffer snapshot reader close error!", topic.getName(), e);
            return null;
        });
    }
}