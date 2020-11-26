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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReplayCallback;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;

import static org.apache.pulsar.common.protocol.Markers.isTxnMarker;

/**
 * Transaction buffer based on normal persistent topic.
 */
@Slf4j
public class TopicTransactionBuffer extends TopicTransactionBufferState implements TransactionBuffer {

    private final PersistentTopic topic;

    private final SpscArrayQueue<Entry> entryQueue;

    private final ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<PositionImpl>> txnBufferCache = new ConcurrentOpenHashMap<>();

    //this is for transaction buffer replay start position
    //this will be stored in managed ledger properties and every 10000 will sync to zk by default.
    private final ConcurrentSkipListSet<PositionImpl> positionsSort = new ConcurrentSkipListSet<>();

    private final ManagedCursor cursor;

    //this is for replay
    private final PositionImpl lastConfirmedEntry;
    //this if for replay
    private PositionImpl currentLoadPosition;

    private final AtomicInteger countToSyncPosition = new AtomicInteger(0);

    private final static String TXN_ON_GOING_POSITION_SUFFIX = "-txnOnGoingPosition";

    private final String txnOnGoingPositionName;

    //TODO this can config
    private int defaultCountToSyncPosition = 10000;

    public TopicTransactionBuffer(PersistentTopic topic) throws ManagedLedgerException {
        super(State.None);
        this.entryQueue = new SpscArrayQueue<>(2000);
        this.topic = topic;
        ManagedLedger managedLedger = topic.getManagedLedger();
        this.lastConfirmedEntry = (PositionImpl) managedLedger.getLastConfirmedEntry();
        this.txnOnGoingPositionName = topic.getName() + TXN_ON_GOING_POSITION_SUFFIX;
        String positionString = managedLedger.getProperties().get(txnOnGoingPositionName);
        if (positionString == null) {
            this.currentLoadPosition = PositionImpl.earliest;
        } else {
            PositionImpl position = PositionImpl.earliest;
            try {
                position = PositionImpl.convertStringToPosition(positionString);
            } catch (Exception e) {
                log.error("Topic : [{}] transaction buffer get replay start position error!", topic.getName());
            }
            this.currentLoadPosition = position;
        }
        this.cursor = managedLedger.newNonDurableCursor(currentLoadPosition);

        new Thread(() -> new TopicTransactionBufferReplayer(new TransactionBufferReplayCallback() {

            @Override
            public void replayComplete() {
                if (!changeToReadyState()) {
                    log.error("Managed ledger transaction metadata store change state error when replay complete");
                }
            }

            @Override
            public void handleMetadataEntry(Position position, MessageMetadata messageMetadata) {
                if (!messageMetadata.hasTxnidMostBits() || !messageMetadata.hasTxnidLeastBits()) {
                    return;
                }
                TxnID txnID = new TxnID(messageMetadata.getTxnidMostBits(),
                        messageMetadata.getTxnidLeastBits());
                if (isTxnMarker(messageMetadata)) {
                    ConcurrentOpenHashSet<PositionImpl> positions = txnBufferCache.remove(txnID);
                    positionsSort.removeAll(positions.values());
                } else {
                    ConcurrentOpenHashSet<PositionImpl> positions =
                            txnBufferCache.computeIfAbsent(txnID, (v) -> new ConcurrentOpenHashSet<>());
                    positions.add((PositionImpl) position);
                    positionsSort.add((PositionImpl) position);
                }
            }
        }).start()).start();
    }

    //TODO numberOfEntriesToRead can config
    private void readAsync(int numberOfEntriesToRead,
                           AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        cursor.asyncReadEntries(numberOfEntriesToRead, readEntriesCallback, System.nanoTime());
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return null;
    }

    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new ServiceUnitNotReadyException("Topic : " + topic.getName()
                            +  " transaction buffer haven't finish replay!"));
        }
        CompletableFuture<Position> completableFuture = new CompletableFuture<>();
        topic.publishMessage(buffer, (e, ledgerId, entryId) -> {
            if (e != null) {
                log.error("Failed to append buffer to txn {}", txnId, e);
                completableFuture.completeExceptionally(e);
                return;
            }
            ConcurrentOpenHashSet<PositionImpl> positions =
                    txnBufferCache.computeIfAbsent(txnId, (v) -> new ConcurrentOpenHashSet<>());
            PositionImpl position = PositionImpl.get(ledgerId, entryId);
            positions.add(position);
            positionsSort.add(position);
            completableFuture.complete(position);
            if (countToSyncPosition.incrementAndGet() == defaultCountToSyncPosition) {
                try {
                    PositionImpl syncPosition = positionsSort.pollFirst();
                    if (syncPosition != null) {
                        topic.getManagedLedger().setProperty(txnOnGoingPositionName, syncPosition.toString());
                    }
                } catch (ManagedLedgerException | InterruptedException exception) {
                    log.error("Topic : [{}] Position : [{}], transaction buffer " +
                            "sync replay position fail!", topic.getName(), position);
                }
                countToSyncPosition.addAndGet(defaultCountToSyncPosition);
            }
        });
        return completableFuture;
    }

    @Override
    public CompletableFuture<TransactionBufferReader> openTransactionBufferReader(TxnID txnID, long startSequenceId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, List<MessageIdData> sendMessageIdList) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new ServiceUnitNotReadyException("Topic : " + topic.getName()
                            +  " transaction buffer haven't finish replay!"));
        }
        if (log.isDebugEnabled()) {
            log.debug("Transaction {} commit on topic {}.", txnID.toString(), topic.getName());
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        ConcurrentOpenHashSet<PositionImpl> positions = txnBufferCache.get(txnID);
        if (positions != null) {
            ByteBuf commitMarker = Markers.newTxnCommitMarker(-1L, txnID.getMostSigBits(),
                    txnID.getLeastSigBits(), convertPositionToMessageIdData(positions.values()));
            topic.publishMessage(commitMarker, (e, ledgerId, entryId) -> {
                if (e != null) {
                    log.error("Failed to commit for txn {}", txnID, e);
                    completableFuture.completeExceptionally(e);
                    return;
                }
                txnBufferCache.remove(txnID);
                completableFuture.complete(null);
                positionsSort.removeAll(positions.values());
            });
        } else {
            completableFuture.complete(null);
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID, List<MessageIdData> sendMessageIdList) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new ServiceUnitNotReadyException("Topic : " + topic.getName()
                            +  " transaction buffer haven't finish replay!"));
        }
        if (log.isDebugEnabled()) {
            log.debug("Transaction {} abort on topic {}.", txnID.toString(), topic.getName());
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        ConcurrentOpenHashSet<PositionImpl> positions = txnBufferCache.get(txnID);
        if (positions != null) {
            ByteBuf abortMarker = Markers.newTxnAbortMarker(
                    -1L, txnID.getMostSigBits(),
                    txnID.getLeastSigBits(), convertPositionToMessageIdData(positions.values()));
            topic.publishMessage(abortMarker, (e, ledgerId, entryId) -> {
                if (e != null) {
                    log.error("Failed to abort for txn {}", txnID, e);
                    completableFuture.completeExceptionally(e);
                    return;
                }
                txnBufferCache.remove(txnID);
                completableFuture.complete(null);
                positionsSort.removeAll(positions.values());
            });
        } else {
            completableFuture.complete(null);
        }
        return completableFuture;
    }

    private static List<PulsarMarkers.MessageIdData> getMessageIdDataList(List<MessageIdData> sendMessageIdList) {
        List<PulsarMarkers.MessageIdData> messageIdDataList = new ArrayList<>(sendMessageIdList.size());
        for (MessageIdData msgIdData : sendMessageIdList) {
            messageIdDataList.add(
                    PulsarMarkers.MessageIdData.newBuilder()
                            .setLedgerId(msgIdData.getLedgerId())
                            .setEntryId(msgIdData.getEntryId()).build());
        }
        return messageIdDataList;
    }

    private static List<PulsarMarkers.MessageIdData> convertPositionToMessageIdData(List<PositionImpl> positions) {
        List<PulsarMarkers.MessageIdData> messageIdDataList = new ArrayList<>(positions.size());
        for (PositionImpl position : positions) {
            messageIdDataList.add(
                    PulsarMarkers.MessageIdData.newBuilder()
                            .setLedgerId(position.getLedgerId())
                            .setEntryId(position.getEntryId()).build());
        }
        return messageIdDataList;
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        return null;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return null;
    }

    class TopicTransactionBufferReplayer {

        private final TopicTransactionBuffer.FillEntryQueueCallback fillEntryQueueCallback;
        private final TransactionBufferReplayCallback transactionBufferReplayCallback;

        TopicTransactionBufferReplayer(TransactionBufferReplayCallback transactionBufferReplayCallback) {
            this.fillEntryQueueCallback = new TopicTransactionBuffer.FillEntryQueueCallback();
            this.transactionBufferReplayCallback = transactionBufferReplayCallback;
        }

        public void start() {
            changeToInitializingState();
            if (((PositionImpl) cursor.getMarkDeletedPosition()).compareTo(lastConfirmedEntry) == 0) {
                this.transactionBufferReplayCallback.replayComplete();
                return;
            }
            while (lastConfirmedEntry.compareTo(currentLoadPosition) > 0) {
                fillEntryQueueCallback.fillQueue();
                Entry entry = entryQueue.poll();
                if (entry != null) {
                    ByteBuf buffer = entry.getDataBuffer();
                    currentLoadPosition = PositionImpl.get(entry.getLedgerId(), entry.getEntryId());
                    MessageMetadata messageMetadata = Commands.parseMessageMetadata(buffer);;
                    transactionBufferReplayCallback.handleMetadataEntry(entry.getPosition(), messageMetadata);
                    entry.release();
                    messageMetadata.recycle();
                } else {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        //no-op
                    }
                }
            }
            transactionBufferReplayCallback.replayComplete();
        }
    }

    class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);

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
            log.error("Transaction log init fail error!", exception);
            outstandingReadsRequests.decrementAndGet();
        }
    }
}
