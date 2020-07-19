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
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionCursor;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotSealedException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarMarkers.MessageIdData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A persistent transaction buffer implementation.
 */
@Slf4j
public class PersistentTransactionBuffer extends PersistentTopic implements TransactionBuffer {

    private final static String TB_TOPIC_NAME_SUFFIX = "_txnlog";
    private TransactionCursor txnCursor;
    private ManagedCursor retentionCursor;
    private Topic originTopic;

    abstract static class TxnCtx implements PublishContext {
        private final long sequenceId;
        private final CompletableFuture<Position> completableFuture;
        private final String producerName;

        TxnCtx(String producerName, long sequenceId, CompletableFuture<Position> future) {
            this.sequenceId = sequenceId;
            this.completableFuture = future;
            this.producerName = producerName;
        }

        @Override
        public String getProducerName() {
            return this.producerName;
        }

        @Override
        public long getSequenceId() {
            return this.sequenceId;
        }
    }

    public PersistentTransactionBuffer(String topic, ManagedLedger ledger, BrokerService brokerService,
                                       Topic originTopic)
        throws BrokerServiceException.NamingException, ManagedLedgerException {
        super(topic, ledger, brokerService);
        this.txnCursor = new TransactionCursorImpl();
        this.retentionCursor = ledger.newNonDurableCursor(
            PositionImpl.earliest, "txn-buffer-retention");
        this.originTopic = originTopic;
    }

    public static String getTransactionBufferTopicName(String originTopicName) {
        return TopicName.get(originTopicName) + TB_TOPIC_NAME_SUFFIX;
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return txnCursor.getTxnMeta(txnID, false);
    }

    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        return publishMessage(txnId, buffer, sequenceId)
                .thenCompose(position -> appendBuffer(txnId, position, sequenceId));
    }

    private CompletableFuture<Position> appendBuffer(TxnID txnID, Position position, long sequenceId) {
        return txnCursor.getTxnMeta(txnID, true).thenCompose(meta -> meta.appendEntry(sequenceId, position));
    }

    @Override
    public CompletableFuture<TransactionBufferReader> openTransactionBufferReader(TxnID txnID, long startSequenceId) {
        return txnCursor.getTxnMeta(txnID, false).thenCompose(this::createNewReader);
    }

    private CompletableFuture<TransactionBufferReader> createNewReader(TransactionMeta meta) {
        CompletableFuture<TransactionBufferReader> createReaderFuture = new CompletableFuture<>();

        try {
            PersistentTransactionBufferReader reader = new PersistentTransactionBufferReader(meta, ledger);
            createReaderFuture.complete(reader);
        } catch (TransactionNotSealedException e) {
            createReaderFuture.completeExceptionally(e);
        }

        return createReaderFuture;
    }

    @Builder
    final static class Marker {
        long sequenceId;
        ByteBuf marker;
    }

    // append committing marker to TB
    @Override
    public CompletableFuture<Void> committingTxn(TxnID txnID) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        txnCursor.getTxnMeta(txnID, false).whenComplete(((meta, txnMetaThrowable) -> {

            if (txnMetaThrowable != null) {
                completableFuture.completeExceptionally(txnMetaThrowable);
            }

            long sequenceId = meta.lastSequenceId() + 1;
            MessageIdData messageIdData = MessageIdData.newBuilder().setLedgerId(-1L).setEntryId(-1L).build();
            ByteBuf committingMarker = Markers.newTxnCommittingMarker(
                    sequenceId, txnID.getMostSigBits(), txnID.getLeastSigBits(), messageIdData);
            publishMessage(txnID, committingMarker, sequenceId).whenComplete((position, publishThrowable) -> {
                if (publishThrowable != null) {
                    completableFuture.completeExceptionally(publishThrowable);
                } else {
                    completableFuture.complete(null);
                }
            });
        }));
        return completableFuture;
    }

    // append committed marker to partitioned topic
    @Override
    public CompletableFuture<Position> commitPartitionTopic(TxnID txnID) {
        CompletableFuture<Position> positionFuture = new CompletableFuture<>();
        // TODO How to generate sequenceId for commit marker in partitioned topic
        long ptSequenceId = -1;
        MessageIdData messageIdData = MessageIdData.newBuilder().setLedgerId(-1L).setEntryId(-1L).build();
        ByteBuf commitMarker = Markers.newTxnCommitMarker(
                ptSequenceId, txnID.getMostSigBits(), txnID.getLeastSigBits(), messageIdData);

        originTopic.publishMessage(commitMarker, (e, ledgerId, entryId) -> {
            positionFuture.complete(new PositionImpl(ledgerId, entryId));
        });

        return positionFuture;
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, long committedAtLedgerId, long committedAtEntryId) {
        return txnCursor.getTxnMeta(txnID, false)
                        .thenApply(meta -> createCommitMarker(meta, committedAtLedgerId, committedAtEntryId))
                        .thenCompose(marker -> publishMessage(txnID, marker.marker, marker.sequenceId))
                        .thenCompose(position -> txnCursor.commitTxn(committedAtLedgerId, committedAtEntryId, txnID,
                                                                     position));
    }

    private Marker createCommitMarker(TransactionMeta meta, long committedAtLedgerId, long committedAtEntryId) {
        if (log.isDebugEnabled()) {
            log.debug("Transaction {} create a commit marker", meta.id());
        }
        long sequenceId = meta.lastSequenceId() + 1;
        MessageIdData messageIdData = MessageIdData.newBuilder()
                                                   .setLedgerId(committedAtLedgerId)
                                                   .setEntryId(committedAtEntryId)
                                                   .build();
        ByteBuf commitMarker = Markers.newTxnCommitMarker(sequenceId, meta.id().getMostSigBits(),
                                                          meta.id().getLeastSigBits(), messageIdData);
        Marker marker = Marker.builder().sequenceId(sequenceId).marker(commitMarker).build();
        return marker;
    }

    public CompletableFuture<Void> commitTxn(TxnID txnID) {
        return txnCursor.getTxnMeta(txnID, false).thenApply((meta -> {

            // append committing marker to TB
            long sequenceId = meta.lastSequenceId() + 1;
            ByteBuf committingMarker = Markers.newTxnCommittingMarker(
                    sequenceId, txnID.getMostSigBits(), txnID.getLeastSigBits(), null
            );

            CompletableFuture<Marker> tempFuture = new CompletableFuture<>();
            publishMessage(txnID, committingMarker, sequenceId).whenComplete((position, throwable) -> {
                tempFuture.complete(new Marker(sequenceId, null));
            });
            return tempFuture;

        })).thenCompose(preMarkerFuture -> {

            CompletableFuture<Marker> nextMarkerFuture = new CompletableFuture<>();
            preMarkerFuture.whenComplete((preMarker, committingThrowable) -> {
                // append committed marker to partitioned topic
                // TODO How to generate sequenceId for commit marker in partitioned topic
                long ptSequenceId = -1;
                ByteBuf commitMarker = Markers.newTxnCommitMarker(
                        ptSequenceId, txnID.getMostSigBits(), txnID.getLeastSigBits(), null);

                originTopic.publishMessage(commitMarker, (e, ledgerId, entryId) -> {
                    MessageIdData messageIdData = MessageIdData.newBuilder()
                            .setLedgerId(ledgerId)
                            .setEntryId(entryId)
                            .build();
                    ByteBuf tbCommitMarker = Markers.newTxnCommitMarker(preMarker.sequenceId,
                            txnID.getMostSigBits(), txnID.getLeastSigBits(), messageIdData);
                    nextMarkerFuture.complete(new Marker(preMarker.sequenceId, tbCommitMarker));
                });

            });
            return nextMarkerFuture;

        }).thenCompose(preMarkerFuture -> {
            // append committed marker to TB
            CompletableFuture<Void> resultFuture = new CompletableFuture<>();
            publishMessage(txnID, preMarkerFuture.marker, preMarkerFuture.sequenceId).whenComplete(
                    (position, throwable) -> {
                resultFuture.complete(null);
            });
            return resultFuture;
        });
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID) {
        return txnCursor.getTxnMeta(txnID, false)
                        .thenApply(meta -> createAbortMarker(meta))
                        .thenCompose(marker -> publishMessage(txnID, marker.marker, marker.sequenceId))
                        .thenCompose(position -> txnCursor.abortTxn(txnID));
    }

    private Marker createAbortMarker(TransactionMeta meta) {
        if (log.isDebugEnabled()) {
            log.debug("Transaction {} create a abort marker", meta.id());
        }
        long sequenceId = meta.lastSequenceId() + 1;
        ByteBuf abortMarker = Markers.newTxnAbortMarker(sequenceId, meta.id().getMostSigBits(),
                                                        meta.id().getLeastSigBits());
        Marker marker = Marker.builder().sequenceId(sequenceId).marker(abortMarker).build();
        return marker;
    }


    private CompletableFuture<Position> publishMessage(TxnID txnID, ByteBuf msg, long sequenceId) {
        CompletableFuture<Position> publishFuture = new CompletableFuture<>();

        publishMessage(msg, new TxnCtx(txnID.toString(), sequenceId, publishFuture) {
            @Override
            public void completed(Exception e, long ledgerId, long entryId) {
                if (e != null) {
                    publishFuture.completeExceptionally(e);
                } else {
                    publishFuture.complete(PositionImpl.get(ledgerId, entryId));
                }
            }
        });
        return publishFuture;
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        if (log.isDebugEnabled()) {
            log.debug("Begin to purge the ledgers {}", dataLedgers);
        }

        List<CompletableFuture<Void>> futures = dataLedgers.stream().map(dataLedger -> cleanTxnsOnLedger(dataLedger))
                                                           .collect(Collectors.toList());
        return FutureUtil.waitForAll(futures).thenCompose(v -> removeCommittedLedgerFromIndex(dataLedgers));
    }

    private CompletableFuture<Void> removeCommittedLedgerFromIndex(List<Long> dataLedgers) {
        List<CompletableFuture<Void>> removeFutures = dataLedgers.stream().map(
            dataLedger -> txnCursor.removeTxnsCommittedAtLedger(dataLedger)).collect(Collectors.toList());
        return FutureUtil.waitForAll(removeFutures);
    }

    private CompletableFuture<Void> cleanTxnsOnLedger(long dataledger) {
        if (log.isDebugEnabled()) {
            log.debug("Start to clean ledger {}", dataledger);
        }
        return txnCursor.getAllTxnsCommittedAtLedger(dataledger).thenCompose(txnIDS -> deleteTxns(txnIDS));
    }

    private CompletableFuture<Void> deleteTxns(Set<TxnID> txnIDS) {
        if (log.isDebugEnabled()) {
            log.debug("Start delete txns {} under ledger", txnIDS);
        }
        List<CompletableFuture<Void>> futures = txnIDS.stream().map(txnID -> deleteTxn(txnID))
                                                      .collect(Collectors.toList());
        return FutureUtil.waitForAll(futures);
    }

    private CompletableFuture<Void> deleteTxn(TxnID txnID) {
        if (log.isDebugEnabled()) {
            log.debug("Start to delete txn {} entries", txnID);
        }
        return txnCursor.getTxnMeta(txnID, false)
                 .thenCompose(meta -> meta.readEntries(meta.numEntries(), -1L))
                 .thenCompose(longPositionSortedMap -> deleteEntries(longPositionSortedMap, txnID));
    }

    private CompletableFuture<Void> deleteEntries(SortedMap<Long, Position> entriesMap, TxnID txnID) {
        if (log.isDebugEnabled()) {
            log.debug("Delete entries {}", entriesMap);
        }
        List<CompletableFuture<Void>> deleteFutures = entriesMap.values().stream()
            .map(position -> asyncDeletePosition(position, txnID))
            .collect(Collectors.toList());

        return FutureUtil.waitForAll(deleteFutures);
    }

    private CompletableFuture<Void> asyncDeletePosition(Position position, TxnID txnID) {
        if (log.isDebugEnabled()) {
            log.debug("Ready to delete position {} for txn {}", position, txnID);
        }
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
        retentionCursor.asyncMarkDelete(position, new AsyncCallbacks.MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("Success delete transaction `{}` entry on position {}", txnID, position);
                }
                deleteFuture.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Failed delete transaction `{}` entry on position {}", txnID, position, exception);
                deleteFuture.completeExceptionally(exception);
            }
        }, null);

        return deleteFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }
}
