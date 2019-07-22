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
package org.apache.pulsar.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang.SerializationUtils;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.transaction.buffer.TransactionCursor;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotSealedException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

/**
 * A persistent transaction buffer implementation.
 */
@Slf4j
public class PersistentTransactionBuffer extends PersistentTopic implements TransactionBuffer {

    private TransactionCursor txnCursor;
    private ManagedCursor retentionCursor;


    abstract class TxnCtx implements PublishContext{
        private long sequenceId;

        TxnCtx(long sequenceId) {
            this.sequenceId = sequenceId;
        }

        @Override
        public String getProducerName() {
            return "txn-producer";
        }

        @Override
        public long getSequenceId() {
            return this.sequenceId;
        }
    }

    @Builder
    private static final class Marker implements Serializable {
        TxnID txnID;
        TxnStatus status;

        public byte[] serialize() {
            return SerializationUtils.serialize(this);
        }
    }

    public PersistentTransactionBuffer(String topic, ManagedLedger ledger, BrokerService brokerService)
        throws BrokerServiceException.NamingException, ManagedLedgerException {
        super(topic, ledger, brokerService);
        this.txnCursor = new TransactionCursorImpl();
        this.retentionCursor = ledger.newNonDurableCursor(PositionImpl.earliest);
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return txnCursor.getTxnMeta(txnID, false);
    }

    @Override
    public CompletableFuture<Void> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        return publishMessage(buffer, sequenceId).thenCompose(position -> appendBuffer(txnId, position, sequenceId));
    }

    private CompletableFuture<Void> appendBuffer(TxnID txnID, Position position, long sequenceId) {
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
        } catch (ManagedLedgerException | TransactionNotSealedException e) {
            createReaderFuture.completeExceptionally(e);
        }

        return createReaderFuture;
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, long committedAtLedgerId, long committedAtEntryId) {
        Marker commitMarker = Marker.builder().txnID(txnID).status(TxnStatus.COMMITTED).build();
        ByteBuf marker = Unpooled.wrappedBuffer(commitMarker.serialize());

        return txnCursor.getTxnMeta(txnID, false)
                 .thenCompose(meta -> publishMessage(marker, meta.lastSequenceId() + 1))
                 .thenCompose(
                     position -> txnCursor.commitTxn(committedAtLedgerId, committedAtEntryId, txnID, position));
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID) {

        Marker abortMarker = Marker.builder().txnID(txnID).status(TxnStatus.ABORTED).build();
        ByteBuf marker = Unpooled.wrappedBuffer(abortMarker.serialize());

        return txnCursor.getTxnMeta(txnID, false)
                 .thenCompose(meta -> publishMessage(marker, meta.lastSequenceId() + 1))
                 .thenCompose(position -> txnCursor.abortTxn(txnID));
    }

    private CompletableFuture<Position> publishMessage(ByteBuf msg, long sequenceId) {
        CompletableFuture<Position> publishFuture = new CompletableFuture<>();

        CountDownLatch latch = new CountDownLatch(1);

        brokerService.executor().execute(() -> {
            publishMessage(msg, new TxnCtx(sequenceId) {
                @Override
                public void completed(Exception e, long ledgerId, long entryId) {
                    if (e != null) {
                        publishFuture.completeExceptionally(e);
                        latch.countDown();
                        return;
                    }

                    publishFuture.complete(PositionImpl.get(ledgerId, entryId));
                    latch.countDown();
                }
            });
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            publishFuture.completeExceptionally(e);
        }

        return publishFuture;
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        if (log.isDebugEnabled()) {
            log.debug("Begin to purge the ledgers {}", dataLedgers);
        }

        List<CompletableFuture<Void>> futures = dataLedgers.stream().map(dataLedger -> deleteLedger(dataLedger))
                                                           .collect(Collectors.toList());
        return FutureUtil.waitForAll(futures).thenCompose(v -> removeCommittedLedgerFromIndex(dataLedgers));
    }

    private CompletableFuture<Void> removeCommittedLedgerFromIndex(List<Long> dataLedgers) {
        List<CompletableFuture<Void>> removeFutures = dataLedgers.stream().map(
            dataLedger -> txnCursor.removeCommittedLedger(dataLedger)).collect(Collectors.toList());
        return FutureUtil.waitForAll(removeFutures);
    }

    private CompletableFuture<Void> deleteLedger(long dataledger) {
        if (log.isDebugEnabled()) {
            log.debug("Start to clean ledger {}", dataledger);
        }
        return txnCursor.getRemoveTxns(dataledger).thenCompose(txnIDS -> deleteTxns(txnIDS));
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
                log.info("Success delete transaction `{}` entry on position {}", txnID, position);
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
