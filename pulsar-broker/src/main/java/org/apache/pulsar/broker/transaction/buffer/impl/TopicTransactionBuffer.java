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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.protocol.Markers;

/**
 * Transaction buffer based on normal persistent topic.
 */
@Slf4j
public class TopicTransactionBuffer implements TransactionBuffer {

    private final PersistentTopic topic;

    private volatile PositionImpl maxReadPosition = PositionImpl.latest;

    private final LinkedMap<TxnID, PositionImpl> ongoingTxns = new LinkedMap<>();

    private final LinkedMap<TxnID, PositionImpl> aborts = new LinkedMap<>();

    public TopicTransactionBuffer(PersistentTopic topic) {
        this.topic = topic;
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return null;
    }

    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        CompletableFuture<Position> completableFuture = new CompletableFuture<>();
        topic.getManagedLedger().asyncAddEntry(buffer, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, Object ctx) {
                synchronized (TopicTransactionBuffer.this) {
                    if (!ongoingTxns.containsKey(txnId)) {
                        ongoingTxns.put(txnId, (PositionImpl) position);
                        PositionImpl firstPosition = ongoingTxns.get(ongoingTxns.firstKey());
                        //max read position is less than first ongoing transaction message position, so entryId -1
                        maxReadPosition = PositionImpl.get(firstPosition.getLedgerId(), firstPosition.getEntryId() - 1);
                    }
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

        ByteBuf commitMarker = Markers.newTxnCommitMarker(-1L, txnID.getMostSigBits(),
                txnID.getLeastSigBits());

        topic.getManagedLedger().asyncAddEntry(commitMarker, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, Object ctx) {
                synchronized (TopicTransactionBuffer.this) {
                    updateMaxReadPosition(txnID);
                    handleLowWaterMark(txnID, lowWaterMark);
                }
                completableFuture.complete(null);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Failed to commit for txn {}", txnID, exception);
                completableFuture.completeExceptionally(exception);
            }
        }, null);
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID, long lowWaterMark) {
        if (log.isDebugEnabled()) {
            log.debug("Transaction {} abort on topic {}.", txnID.toString(), topic.getName());
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        ByteBuf abortMarker = Markers.newTxnAbortMarker(-1L, txnID.getMostSigBits(), txnID.getLeastSigBits());
        topic.getManagedLedger().asyncAddEntry(abortMarker, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, Object ctx) {
                synchronized (TopicTransactionBuffer.this) {
                    aborts.put(txnID, (PositionImpl) position);
                    updateMaxReadPosition(txnID);
                    handleLowWaterMark(txnID, lowWaterMark);
                }
                completableFuture.complete(null);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Failed to abort for txn {}", txnID, exception);
                completableFuture.completeExceptionally(exception);
            }
        }, null);
        return completableFuture;
    }

    private void handleLowWaterMark(TxnID txnID, long lowWaterMark) {
        if (!ongoingTxns.isEmpty()) {
            TxnID firstTxn = ongoingTxns.firstKey();
            if (firstTxn.getMostSigBits() == txnID.getMostSigBits() && lowWaterMark >= firstTxn.getLeastSigBits()) {
                ByteBuf abortMarker = Markers.newTxnAbortMarker(-1L,
                        txnID.getMostSigBits(), txnID.getLeastSigBits());
                topic.getManagedLedger().asyncAddEntry(abortMarker, new AsyncCallbacks.AddEntryCallback() {
                    @Override
                    public void addComplete(Position position, Object ctx) {
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
            }
        }
    }

    void updateMaxReadPosition(TxnID txnID) {
        ongoingTxns.remove(txnID);
        if (!ongoingTxns.isEmpty()) {
            PositionImpl position = ongoingTxns.get(ongoingTxns.firstKey());
            //max read position is less than first ongoing transaction message position, so entryId -1
            maxReadPosition = PositionImpl.get(position.getLedgerId(), position.getEntryId() - 1);
        } else {
            maxReadPosition = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
        }
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        return null;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return null;
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
            if (ongoingTxns.isEmpty()) {
                maxReadPosition = position;
            }
        }
    }

    @Override
    public PositionImpl getMaxReadPosition() {
        return this.maxReadPosition;
    }
}
