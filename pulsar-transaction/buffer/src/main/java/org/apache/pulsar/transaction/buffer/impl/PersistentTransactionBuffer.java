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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.transaction.buffer.TransactionCursor;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.impl.common.TxnID;

public class PersistentTransactionBuffer extends PersistentTopic implements TransactionBuffer {

    private TransactionCursor txnCursor;

    abstract class TxnCtx implements PublishContext {}

    public PersistentTransactionBuffer(String topic, ManagedLedger ledger, BrokerService brokerService)
        throws BrokerServiceException.NamingException {
        super(topic, ledger, brokerService);
        this.txnCursor = new TransactionCursorImpl();
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        CompletableFuture<TransactionMeta> getFuture = new CompletableFuture<>();

        txnCursor.getTxnMeta(txnID, false).thenCompose(meta -> {
            getFuture.complete(meta);
            return null;
        }).exceptionally(e -> {
            getFuture.completeExceptionally(e);
            return null;
        });

        return getFuture;
    }

    @Override
    public CompletableFuture<Void> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        CompletableFuture<Void> appendFuture = new CompletableFuture<>();

        publishMessage(buffer, new TxnCtx() {
            @Override
            public void completed(Exception e, long ledgerId, long entryId) {
                if (e != null) {
                    appendFuture.completeExceptionally(e);
                    return;
                }

                txnCursor.getTxnMeta(txnId, true).thenCompose(meta -> {
                    meta.appendEntry(sequenceId, PositionImpl.get(ledgerId, entryId));
                    appendFuture.complete(null);
                    return null;
                }).exceptionally(metaNotFound -> {
                    appendFuture.completeExceptionally(metaNotFound);
                    return null;
                });
            }
        });

        return appendFuture;
    }

    @Override
    public CompletableFuture<TransactionBufferReader> openTransactionBufferReader(TxnID txnID, long startSequenceId) {
        CompletableFuture<TransactionBufferReader> readerFuture = new CompletableFuture<>();

        txnCursor.getTxnMeta(txnID, false).thenCompose(meta -> {
            try {
                PersistentTransactionBufferReader reader = new PersistentTransactionBufferReader(meta, ledger);
                readerFuture.complete(reader);
            } catch (ManagedLedgerException e) {
                readerFuture.completeExceptionally(e);
            }
            return null;
        });

        return readerFuture;
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, long committedAtLedgerId, long committedAtEntryId) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, long committedAtLedgerId, long committedAtEntryId,
                                             ByteBuf marker) {
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();

        publishMessage(marker, new TxnCtx() {
            @Override
            public void completed(Exception e, long ledgerId, long entryId) {
                if (e != null) {
                    commitFuture.completeExceptionally(e);
                    return;
                }

                txnCursor.commitTxn(committedAtLedgerId, committedAtEntryId, txnID, PositionImpl.get(ledgerId, entryId))
                         .thenCompose(v -> {
                             commitFuture.complete(null);
                             return null;
                         })
                         .exceptionally(error -> {
                             commitFuture.completeExceptionally(error);
                             return null;
                         });
            }
        });

        return commitFuture;
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID, ByteBuf marker) {

        CompletableFuture<Void> abortFuture = new CompletableFuture<>();

        publishMessage(marker, new TxnCtx() {
            @Override
            public void completed(Exception e, long ledgerId, long entryId) {
                if (e != null) {
                    abortFuture.completeExceptionally(e);
                    return;
                }

                txnCursor.abortTxn(txnID).thenCompose(v -> {
                    abortFuture.complete(null);
                    return null;
                }).exceptionally(error -> {
                    abortFuture.completeExceptionally(error);
                    return null;
                });
            }
        });

        return abortFuture;
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> closeBuffer() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }
}
