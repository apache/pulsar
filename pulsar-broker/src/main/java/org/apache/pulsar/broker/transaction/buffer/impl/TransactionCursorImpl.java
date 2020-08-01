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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.transaction.buffer.TransactionCursor;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.broker.transaction.buffer.exceptions.NoTxnsCommittedAtLedgerException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.client.api.transaction.TxnID;

public class TransactionCursorImpl implements TransactionCursor {


    private final ConcurrentMap<TxnID, TransactionMetaImpl> txnIndex;
    private final Map<Long, Set<TxnID>> committedLedgerTxnIndex;

    TransactionCursorImpl() {
        this.txnIndex = new ConcurrentHashMap<>();
        this.committedLedgerTxnIndex = new TreeMap<>();
    }

    @Override
    public CompletableFuture<TransactionMeta> getTxnMeta(TxnID txnID, boolean createIfNotExist) {
        CompletableFuture<TransactionMeta> getFuture = new CompletableFuture<>();
        TransactionMeta meta = txnIndex.get(txnID);
        if (null == meta) {
            if (!createIfNotExist) {
                getFuture.completeExceptionally(
                    new TransactionNotFoundException("Transaction `" + txnID + "` doesn't" + " exist"));
                return getFuture;
            }

            TransactionMetaImpl newMeta = new TransactionMetaImpl(txnID);
            TransactionMeta oldMeta = txnIndex.putIfAbsent(txnID, newMeta);
            if (null != oldMeta) {
                meta = oldMeta;
            } else {
                meta = newMeta;
            }
        }
        getFuture.complete(meta);

        return getFuture;
    }

    @Override
    public CompletableFuture<Void> commitTxn(long committedLedgerId, long committedEntryId, TxnID txnID,
                                             Position position) {
        return getTxnMeta(txnID, false)
            .thenCompose(meta -> meta.commitTxn(committedLedgerId, committedEntryId))
            .thenAccept(meta -> addTxnToCommittedIndex(txnID, committedLedgerId));
    }

    private void addTxnToCommittedIndex(TxnID txnID, long committedAtLedgerId) {
        synchronized (committedLedgerTxnIndex) {
            committedLedgerTxnIndex.computeIfAbsent(committedAtLedgerId, ledgerId -> new HashSet<>()).add(txnID);
        }
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID) {
        return getTxnMeta(txnID, false)
            .thenCompose(meta -> meta.abortTxn())
            .thenApply(meta -> null);
    }

    public CompletableFuture<Set<TxnID>> getAllTxnsCommittedAtLedger(long ledgerId) {
        CompletableFuture<Set<TxnID>> removeFuture = new CompletableFuture<>();

        Set<TxnID> txnIDS = committedLedgerTxnIndex.get(ledgerId);

        if (null == txnIDS) {
            removeFuture.completeExceptionally(new NoTxnsCommittedAtLedgerException(
                "Transaction committed ledger id `" + ledgerId + "` doesn't exist") {
            });
            return removeFuture;
        }

        removeFuture.complete(txnIDS);
        return removeFuture;
    }

    @Override
    public CompletableFuture<Void> removeTxnsCommittedAtLedger(long ledgerId) {
        CompletableFuture<Void> removeFuture = new CompletableFuture<>();

        synchronized (committedLedgerTxnIndex) {
            Set<TxnID> txnIDS = committedLedgerTxnIndex.remove(ledgerId);
            if (null == txnIDS) {
                removeFuture.completeExceptionally(new NoTxnsCommittedAtLedgerException(
                    "Transaction committed ledger id `" + ledgerId + "` doesn't exist"));
            } else {
                txnIDS.forEach(txnID -> {
                    txnIndex.remove(txnID);
                });
                removeFuture.complete(null);
            }
        }

        return removeFuture;
    }
}
