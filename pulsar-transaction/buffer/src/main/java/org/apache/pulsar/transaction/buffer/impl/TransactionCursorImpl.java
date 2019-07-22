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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.transaction.buffer.TransactionCursor;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionCommitLedgerNotFoundException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnID;

public class TransactionCursorImpl implements TransactionCursor {


    private ConcurrentMap<TxnID, TransactionMetaImpl> txnIndex;
    private Map<Long, Set<TxnID>> committedTxnIndex;

    TransactionCursorImpl() {
        this.txnIndex = new ConcurrentHashMap<>();
        this.committedTxnIndex = new TreeMap<>();
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
            txnIndex.putIfAbsent(txnID, newMeta);
            meta = newMeta;
        }
        getFuture.complete(meta);

        return getFuture;
    }

    @Override
    public CompletableFuture<Void> commitTxn(long committedLedgerId, long committedEntryId, TxnID txnID,
                                             Position position) {
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        TransactionMeta meta = getMeta(txnID, commitFuture);
        if (commitFuture.isCompletedExceptionally()) {
            return commitFuture;
        }

        synchronized (meta) {
            meta.commitTxn(committedLedgerId, committedEntryId).thenCompose(commit -> {
                addTxnToCommittedIndex(txnID, committedLedgerId);
                commitFuture.complete(null);
                return null;
            }).exceptionally(e -> {
                commitFuture.completeExceptionally(e);
                return null;
            });
        }

        return commitFuture;
    }

    private void addTxnToCommittedIndex(TxnID txnID, long committedAtLedgerId) {
        synchronized (committedTxnIndex) {
            committedTxnIndex.computeIfAbsent(committedAtLedgerId, ledgerId -> new HashSet<>()).add(txnID);
        }
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID) {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();

        TransactionMeta meta = getMeta(txnID, abortFuture);
        if (abortFuture.isCompletedExceptionally()) {
            return abortFuture;
        }

        synchronized (meta) {
            meta.abortTxn().thenCompose(abortMeta -> {
                abortFuture.complete(null);
                return null;
            }).exceptionally(e -> {
                abortFuture.completeExceptionally(e);
                return null;
            });
        }

        return abortFuture;
    }

    private TransactionMeta getMeta(TxnID txnID, CompletableFuture<Void> future) {
        TransactionMeta meta = txnIndex.get(txnID);
        if (meta == null) {
            future.completeExceptionally(new TransactionNotFoundException("Transaction `" + txnID + "` doesn't exist"));
        }

        return meta;
    }

    public CompletableFuture<Set<TxnID>> getRemoveTxns(long ledgerId) {
        CompletableFuture<Set<TxnID>> removeFuture = new CompletableFuture<>();

        Set<TxnID> txnIDS = committedTxnIndex.get(ledgerId);

        if (!committedTxnIndex.keySet().contains(ledgerId)) {
            removeFuture.completeExceptionally(new TransactionCommitLedgerNotFoundException(
                "Transaction commited " + "ledger id `" + ledgerId + "` doesn't exist") {
            });
            return removeFuture;
        }

        removeFuture.complete(txnIDS);
        return removeFuture;
    }

    @Override
    public CompletableFuture<Void> removeCommittedLedger(long ledgerId) {

        synchronized (committedTxnIndex) {
            Set<TxnID> txnIDS = committedTxnIndex.remove(ledgerId);
            txnIDS.forEach(txnID -> {
                txnIndex.remove(txnID);
            });
        }

        return CompletableFuture.completedFuture(null);
    }
}
