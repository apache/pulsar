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
package org.apache.pulsar.transaction.coordinator.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

/**
 * An in-memory implementation of {@link TransactionMetadataStore}.
 */
class InMemTransactionMetadataStore implements TransactionMetadataStore {

    private final TransactionCoordinatorID tcID;
    private final AtomicLong localID;
    private final ConcurrentMap<TxnID, TxnMetaImpl> transactions;

    InMemTransactionMetadataStore(TransactionCoordinatorID tcID) {
        this.tcID = tcID;
        this.localID = new AtomicLong(0L);
        this.transactions = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnid) {
        CompletableFuture<TxnMeta> getFuture = new CompletableFuture<>();
        TxnMetaImpl txn = transactions.get(txnid);
        if (null == txn) {
            getFuture.completeExceptionally(
                new TransactionNotFoundException("Transaction not found :" + txnid));
        } else {
            getFuture.complete(txn);
        }
        return getFuture;
    }

    @Override
    public CompletableFuture<TxnID> newTransaction() {
        TxnID txnID = new TxnID(
            tcID.getId(),
            localID.getAndIncrement()
        );
        TxnMetaImpl txn = new TxnMetaImpl(txnID);
        transactions.put(txnID, txn);
        return CompletableFuture.completedFuture(txnID);
    }

    @Override
    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnid, List<String> partitions) {
        return getTxnMeta(txnid).thenCompose(txn -> {
            try {
                txn.addProducedPartitions(partitions);
                return CompletableFuture.completedFuture(null);
            } catch (InvalidTxnStatusException e) {
                CompletableFuture<Void> error = new CompletableFuture<>();
                error.completeExceptionally(e);
                return error;
            }
        });
    }

    @Override
    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnid, List<TransactionSubscription> partitions) {
        return getTxnMeta(txnid).thenCompose(txn -> {
            try {
                txn.addAckedPartitions(partitions);
                return CompletableFuture.completedFuture(null);
            } catch (InvalidTxnStatusException e) {
                CompletableFuture<Void> error = new CompletableFuture<>();
                error.completeExceptionally(e);
                return error;
            }
        });
    }

    @Override
    public CompletableFuture<Void> updateTxnStatus(TxnID txnid, TxnStatus newStatus, TxnStatus expectedStatus) {
        return getTxnMeta(txnid).thenCompose(txn -> {
            try {
                txn.updateTxnStatus(newStatus, expectedStatus);
                return CompletableFuture.completedFuture(null);
            } catch (InvalidTxnStatusException e) {
                CompletableFuture<Void> error = new CompletableFuture<>();
                error.completeExceptionally(e);
                return error;
            }
        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        transactions.clear();
        return CompletableFuture.completedFuture(null);
    }
}
