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

import org.apache.bookkeeper.statelib.api.kv.KVAsyncStore;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorMetadataStoreException;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class PersistentTransactionMetadataStore implements TransactionMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(PersistentTransactionMetadataStore.class);

    private final TransactionCoordinatorID tcID;
    private final AtomicLong localID;
    private final KVAsyncStore<TxnID, TxnMeta> kvStore;


    public PersistentTransactionMetadataStore(TransactionCoordinatorID tcID, Supplier<KVAsyncStore<TxnID, TxnMeta>> kvStoreSupplier) throws IOException {
        this.tcID = tcID;
        this.localID = new AtomicLong(0L);
        this.kvStore = kvStoreSupplier.get();
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnID) {
        return kvStore.get(txnID);
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public CompletableFuture<TxnID> newTransaction() {
        CompletableFuture<TxnID> future = new CompletableFuture<>();
        TxnID txnID = new TxnID(tcID.getId(), localID.getAndIncrement());
        TxnMeta txnMeta = new TxnMetaImpl(txnID);
        kvStore.put(txnID, txnMeta).thenRun(() -> future.complete(txnID));
        return future;
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnID, List<String> partitions) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        kvStore.get(txnID).thenAccept(txnMeta -> {
            try {
                txnMeta.addProducedPartitions(partitions);
                kvStore.put(txnID, txnMeta).thenRun(() -> future.complete(null));
            } catch (InvalidTxnStatusException e) {
                String msg = "[" + tcID + "] Error trying to add produced partition to transactions with id:" + txnID +
                        " transaction is not in Open status";
                log.error(msg, e);
                future.completeExceptionally(new CoordinatorMetadataStoreException(msg));
            }
        });

        return future;
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnID, List<String> partitions) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        kvStore.get(txnID).thenAccept(txnMeta -> {
            try {
                txnMeta.addAckedPartitions(partitions);
                kvStore.put(txnID, txnMeta).thenRun(() -> future.complete(null));
            } catch (InvalidTxnStatusException e) {
                String msg = "[" + tcID + "] Error trying to add acked partitions to transaction with id:" + txnID +
                        " transaction is not in Open status";
                log.error(msg);
                future.completeExceptionally(new CoordinatorMetadataStoreException(msg));
            }
        });

        return future;
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public CompletableFuture<Void> updateTxnStatus(TxnID txnID, TxnStatus newStatus, TxnStatus expectedStatus) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        kvStore.get(txnID).thenAccept(txnMeta -> {
            try {
                txnMeta.updateTxnStatus(newStatus, expectedStatus);
                kvStore.put(txnID, txnMeta).thenRun(() -> future.complete(null));
            } catch (InvalidTxnStatusException e) {
                String msg = "[" + tcID + "] Error trying to update transaction status to transaction with id:" + txnID +
                        " transaction is not in Open status";
                log.error(msg);
                future.completeExceptionally(new CoordinatorMetadataStoreException(msg));
            }
        });

        return future;
    }
}
