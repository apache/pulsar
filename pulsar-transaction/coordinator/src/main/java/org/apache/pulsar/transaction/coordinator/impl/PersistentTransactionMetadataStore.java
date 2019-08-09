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

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.ByteArrayCoder;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.kv.KVAsyncStore;
import org.apache.pulsar.transaction.configuration.CoordinatorConfiguration;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorMetadataStoreException;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@Slf4j
public class PersistentTransactionMetadataStore implements TransactionMetadataStore {

    private final TransactionCoordinatorID tcID;
    private final AtomicLong localID;
    private final KVAsyncStore<TxnID, TxnMeta> kvStore;


    public PersistentTransactionMetadataStore(TransactionCoordinatorID tcID, KVAsyncStore<TxnID, TxnMeta> kvStore) throws ExecutionException, InterruptedException {
        this.tcID = tcID;
        this.localID = new AtomicLong(0L);
        this.kvStore = kvStore;
    }

    public CompletableFuture<Void> init(CoordinatorConfiguration coordinatorConfiguration) {
        StateStoreSpec spec = StateStoreSpec.builder()
                .name("a")
                .stream("a")
                .localStateStoreDir(new File(coordinatorConfiguration.getDlLocalStateStoreDir()))
                .keyCoder(ByteArrayCoder.of())
                .valCoder(ByteArrayCoder.of())
                .build();
        return this.kvStore.init(spec);
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
        try {
            kvStore.put(txnID, txnMeta).get();
            future.complete(txnID);
        } catch (Exception e) {
            e.printStackTrace();
        }

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
