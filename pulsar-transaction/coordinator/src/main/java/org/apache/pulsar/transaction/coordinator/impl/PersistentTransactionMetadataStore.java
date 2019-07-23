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

import org.apache.pulsar.shade.org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageRocksDB;
import org.apache.pulsar.shade.org.apache.commons.lang3.SerializationUtils;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
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

public class PersistentTransactionMetadataStore implements TransactionMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(PersistentTransactionMetadataStore.class);

    private final TransactionCoordinatorID tcID;
    private final AtomicLong localID;
    private final KeyValueStorageRocksDB rocksDB;


    public PersistentTransactionMetadataStore(TransactionCoordinatorID tcID) throws IOException {
        this.tcID = tcID;
        this.localID = new AtomicLong(0L);
        this.rocksDB = new KeyValueStorageRocksDB(null, null, null);
    }

    @Override
    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnID) {
        CompletableFuture<TxnMeta> future = new CompletableFuture<>();
        try {
            byte[] key = rocksDB.get(SerializationUtils.serialize(txnID));
            future.complete(SerializationUtils.deserialize(rocksDB.get(key)));
        } catch (IOException e) {
            String msg = "[" + tcID + "] Error trying to get transaction meta data for transaction with id:" + txnID;
            log.error(msg);
            future.completeExceptionally(new CoordinatorMetadataStoreException(msg));
        }
        return future;
    }

    @Override
    public CompletableFuture<TxnID> newTransaction() {
        CompletableFuture<TxnID> future = new CompletableFuture<>();
        TxnID txnID = new TxnID(tcID.getId(), localID.getAndIncrement());
        TxnMeta txnMeta = new TxnMetaImpl(txnID);
        try {
            rocksDB.put(SerializationUtils.serialize(txnID), SerializationUtils.serialize(txnMeta));
            future.complete(txnID);
        } catch (IOException e) {
            String msg = "[" + tcID + "] Error trying to store transaction meta data for transaction with id:" + txnID;
            log.error(msg);
            future.completeExceptionally(new CoordinatorMetadataStoreException(msg));
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnID, List<String> partitions) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            byte[] key = SerializationUtils.serialize(txnID);
            TxnMeta txnMeta = SerializationUtils.deserialize(rocksDB.get(key));
            txnMeta.addProducedPartitions(partitions);
            rocksDB.put(key, SerializationUtils.serialize(txnMeta));
            future.complete(null);
        } catch (InvalidTxnStatusException e) {
            String msg = "[" + tcID + "] Error trying to add produced partition to transactions with id:" + txnID +
                    " transaction is no in Open status";
            log.error(msg);
            future.completeExceptionally(new CoordinatorMetadataStoreException(msg));
        } catch (IOException e) {
            String msg = "[" + tcID + "] Error trying to update transaction meta data with new produced partitions " +
                    "for transaction with id:" + txnID;
            log.error(msg);
            future.completeExceptionally(new CoordinatorMetadataStoreException(msg));
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnID, List<String> partitions) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            byte[] key = SerializationUtils.serialize(txnID);
            TxnMeta txnMeta = SerializationUtils.deserialize(rocksDB.get(key));
            txnMeta.addAckedPartitions(partitions);
            rocksDB.put(key, SerializationUtils.serialize(txnMeta));
            future.complete(null);
        } catch (InvalidTxnStatusException e) {
            String msg = "[" + tcID + "] Error trying to add acked partitions to transaction with id:" + txnID +
                    " transaction is no in Open status";
            log.error(msg);
            future.completeExceptionally(new CoordinatorMetadataStoreException(msg));
        } catch (IOException e) {
            String msg = "[" + tcID + "] Error trying to update transaction meta data with new acked partitions " +
                    "for transaction with id:" + txnID;
            log.error(msg);
            future.completeExceptionally(new CoordinatorMetadataStoreException(msg));
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> updateTxnStatus(TxnID txnID, TxnStatus newStatus, TxnStatus expectedStatus) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            byte[] key = SerializationUtils.serialize(txnID);
            TxnMeta txnMeta = SerializationUtils.deserialize(rocksDB.get(key));
            txnMeta.updateTxnStatus(newStatus, expectedStatus);
            rocksDB.put(key, SerializationUtils.serialize(txnMeta));
            future.complete(null);
        }  catch (IOException e) {
            e.printStackTrace();
            future.completeExceptionally(e);
        } catch (InvalidTxnStatusException e) {
            e.printStackTrace();
            future.completeExceptionally(e);
        }
        return future;
    }
}
