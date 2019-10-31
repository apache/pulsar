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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry.TransactionMetadataOp;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;


/**
 * The provider that offers in-memory implementation of {@link TransactionMetadataStore}.
 */
public class TopicBaseTransactionMetadataStore implements TransactionMetadataStore {

    private final TransactionCoordinatorID tcID;
    private final AtomicLong sequenceId;
    private final TopicBaseTransactionReader reader;
    private final TopicBaseTransactionWriter writer;

    TopicBaseTransactionMetadataStore(TransactionCoordinatorID tcID, ManagedLedgerFactory managedLedgerFactory) throws Exception{
        this.tcID = tcID;
        this.reader = new TopicBaseTransactionReaderImpl(tcID.toString(), managedLedgerFactory);
        this.sequenceId = new AtomicLong(reader.readSequenceId());
        this.writer = new TopicBaseTransactionWriterImpl(tcID.toString(), managedLedgerFactory);
    }

    @Override
    public CompletableFuture<TxnStatus> getTxnStatus(TxnID txnid) {

        return CompletableFuture.completedFuture(reader.getTxnStatus(txnid));
    }

    @Override
    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnid) {

        return CompletableFuture.completedFuture(reader.getTxnMeta(txnid));
    }

    @Override
    public CompletableFuture<TxnID> newTransaction() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<TxnID> newTransaction(long timeOut) {
        long mostSigBits = tcID.getId();
        long leastSigBits = sequenceId.getAndIncrement();

        TxnID txnID = new TxnID(
                mostSigBits,
                leastSigBits
        );
        long currentTimeMillis = System.currentTimeMillis();
        TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                .newBuilder()
                .setTxnidMostBits(mostSigBits)
                .setTxnidLeastBits(leastSigBits)
                .setTxnStartTime(currentTimeMillis)
                .setTxnTimeoutMs(timeOut)
                .setMetadataOp(TransactionMetadataOp.NEW)
                .setTxnLastModificationTime(currentTimeMillis)
                .build();

        return writer.write(transactionMetadataEntry)
                .thenCompose(txn -> {
                    reader.addNewTxn(new TxnMetaImpl(txnID));
                    transactionMetadataEntry.recycle();
                    return CompletableFuture.completedFuture(txnID);
                });
    }

    @Override
    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnid, List<String> partitions) {
        return getTxnMeta(txnid).thenCompose(txn -> {
            TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                    .newBuilder()
                    .setTxnidMostBits(txnid.getMostSigBits())
                    .setTxnidLeastBits(txnid.getLeastSigBits())
                    .setMetadataOp(TransactionMetadataOp.ADD_PARTITION)
                    .build();

            return writer.write(transactionMetadataEntry)
                    .thenCompose(txnMeta -> {
                        try {
                            txn.addProducedPartitions(partitions);
                            transactionMetadataEntry.recycle();
                            return CompletableFuture.completedFuture(null);
                        } catch (InvalidTxnStatusException e) {
                            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                            completableFuture.completeExceptionally(e);
                            transactionMetadataEntry.recycle();
                            return completableFuture;
                        }
                    });
        });
    }

    @Override
    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnid, List<String> partitions) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());

    }

    @Override
    public CompletableFuture<Void> updateTxnStatus(TxnID txnid, TxnStatus newStatus, TxnStatus expectedStatus) {
        return getTxnMeta(txnid).thenCompose(txn -> {
            TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                    .newBuilder()
                    .setTxnidMostBits(txnid.getMostSigBits())
                    .setTxnidLeastBits(txnid.getLeastSigBits())
                    .setExpectedStatus(expectedStatus)
                    .setNewStatus(newStatus)
                    .build();
            return writer.write(transactionMetadataEntry)
                    .thenCompose(txnMeta -> {
                        try {
                            txn.updateTxnStatus(newStatus, expectedStatus);
                            return CompletableFuture.completedFuture(null);
                        } catch (InvalidTxnStatusException e) {
                            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                            completableFuture.completeExceptionally(e);
                            transactionMetadataEntry.recycle();
                            return completableFuture;
                        }
                    });
        });
    }

    public interface TopicBaseTransactionReader {

        /**
         * Query the {@link TxnMeta} of a given transaction <tt>txnid</tt>.
         *
         * @param txnid transaction id
         * @return a future represents the result of this operation.
         *         it returns {@link TxnMeta} of the given transaction.
         */
        TxnMeta getTxnMeta(TxnID txnid);

        /**
         * Query the sequenceId for new {@link TxnID}
         *
         * @return {@link Long} for generate sequenceId
         */
        Long readSequenceId();

        void addNewTxn(TxnMeta txnMeta);

        TxnStatus getTxnStatus(TxnID txnID);

    }

    public interface TopicBaseTransactionWriter {

        CompletableFuture<Void> write(TransactionMetadataEntry transactionMetadataEntry);
    }
}
