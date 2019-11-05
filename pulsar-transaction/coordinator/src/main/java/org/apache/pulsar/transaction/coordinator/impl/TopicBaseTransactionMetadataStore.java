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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;

import org.apache.pulsar.common.api.proto.PulsarApi.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry.TransactionMetadataOp;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;
import org.apache.pulsar.common.util.FutureUtil;

import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.TxnSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;


/**
 * The provider that offers topic-base-memory implementation of {@link TransactionMetadataStore}.
 */
public class TopicBaseTransactionMetadataStore implements TransactionMetadataStore {

    private final TransactionCoordinatorID tcID;
    private final AtomicLong sequenceId;
    private final TopicBaseTransactionReader reader;
    private final TopicBaseTransactionWriter writer;

    public TopicBaseTransactionMetadataStore(TransactionCoordinatorID tcID,
                                      ManagedLedgerFactory managedLedgerFactory) throws Exception {
        this.tcID = tcID;
        this.writer = new TopicBaseTransactionWriterImpl(tcID.toString(), managedLedgerFactory);
        this.reader = new TopicBaseTransactionReaderImpl(tcID.toString(), managedLedgerFactory);
        this.sequenceId = new AtomicLong(reader.readSequenceId());
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
                    .addAllPartitions(partitions)
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
    public CompletableFuture<Void> addAckedSubscriptionToTxn(TxnID txnid, List<TxnSubscription> txnSubscriptions) {
        return getTxnMeta(txnid).thenCompose(txn -> {
            TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                    .newBuilder()
                    .setTxnidMostBits(txnid.getMostSigBits())
                    .setTxnidLeastBits(txnid.getLeastSigBits())
                    .setMetadataOp(TransactionMetadataOp.ADD_SUBSCRIPTION)
                    .addAllSubscriptions(txnSubscriptionToSubscription(txnSubscriptions))
                    .build();

            return writer.write(transactionMetadataEntry)
                    .thenCompose(txnMeta -> {
                        try {
                            txn.addTxnSubscription(txnSubscriptions);
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
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        return getTxnMeta(txnid).thenCompose(txn -> {
            TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                    .newBuilder()
                    .setTxnidMostBits(txnid.getMostSigBits())
                    .setTxnidLeastBits(txnid.getLeastSigBits())
                    .setExpectedStatus(expectedStatus)
                    .setMetadataOp(TransactionMetadataOp.UPDATE)
                    .setNewStatus(newStatus)
                    .build();
            return writer.write(transactionMetadataEntry)
                    .thenCompose(txnMeta -> {
                        try {
                            txn.updateTxnStatus(newStatus, expectedStatus);
                            transactionMetadataEntry.recycle();
                            completableFuture.complete(null);
                            return completableFuture;
                        } catch (InvalidTxnStatusException e) {
                            completableFuture.completeExceptionally(e);
                            transactionMetadataEntry.recycle();
                            return completableFuture;
                        }
                    });
        });
    }

    protected static List<Subscription> txnSubscriptionToSubscription(List<TxnSubscription> tnxSubscriptions) {
        List<Subscription> subscriptions = new ArrayList<>(tnxSubscriptions.size());
        for (int i = 0; i < tnxSubscriptions.size(); i++) {
            Subscription subscription = Subscription.newBuilder()
                    .setSubscription(tnxSubscriptions.get(i).getSubscription())
                    .setTopic(tnxSubscriptions.get(i).getTopic()).build();
            subscriptions.add(subscription);
        }
        return subscriptions;
    }

    /**
     * A reader for read transaction metadata.
     */
    protected interface TopicBaseTransactionReader {

        /**
         * Query the {@link TxnMeta} of a given transaction <tt>txnid</tt>.
         *
         * @param txnid transaction id
         * @return a future represents the result of this operation.
         *         it returns {@link TxnMeta} of the given transaction.
         */
        TxnMeta getTxnMeta(TxnID txnid);

        /**
         * Get the last sequenceId for new {@link TxnID}.
         *
         * @return {@link Long} for lst sequenceId.
         */
        Long readSequenceId();

        /**
         * Add the new {@link TxnMeta} to the cache.
         */
        void addNewTxn(TxnMeta txnMeta);

        /**
         * Get the transaction status from the {@link TxnID}.
         *
         * @return the {@link TxnID} corresponding transaction status.
         */
        TxnStatus getTxnStatus(TxnID txnID);

    }

    /**
     * A writer for write transaction metadata.
     */
    protected interface TopicBaseTransactionWriter {

        /**
         * Write the transaction operation to the transaction log.
         *
         * @param transactionMetadataEntry transaction metadata entry
         * @return a future represents the result of this operation
         */
        CompletableFuture<Void> write(TransactionMetadataEntry transactionMetadataEntry);
    }
}
