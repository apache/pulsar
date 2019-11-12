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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.mledger.ManagedLedgerException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The provider that offers managed ledger implementation of {@link TransactionMetadataStore}.
 */
public class ManagedLedgerTransactionMetadataStore implements TransactionMetadataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedLedgerTransactionReaderImpl.class);

    private final TransactionCoordinatorID tcID;
    private AtomicLong sequenceId;
    private final ManagedLedgerTransactionReader reader;
    private final ManagedLedgerTransactionWriter writer;
    protected static final long TC_ID_NOT_USED = -1L;
    private volatile State state;

    public State getState() {
        return state;
    }

    public ManagedLedgerTransactionMetadataStore(TransactionCoordinatorID tcID,
                                                 ManagedLedgerFactory managedLedgerFactory) throws Exception {
        this.tcID = tcID;
        this.state = State.NONE;
        this.writer = new ManagedLedgerTransactionWriterImpl(tcID.toString(), managedLedgerFactory);
        this.reader = new ManagedLedgerTransactionReaderImpl(tcID.toString(), managedLedgerFactory);
        this.reader.init(this);
    }

    @Override
    public CompletableFuture<TxnStatus> getTxnStatus(TxnID txnID) {
        return CompletableFuture.completedFuture(reader.getTxnStatus(txnID));
    }

    @Override
    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnID) {
        return CompletableFuture.completedFuture(reader.getTxnMeta(txnID));
    }

    @Override
    public CompletableFuture<TxnID> newTransaction() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<TxnID> newTransaction(long timeOut) {
        checkArgument(state == State.READY, "Transaction metadata store " + state.name());
        checkArgument(state == State.READY);
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
                .setStartTime(currentTimeMillis)
                .setTimeoutMs(timeOut)
                .setMetadataOp(TransactionMetadataOp.NEW)
                .setLastModificationTime(currentTimeMillis)
                .build();
        CompletableFuture completableFuture = new CompletableFuture();
        writer.write(transactionMetadataEntry)
                .thenCompose(txn -> {
                    reader.addNewTxn(new TxnMetaImpl(txnID));
                    transactionMetadataEntry.recycle();
                    completableFuture.complete(txnID);
                    return null;
                }).exceptionally(e -> {
                    LOGGER.error("Transaction-log new transaction error", e);
                    completableFuture.completeExceptionally(e);
                    return null;
                });
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnID, List<String> partitions) {
        checkArgument(state == State.READY, "Transaction metadata store " + state.name());
        return getTxnMeta(txnID).thenCompose(txn -> {
            checkArgument(txn != null);
            TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                    .newBuilder()
                    .setTxnidMostBits(txnID.getMostSigBits())
                    .setTxnidLeastBits(txnID.getLeastSigBits())
                    .setMetadataOp(TransactionMetadataOp.ADD_PARTITION)
                    .addAllPartitions(partitions)
                    .setLastModificationTime(System.currentTimeMillis())
                    .build();

            return writer.write(transactionMetadataEntry)
                    .thenCompose(v -> {
                        try {
                            txn.addProducedPartitions(partitions);
                            transactionMetadataEntry.recycle();
                            return CompletableFuture.completedFuture(null);
                        } catch (InvalidTxnStatusException e) {
                            LOGGER.error("TxnID : " + txn.id().toString()
                                    + " add produced partition error with TxnStatus : "
                                    + txn.status().name(), e);
                            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                            completableFuture.completeExceptionally(e);
                            transactionMetadataEntry.recycle();
                            return completableFuture;
                        }
                    });
        });
    }

    @Override
    public CompletableFuture<Void> addAckedSubscriptionToTxn(TxnID txnID, List<TxnSubscription> txnSubscriptions) {
        checkArgument(state == State.READY, "Transaction metadata store " + state.name());
        return getTxnMeta(txnID).thenCompose(txn -> {
            checkArgument(txn != null);
            TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                    .newBuilder()
                    .setTxnidMostBits(txnID.getMostSigBits())
                    .setTxnidLeastBits(txnID.getLeastSigBits())
                    .setMetadataOp(TransactionMetadataOp.ADD_SUBSCRIPTION)
                    .addAllSubscriptions(txnSubscriptionToSubscription(txnSubscriptions))
                    .setLastModificationTime(System.currentTimeMillis())
                    .build();

            return writer.write(transactionMetadataEntry)
                    .thenCompose(txnMeta -> {
                        try {
                            txn.addTxnSubscription(txnSubscriptions);
                            transactionMetadataEntry.recycle();
                            return CompletableFuture.completedFuture(null);
                        } catch (InvalidTxnStatusException e) {
                            LOGGER.error("TxnID : " + txn.id().toString()
                                    + " add acked subscription error with TxnStatus : "
                                    + txn.status().name(), e);
                            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                            completableFuture.completeExceptionally(e);
                            transactionMetadataEntry.recycle();
                            return completableFuture;
                        }
                    });
        });
    }

    @Override
    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnID, List<String> partitions) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());

    }

    @Override
    public CompletableFuture<Void> updateTxnStatus(TxnID txnID, TxnStatus newStatus, TxnStatus expectedStatus) {
        checkArgument(state == State.READY, "Transaction metadata store " + state.name());
        return getTxnMeta(txnID).thenCompose(txn -> {
            checkArgument(txn != null);
            TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                    .newBuilder()
                    .setTxnidMostBits(txnID.getMostSigBits())
                    .setTxnidLeastBits(txnID.getLeastSigBits())
                    .setExpectedStatus(expectedStatus)
                    .setMetadataOp(TransactionMetadataOp.UPDATE)
                    .setLastModificationTime(System.currentTimeMillis())
                    .setNewStatus(newStatus)
                    .build();
            return writer.write(transactionMetadataEntry)
                    .thenCompose(txnMeta -> {
                        try {
                            txn.updateTxnStatus(newStatus, expectedStatus);
                            transactionMetadataEntry.recycle();
                            return CompletableFuture.completedFuture(null);
                        } catch (InvalidTxnStatusException e) {
                            LOGGER.error("TxnID : " + txn.id().toString()
                                    + " add update txn status error with TxnStatus : "
                                    + txn.status().name(), e);
                            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                            completableFuture.completeExceptionally(e);
                            transactionMetadataEntry.recycle();
                            return completableFuture;
                        }
                    });
        });
    }



    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture completableFuture = new CompletableFuture();
        try {
            this.reader.close();
            this.writer.close();
        } catch (Exception e) {
            completableFuture.completeExceptionally(e);
        }
        completableFuture.complete(null);
        this.updateMetadataStoreState(State.CLOSE);
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> updateMetadataStoreState(State state) {
        this.state = state;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> setTxnSequenceId(long sequenceId) {
        if (reader.readSequenceId() == TC_ID_NOT_USED) {
            this.sequenceId = new AtomicLong(0L);
        } else {
            this.sequenceId = new AtomicLong(reader.readSequenceId() + 1L);
        }
        return CompletableFuture.completedFuture(null);
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
    protected interface ManagedLedgerTransactionReader {

        /**
         * Query the {@link TxnMeta} of a given transaction <tt>txnID</tt>.
         *
         * @param txnID transaction id
         * @return {@link TxnMeta}
         */
        TxnMeta getTxnMeta(TxnID txnID);

        /**
         * Get the last sequenceId for new {@link TxnID}.
         *
         * @return {@link Long} for last sequenceId.
         */
        Long readSequenceId();

        /**
         * Add the new {@link TxnMeta} to the cache.
         *
         * @param txnMeta transaction metadata
         */
        void addNewTxn(TxnMeta txnMeta);

        /**
         * Get the transaction status from the {@link txnID}.
         *
         * @param txnID {@link TxnID}
         * @return the {@link TxnStatus} corresponding transaction status.
         */
        TxnStatus getTxnStatus(TxnID txnID);

        /**
         * Init the managedLedger reader.
         *
         * @param transactionMetadataStore {@link TransactionMetadataStore}
         *
         * @return a future represents the result of this operation
         */
        CompletableFuture<Void> init(TransactionMetadataStore transactionMetadataStore);
        /**
         * Close the reader.
         */
        void close() throws ManagedLedgerException, InterruptedException;

    }

    /**
     * A writer for write transaction metadata.
     */
    protected interface ManagedLedgerTransactionWriter {

        /**
         * Write the transaction operation to the transaction log.
         *
         * @param transactionMetadataEntry {@link TransactionMetadataEntry} transaction metadata entry
         * @return a future represents the result of this operation
         */
        CompletableFuture<Void> write(TransactionMetadataEntry transactionMetadataEntry);

        /**
         * Close the writer.
         */
        void close() throws ManagedLedgerException, InterruptedException;
    }
}

