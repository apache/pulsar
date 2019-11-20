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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import org.apache.pulsar.transaction.coordinator.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.coordinator.exceptions.TxnStoreStateUpdateException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.util.TransactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The provider that offers managed ledger implementation of {@link TransactionMetadataStore}.
 */
public class ManagedLedgerTransactionMetadataStore implements TransactionMetadataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedLedgerTransactionMetadataStore.class);

    private final TransactionCoordinatorID tcID;
    private AtomicLong sequenceId = new AtomicLong(TC_ID_NOT_USED);
    private final ManagedLedgerTransactionLog transactionLog;
    private static final long TC_ID_NOT_USED = -1L;
    private volatile State state;
    private ConcurrentMap<TxnID, TxnMeta> txnMetaMap = new ConcurrentHashMap<>();

    public ManagedLedgerTransactionMetadataStore(TransactionCoordinatorID tcID,
                                                 ManagedLedgerFactory managedLedgerFactory) throws Exception {
        this.tcID = tcID;
        this.state = State.NONE;
        this.transactionLog = new ManagedLedgerTransactionLogImpl(tcID.getId(), managedLedgerFactory, this);
        this.transactionLog.init();
    }

    ConcurrentMap<TxnID, TxnMeta> getTxnMetaMap() {
        return txnMetaMap;
    }

    public State getState() {
        return state;
    }


    public AtomicLong getSequenceId() {
        return sequenceId;
    }

    @Override
    public CompletableFuture<TxnStatus> getTxnStatus(TxnID txnID) {
        return CompletableFuture.completedFuture(txnMetaMap.get(txnID).status());
    }

    @Override
    public CompletableFuture<TxnMeta> getTxnMetaAsync(TxnID txnID) {
        return CompletableFuture.completedFuture(txnMetaMap.get(txnID));
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync(long timeOut) {
        if (state != State.READY) {
            return FutureUtil.failedFuture(new TxnStoreStateUpdateException("Transaction metadata store state : "
                    + state.name() + " when new transaction with tcId : " + tcID));
        }
        long mostSigBits = tcID.getId();
        long leastSigBits = sequenceId.incrementAndGet();

        TxnID txnID = new TxnID(mostSigBits, leastSigBits);
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
        CompletableFuture<TxnID> completableFuture = new CompletableFuture<>();
        transactionLog.write(transactionMetadataEntry)
                .whenComplete((v, e) -> {
                    if (e == null) {
                        txnMetaMap.put(txnID, new TxnMetaImpl(txnID));
                        completableFuture.complete(txnID);
                    } else {
                        completableFuture.completeExceptionally(e);
                    }
                    transactionMetadataEntry.recycle();
                });
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> addProducedPartitionToTxnAsync(TxnID txnID, List<String> partitions) {
        if (state != State.READY) {
            return FutureUtil.failedFuture(new TxnStoreStateUpdateException("Transaction metadata store state : "
                    + state.name() + " when add have produced partition with txnID : " + txnID));
        }
        return getTxnMetaAsync(txnID).thenCompose(txn -> {
            if (txn == null) {
                return FutureUtil.failedFuture(new TransactionNotFoundException("Transaction not found :" + txnID));
            } else {
                TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                        .newBuilder()
                        .setTxnidMostBits(txnID.getMostSigBits())
                        .setTxnidLeastBits(txnID.getLeastSigBits())
                        .setMetadataOp(TransactionMetadataOp.ADD_PARTITION)
                        .addAllPartitions(partitions)
                        .setLastModificationTime(System.currentTimeMillis())
                        .build();

                return transactionLog.write(transactionMetadataEntry)
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
            }
        });
    }

    @Override
    public CompletableFuture<Void> addAckedPartitionToTxnAsync(TxnID txnID, List<TxnSubscription> txnSubscriptions) {
        if (state != State.READY) {
            return FutureUtil.failedFuture(new TxnStoreStateUpdateException("Transaction metadata store state : "
                    + state.name() + " when add acked partition with txnID : " + txnID));
        }
        return getTxnMetaAsync(txnID).thenCompose(txn -> {
            if (txn == null) {
                return FutureUtil.failedFuture(new TransactionNotFoundException("Transaction not found :" + txnID));
            } else {
                TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                        .newBuilder()
                        .setTxnidMostBits(txnID.getMostSigBits())
                        .setTxnidLeastBits(txnID.getLeastSigBits())
                        .setMetadataOp(TransactionMetadataOp.ADD_SUBSCRIPTION)
                        .addAllSubscriptions(txnSubscriptionToSubscription(txnSubscriptions))
                        .setLastModificationTime(System.currentTimeMillis())
                        .build();

                return transactionLog.write(transactionMetadataEntry)
                        .thenCompose(txnMeta -> {
                            try {
                                txn.addAckedPartitions(txnSubscriptions);
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
            }
        });
    }

    @Override
    public CompletableFuture<Void> updateTxnStatusAsync(TxnID txnID, TxnStatus newStatus, TxnStatus expectedStatus) {
        if (state != State.READY) {
            return FutureUtil.failedFuture(new TxnStoreStateUpdateException("Transaction metadata store state : "
                    + state.name() + " when update txn status with txnID : " + txnID));
        }
        return getTxnMetaAsync(txnID).thenCompose(txn -> {
            if (txn == null) {
                return FutureUtil.failedFuture(new TransactionNotFoundException("Transaction not found :" + txnID));
            } else {
                synchronized (txn) {
                    try {
                        txn.checkTxnStatus(expectedStatus);
                    } catch (InvalidTxnStatusException e) {
                        return FutureUtil.failedFuture(e);
                    }
                    if (!TransactionUtil.canTransitionTo(txn.status(), newStatus)) {
                        return FutureUtil.failedFuture(new InvalidTxnStatusException(
                                "Transaction `" + txnID + "` CANNOT transaction from status "
                                        + txn.status() + " to " + newStatus));
                    }
                TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                        .newBuilder()
                        .setTxnidMostBits(txnID.getMostSigBits())
                        .setTxnidLeastBits(txnID.getLeastSigBits())
                        .setExpectedStatus(expectedStatus)
                        .setMetadataOp(TransactionMetadataOp.UPDATE)
                        .setLastModificationTime(System.currentTimeMillis())
                        .setNewStatus(newStatus)
                        .build();
                    return transactionLog.write(transactionMetadataEntry)
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
                }
            }
        });
    }



    @Override
    public CompletableFuture<Void> closeAsync() {
        try {
            transactionLog.close();
            this.updateMetadataStoreState(State.CLOSE);
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
        return CompletableFuture.completedFuture(null);
    }

    void updateMetadataStoreState(State state) throws TxnStoreStateUpdateException {
        switch (state) {
            case NONE:
                if (!this.state.equals(State.NONE)) {
                    throw new TxnStoreStateUpdateException(tcID.toString(), this.state, state);
                }
                this.state = state;
                break;
            case INITIALIZING:
                if (!this.state.equals(State.NONE)) {
                    throw new TxnStoreStateUpdateException(tcID.toString(), this.state, state);
                }
                this.state = state;
                break;
            case READY:
                if (!this.state.equals(State.INITIALIZING)) {
                    throw new TxnStoreStateUpdateException(tcID.toString(), this.state, state);
                }
                this.state = state;
                break;
            case CLOSE:
                if (this.state.equals(State.CLOSE)) {
                    throw new TxnStoreStateUpdateException(tcID.toString(), this.state, state);
                }
                this.state = state;
                break;
            default:
                throw new TxnStoreStateUpdateException("Unknown state the transaction store to be change");
        }
    }

    private static List<Subscription> txnSubscriptionToSubscription(List<TxnSubscription> tnxSubscriptions) {
        List<Subscription> subscriptions = new ArrayList<>(tnxSubscriptions.size());
        for (TxnSubscription tnxSubscription : tnxSubscriptions) {
            Subscription subscription = Subscription.newBuilder()
                    .setSubscription(tnxSubscription.getSubscription())
                    .setTopic(tnxSubscription.getTopic()).build();
            subscriptions.add(subscription);
        }
        return subscriptions;
    }

    /**
     * A reader for read transaction metadata.
     */
    protected interface ManagedLedgerTransactionLog {

        /**
         * Init the managedLedger log.
         */
        void init();

        /**
         * Close the transaction log.
         */
        void close() throws ManagedLedgerException, InterruptedException;

        /**
         * Write the transaction operation to the transaction log.
         *
         * @param transactionMetadataEntry {@link TransactionMetadataEntry} transaction metadata entry
         * @return a future represents the result of this operation
         */
        CompletableFuture<Void> write(TransactionMetadataEntry transactionMetadataEntry);

    }
}

