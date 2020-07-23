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

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry.TransactionMetadataOp;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;
import org.apache.pulsar.common.util.FutureUtil;

import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.TxnSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionNotFoundException;
import org.apache.pulsar.transaction.coordinator.timeout.MLTransactionTimeoutTrackerFactory;
import org.apache.pulsar.transaction.coordinator.timeout.TransactionTimeoutTracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The provider that offers managed ledger implementation of {@link TransactionMetadataStore}.
 */
public class MLTransactionMetadataStore
        extends TransactionMetadataStoreState implements TransactionMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(MLTransactionMetadataStore.class);

    private final TransactionCoordinatorID tcID;
    private final AtomicLong sequenceId = new AtomicLong(TC_ID_NOT_USED);
    private final MLTransactionLogImpl transactionLog;
    private static final long TC_ID_NOT_USED = -1L;
    private final ConcurrentMap<TxnID, TxnMeta> txnMetaMap = new ConcurrentHashMap<>();
    private final TransactionTimeoutTracker transactionTimeoutTracker;

    public MLTransactionMetadataStore(TransactionCoordinatorID tcID,
                                      MLTransactionLogImpl mlTransactionLog,
                                      MLTransactionTimeoutTrackerFactory mlTransactionTimeoutTrackerFactory) {
        super(State.None);
        this.tcID = tcID;
        this.transactionLog = mlTransactionLog;
        this.transactionTimeoutTracker = mlTransactionTimeoutTrackerFactory.newTracker(this);

        if (!changeToInitializingState()) {
            log.error("Managed ledger transaction metadata store change state error when init it");
            return;
        }
        new Thread(() -> transactionLog.replayAsync(new TransactionLogReplayCallback() {

            @Override
            public void replayComplete() {
                if (!changeToReadyState()) {
                    log.error("Managed ledger transaction metadata store change state error when replay complete");
                }
            }

            @Override
            public void handleMetadataEntry(Position position, TransactionMetadataEntry transactionMetadataEntry) {

                try {

                    TxnID txnID = new TxnID(transactionMetadataEntry.getTxnidMostBits(),
                            transactionMetadataEntry.getTxnidLeastBits());
                    TxnMeta txn;
                    switch (transactionMetadataEntry.getMetadataOp()) {
                        case NEW:
                            if (sequenceId.get() < transactionMetadataEntry.getTxnidLeastBits()) {
                                sequenceId.set(transactionMetadataEntry.getTxnidLeastBits());
                            }
                            txn = TxnMetaImpl.create(txnID);
                            txnMetaMap.put(txnID, txn);
                            transactionTimeoutTracker
                                    .addTransaction(sequenceId.get(), transactionMetadataEntry.getTimeoutMs());
                            break;
                        case ADD_PARTITION:
                            txn = txnMetaMap.get(txnID);
                            if (txn != null) {
                                txn.addProducedPartitions(transactionMetadataEntry.getPartitionsList());
                                txn.addTxnPosition(position);
                            } else {
                                throw new TransactionNotFoundException(txnID);
                            }
                            break;
                        case ADD_SUBSCRIPTION:
                            txn = txnMetaMap.get(txnID);
                            if (txn != null) {
                                txn.addAckedPartitions(
                                        subscriptionToTxnSubscription
                                                (transactionMetadataEntry.getSubscriptionsList()));
                                txn.addTxnPosition(position);
                            } else {
                                throw new TransactionNotFoundException(txnID);
                            }
                            break;
                        case UPDATE:
                            txn = txnMetaMap.get(txnID);
                            if (txn != null) {
                                TxnStatus newStatus = transactionMetadataEntry.getNewStatus();
                                txn.addTxnPosition(position);
                                if (newStatus == TxnStatus.COMMITTED || newStatus == TxnStatus.ABORTED) {
                                    transactionLog.deletePosition(txn.positions()).thenAccept(v -> {
                                        txnMetaMap.remove(txnID);
                                        ((TxnMetaImpl) txn).recycle();
                                    });
                                } else {
                                    txn.updateTxnStatus(transactionMetadataEntry.getNewStatus(),
                                            transactionMetadataEntry.getExpectedStatus());
                                }
                            } else {
                                throw new TransactionNotFoundException(txnID);
                            }
                            break;
                        default:
                            throw new InvalidTxnStatusException("Transaction `"
                                    + txnID + "` load replay metadata operation "
                                    + "from transaction log with unknown operation");
                    }
                } catch (InvalidTxnStatusException | TransactionNotFoundException e) {
                    log.error(e.getMessage(), e);
                }
            }
        })).start();
    }

    @Override
    public CompletableFuture<TxnStatus> getTxnStatusAsync(TxnID txnID) {
        return CompletableFuture.completedFuture(txnMetaMap.get(txnID).status());
    }

    @Override
    public CompletableFuture<TxnMeta> getTxnMetaAsync(TxnID txnID) {
        return CompletableFuture.completedFuture(txnMetaMap.get(txnID));
    }

    @Override
    public CompletableFuture<TxnID> newTransactionAsync(long timeOut) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new CoordinatorException
                            .TransactionMetadataStoreStateException(tcID, State.Ready, getState(), "new Transaction"));
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
        return transactionLog.append(transactionMetadataEntry)
                .thenCompose(v -> {
                    TxnMeta txn = TxnMetaImpl.create(txnID);
                    txnMetaMap.put(txnID, txn);
                    txn.addTxnPosition(v);
                    transactionTimeoutTracker
                            .addTransaction(leastSigBits, timeOut);
                    transactionMetadataEntry.recycle();
                    return CompletableFuture.completedFuture(txnID);
                });
    }

    @Override
    public CompletableFuture<Void> addProducedPartitionToTxnAsync(TxnID txnID, List<String> partitions) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new CoordinatorException.TransactionMetadataStoreStateException(tcID,
                            State.Ready, getState(), "add produced partition"));
        }
        return getTxnMetaAsync(txnID).thenCompose(txn -> {
            if (txn == null) {
                return FutureUtil.failedFuture(new TransactionNotFoundException(txnID));
            } else {
                TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                        .newBuilder()
                        .setTxnidMostBits(txnID.getMostSigBits())
                        .setTxnidLeastBits(txnID.getLeastSigBits())
                        .setMetadataOp(TransactionMetadataOp.ADD_PARTITION)
                        .addAllPartitions(partitions)
                        .setLastModificationTime(System.currentTimeMillis())
                        .build();

                return transactionLog.append(transactionMetadataEntry)
                        .thenCompose(position -> {
                            try {
                                txn.addProducedPartitions(partitions);
                                txn.addTxnPosition(position);
                                return CompletableFuture.completedFuture(null);
                            } catch (InvalidTxnStatusException e) {
                                log.error("TxnID : " + txn.id().toString()
                                        + " add produced partition error with TxnStatus : "
                                        + txn.status().name(), e);
                                return FutureUtil.failedFuture(e);
                            } finally {
                                transactionMetadataEntry.recycle();
                            }
                        });
            }
        });
    }

    @Override
    public CompletableFuture<Void> addAckedPartitionToTxnAsync(TxnID txnID, List<TxnSubscription> txnSubscriptions) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new CoordinatorException.TransactionMetadataStoreStateException(tcID,
                            State.Ready, getState(), "add acked partition"));
        }
        return getTxnMetaAsync(txnID).thenCompose(txn -> {
            if (txn == null) {
                return FutureUtil.failedFuture(new TransactionNotFoundException(txnID));
            } else {
                TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                        .newBuilder()
                        .setTxnidMostBits(txnID.getMostSigBits())
                        .setTxnidLeastBits(txnID.getLeastSigBits())
                        .setMetadataOp(TransactionMetadataOp.ADD_SUBSCRIPTION)
                        .addAllSubscriptions(txnSubscriptionToSubscription(txnSubscriptions))
                        .setLastModificationTime(System.currentTimeMillis())
                        .build();

                return transactionLog.append(transactionMetadataEntry)
                        .thenCompose(position -> {
                            try {
                                txn.addAckedPartitions(txnSubscriptions);
                                txn.addTxnPosition(position);
                                return CompletableFuture.completedFuture(null);
                            } catch (InvalidTxnStatusException e) {
                                log.error("TxnID : " + txn.id().toString()
                                        + " add acked subscription error with TxnStatus : "
                                        + txn.status().name(), e);
                                return FutureUtil.failedFuture(e);
                            } finally {
                                transactionMetadataEntry.recycle();
                            }
                        });
            }
        });
    }

    @Override
    public CompletableFuture<Void> updateTxnStatusAsync(TxnID txnID, TxnStatus newStatus, TxnStatus expectedStatus) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new CoordinatorException.TransactionMetadataStoreStateException(tcID,
                            State.Ready, getState(), "update transaction status"));
        }
        return getTxnMetaAsync(txnID).thenCompose(txn -> {
            if (txn == null) {
                return FutureUtil.failedFuture(new TransactionNotFoundException(txnID));
            } else {
                TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                        .newBuilder()
                        .setTxnidMostBits(txnID.getMostSigBits())
                        .setTxnidLeastBits(txnID.getLeastSigBits())
                        .setExpectedStatus(expectedStatus)
                        .setMetadataOp(TransactionMetadataOp.UPDATE)
                        .setLastModificationTime(System.currentTimeMillis())
                        .setNewStatus(newStatus)
                        .build();

                return transactionLog.append(transactionMetadataEntry).thenCompose(position -> {
                    try {
                        txn.updateTxnStatus(newStatus, expectedStatus);
                        txn.addTxnPosition(position);
                        if (newStatus == TxnStatus.COMMITTED || newStatus == TxnStatus.ABORTED) {
                            return transactionLog.deletePosition(txn.positions()).thenCompose(v -> {
                                txnMetaMap.remove(txnID);
                                ((TxnMetaImpl) txn).recycle();
                                return CompletableFuture.completedFuture(null);
                            });
                        }
                        return CompletableFuture.completedFuture(null);
                    } catch (InvalidTxnStatusException e) {
                        log.error("TxnID : " + txn.id().toString()
                                + " add update txn status error with TxnStatus : "
                                + txn.status().name(), e);
                        return FutureUtil.failedFuture(e);
                    } finally {
                        transactionMetadataEntry.recycle();
                    }
                });
            }
        });
    }

    @Override
    public TransactionCoordinatorID getTransactionCoordinatorID() {
        return tcID;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return transactionLog.closeAsync().thenCompose(v -> {
            txnMetaMap.clear();
            transactionTimeoutTracker.close();
            if (!this.changeToCloseState()) {
                return FutureUtil.failedFuture(
                        new IllegalStateException("Managed ledger transaction metadata store state to close error!"));
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    private static List<PulsarApi.Subscription> txnSubscriptionToSubscription(List<TxnSubscription> tnxSubscriptions) {
        List<PulsarApi.Subscription> subscriptions = new ArrayList<>(tnxSubscriptions.size());
        for (TxnSubscription tnxSubscription : tnxSubscriptions) {
            PulsarApi.Subscription subscription = PulsarApi.Subscription.newBuilder()
                    .setSubscription(tnxSubscription.getSubscription())
                    .setTopic(tnxSubscription.getTopic()).build();
            subscriptions.add(subscription);
        }
        return subscriptions;
    }

    private static List<TxnSubscription> subscriptionToTxnSubscription(List<PulsarApi.Subscription> subscriptions) {
        List<TxnSubscription> txnSubscriptions = new ArrayList<>(subscriptions.size());
        for (PulsarApi.Subscription subscription : subscriptions) {
            txnSubscriptions
                    .add(new TxnSubscription(subscription.getTopic(), subscription.getSubscription()));
            subscription.recycle();
        }
        return txnSubscriptions;
    }

    @VisibleForTesting
    public ConcurrentMap<TxnID, TxnMeta> getTxnMetaMap() {
        return txnMetaMap;
    }
}

