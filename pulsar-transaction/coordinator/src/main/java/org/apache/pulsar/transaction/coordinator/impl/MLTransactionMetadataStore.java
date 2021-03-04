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
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.Subscription;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionNotFoundException;
import org.apache.pulsar.transaction.coordinator.proto.TransactionMetadataEntry;
import org.apache.pulsar.transaction.coordinator.proto.TransactionMetadataEntry.TransactionMetadataOp;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
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
    private final ConcurrentMap<TxnID, Pair<TxnMeta, List<Position>>> txnMetaMap = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<Long> txnIdSortedSet = new ConcurrentSkipListSet<>();
    private final TransactionTimeoutTracker timeoutTracker;

    public MLTransactionMetadataStore(TransactionCoordinatorID tcID,
                                      MLTransactionLogImpl mlTransactionLog,
                                      TransactionTimeoutTracker timeoutTracker) {
        super(State.None);
        this.tcID = tcID;
        this.transactionLog = mlTransactionLog;
        this.timeoutTracker = timeoutTracker;

        if (!changeToInitializingState()) {
            log.error("Managed ledger transaction metadata store change state error when init it");
            return;
        }
        new Thread(() -> transactionLog.replayAsync(new TransactionLogReplayCallback() {

            @Override
            public void replayComplete() {
                if (!changeToReadyState()) {
                    log.error("Managed ledger transaction metadata store change state error when replay complete");
                } else {
                    timeoutTracker.start();
                }
            }

            @Override
            public void handleMetadataEntry(Position position, TransactionMetadataEntry transactionMetadataEntry) {

                try {

                    TxnID txnID = new TxnID(transactionMetadataEntry.getTxnidMostBits(),
                            transactionMetadataEntry.getTxnidLeastBits());
                    switch (transactionMetadataEntry.getMetadataOp()) {
                        case NEW:
                            if (sequenceId.get() < transactionMetadataEntry.getTxnidLeastBits()) {
                                sequenceId.set(transactionMetadataEntry.getTxnidLeastBits());
                            }
                            if (txnMetaMap.containsKey(txnID)) {
                                txnMetaMap.get(txnID).getRight().add(position);
                            } else {
                                List<Position> positions = new ArrayList<>();
                                positions.add(position);
                                txnMetaMap.put(txnID, MutablePair.of(new TxnMetaImpl(txnID), positions));
                                txnIdSortedSet.add(transactionMetadataEntry.getTxnidLeastBits());
                                timeoutTracker.replayAddTransaction(transactionMetadataEntry.getTxnidLeastBits(),
                                        transactionMetadataEntry.getTimeoutMs());
                            }
                            break;
                        case ADD_PARTITION:
                            if (!txnMetaMap.containsKey(txnID)) {
                                transactionLog.deletePosition(Collections.singletonList(position));
                            } else {
                                txnMetaMap.get(txnID).getLeft()
                                        .addProducedPartitions(transactionMetadataEntry.getPartitionsList());
                                txnMetaMap.get(txnID).getRight().add(position);
                            }
                            break;
                        case ADD_SUBSCRIPTION:
                            if (!txnMetaMap.containsKey(txnID)) {
                                transactionLog.deletePosition(Collections.singletonList(position));
                            } else {
                                txnMetaMap.get(txnID).getLeft()
                                        .addAckedPartitions(subscriptionToTxnSubscription(
                                                transactionMetadataEntry.getSubscriptionsList()));
                                txnMetaMap.get(txnID).getRight().add(position);
                            }
                            break;
                        case UPDATE:
                            if (!txnMetaMap.containsKey(txnID)) {
                                transactionLog.deletePosition(Collections.singletonList(position));
                            } else {
                                TxnStatus newStatus = transactionMetadataEntry.getNewStatus();
                                if (newStatus == TxnStatus.COMMITTED || newStatus == TxnStatus.ABORTED) {
                                    transactionLog.deletePosition(txnMetaMap.get(txnID).getRight()).thenAccept(v -> {
                                        TxnMeta txnMeta = txnMetaMap.remove(txnID).getLeft();
                                        txnIdSortedSet.remove(transactionMetadataEntry.getTxnidLeastBits());
                                    });
                                } else {
                                    txnMetaMap.get(txnID).getLeft()
                                            .updateTxnStatus(transactionMetadataEntry.getNewStatus(),
                                                    transactionMetadataEntry.getExpectedStatus());
                                }
                                txnMetaMap.get(txnID).getRight().add(position);
                            }
                            break;
                        default:
                            throw new InvalidTxnStatusException("Transaction `"
                                    + txnID + "` load replay metadata operation "
                                    + "from transaction log with unknown operation");
                    }
                } catch (InvalidTxnStatusException  e) {
                    log.error(e.getMessage(), e);
                }
            }
        })).start();
    }

    @Override
    public CompletableFuture<TxnStatus> getTxnStatus(TxnID txnID) {
        return CompletableFuture.completedFuture(txnMetaMap.get(txnID).getLeft().status());
    }

    @Override
    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnID) {
        Pair<TxnMeta, List<Position>> txnMetaListPair = txnMetaMap.get(txnID);
        CompletableFuture<TxnMeta> completableFuture = new CompletableFuture<>();
        if (txnMetaListPair == null) {
            completableFuture.completeExceptionally(new TransactionNotFoundException(txnID));
        } else {
            completableFuture.complete(txnMetaListPair.getLeft());
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<TxnID> newTransaction(long timeOut) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new CoordinatorException
                            .TransactionMetadataStoreStateException(tcID, State.Ready, getState(), "new Transaction"));
        }

        long mostSigBits = tcID.getId();
        long leastSigBits = sequenceId.incrementAndGet();
        TxnID txnID = new TxnID(mostSigBits, leastSigBits);
        long currentTimeMillis = System.currentTimeMillis();
        TransactionMetadataEntry transactionMetadataEntry = new TransactionMetadataEntry()
                .setTxnidMostBits(mostSigBits)
                .setTxnidLeastBits(leastSigBits)
                .setStartTime(currentTimeMillis)
                .setTimeoutMs(timeOut)
                .setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.NEW)
                .setLastModificationTime(currentTimeMillis);
        return transactionLog.append(transactionMetadataEntry)
                .thenCompose(position -> {
                    TxnMeta txn = new TxnMetaImpl(txnID);
                    List<Position> positions = new ArrayList<>();
                    positions.add(position);
                    Pair<TxnMeta, List<Position>> pair = MutablePair.of(txn, positions);
                    txnMetaMap.put(txnID, pair);
                    this.timeoutTracker.addTransaction(leastSigBits, timeOut);
                    this.txnIdSortedSet.add(leastSigBits);
                    return CompletableFuture.completedFuture(txnID);
                });
    }

    @Override
    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnID, List<String> partitions) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new CoordinatorException.TransactionMetadataStoreStateException(tcID,
                            State.Ready, getState(), "add produced partition"));
        }
        return getTxnPositionPair(txnID).thenCompose(txnMetaListPair -> {
            TransactionMetadataEntry transactionMetadataEntry = new TransactionMetadataEntry()
                    .setTxnidMostBits(txnID.getMostSigBits())
                    .setTxnidLeastBits(txnID.getLeastSigBits())
                    .setMetadataOp(TransactionMetadataOp.ADD_PARTITION)
                    .addAllPartitions(partitions)
                    .setLastModificationTime(System.currentTimeMillis());

            return transactionLog.append(transactionMetadataEntry)
                    .thenCompose(position -> {
                        try {
                            synchronized (txnMetaListPair.getLeft()) {
                                txnMetaListPair.getLeft().addProducedPartitions(partitions);
                                txnMetaMap.get(txnID).getRight().add(position);
                            }
                            return CompletableFuture.completedFuture(null);
                        } catch (InvalidTxnStatusException e) {
                            transactionLog.deletePosition(Collections.singletonList(position));
                            log.error("TxnID : " + txnMetaListPair.getLeft().id().toString()
                                    + " add produced partition error with TxnStatus : "
                                    + txnMetaListPair.getLeft().status().name(), e);
                            return FutureUtil.failedFuture(e);
                        }
                    });
        });
    }

    @Override
    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnID,
                                                          List<TransactionSubscription> txnSubscriptions) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new CoordinatorException.TransactionMetadataStoreStateException(tcID,
                            State.Ready, getState(), "add acked partition"));
        }
        return getTxnPositionPair(txnID).thenCompose(txnMetaListPair -> {
            TransactionMetadataEntry transactionMetadataEntry = new TransactionMetadataEntry()
                    .setTxnidMostBits(txnID.getMostSigBits())
                    .setTxnidLeastBits(txnID.getLeastSigBits())
                    .setMetadataOp(TransactionMetadataOp.ADD_SUBSCRIPTION)
                    .addAllSubscriptions(txnSubscriptionToSubscription(txnSubscriptions))
                    .setLastModificationTime(System.currentTimeMillis());

            return transactionLog.append(transactionMetadataEntry)
                    .thenCompose(position -> {
                        try {
                            synchronized (txnMetaListPair.getLeft()) {
                                txnMetaListPair.getLeft().addAckedPartitions(txnSubscriptions);
                                txnMetaMap.get(txnID).getRight().add(position);
                            }
                            return CompletableFuture.completedFuture(null);
                        } catch (InvalidTxnStatusException e) {
                            transactionLog.deletePosition(Collections.singletonList(position));
                            log.error("TxnID : " + txnMetaListPair.getLeft().id().toString()
                                    + " add acked subscription error with TxnStatus : "
                                    + txnMetaListPair.getLeft().status().name(), e);
                            return FutureUtil.failedFuture(e);
                        }
                    });
        });
    }

    @Override
    public CompletableFuture<Void> updateTxnStatus(TxnID txnID, TxnStatus newStatus, TxnStatus expectedStatus) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new CoordinatorException.TransactionMetadataStoreStateException(tcID,
                            State.Ready, getState(), "update transaction status"));
        }
        return getTxnPositionPair(txnID).thenCompose(txnMetaListPair -> {
            if (txnMetaListPair.getLeft().status() == newStatus) {
                return CompletableFuture.completedFuture(null);
            }
            TransactionMetadataEntry transactionMetadataEntry = new TransactionMetadataEntry()
                    .setTxnidMostBits(txnID.getMostSigBits())
                    .setTxnidLeastBits(txnID.getLeastSigBits())
                    .setExpectedStatus(expectedStatus)
                    .setMetadataOp(TransactionMetadataOp.UPDATE)
                    .setLastModificationTime(System.currentTimeMillis())
                    .setNewStatus(newStatus);

            return transactionLog.append(transactionMetadataEntry).thenCompose(position -> {
                try {
                    synchronized (txnMetaListPair.getLeft()) {
                        txnMetaListPair.getLeft().updateTxnStatus(newStatus, expectedStatus);
                        txnMetaListPair.getRight().add(position);
                    }
                    if (newStatus == TxnStatus.COMMITTED || newStatus == TxnStatus.ABORTED) {
                        return transactionLog.deletePosition(txnMetaListPair.getRight()).thenCompose(v -> {
                            txnMetaMap.remove(txnID);
                            txnIdSortedSet.remove(txnID.getLeastSigBits());
                            return CompletableFuture.completedFuture(null);
                        });
                    }
                    return CompletableFuture.completedFuture(null);
                } catch (InvalidTxnStatusException e) {
                    transactionLog.deletePosition(Collections.singletonList(position));
                    log.error("TxnID : " + txnMetaListPair.getLeft().id().toString()
                            + " add update txn status error with TxnStatus : "
                            + txnMetaListPair.getLeft().status().name(), e);
                    return FutureUtil.failedFuture(e);
                }
            });
        });
    }

    @Override
    public long getLowWaterMark() {
        try {
            return this.txnIdSortedSet.first() - 1;
        } catch (NoSuchElementException e) {
            return 0L;
        }
    }

    @Override
    public TransactionCoordinatorID getTransactionCoordinatorID() {
        return tcID;
    }

    private CompletableFuture<Pair<TxnMeta, List<Position>>> getTxnPositionPair(TxnID txnID) {
        CompletableFuture<Pair<TxnMeta, List<Position>>> completableFuture = new CompletableFuture<>();
        Pair<TxnMeta, List<Position>> txnMetaListPair = txnMetaMap.get(txnID);
        if (txnMetaListPair == null) {
            completableFuture.completeExceptionally(new TransactionNotFoundException(txnID));
        } else {
            completableFuture.complete(txnMetaListPair);
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return transactionLog.closeAsync().thenCompose(v -> {
            txnMetaMap.clear();
            if (!this.changeToCloseState()) {
                return FutureUtil.failedFuture(
                        new IllegalStateException("Managed ledger transaction metadata store state to close error!"));
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    public static List<Subscription> txnSubscriptionToSubscription(List<TransactionSubscription> tnxSubscriptions) {
        List<Subscription> subscriptions = new ArrayList<>(tnxSubscriptions.size());
        for (TransactionSubscription transactionSubscription : tnxSubscriptions) {
            Subscription subscription = new Subscription()
                    .setSubscription(transactionSubscription.getSubscription())
                    .setTopic(transactionSubscription.getTopic());
            subscriptions.add(subscription);
        }
        return subscriptions;
    }

    public static List<TransactionSubscription> subscriptionToTxnSubscription(
            List<Subscription> subscriptions) {
        List<TransactionSubscription> transactionSubscriptions = new ArrayList<>(subscriptions.size());
        for (Subscription subscription : subscriptions) {
            TransactionSubscription.TransactionSubscriptionBuilder transactionSubscriptionBuilder  =
                    TransactionSubscription.builder();
            transactionSubscriptionBuilder.subscription(subscription.getSubscription());
            transactionSubscriptionBuilder.topic(subscription.getTopic());
            transactionSubscriptions
                    .add(transactionSubscriptionBuilder.build());
        }
        return transactionSubscriptions;
    }
}