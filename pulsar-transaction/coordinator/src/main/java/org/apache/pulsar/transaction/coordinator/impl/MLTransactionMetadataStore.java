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

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.Subscription;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
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
    private final MLTransactionLogImpl transactionLog;
    private final ConcurrentSkipListMap<Long, Pair<TxnMeta, List<Position>>> txnMetaMap = new ConcurrentSkipListMap<>();
    private final TransactionTimeoutTracker timeoutTracker;
    private final TransactionMetadataStoreStats transactionMetadataStoreStats;
    private final LongAdder createdTransactionCount;
    private final LongAdder committedTransactionCount;
    private final LongAdder abortedTransactionCount;
    private final LongAdder transactionTimeoutCount;
    private final LongAdder appendLogCount;
    private final MLTransactionSequenceIdGenerator sequenceIdGenerator;
    private final ExecutorService internalPinnedExecutor;

    public MLTransactionMetadataStore(TransactionCoordinatorID tcID,
                                      MLTransactionLogImpl mlTransactionLog,
                                      TransactionTimeoutTracker timeoutTracker,
                                      MLTransactionSequenceIdGenerator sequenceIdGenerator) {
        super(State.None);
        this.sequenceIdGenerator = sequenceIdGenerator;
        this.tcID = tcID;
        this.transactionLog = mlTransactionLog;
        this.timeoutTracker = timeoutTracker;
        this.transactionMetadataStoreStats = new TransactionMetadataStoreStats();

        this.createdTransactionCount = new LongAdder();
        this.committedTransactionCount = new LongAdder();
        this.abortedTransactionCount = new LongAdder();
        this.transactionTimeoutCount = new LongAdder();
        this.appendLogCount = new LongAdder();
        DefaultThreadFactory threadFactory = new DefaultThreadFactory("transaction_coordinator_"
                + tcID.toString() + "thread_factory");
        this.internalPinnedExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    public CompletableFuture<TransactionMetadataStore> init(TransactionRecoverTracker recoverTracker) {
        CompletableFuture<TransactionMetadataStore> completableFuture = new CompletableFuture<>();
        if (!changeToInitializingState()) {
            log.error("Managed ledger transaction metadata store change state error when init it");
            completableFuture
                    .completeExceptionally(new TransactionCoordinatorClientException
                    .CoordinatorNotFoundException("transaction metadata store with tcId "
                            + tcID.toString() + " change state to Initializing error when init it"));
        } else {
            FutureUtil.safeRunAsync(() -> transactionLog.replayAsync(new TransactionLogReplayCallback() {
                @Override
                public void replayComplete() {
                    recoverTracker.appendOpenTransactionToTimeoutTracker();
                    if (!changeToReadyState()) {
                        log.error("Managed ledger transaction metadata store change state error when replay complete");
                        completableFuture
                                .completeExceptionally(new TransactionCoordinatorClientException
                                        .CoordinatorNotFoundException("transaction metadata store with tcId "
                                        + tcID.toString() + " change state to Ready error when init it"));

                    } else {
                        completableFuture.complete(MLTransactionMetadataStore.this);
                        recoverTracker.handleCommittingAndAbortingTransaction();
                        timeoutTracker.start();
                    }
                }

                @Override
                public void handleMetadataEntry(Position position, TransactionMetadataEntry transactionMetadataEntry) {

                    try {
                        TxnID txnID = new TxnID(transactionMetadataEntry.getTxnidMostBits(),
                            transactionMetadataEntry.getTxnidLeastBits());
                        long transactionId = transactionMetadataEntry.getTxnidLeastBits();
                        switch (transactionMetadataEntry.getMetadataOp()) {
                            case NEW:
                                long txnSequenceId = transactionMetadataEntry.getTxnidLeastBits();
                                if (txnMetaMap.containsKey(transactionId)) {
                                    txnMetaMap.get(transactionId).getRight().add(position);
                                } else {
                                    List<Position> positions = new ArrayList<>();
                                    positions.add(position);
                                    long openTimestamp = transactionMetadataEntry.getStartTime();
                                    long timeoutAt = transactionMetadataEntry.getTimeoutMs();
                                    final String owner = transactionMetadataEntry.hasOwner()
                                            ? transactionMetadataEntry.getOwner() : null;
                                    final TxnMetaImpl left = new TxnMetaImpl(txnID,
                                            openTimestamp, timeoutAt, owner);
                                    txnMetaMap.put(transactionId, MutablePair.of(left, positions));
                                    recoverTracker.handleOpenStatusTransaction(txnSequenceId,
                                            timeoutAt + openTimestamp);
                                }
                                break;
                            case ADD_PARTITION:
                                if (!txnMetaMap.containsKey(transactionId)) {
                                    transactionLog.deletePosition(Collections.singletonList(position));
                                } else {
                                    txnMetaMap.get(transactionId).getLeft()
                                            .addProducedPartitions(transactionMetadataEntry.getPartitionsList());
                                    txnMetaMap.get(transactionId).getRight().add(position);
                                }
                                break;
                            case ADD_SUBSCRIPTION:
                                if (!txnMetaMap.containsKey(transactionId)) {
                                    transactionLog.deletePosition(Collections.singletonList(position));
                                } else {
                                    txnMetaMap.get(transactionId).getLeft()
                                            .addAckedPartitions(subscriptionToTxnSubscription(
                                                    transactionMetadataEntry.getSubscriptionsList()));
                                    txnMetaMap.get(transactionId).getRight().add(position);
                                }
                                break;
                            case UPDATE:
                                if (!txnMetaMap.containsKey(transactionId)) {
                                    transactionLog.deletePosition(Collections.singletonList(position));
                                } else {
                                    TxnStatus newStatus = transactionMetadataEntry.getNewStatus();
                                    txnMetaMap.get(transactionId).getLeft()
                                            .updateTxnStatus(transactionMetadataEntry.getNewStatus(),
                                                    transactionMetadataEntry.getExpectedStatus());
                                    txnMetaMap.get(transactionId).getRight().add(position);
                                    recoverTracker.updateTransactionStatus(txnID.getLeastSigBits(), newStatus);
                                    if (newStatus == TxnStatus.COMMITTED || newStatus == TxnStatus.ABORTED) {
                                        transactionLog.deletePosition(txnMetaMap
                                                .get(transactionId).getRight()).thenAccept(v ->
                                                txnMetaMap.remove(transactionId).getLeft());
                                    }
                                }
                                break;
                            default:
                                throw new InvalidTxnStatusException("Transaction `"
                                        + txnID + "` load replay metadata operation "
                                        + "from transaction log with unknown operation");
                        }
                    } catch (InvalidTxnStatusException  e) {
                        transactionLog.deletePosition(Collections.singletonList(position));
                        log.error(e.getMessage(), e);
                    }
                }
            }), internalPinnedExecutor, completableFuture);
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<TxnStatus> getTxnStatus(TxnID txnID) {
        return CompletableFuture.completedFuture(txnMetaMap.get(txnID.getLeastSigBits()).getLeft().status());
    }

    @Override
    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnID) {
        Pair<TxnMeta, List<Position>> txnMetaListPair = txnMetaMap.get(txnID.getLeastSigBits());
        CompletableFuture<TxnMeta> completableFuture = new CompletableFuture<>();
        if (txnMetaListPair == null) {
            completableFuture.completeExceptionally(new TransactionNotFoundException(txnID));
        } else {
            completableFuture.complete(txnMetaListPair.getLeft());
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<TxnID> newTransaction(long timeOut, String owner) {
        CompletableFuture<TxnID> completableFuture = new CompletableFuture<>();
        FutureUtil.safeRunAsync(() -> {
            if (!checkIfReady()) {
                completableFuture.completeExceptionally(new CoordinatorException
                        .TransactionMetadataStoreStateException(tcID, State.Ready, getState(), "new Transaction"));
                return;
            }

            long mostSigBits = tcID.getId();
            long leastSigBits = sequenceIdGenerator.generateSequenceId();
            TxnID txnID = new TxnID(mostSigBits, leastSigBits);
            long currentTimeMillis = System.currentTimeMillis();
            TransactionMetadataEntry transactionMetadataEntry = new TransactionMetadataEntry()
                    .setTxnidMostBits(mostSigBits)
                    .setTxnidLeastBits(leastSigBits)
                    .setStartTime(currentTimeMillis)
                    .setTimeoutMs(timeOut)
                    .setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.NEW)
                    .setLastModificationTime(currentTimeMillis)
                    .setMaxLocalTxnId(sequenceIdGenerator.getCurrentSequenceId());
            if (owner != null) {
                if (StringUtils.isBlank(owner)) {
                    completableFuture.completeExceptionally(new IllegalArgumentException("Owner can't be blank"));
                    return;
                }
                transactionMetadataEntry.setOwner(owner);
            }
            transactionLog.append(transactionMetadataEntry)
                    .whenComplete((position, throwable) -> {
                        if (throwable != null) {
                            completableFuture.completeExceptionally(throwable);
                        } else {
                            appendLogCount.increment();
                            TxnMeta txn = new TxnMetaImpl(txnID, currentTimeMillis, timeOut, owner);
                            List<Position> positions = new ArrayList<>();
                            positions.add(position);
                            Pair<TxnMeta, List<Position>> pair = MutablePair.of(txn, positions);
                            txnMetaMap.put(leastSigBits, pair);
                            this.timeoutTracker.addTransaction(leastSigBits, timeOut);
                            createdTransactionCount.increment();
                            completableFuture.complete(txnID);
                        }
                    });
        }, internalPinnedExecutor, completableFuture);
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnID, List<String> partitions) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        FutureUtil.safeRunAsync(() -> {
            if (!checkIfReady()) {
                promise
                        .completeExceptionally(new CoordinatorException.TransactionMetadataStoreStateException(tcID,
                                State.Ready, getState(), "add produced partition"));
                return;
            }
            getTxnPositionPair(txnID).thenCompose(txnMetaListPair -> {
                TransactionMetadataEntry transactionMetadataEntry = new TransactionMetadataEntry()
                        .setTxnidMostBits(txnID.getMostSigBits())
                        .setTxnidLeastBits(txnID.getLeastSigBits())
                        .setMetadataOp(TransactionMetadataOp.ADD_PARTITION)
                        .addAllPartitions(partitions)
                        .setLastModificationTime(System.currentTimeMillis())
                        .setMaxLocalTxnId(sequenceIdGenerator.getCurrentSequenceId());

                return transactionLog.append(transactionMetadataEntry)
                        .thenAccept(position -> {
                            appendLogCount.increment();
                            try {
                                synchronized (txnMetaListPair.getLeft()) {
                                    txnMetaListPair.getLeft().addProducedPartitions(partitions);
                                    txnMetaMap.get(txnID.getLeastSigBits()).getRight().add(position);
                                }
                                promise.complete(null);
                            } catch (InvalidTxnStatusException e) {
                                transactionLog.deletePosition(Collections.singletonList(position));
                                log.error("TxnID {} add produced partition error"
                                                + " with TxnStatus: {}", txnMetaListPair.getLeft().id().toString()
                                        , txnMetaListPair.getLeft().status().name(), e);
                                promise.completeExceptionally(e);
                            }
                        });
            }).exceptionally(ex -> {
                promise.completeExceptionally(ex);
                return null;
            });
        }, internalPinnedExecutor, promise);
        return promise;
    }

    @Override
    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnID,
                                                          List<TransactionSubscription> txnSubscriptions) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        FutureUtil.safeRunAsync(() -> {
            if (!checkIfReady()) {
                promise.completeExceptionally(new CoordinatorException
                        .TransactionMetadataStoreStateException(tcID, State.Ready, getState(), "add acked partition"));
                return;
            }
            getTxnPositionPair(txnID).thenCompose(txnMetaListPair -> {
                TransactionMetadataEntry transactionMetadataEntry = new TransactionMetadataEntry()
                        .setTxnidMostBits(txnID.getMostSigBits())
                        .setTxnidLeastBits(txnID.getLeastSigBits())
                        .setMetadataOp(TransactionMetadataOp.ADD_SUBSCRIPTION)
                        .addAllSubscriptions(txnSubscriptionToSubscription(txnSubscriptions))
                        .setLastModificationTime(System.currentTimeMillis())
                        .setMaxLocalTxnId(sequenceIdGenerator.getCurrentSequenceId());

                return transactionLog.append(transactionMetadataEntry)
                        .thenAccept(position -> {
                            appendLogCount.increment();
                            try {
                                synchronized (txnMetaListPair.getLeft()) {
                                    txnMetaListPair.getLeft().addAckedPartitions(txnSubscriptions);
                                    txnMetaMap.get(txnID.getLeastSigBits()).getRight().add(position);
                                }
                                promise.complete(null);
                            } catch (InvalidTxnStatusException e) {
                                transactionLog.deletePosition(Collections.singletonList(position));
                                log.error("TxnID : " + txnMetaListPair.getLeft().id().toString()
                                        + " add acked subscription error with TxnStatus : "
                                        + txnMetaListPair.getLeft().status().name(), e);
                                promise.completeExceptionally(e);
                            }
                        });
            }).exceptionally(ex -> {
                promise.completeExceptionally(ex);
                return null;
            });
        }, internalPinnedExecutor, promise);
        return promise;
    }

    @Override
    public CompletableFuture<Void> updateTxnStatus(TxnID txnID, TxnStatus newStatus,
                                                                TxnStatus expectedStatus, boolean isTimeout) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        FutureUtil.safeRunAsync(() -> {
            if (!checkIfReady()) {
                promise.completeExceptionally(new CoordinatorException
                        .TransactionMetadataStoreStateException(tcID,
                        State.Ready, getState(), "update transaction status"));
                return;
            }
            getTxnPositionPair(txnID).thenCompose(txnMetaListPair -> {
                if (txnMetaListPair.getLeft().status() == newStatus) {
                    promise.complete(null);
                    return promise;
                }
                TransactionMetadataEntry transactionMetadataEntry = new TransactionMetadataEntry()
                        .setTxnidMostBits(txnID.getMostSigBits())
                        .setTxnidLeastBits(txnID.getLeastSigBits())
                        .setExpectedStatus(expectedStatus)
                        .setMetadataOp(TransactionMetadataOp.UPDATE)
                        .setLastModificationTime(System.currentTimeMillis())
                        .setNewStatus(newStatus)
                        .setMaxLocalTxnId(sequenceIdGenerator.getCurrentSequenceId());

                return transactionLog.append(transactionMetadataEntry)
                        .thenAccept(position -> {
                            appendLogCount.increment();
                            try {
                                synchronized (txnMetaListPair.getLeft()) {
                                    txnMetaListPair.getLeft().updateTxnStatus(newStatus, expectedStatus);
                                    txnMetaListPair.getRight().add(position);
                                }
                                if (newStatus == TxnStatus.ABORTING && isTimeout) {
                                    this.transactionTimeoutCount.increment();
                                }
                                if (newStatus == TxnStatus.COMMITTED || newStatus == TxnStatus.ABORTED) {
                                    this.transactionMetadataStoreStats
                                            .addTransactionExecutionLatencySample(System.currentTimeMillis()
                                                    - txnMetaListPair.getLeft().getOpenTimestamp());
                                    if (newStatus == TxnStatus.COMMITTED) {
                                        committedTransactionCount.increment();
                                    } else {
                                        abortedTransactionCount.increment();
                                    }
                                    txnMetaMap.remove(txnID.getLeastSigBits());
                                    transactionLog.deletePosition(txnMetaListPair.getRight()).exceptionally(ex -> {
                                        log.warn("Failed to delete transaction log position "
                                                + "at end transaction [{}]", txnID);
                                        return null;
                                    });
                                }
                                promise.complete(null);
                            } catch (InvalidTxnStatusException e) {
                                transactionLog.deletePosition(Collections.singletonList(position));
                                log.error("TxnID : " + txnMetaListPair.getLeft().id().toString()
                                        + " add update txn status error with TxnStatus : "
                                        + txnMetaListPair.getLeft().status().name(), e);
                                promise.completeExceptionally(e);
                            }
                        });
            }).exceptionally(ex -> {
                promise.completeExceptionally(ex);
                return null;
            });
        }, internalPinnedExecutor, promise);
       return promise;
    }

    @Override
    public long getLowWaterMark() {
        try {
            return this.txnMetaMap.firstKey() - 1;
        } catch (NoSuchElementException e) {
            return 0L;
        }
    }

    @Override
    public TransactionCoordinatorID getTransactionCoordinatorID() {
        return tcID;
    }

    @Override
    public TransactionCoordinatorStats getCoordinatorStats() {
        TransactionCoordinatorStats transactionCoordinatorstats = new TransactionCoordinatorStats();
        transactionCoordinatorstats.setLowWaterMark(getLowWaterMark());
        transactionCoordinatorstats.setState(getState().name());
        transactionCoordinatorstats.setLeastSigBits(sequenceIdGenerator.getCurrentSequenceId());
        return transactionCoordinatorstats;
    }

    private CompletableFuture<Pair<TxnMeta, List<Position>>> getTxnPositionPair(TxnID txnID) {
        CompletableFuture<Pair<TxnMeta, List<Position>>> completableFuture = new CompletableFuture<>();
        Pair<TxnMeta, List<Position>> txnMetaListPair = txnMetaMap.get(txnID.getLeastSigBits());
        if (txnMetaListPair == null) {
            completableFuture.completeExceptionally(new TransactionNotFoundException(txnID));
        } else {
            completableFuture.complete(txnMetaListPair);
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (changeToClosingState()) {
            // Disable new tasks from being submitted
            internalPinnedExecutor.shutdown();
            return transactionLog.closeAsync().thenCompose(v -> {
                txnMetaMap.clear();
                this.timeoutTracker.close();
                if (!this.changeToCloseState()) {
                    return FutureUtil.failedFuture(
                            new IllegalStateException(
                                    "Managed ledger transaction metadata store state to close error!"));
                }
                // Shutdown the ExecutorService
                MoreExecutors.shutdownAndAwaitTermination(internalPinnedExecutor, Duration.ofSeconds(5L));
                return CompletableFuture.completedFuture(null);
            });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public TransactionMetadataStoreStats getMetadataStoreStats() {
        this.transactionMetadataStoreStats.setCoordinatorId(tcID.getId());
        this.transactionMetadataStoreStats.setActives(txnMetaMap.size());
        this.transactionMetadataStoreStats.setCreatedCount(this.createdTransactionCount.longValue());
        this.transactionMetadataStoreStats.setCommittedCount(this.committedTransactionCount.longValue());
        this.transactionMetadataStoreStats.setAbortedCount(this.abortedTransactionCount.longValue());
        this.transactionMetadataStoreStats.setTimeoutCount(this.transactionTimeoutCount.longValue());
        this.transactionMetadataStoreStats.setAppendLogCount(this.appendLogCount.longValue());
        return transactionMetadataStoreStats;
    }

    @Override
    public List<TxnMeta> getSlowTransactions(long timeout) {
        List<TxnMeta> txnMetas = new ArrayList<>();
        txnMetaMap.forEach((k, v) -> {
            if (v.getLeft().getTimeoutAt() > timeout) {
                txnMetas.add(v.getLeft());
            }
        });
        return txnMetas;
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

    public ManagedLedger getManagedLedger() {
        return this.transactionLog.getManagedLedger();
    }
}
