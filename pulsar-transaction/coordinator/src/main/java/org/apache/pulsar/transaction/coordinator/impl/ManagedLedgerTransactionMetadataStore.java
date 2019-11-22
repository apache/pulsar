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

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;

import org.apache.pulsar.common.api.proto.PulsarApi.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry.TransactionMetadataOp;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;
import org.apache.pulsar.common.util.FutureUtil;

import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.TxnSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.exceptions.TransactionMetadataStoreStateException;
import org.apache.pulsar.transaction.coordinator.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnID;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The provider that offers managed ledger implementation of {@link TransactionMetadataStore}.
 */
public class ManagedLedgerTransactionMetadataStore
        extends TransactionMetadataStoreState implements TransactionMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerTransactionMetadataStore.class);

    private final TransactionCoordinatorID tcID;
    private AtomicLong sequenceId = new AtomicLong(TC_ID_NOT_USED);
    private final ManagedLedgerTransactionLogImpl transactionLog;
    private static final long TC_ID_NOT_USED = -1L;
    private ConcurrentMap<TxnID, TxnMeta> txnMetaMap = new ConcurrentHashMap<>();
    private SpscArrayQueue<Entry> entryQueue;
    private volatile long loadCount;

    public ManagedLedgerTransactionMetadataStore(TransactionCoordinatorID tcID,
                                                 ManagedLedgerFactory managedLedgerFactory) throws Exception {
        super(State.None);
        this.tcID = tcID;
        this.transactionLog =
                new ManagedLedgerTransactionLogImpl(tcID.getId(), managedLedgerFactory);
        this.entryQueue = new SpscArrayQueue<>(2000);
        init();
    }

    private ConcurrentMap<TxnID, TxnMeta> getTxnMetaMap() {
        return txnMetaMap;
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
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new TransactionMetadataStoreStateException(tcID, State.Ready, getState(), "new Transaction"));
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
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new TransactionMetadataStoreStateException(txnID,
                            State.Ready, getState(), "add produced partition"));
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
                                log.error("TxnID : " + txn.id().toString()
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
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new TransactionMetadataStoreStateException(txnID,
                            State.Ready, getState(), "add acked partition"));
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
                                log.error("TxnID : " + txn.id().toString()
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
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(
                    new TransactionMetadataStoreStateException(txnID,
                            State.Ready, getState(), "update transaction status"));
        }
        return getTxnMetaAsync(txnID).thenCompose(txn -> {
            if (txn == null) {
                return FutureUtil.failedFuture(new TransactionNotFoundException("Transaction not found :" + txnID));
            } else {
                CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                synchronized (txn) {
                    TransactionMetadataEntry transactionMetadataEntry = TransactionMetadataEntry
                            .newBuilder()
                            .setTxnidMostBits(txnID.getMostSigBits())
                            .setTxnidLeastBits(txnID.getLeastSigBits())
                            .setExpectedStatus(expectedStatus)
                            .setMetadataOp(TransactionMetadataOp.UPDATE)
                            .setLastModificationTime(System.currentTimeMillis())
                            .setNewStatus(newStatus)
                            .build();
                    try {
                        transactionLog.write(transactionMetadataEntry).get();
                        txn.updateTxnStatus(newStatus, expectedStatus);
                        completableFuture.complete(null);
                    } catch (InterruptedException | ExecutionException |InvalidTxnStatusException e) {
                        log.error("TxnID : " + txn.id().toString()
                                + " add update txn status error with TxnStatus : "
                                + txn.status().name(), e);
                        completableFuture.completeExceptionally(e);
                    }
                    transactionMetadataEntry.recycle();
                }
                return completableFuture;
            }
        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return transactionLog.close().thenCompose(v -> {
            entryQueue.clear();
            txnMetaMap.clear();
            if (!this.changeToClose()) {
                return FutureUtil.failedFuture(
                        new IllegalStateException("Managed ledger transaction metadata store state to close error!"));
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    public void init() {

        if (!changeToInitializingState()) {
            log.error("Managed ledger transaction metadata store change state error when init it");
            return;
        }
        new Thread(new LoadLogToCache(this, transactionLog),
                "Transaction log read entry thread").start();
    }

    class LoadLogToCache implements Runnable {

        private final ConcurrentMap<TxnID, TxnMeta> txnMetaMap;
        private final AtomicLong sequenceId;
        private volatile long addCount = 0;
        private ManagedLedgerTransactionLogImpl ledgerTransactionLog;
        private ManagedLedgerTransactionMetadataStore metadataStore;
        private FillEntryQueueCallback fillEntryQueueCallback;

        LoadLogToCache(ManagedLedgerTransactionMetadataStore metadataStore,
                  ManagedLedgerTransactionLogImpl ledgerTransactionLog) {
            this.txnMetaMap = metadataStore.getTxnMetaMap();
            this.sequenceId = metadataStore.getSequenceId();
            this.ledgerTransactionLog = ledgerTransactionLog;
            this.metadataStore = metadataStore;
            this.fillEntryQueueCallback = new FillEntryQueueCallback(metadataStore, ledgerTransactionLog);
        }

        @Override
        public void run() {
            while ((addCount != metadataStore.loadCount
                    || (addCount == metadataStore.loadCount && !fillEntryQueueCallback.isFinished()))
                    && checkCurrentState(State.Initializing)) {
                fillEntryQueueCallback.fillQueue();
                convertEntry(metadataStore.entryQueue.poll());
            }
            try {
                ledgerTransactionLog.getReadOnlyCursor().close();
            } catch (InterruptedException | ManagedLedgerException e) {
                log.error("Transaction log close ReadOnlyCursor fail", e);
            }
            if (!changeToReadyState()) {
                log.error("Managed ledger transaction metadata store load log to cache change state to ready error!");
            }
        }

        private void convertEntry(Entry entry) {
            if (entry != null) {
                addCount++;
                try {
                    ByteBuf buffer = entry.getDataBuffer();
                    ByteBufCodedInputStream stream = ByteBufCodedInputStream.get(buffer);
                    TransactionMetadataEntry.Builder transactionMetadataEntryBuilder =
                            TransactionMetadataEntry.newBuilder();
                    TransactionMetadataEntry transactionMetadataEntry =
                            transactionMetadataEntryBuilder.mergeFrom(stream, null).build();
                    TxnID txnID = new TxnID(transactionMetadataEntry.getTxnidMostBits(),
                            transactionMetadataEntry.getTxnidLeastBits());
                    switch (transactionMetadataEntry.getMetadataOp()) {
                        case NEW:
                            if (sequenceId.get() < transactionMetadataEntry.getTxnidLeastBits()) {
                                sequenceId.set(transactionMetadataEntry.getTxnidLeastBits());
                            }
                            txnMetaMap.put(txnID, new TxnMetaImpl(txnID));
                            transactionMetadataEntryBuilder.recycle();
                            stream.recycle();
                            break;
                        case ADD_PARTITION:
                            txnMetaMap.get(txnID)
                                    .addProducedPartitions(transactionMetadataEntry.getPartitionsList());
                            transactionMetadataEntryBuilder.recycle();
                            stream.recycle();
                            break;
                        case ADD_SUBSCRIPTION:
                            txnMetaMap.get(txnID)
                                    .addAckedPartitions(
                                            subscriptionToTxnSubscription
                                                    (transactionMetadataEntry.getSubscriptionsList()));
                            transactionMetadataEntryBuilder.recycle();
                            stream.recycle();
                            break;
                        case UPDATE:
                            txnMetaMap.get(txnID)
                                    .updateTxnStatus(transactionMetadataEntry.getNewStatus(),
                                            transactionMetadataEntry.getExpectedStatus());
                            transactionMetadataEntryBuilder.recycle();
                            stream.recycle();
                            break;
                        default:

                            throw new InvalidTxnStatusException("Transaction `"
                                    + txnID + "` load bad metadata operation from transaction log ");
                    }
                    entry.release();
                } catch (InvalidTxnStatusException e) {
                    log.error(e.getMessage(), e);
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                    throw new RuntimeException("TransactionLog convert entry error : ", e);
                }
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    //no-op
                }
            }
        }
    }

    class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private ManagedLedgerTransactionLogImpl managedLedgerTransactionLog;

        private ManagedLedgerTransactionMetadataStore managedLedgerTransactionMetadataStore;

        private AtomicLong outstandingReadsRequests = new AtomicLong(0);

        private volatile boolean isDone = false;

        FillEntryQueueCallback(ManagedLedgerTransactionMetadataStore managedLedgerTransactionMetadataStore,
                               ManagedLedgerTransactionLogImpl managedLedgerTransactionLog) {
            this.managedLedgerTransactionMetadataStore = managedLedgerTransactionMetadataStore;
            this.managedLedgerTransactionLog = managedLedgerTransactionLog;
        }

        void fillQueue() {
            if (!isDone && managedLedgerTransactionMetadataStore.entryQueue.size()
                    < managedLedgerTransactionMetadataStore.entryQueue.capacity()
                    && outstandingReadsRequests.get() == 0) {
                if (managedLedgerTransactionLog.getReadOnlyCursor().hasMoreEntries()) {
                    outstandingReadsRequests.incrementAndGet();
                    managedLedgerTransactionLog.read(100, this, System.nanoTime());
                } else {
                    isDone = true;
                }
            }
        }

        public boolean isFinished() {
            return isDone && outstandingReadsRequests.get() == 0;
        }

        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
            entryQueue.fill(new MessagePassingQueue.Supplier<Entry>() {
                private int i = 0;
                @Override
                public Entry get() {
                    Entry entry = entries.get(i);
                    i++;
                    return entry;
                }
            }, entries.size());
            managedLedgerTransactionMetadataStore.loadCount += entries.size();
            outstandingReadsRequests.decrementAndGet();
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            log.error("Transaction log init fail error", exception);
            outstandingReadsRequests.decrementAndGet();
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

    private static List<TxnSubscription> subscriptionToTxnSubscription(List<Subscription> subscriptions) {
        List<TxnSubscription> txnSubscriptions = new ArrayList<>(subscriptions.size());
        for (Subscription subscription : subscriptions) {
            txnSubscriptions
                    .add(new TxnSubscription(subscription.getTopic(), subscription.getSubscription()));
            subscription.recycle();
        }
        return txnSubscriptions;
    }
}

