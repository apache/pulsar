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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.TxnSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.exceptions.TxnStoreStateUpdateException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ManagedLedgerTransactionLogImpl implements
        ManagedLedgerTransactionMetadataStore.ManagedLedgerTransactionLog {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerTransactionLogImpl.class);

    private ManagedLedgerTransactionMetadataStore matadataStore;

    private final ManagedLedger managedLedger;

    private final ManagedLedgerFactory managedLedgerFactory;

    private SpscArrayQueue<Entry> entryQueue;

    private volatile long loadCount;

    private final long tcID;

    private final static String TRANSACTION_LOG_PREFIX = "transaction/log/";

    ManagedLedgerTransactionLogImpl(long tcID,
                                    ManagedLedgerFactory managedLedgerFactory,
                                    ManagedLedgerTransactionMetadataStore metadataStore) throws Exception {
        this.tcID = tcID;
        this.managedLedger = managedLedgerFactory.open(TRANSACTION_LOG_PREFIX + tcID);
        this.matadataStore = metadataStore;
        this.managedLedgerFactory = managedLedgerFactory;
        this.entryQueue = new SpscArrayQueue<>(2000);
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

    @Override
    public void init() {
        try {
            matadataStore.updateMetadataStoreState(TransactionMetadataStore.State.INITIALIZING);
        } catch (TxnStoreStateUpdateException e) {
            log.error(e.getMessage(), e);
        }
        new Thread(new ReadEntry(matadataStore, this, managedLedgerFactory),
                "Transaction log read entry thread").start();
    }

    @Override
    public void close() throws ManagedLedgerException, InterruptedException {
        managedLedger.close();
    }

    @Override
    public CompletableFuture<Void> write(TransactionMetadataEntry transactionMetadataEntry) {
        int transactionMetadataEntrySize = transactionMetadataEntry.getSerializedSize();

        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(transactionMetadataEntrySize, transactionMetadataEntrySize);

        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(buf);

        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        try {
            transactionMetadataEntry.writeTo(outStream);
            managedLedger.asyncAddEntry(buf, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, Object ctx) {
                    buf.release();
                    completableFuture.complete(null);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Transaction log write transaction operation error" + exception);
                    buf.release();
                    completableFuture.completeExceptionally(exception);
                }
            } , null);
        } catch (IOException e) {
            log.error("Transaction log write transaction operation error" + e);
            completableFuture.completeExceptionally(e);
        } finally {
            outStream.recycle();
        }
        return completableFuture;
    }

    class ReadEntryQueue implements ReadEntriesCallback {

        private ReadOnlyCursor readOnlyCursor;

        private ManagedLedgerTransactionLogImpl managedLedgerTransactionLog;

        private AtomicLong outstandingReadsRequests = new AtomicLong(0);

        private volatile boolean isDone = false;

        ReadEntryQueue(ManagedLedgerTransactionLogImpl managedLedgerTransactionLog,
                       ReadOnlyCursor readOnlyCursor) {
            this.managedLedgerTransactionLog = managedLedgerTransactionLog;
            this.readOnlyCursor = readOnlyCursor;
        }

        void fillQueue() {
            if (!isDone && managedLedgerTransactionLog.entryQueue.size()
                    < managedLedgerTransactionLog.entryQueue.capacity() && outstandingReadsRequests.get() == 0) {
                if (readOnlyCursor.hasMoreEntries()) {
                    outstandingReadsRequests.incrementAndGet();
                    readOnlyCursor.asyncReadEntries(100, this, System.nanoTime());
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
            managedLedgerTransactionLog.loadCount += entries.size();
            outstandingReadsRequests.decrementAndGet();
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            log.error("Transaction log init fail error", exception);
            outstandingReadsRequests.decrementAndGet();
        }
    }

    class ReadEntry implements Runnable {

        private final ConcurrentMap<TxnID, TxnMeta> txnMetaMap;
        private final AtomicLong sequenceId;
        private volatile long addCount = 0;
        private ManagedLedgerTransactionLogImpl ledgerTransactionLog;
        private ManagedLedgerTransactionMetadataStore metadataStore;
        private ReadEntryQueue readEntryQueue;
        private ReadOnlyCursor readOnlyCursor;

        ReadEntry(ManagedLedgerTransactionMetadataStore metadataStore,
                  ManagedLedgerTransactionLogImpl ledgerTransactionLog,
                  ManagedLedgerFactory managedLedgerFactory) {
            this.txnMetaMap = metadataStore.getTxnMetaMap();
            this.sequenceId = metadataStore.getSequenceId();
            this.ledgerTransactionLog = ledgerTransactionLog;
            this.metadataStore = metadataStore;
            try {
                this.readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(TRANSACTION_LOG_PREFIX + tcID,
                        PositionImpl.earliest, new ManagedLedgerConfig());
            } catch (InterruptedException | ManagedLedgerException e) {
                log.error("Transaction log open ReadOnlyCursor fail", e);
            }
            this.readEntryQueue = new ReadEntryQueue(ledgerTransactionLog, readOnlyCursor);
        }

        @Override
        public void run() {
            while ((addCount != ledgerTransactionLog.loadCount
                    || (addCount == ledgerTransactionLog.loadCount && !readEntryQueue.isFinished()))
                    && metadataStore.getState() == TransactionMetadataStore.State.INITIALIZING) {
                readEntryQueue.fillQueue();
                convertEntry(ledgerTransactionLog.entryQueue.poll());
            }
            try {
                readOnlyCursor.close();
            } catch (InterruptedException | ManagedLedgerException e) {
                log.error("Transaction log close ReadOnlyCursor fail", e);
            }
            try {
                this.metadataStore.updateMetadataStoreState(TransactionMetadataStore.State.READY);
            } catch (TxnStoreStateUpdateException e) {
                log.error(e.getMessage(), e);
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
}
