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
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.pulsar.transaction.coordinator.ReplayCallback;
import org.apache.pulsar.transaction.coordinator.TransactionLog;

import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.TxnSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ManagedLedgerTransactionLogImpl implements TransactionLog {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerTransactionLogImpl.class);

    private final ManagedLedger managedLedger;

    private final static String TRANSACTION_LOG_PREFIX = "transaction/log/";

    private final ReadOnlyCursor readOnlyCursor;

    private SpscArrayQueue<Entry> entryQueue;

    private final PositionImpl lastConfirmedEntry;

    private final long tcId;

    ManagedLedgerTransactionLogImpl(long tcID,
                                    ManagedLedgerFactory managedLedgerFactory) throws Exception {
        this.tcId = tcID;
        this.managedLedger = managedLedgerFactory.open(TRANSACTION_LOG_PREFIX + tcID);
        this.readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(TRANSACTION_LOG_PREFIX + tcID,
                PositionImpl.earliest, new ManagedLedgerConfig());
        this.entryQueue = new SpscArrayQueue<>(2000);
        this.lastConfirmedEntry = (PositionImpl) managedLedger.getLastConfirmedEntry();

    }

    @Override
    public void replayAsync(ConcurrentMap<TxnID, TxnMeta> txnMetaMap,
                       AtomicLong sequenceId, ReplayCallback replayCallback) {
        new Thread(new TransactionLogReplayer(txnMetaMap, sequenceId, replayCallback),
                "ReplayTransactionLogThread-" + tcId).start();
    }

    @Override
    public void readAsync(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        readOnlyCursor.asyncReadEntries(numberOfEntriesToRead, callback, System.nanoTime());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        managedLedger.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                log.info("Transaction log with tcId : {} close managedLedger successful!", tcId);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Transaction log with tcId : {} close managedLedger fail!", tcId);
                completableFuture.completeExceptionally(exception);
            }
        }, null);
        return completableFuture;
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
                    log.error("Transaction log write transaction operation error", exception);
                    buf.release();
                    completableFuture.completeExceptionally(exception);
                }
            } , null);
        } catch (IOException e) {
            log.error("Transaction log write transaction operation error", e);
            completableFuture.completeExceptionally(e);
        } finally {
            outStream.recycle();
        }
        return completableFuture;
    }

    class TransactionLogReplayer implements Runnable {

        private final AtomicLong sequenceId;
        private FillEntryQueueCallback fillEntryQueueCallback;
        private long currentLoadEntryId;
        private ConcurrentMap<TxnID, TxnMeta> txnMetaMap;
        private ReplayCallback replayCallback;

        TransactionLogReplayer(ConcurrentMap<TxnID, TxnMeta> txnMetaMap,
                               AtomicLong sequenceId, ReplayCallback replayCallback) {
            this.txnMetaMap = txnMetaMap;
            this.sequenceId = sequenceId;
            this.fillEntryQueueCallback = new FillEntryQueueCallback();
            this.replayCallback = replayCallback;
        }

        @Override
        public void run() {
            while (currentLoadEntryId < lastConfirmedEntry.getEntryId()) {
                fillEntryQueueCallback.fillQueue();
                handleEntry(entryQueue.poll());
            }
            readOnlyCursor.asyncClose(new AsyncCallbacks.CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    log.info("Transaction log with tcId : {} close ReadOnlyCursor successful!", tcId);
                }

                @Override
                public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Transaction log with tcId : " + tcId + " close ReadOnlyCursor fail", exception);
                }
            }, null);
            replayCallback.replayComplete();
        }

        private void handleEntry(Entry entry) {
            if (entry != null) {
                try {
                    ByteBuf buffer = entry.getDataBuffer();
                    currentLoadEntryId = entry.getEntryId();
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
                                    + txnID + "` load replay metadata operation from transaction log ");
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

        private AtomicLong outstandingReadsRequests = new AtomicLong(0);

        FillEntryQueueCallback() {
        }

        void fillQueue() {
            if (entryQueue.size() < entryQueue.capacity() && outstandingReadsRequests.get() == 0) {
                if (readOnlyCursor.hasMoreEntries()) {
                    outstandingReadsRequests.incrementAndGet();
                    readAsync(2, this, System.nanoTime());
                }
            }
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
            outstandingReadsRequests.decrementAndGet();
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            log.error("Transaction log init fail error", exception);
            outstandingReadsRequests.decrementAndGet();
        }
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
}
