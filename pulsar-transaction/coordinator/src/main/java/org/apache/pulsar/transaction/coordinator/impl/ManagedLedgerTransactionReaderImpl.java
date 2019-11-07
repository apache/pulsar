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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.TxnSubscription;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ManagedLedgerTransactionReaderImpl implements
        ManagedLedgerTransactionMetadataStore.ManagedLedgerTransactionReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedLedgerTransactionReaderImpl.class);

    private ConcurrentMap<TxnID, TxnMeta> txnMetaMap = new ConcurrentHashMap<>();

    private AtomicLong sequenceId = new AtomicLong(ManagedLedgerTransactionMetadataStore.TC_ID_NOT_USED);

    private final ReadOnlyCursor readOnlyCursor;

    private Exception initCacheException;

    public ManagedLedgerTransactionReaderImpl(String tcId, ManagedLedgerFactory managedLedgerFactory) throws Exception {
        this.readOnlyCursor = managedLedgerFactory
                    .openReadOnlyCursor(tcId,
                            PositionImpl.earliest, new ManagedLedgerConfig());
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.countDown();
        while (readOnlyCursor.hasMoreEntries()) {
            if (initCacheException != null) {
                throw initCacheException;
            }
            List<Entry> entries = readOnlyCursor.readEntries(2);
            countDownLatch.await();
            countDownLatch = new CountDownLatch(1);
            new Thread(new ReadOnce(countDownLatch,
                    txnMetaMap, sequenceId, this, entries)).start();
        }

        countDownLatch.await();

        if (initCacheException != null) {
            throw initCacheException;
        }
    }

    private static List<TxnSubscription> subscriptionToTxnSubscription(List<Subscription> subscriptions) {
        List<TxnSubscription> txnSubscriptions = new ArrayList<>(subscriptions.size());
        for (int i = 0; i < subscriptions.size(); i++) {
            txnSubscriptions
                    .add(new TxnSubscription(subscriptions.get(i).getTopic(), subscriptions.get(i).getSubscription()));
        }
        return txnSubscriptions;
    }


    @Override
    public TxnMeta getTxnMeta(TxnID txnid) {
        return txnMetaMap.get(txnid);
    }

    @Override
    public Long readSequenceId() {
        return sequenceId.get();
    }

    @Override
    public void addNewTxn(TxnMeta txnMeta) {
        txnMetaMap.put(txnMeta.id(), txnMeta);
    }

    @Override
    public TxnStatus getTxnStatus(TxnID txnID) {
        return txnMetaMap.get(txnID).status();
    }

    @Override
    public void close() throws ManagedLedgerException, InterruptedException {
        txnMetaMap.clear();
        readOnlyCursor.close();
    }

    class ReadOnce implements Runnable{
        private final CountDownLatch countDownLatch;
        private final ConcurrentMap<TxnID, TxnMeta> txnMetaMap;
        private AtomicLong sequenceId;
        private List<Entry> entries;
        private ManagedLedgerTransactionReaderImpl managedLedgerTransactionReader;
        ReadOnce(CountDownLatch countDownLatch,
                 ConcurrentMap<TxnID, TxnMeta> txnMetaMap,
                 AtomicLong sequenceId,
                 ManagedLedgerTransactionReaderImpl managedLedgerTransactionReader,
                 List<Entry> entries) {
            this.countDownLatch = countDownLatch;
            this.txnMetaMap = txnMetaMap;
            this.sequenceId = sequenceId;
            this.entries = entries;
            this.managedLedgerTransactionReader = managedLedgerTransactionReader;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < entries.size(); i++) {
                    ByteBuf buffer = entries.get(i).getDataBuffer();
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
                            txnMetaMap.get(txnID).addProducedPartitions(transactionMetadataEntry.getPartitionsList());
                            transactionMetadataEntryBuilder.recycle();
                            stream.recycle();
                            break;
                        case ADD_SUBSCRIPTION:
                            txnMetaMap.get(txnID)
                                    .addTxnSubscription(
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
                    entries.get(i).release();
                }
                countDownLatch.countDown();
            } catch (Exception e) {
                managedLedgerTransactionReader.setInitCacheException(e);
            }
        }
    }

    public void setInitCacheException(Exception e) {
        this.initCacheException = e;
    }
}
