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
package org.apache.pulsar.transaction.coordinator;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionNotFoundException;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.apache.pulsar.transaction.coordinator.test.MockedBookKeeperTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MLTransactionMetadataStoreTest extends MockedBookKeeperTestCase {

    public MLTransactionMetadataStoreTest() {
        super(3);
    }

    @Test
    public void testTransactionOperation() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                new ManagedLedgerConfig());
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog);
        int checkReplayRetryCount = 0;
        while (true) {
            checkReplayRetryCount++;
            if (checkReplayRetryCount > 3) {
                Assert.fail();
                break;
            }
            if (transactionMetadataStore.checkIfReady()) {
                TxnID txnID = transactionMetadataStore.newTransaction(5000).get();
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID).get(), TxnStatus.OPEN);

                List<String> partitions = new ArrayList<>();
                partitions.add("pt-1");
                partitions.add("pt-2");
                transactionMetadataStore.addProducedPartitionToTxn(txnID, partitions).get();
                Assert.assertEquals(transactionMetadataStore.getTxnMeta(txnID).get().producedPartitions(), partitions);

                partitions.add("pt-3");
                transactionMetadataStore.addProducedPartitionToTxn(txnID, partitions).get();
                Assert.assertEquals(transactionMetadataStore.getTxnMeta(txnID).get().producedPartitions(),
                        partitions);

                List<TransactionSubscription> subscriptions = new ArrayList<>();
                subscriptions.add(new TransactionSubscription("topic1", "sub1"));
                subscriptions.add(new TransactionSubscription("topic2", "sub2"));
                transactionMetadataStore.addAckedPartitionToTxn(txnID, subscriptions).get();
                Assert.assertTrue(transactionMetadataStore.getTxnMeta(txnID).get().ackedPartitions().containsAll(subscriptions));

                transactionMetadataStore.addAckedPartitionToTxn(txnID, subscriptions).get();
                Assert.assertEquals(transactionMetadataStore.getTxnMeta(txnID).get().producedPartitions(),
                        partitions);

                transactionMetadataStore.updateTxnStatus(txnID, TxnStatus.COMMITTING, TxnStatus.OPEN).get();
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID).get(), TxnStatus.COMMITTING);

                transactionMetadataStore.updateTxnStatus(txnID, TxnStatus.COMMITTED, TxnStatus.COMMITTING).get();

                try {
                    transactionMetadataStore.getTxnMeta(txnID).get();
                    Assert.fail();
                } catch (ExecutionException e) {
                    Assert.assertTrue(e.getCause() instanceof TransactionNotFoundException);
                }
                break;
            } else {
                checkReplayRetryCount++;
                Thread.sleep(100);
            }
        }
    }

    @Test
    public void testInitTransactionReader() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setMaxEntriesPerLedger(2);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                managedLedgerConfig);
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog);
        int checkReplayRetryCount = 0;
        while (true) {
            if (checkReplayRetryCount > 3) {
                Assert.fail();
                break;
            }
            if (transactionMetadataStore.checkIfReady()) {
                TxnID txnID1 = transactionMetadataStore.newTransaction(1000).get();
                TxnID txnID2 = transactionMetadataStore.newTransaction(1000).get();
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID1).get(), TxnStatus.OPEN);
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID2).get(), TxnStatus.OPEN);

                List<String> partitions = new ArrayList<>();
                partitions.add("pt-1");
                partitions.add("pt-2");
                transactionMetadataStore.addProducedPartitionToTxn(txnID1, partitions).get();
                transactionMetadataStore.addProducedPartitionToTxn(txnID2, partitions).get();

                List<TransactionSubscription> subscriptions = new ArrayList<>();
                subscriptions.add(new TransactionSubscription("topic1", "sub1"));
                subscriptions.add(new TransactionSubscription("topic2", "sub2"));

                transactionMetadataStore.addAckedPartitionToTxn(txnID1, subscriptions).get();
                transactionMetadataStore.addAckedPartitionToTxn(txnID2, subscriptions).get();
                List<TransactionSubscription> subscriptions1 = new ArrayList<>();
                subscriptions1.add(new TransactionSubscription("topic1", "sub1"));
                subscriptions1.add(new TransactionSubscription("topic3", "sub3"));
                subscriptions1.add(new TransactionSubscription("topic3", "sub3"));
                transactionMetadataStore.addAckedPartitionToTxn(txnID1, subscriptions1).get();
                transactionMetadataStore.addAckedPartitionToTxn(txnID2, subscriptions1).get();

                transactionMetadataStore.updateTxnStatus(txnID1, TxnStatus.COMMITTING, TxnStatus.OPEN).get();
                transactionMetadataStore.updateTxnStatus(txnID2, TxnStatus.COMMITTING, TxnStatus.OPEN).get();

                transactionMetadataStore.closeAsync();

                MLTransactionMetadataStore transactionMetadataStoreTest =
                        new MLTransactionMetadataStore(transactionCoordinatorID,

                                new MLTransactionLogImpl(transactionCoordinatorID, factory, new ManagedLedgerConfig()));

                while (true) {
                    if (checkReplayRetryCount > 6) {
                        Assert.fail();
                        break;
                    }
                    if (transactionMetadataStoreTest.checkIfReady()) {
                        subscriptions.add(new TransactionSubscription("topic3", "sub3"));
                        TxnMeta txnMeta1 = transactionMetadataStoreTest.getTxnMeta(txnID1).get();
                        TxnMeta txnMeta2 = transactionMetadataStoreTest.getTxnMeta(txnID2).get();
                        Assert.assertEquals(txnMeta1.producedPartitions(), partitions);
                        Assert.assertEquals(txnMeta2.producedPartitions(), partitions);
                        Assert.assertEquals(txnMeta1.ackedPartitions().size(), subscriptions.size());
                        Assert.assertEquals(txnMeta2.ackedPartitions().size(), subscriptions.size());
                        Assert.assertTrue(subscriptions.containsAll(txnMeta1.ackedPartitions()));
                        Assert.assertTrue(subscriptions.containsAll(txnMeta2.ackedPartitions()));
                        Assert.assertEquals(txnMeta1.status(), TxnStatus.COMMITTING);
                        Assert.assertEquals(txnMeta2.status(), TxnStatus.COMMITTING);
                        transactionMetadataStoreTest
                                .updateTxnStatus(txnID1, TxnStatus.COMMITTED, TxnStatus.COMMITTING).get();
                        transactionMetadataStoreTest
                                .updateTxnStatus(txnID2, TxnStatus.COMMITTED, TxnStatus.COMMITTING).get();
                        try {
                            transactionMetadataStoreTest.getTxnMeta(txnID1).get();
                            Assert.fail();
                        } catch (ExecutionException e) {
                            Assert.assertTrue(e.getCause() instanceof TransactionNotFoundException);
                        }

                        try {
                            transactionMetadataStoreTest.getTxnMeta(txnID2).get();
                            Assert.fail();
                        } catch (ExecutionException e) {
                            Assert.assertTrue(e.getCause() instanceof TransactionNotFoundException);
                        }
                        TxnID txnID = transactionMetadataStoreTest.newTransaction(1000).get();
                        Assert.assertEquals(txnID.getLeastSigBits(), 2L);
                        break;
                    } else {
                        checkReplayRetryCount++;
                        Thread.sleep(100);
                    }
                }
                break;
            } else {
                checkReplayRetryCount++;
                Thread.sleep(100);
            }
        }
    }

    @Test
    public void testDeleteLog() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                new ManagedLedgerConfig());
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog);
        int checkReplayRetryCount = 0;
        while (true) {
            if (checkReplayRetryCount > 3) {
                Assert.fail();
                break;
            }
            if (transactionMetadataStore.checkIfReady()) {
                TxnID txnID1 = transactionMetadataStore.newTransaction(1000).get();
                TxnID txnID2 = transactionMetadataStore.newTransaction(1000).get();
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID1).get(), TxnStatus.OPEN);
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID2).get(), TxnStatus.OPEN);

                List<String> partitions = new ArrayList<>();
                partitions.add("pt-1");
                partitions.add("pt-2");
                transactionMetadataStore.addProducedPartitionToTxn(txnID1, partitions).get();
                transactionMetadataStore.addProducedPartitionToTxn(txnID2, partitions).get();

                List<TransactionSubscription> subscriptions = new ArrayList<>();
                subscriptions.add(new TransactionSubscription("topic1", "sub1"));
                subscriptions.add(new TransactionSubscription("topic2", "sub2"));

                transactionMetadataStore.addAckedPartitionToTxn(txnID1, subscriptions).get();
                transactionMetadataStore.addAckedPartitionToTxn(txnID2, subscriptions).get();
                List<TransactionSubscription> subscriptions1 = new ArrayList<>();
                subscriptions1.add(new TransactionSubscription("topic1", "sub1"));
                subscriptions1.add(new TransactionSubscription("topic3", "sub3"));
                subscriptions1.add(new TransactionSubscription("topic3", "sub3"));
                transactionMetadataStore.addAckedPartitionToTxn(txnID1, subscriptions1).get();
                transactionMetadataStore.addAckedPartitionToTxn(txnID2, subscriptions1).get();

                transactionMetadataStore.updateTxnStatus(txnID1, TxnStatus.COMMITTING, TxnStatus.OPEN).get();
                transactionMetadataStore.updateTxnStatus(txnID2, TxnStatus.ABORTING, TxnStatus.OPEN).get();

                transactionMetadataStore.updateTxnStatus(txnID1, TxnStatus.COMMITTED, TxnStatus.COMMITTING).get();
                transactionMetadataStore.updateTxnStatus(txnID2, TxnStatus.ABORTED, TxnStatus.ABORTING).get();
                Field field = mlTransactionLog.getClass().getDeclaredField("cursor");
                field.setAccessible(true);
                ManagedCursor cursor = (ManagedCursor) field.get(mlTransactionLog);
                Assert.assertEquals(cursor.getMarkDeletedPosition(), cursor.getManagedLedger().getLastConfirmedEntry());

                break;
            } else {
                checkReplayRetryCount++;
                Thread.sleep(100);
            }
        }
    }
}