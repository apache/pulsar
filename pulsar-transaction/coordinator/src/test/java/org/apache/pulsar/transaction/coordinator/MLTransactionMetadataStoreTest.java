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

import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionNotFoundException;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionSequenceIdGenerator;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.apache.pulsar.transaction.coordinator.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.State.WriteFailed;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class MLTransactionMetadataStoreTest extends MockedBookKeeperTestCase {

    public MLTransactionMetadataStoreTest() {
        super(3);
    }

    @Test
    public void testTransactionOperation() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        MLTransactionSequenceIdGenerator mlTransactionSequenceIdGenerator = new MLTransactionSequenceIdGenerator();
        managedLedgerConfig.setManagedLedgerInterceptor(mlTransactionSequenceIdGenerator);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                managedLedgerConfig);
        mlTransactionLog.initialize().join();
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog,
                        new TransactionTimeoutTrackerImpl(), new TransactionRecoverTrackerImpl(),
                        mlTransactionSequenceIdGenerator);
        int checkReplayRetryCount = 0;
        while (true) {
            checkReplayRetryCount++;
            if (checkReplayRetryCount > 3) {
                fail();
                break;
            }
            if (transactionMetadataStore.checkIfReady()) {
                TxnID txnID = transactionMetadataStore.newTransaction(5000).get();
                assertEquals(transactionMetadataStore.getTxnStatus(txnID).get(), TxnStatus.OPEN);

                List<String> partitions = new ArrayList<>();
                partitions.add("pt-1");
                partitions.add("pt-2");
                transactionMetadataStore.addProducedPartitionToTxn(txnID, partitions).get();
                assertEquals(transactionMetadataStore.getTxnMeta(txnID).get().producedPartitions(), partitions);

                partitions.add("pt-3");
                transactionMetadataStore.addProducedPartitionToTxn(txnID, partitions).get();
                assertEquals(transactionMetadataStore.getTxnMeta(txnID).get().producedPartitions(),
                        partitions);

                List<TransactionSubscription> subscriptions = new ArrayList<>();
                subscriptions.add(new TransactionSubscription("topic1", "sub1"));
                subscriptions.add(new TransactionSubscription("topic2", "sub2"));
                transactionMetadataStore.addAckedPartitionToTxn(txnID, subscriptions).get();
                Assert.assertTrue(transactionMetadataStore.getTxnMeta(txnID).get().ackedPartitions().containsAll(subscriptions));

                transactionMetadataStore.addAckedPartitionToTxn(txnID, subscriptions).get();
                assertEquals(transactionMetadataStore.getTxnMeta(txnID).get().producedPartitions(),
                        partitions);

                transactionMetadataStore.updateTxnStatus(txnID, TxnStatus.COMMITTING, TxnStatus.OPEN, false).get();
                Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID).get(), TxnStatus.COMMITTING);

                transactionMetadataStore.updateTxnStatus(txnID, TxnStatus.COMMITTED, TxnStatus.COMMITTING, false).get();

                try {
                    transactionMetadataStore.getTxnMeta(txnID).get();
                    fail();
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

    @DataProvider(name = "isUseManagedLedgerProperties")
    public Object[][] versions() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "isUseManagedLedgerProperties")
    public void testRecoverSequenceId(boolean isUseManagedLedgerProperties) throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        MLTransactionSequenceIdGenerator mlTransactionSequenceIdGenerator = new MLTransactionSequenceIdGenerator();
        managedLedgerConfig.setManagedLedgerInterceptor(mlTransactionSequenceIdGenerator);
        managedLedgerConfig.setMaxEntriesPerLedger(3);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                managedLedgerConfig);
        mlTransactionLog.initialize().join();
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog,
                        new TransactionTimeoutTrackerImpl(), new TransactionRecoverTrackerImpl(),
                        mlTransactionSequenceIdGenerator);

        Awaitility.await().until(transactionMetadataStore::checkIfReady);
        TxnID txnID = transactionMetadataStore.newTransaction(20000).get();
        transactionMetadataStore.updateTxnStatus(txnID, TxnStatus.COMMITTING, TxnStatus.OPEN, false).get();
        if (isUseManagedLedgerProperties) {
            transactionMetadataStore.updateTxnStatus(txnID, TxnStatus.COMMITTED, TxnStatus.COMMITTING, false).get();
        }
        assertEquals(txnID.getLeastSigBits(), 0);
        Field field = MLTransactionLogImpl.class.getDeclaredField("managedLedger");
        field.setAccessible(true);
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) field.get(mlTransactionLog);
        Position position = managedLedger.getLastConfirmedEntry();
        if (isUseManagedLedgerProperties) {
            Awaitility.await().until(() -> {
                managedLedger.rollCurrentLedgerIfFull();
                return !managedLedger.ledgerExists(position.getLedgerId());
            });
        }
        mlTransactionLog.closeAsync().get();
        mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                managedLedgerConfig);
        mlTransactionLog.initialize().join();
        transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog,
                        new TransactionTimeoutTrackerImpl(), new TransactionRecoverTrackerImpl(),
                        mlTransactionSequenceIdGenerator);

        Awaitility.await().until(transactionMetadataStore::checkIfReady);
        txnID = transactionMetadataStore.newTransaction(100000).get();
        assertEquals(txnID.getLeastSigBits(), 1);
    }

    @Test
    public void testInitTransactionReader() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setMaxEntriesPerLedger(2);
        MLTransactionSequenceIdGenerator mlTransactionSequenceIdGenerator = new MLTransactionSequenceIdGenerator();
        managedLedgerConfig.setManagedLedgerInterceptor(mlTransactionSequenceIdGenerator);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                managedLedgerConfig);
        mlTransactionLog.initialize().join();
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog,
                        new TransactionTimeoutTrackerImpl(), new TransactionRecoverTrackerImpl(),
                        mlTransactionSequenceIdGenerator);
        int checkReplayRetryCount = 0;
        while (true) {
            if (checkReplayRetryCount > 3) {
                fail();
                break;
            }
            if (transactionMetadataStore.checkIfReady()) {
                TxnID txnID1 = transactionMetadataStore.newTransaction(1000).get();
                TxnID txnID2 = transactionMetadataStore.newTransaction(1000).get();
                assertEquals(transactionMetadataStore.getTxnStatus(txnID1).get(), TxnStatus.OPEN);
                assertEquals(transactionMetadataStore.getTxnStatus(txnID2).get(), TxnStatus.OPEN);

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

                transactionMetadataStore.updateTxnStatus(txnID1, TxnStatus.COMMITTING, TxnStatus.OPEN, false).get();
                transactionMetadataStore.updateTxnStatus(txnID2, TxnStatus.COMMITTING, TxnStatus.OPEN, false).get();

                transactionMetadataStore.closeAsync();

                MLTransactionLogImpl txnLog2 = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                        managedLedgerConfig);
                txnLog2.initialize().join();
                MLTransactionMetadataStore transactionMetadataStoreTest =
                        new MLTransactionMetadataStore(transactionCoordinatorID,
                                txnLog2, new TransactionTimeoutTrackerImpl(), new TransactionRecoverTrackerImpl(),
                                mlTransactionSequenceIdGenerator);

                while (true) {
                    if (checkReplayRetryCount > 6) {
                        fail();
                        break;
                    }
                    if (transactionMetadataStoreTest.checkIfReady()) {
                        subscriptions.add(new TransactionSubscription("topic3", "sub3"));
                        TxnMeta txnMeta1 = transactionMetadataStoreTest.getTxnMeta(txnID1).get();
                        TxnMeta txnMeta2 = transactionMetadataStoreTest.getTxnMeta(txnID2).get();
                        assertEquals(txnMeta1.producedPartitions(), partitions);
                        assertEquals(txnMeta2.producedPartitions(), partitions);
                        assertEquals(txnMeta1.ackedPartitions().size(), subscriptions.size());
                        assertEquals(txnMeta2.ackedPartitions().size(), subscriptions.size());
                        Assert.assertTrue(subscriptions.containsAll(txnMeta1.ackedPartitions()));
                        Assert.assertTrue(subscriptions.containsAll(txnMeta2.ackedPartitions()));
                        assertEquals(txnMeta1.status(), TxnStatus.COMMITTING);
                        assertEquals(txnMeta2.status(), TxnStatus.COMMITTING);
                        transactionMetadataStoreTest
                                .updateTxnStatus(txnID1, TxnStatus.COMMITTED, TxnStatus.COMMITTING, false).get();
                        transactionMetadataStoreTest
                                .updateTxnStatus(txnID2, TxnStatus.COMMITTED, TxnStatus.COMMITTING, false).get();
                        try {
                            transactionMetadataStoreTest.getTxnMeta(txnID1).get();
                            fail();
                        } catch (ExecutionException e) {
                            Assert.assertTrue(e.getCause() instanceof TransactionNotFoundException);
                        }

                        try {
                            transactionMetadataStoreTest.getTxnMeta(txnID2).get();
                            fail();
                        } catch (ExecutionException e) {
                            Assert.assertTrue(e.getCause() instanceof TransactionNotFoundException);
                        }
                        TxnID txnID = transactionMetadataStoreTest.newTransaction(1000).get();
                        assertEquals(txnID.getLeastSigBits(), 2L);
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

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        MLTransactionSequenceIdGenerator mlTransactionSequenceIdGenerator = new MLTransactionSequenceIdGenerator();
        managedLedgerConfig.setManagedLedgerInterceptor(mlTransactionSequenceIdGenerator);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                managedLedgerConfig);
        mlTransactionLog.initialize().join();
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog,
                        new TransactionTimeoutTrackerImpl(), new TransactionRecoverTrackerImpl(),
                        mlTransactionSequenceIdGenerator);
        int checkReplayRetryCount = 0;
        while (true) {
            if (checkReplayRetryCount > 3) {
                fail();
                break;
            }
            if (transactionMetadataStore.checkIfReady()) {
                TxnID txnID1 = transactionMetadataStore.newTransaction(1000).get();
                TxnID txnID2 = transactionMetadataStore.newTransaction(1000).get();
                assertEquals(transactionMetadataStore.getTxnStatus(txnID1).get(), TxnStatus.OPEN);
                assertEquals(transactionMetadataStore.getTxnStatus(txnID2).get(), TxnStatus.OPEN);

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

                transactionMetadataStore.updateTxnStatus(txnID1, TxnStatus.COMMITTING, TxnStatus.OPEN, false).get();
                transactionMetadataStore.updateTxnStatus(txnID2, TxnStatus.ABORTING, TxnStatus.OPEN, false).get();

                transactionMetadataStore.updateTxnStatus(txnID1, TxnStatus.COMMITTED, TxnStatus.COMMITTING, false).get();
                transactionMetadataStore.updateTxnStatus(txnID2, TxnStatus.ABORTED, TxnStatus.ABORTING, false).get();
                Field field = mlTransactionLog.getClass().getDeclaredField("cursor");
                field.setAccessible(true);
                ManagedCursor cursor = (ManagedCursor) field.get(mlTransactionLog);
                assertEquals(cursor.getMarkDeletedPosition(), cursor.getManagedLedger().getLastConfirmedEntry());

                break;
            } else {
                checkReplayRetryCount++;
                Thread.sleep(100);
            }
        }
    }

    @Test
    public void testRecoverWhenDeleteFromCursor() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        MLTransactionSequenceIdGenerator mlTransactionSequenceIdGenerator = new MLTransactionSequenceIdGenerator();
        managedLedgerConfig.setManagedLedgerInterceptor(mlTransactionSequenceIdGenerator);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                managedLedgerConfig);
        mlTransactionLog.initialize().join();
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog,
                        new TransactionTimeoutTrackerImpl(), new TransactionRecoverTrackerImpl(),
                        mlTransactionSequenceIdGenerator);


        Awaitility.await().until(transactionMetadataStore::checkIfReady);

        // txnID1 have not deleted from cursor, we can recover from transaction log
        TxnID txnID1 = transactionMetadataStore.newTransaction(1000).get();
        // txnID2 have deleted from cursor.
        TxnID txnID2 = transactionMetadataStore.newTransaction(1000).get();

        transactionMetadataStore.updateTxnStatus(txnID2, TxnStatus.ABORTING, TxnStatus.OPEN, false).get();
        transactionMetadataStore.updateTxnStatus(txnID2, TxnStatus.ABORTED, TxnStatus.ABORTING, false).get();

        mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                managedLedgerConfig);
        mlTransactionLog.initialize().join();
        transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog,
                        new TransactionTimeoutTrackerImpl(), new TransactionRecoverTrackerImpl(),
                        mlTransactionSequenceIdGenerator);

        Awaitility.await().until(transactionMetadataStore::checkIfReady);
    }

    @Test
    public void testManageLedgerWriteFailState() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        MLTransactionSequenceIdGenerator mlTransactionSequenceIdGenerator = new MLTransactionSequenceIdGenerator();
        managedLedgerConfig.setManagedLedgerInterceptor(mlTransactionSequenceIdGenerator);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory,
                managedLedgerConfig);
        mlTransactionLog.initialize().join();
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog,
                        new TransactionTimeoutTrackerImpl(), new TransactionRecoverTrackerImpl(),
                        mlTransactionSequenceIdGenerator);

        Awaitility.await().until(transactionMetadataStore::checkIfReady);
        transactionMetadataStore.newTransaction(5000).get();
        Field field = MLTransactionLogImpl.class.getDeclaredField("managedLedger");
        field.setAccessible(true);
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) field.get(mlTransactionLog);
        field = ManagedLedgerImpl.class.getDeclaredField("STATE_UPDATER");
        field.setAccessible(true);
        AtomicReferenceFieldUpdater state = (AtomicReferenceFieldUpdater) field.get(managedLedger);
        state.set(managedLedger, WriteFailed);
        try {
            transactionMetadataStore.newTransaction(5000).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ManagedLedgerException.ManagedLedgerAlreadyClosedException);
        }
        transactionMetadataStore.newTransaction(5000).get();

    }

    public class TransactionTimeoutTrackerImpl implements TransactionTimeoutTracker {

        @Override
        public CompletableFuture<Boolean> addTransaction(long sequenceId, long timeout) {
            return null;
        }

        @Override
        public void replayAddTransaction(long sequenceId, long timeout) {

        }

        @Override
        public void start() {

        }

        @Override
        public void close() {

        }
    }

    public static class TransactionRecoverTrackerImpl implements TransactionRecoverTracker {

        @Override
        public void updateTransactionStatus(long sequenceId, TxnStatus txnStatus) throws CoordinatorException.InvalidTxnStatusException {

        }

        @Override
        public void handleOpenStatusTransaction(long sequenceId, long timeout) {

        }

        @Override
        public void appendOpenTransactionToTimeoutTracker() {

        }

        @Override
        public void handleCommittingAndAbortingTransaction() {

        }
    }
}