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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.ByteArrayCoder;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.LocalDLMEmulator;
import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.transaction.configuration.CoordinatorConfiguration;
import org.apache.pulsar.transaction.coordinator.impl.PersistentTransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.apache.pulsar.transaction.coordinator.exceptions.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.coordinator.impl.InMemTransactionMetadataStoreProvider;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Unit test different transaction metadata store provider.
 */
@Slf4j
public class TransactionMetadataStoreProviderTest {

    @DataProvider(name = "providers")
    public static Object[][] providers() {
        return new Object[][] {
            //{ InMemTransactionMetadataStoreProvider.class.getName() },
            { PersistentTransactionMetadataStoreProvider.class.getName(),
              new Class[] {PulsarAdmin.class, CoordinatorConfiguration.class},
              new Object[] {mockPulsarAdmin, mockCoordinatorConfiguration}}
        };
    }

    private static ZooKeeperServerShim zks;
    private static String zkServers;
    private static int zkPort;
    private static int numBookies = 3;
    private static LocalDLMEmulator bkutil;
    private static File zkTmpDir;
    private static PulsarAdmin mockPulsarAdmin = mock(PulsarAdmin.class);
    private static CoordinatorConfiguration mockCoordinatorConfiguration = mock(CoordinatorConfiguration.class);
    private static Brokers mockBrokers = mock(Brokers.class);
    private static InternalConfigurationData mockInternalConfigurationData = mock(InternalConfigurationData.class);

    private final String providerClassName;
    private TransactionMetadataStoreProvider provider;
    private TransactionCoordinatorID tcId;
    private TransactionMetadataStore store;
    private ZooKeeper zkc;

    @Factory(dataProvider = "providers")
    public TransactionMetadataStoreProviderTest(String providerClassName,
                                                Class[] constructorArgClazzes,
                                                Object[] constructorArgs) throws Exception {
        if (providerClassName == PersistentTransactionMetadataStoreProvider.class.getName()) {
            setupCluster();
            setupMock();
        }
        this.providerClassName = providerClassName;
        this.provider = TransactionMetadataStoreProvider.newProvider(providerClassName,
                                                                    constructorArgClazzes,
                                                                    constructorArgs);
    }

    @BeforeMethod
    public void setup() throws Exception {
        try {
            zkc = LocalDLMEmulator.connectZooKeeper("127.0.0.1", zkPort);
        } catch (Exception ex) {
            log.error("hit exception connecting to zookeeper at {}:{}", "127.0.0.1", zkPort, ex);
            throw ex;
        }
        try {
            zkc.create("/pulsar/transaction/coordinator", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception ex) {
        }
        this.tcId = new TransactionCoordinatorID(1L);
        this.store = this.provider.openStore(tcId).get();
    }

    @Test
    public void testKVStore() throws Exception {
        this.store.init(mockCoordinatorConfiguration).get();
        TxnID txnID = this.store.newTransaction().get();
        System.out.println(txnID);
    }

    @Test
    public void testGetTxnStatusNotFound() throws Exception {
        try {
            this.store.getTxnStatus(
                new TxnID(tcId.getId(), 12345L)).get();
            fail("Should fail to get txn status of a non-existent transaction");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof TransactionNotFoundException);
        }
    }

    @Test
    public void testGetTxnStatusSuccess() throws Exception {
        TxnID txnID = this.store.newTransaction().get();
        TxnStatus txnStatus = this.store.getTxnStatus(txnID).get();
        assertEquals(txnStatus, TxnStatus.OPEN);
    }

    @Test
    public void testUpdateTxnStatusSuccess() throws Exception {
        TxnID txnID = this.store.newTransaction().get();
        TxnStatus txnStatus = this.store.getTxnStatus(txnID).get();
        assertEquals(txnStatus, TxnStatus.OPEN);

        // update the status
        this.store.updateTxnStatus(txnID, TxnStatus.COMMITTING, TxnStatus.OPEN).get();

        // get the new status
        TxnStatus newTxnStatus = this.store.getTxnStatus(txnID).get();
        assertEquals(newTxnStatus, TxnStatus.COMMITTING);
    }

    @Test
    public void testUpdateTxnStatusNotExpectedStatus() throws Exception {
        TxnID txnID = this.store.newTransaction().get();
        TxnStatus txnStatus = this.store.getTxnStatus(txnID).get();
        assertEquals(txnStatus, TxnStatus.OPEN);

        // update the status
        try {
            this.store.updateTxnStatus(txnID, TxnStatus.COMMITTING, TxnStatus.COMMITTING).get();
            fail("Should fail to update txn status if it is not in expected status");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof InvalidTxnStatusException);
        }

        // get the txn status, it should be changed.
        TxnStatus newTxnStatus = this.store.getTxnStatus(txnID).get();
        assertEquals(newTxnStatus, TxnStatus.OPEN);
    }

    @Test
    public void testUpdateTxnStatusCannotTransition() throws Exception {
        TxnID txnID = this.store.newTransaction().get();
        TxnStatus txnStatus = this.store.getTxnStatus(txnID).get();
        assertEquals(txnStatus, TxnStatus.OPEN);

        // update the status
        try {
            this.store.updateTxnStatus(txnID, TxnStatus.COMMITTED, TxnStatus.OPEN).get();
            fail("Should fail to update txn status if it can not transition to the new status");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof InvalidTxnStatusException);
        }

        // get the txn status, it should be changed.
        TxnStatus newTxnStatus = this.store.getTxnStatus(txnID).get();
        assertEquals(newTxnStatus, TxnStatus.OPEN);
    }

    @Test
    public void testAddProducedPartition() throws Exception {
        TxnID txnID = this.store.newTransaction().get();
        TxnStatus txnStatus = this.store.getTxnStatus(txnID).get();
        assertEquals(txnStatus, TxnStatus.OPEN);

        List<String> partitions = new ArrayList<>();
        partitions.add("ptn-0");
        partitions.add("ptn-1");
        partitions.add("ptn-2");

        // add the list of partitions to the transaction
        this.store.addProducedPartitionToTxn(txnID, partitions).get();

        TxnMeta txn = this.store.getTxnMeta(txnID).get();
        assertEquals(txn.status(), TxnStatus.OPEN);
        assertEquals(txn.producedPartitions(), partitions);

        // add another list of partition. duplicated partitions should be removed
        List<String> newPartitions = new ArrayList<>();
        newPartitions.add("ptn-2");
        newPartitions.add("ptn-3");
        newPartitions.add("ptn-4");
        this.store.addProducedPartitionToTxn(txnID, newPartitions);

        txn = this.store.getTxnMeta(txnID).get();
        assertEquals(txn.status(), TxnStatus.OPEN);
        List<String> finalPartitions = new ArrayList<>();
        finalPartitions.add("ptn-0");
        finalPartitions.add("ptn-1");
        finalPartitions.add("ptn-2");
        finalPartitions.add("ptn-3");
        finalPartitions.add("ptn-4");
        assertEquals(txn.producedPartitions(), finalPartitions);

        // change the transaction to `COMMITTING`
        this.store.updateTxnStatus(txnID, TxnStatus.COMMITTING, TxnStatus.OPEN).get();

        // add partitions should fail if it is already committing.
        List<String> newPartitions2 = new ArrayList<>();
        newPartitions2.add("ptn-5");
        newPartitions2.add("ptn-6");
        try {
            this.store.addProducedPartitionToTxn(txnID, newPartitions2).get();
            fail("Should fail to add produced partitions if the transaction is not in OPEN status");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof InvalidTxnStatusException);
        }

        txn = this.store.getTxnMeta(txnID).get();
        assertEquals(txn.status(), TxnStatus.COMMITTING);
        assertEquals(txn.producedPartitions(), finalPartitions);
    }

    @Test
    public void testAddAckedPartition() throws Exception {
        TxnID txnID = this.store.newTransaction().get();
        TxnStatus txnStatus = this.store.getTxnStatus(txnID).get();
        assertEquals(txnStatus, TxnStatus.OPEN);

        List<String> partitions = new ArrayList<>();
        partitions.add("ptn-0");
        partitions.add("ptn-1");
        partitions.add("ptn-2");

        // add the list of partitions to the transaction
        this.store.addAckedPartitionToTxn(txnID, partitions).get();

        TxnMeta txn = this.store.getTxnMeta(txnID).get();
        assertEquals(txn.status(), TxnStatus.OPEN);
        assertEquals(txn.ackedPartitions(), partitions);

        // add another list of partition. duplicated partitions should be removed
        List<String> newPartitions = new ArrayList<>();
        newPartitions.add("ptn-2");
        newPartitions.add("ptn-3");
        newPartitions.add("ptn-4");
        this.store.addAckedPartitionToTxn(txnID, newPartitions);

        txn = this.store.getTxnMeta(txnID).get();
        assertEquals(txn.status(), TxnStatus.OPEN);
        List<String> finalPartitions = new ArrayList<>();
        finalPartitions.add("ptn-0");
        finalPartitions.add("ptn-1");
        finalPartitions.add("ptn-2");
        finalPartitions.add("ptn-3");
        finalPartitions.add("ptn-4");
        assertEquals(txn.ackedPartitions(), finalPartitions);

        // change the transaction to `COMMITTING`
        this.store.updateTxnStatus(txnID, TxnStatus.COMMITTING, TxnStatus.OPEN).get();

        // add partitions should fail if it is already committing.
        List<String> newPartitions2 = new ArrayList<>();
        newPartitions2.add("ptn-5");
        newPartitions2.add("ptn-6");
        try {
            this.store.addAckedPartitionToTxn(txnID, newPartitions2).get();
            fail("Should fail to add acked partitions if the transaction is not in OPEN status");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof InvalidTxnStatusException);
        }

        txn = this.store.getTxnMeta(txnID).get();
        assertEquals(txn.status(), TxnStatus.COMMITTING);
        assertEquals(txn.ackedPartitions(), finalPartitions);
    }

    private static void setupCluster() throws Exception {
        zkTmpDir = IOUtils.createTempDir("zookeeper", "distrlog");
        Pair<ZooKeeperServerShim, Integer> serverAndPort = LocalDLMEmulator.runZookeeperOnAnyPort(zkTmpDir);
        zks = serverAndPort.getLeft();
        zkPort = serverAndPort.getRight();
        bkutil = LocalDLMEmulator.newBuilder()
                .numBookies(numBookies)
                .zkHost("127.0.0.1")
                .zkPort(zkPort)
                .serverConf(DLMTestUtil.loadTestBkConf())
                .shouldStartZK(false)
                .build();
        bkutil.start();
        zkServers = "127.0.0.1:" + zkPort;
    }

    private static void setupMock() throws Exception {
        when(mockInternalConfigurationData.getZookeeperServers()).thenReturn("127.0.0.1:" + zkPort);
        when(mockInternalConfigurationData.getLedgersRootPath()).thenReturn("/ledgerRoot");
        when(mockCoordinatorConfiguration.getDlLocalStateStoreDir()).thenReturn("./temp");
        when(mockBrokers.getInternalConfigurationData()).thenReturn(mockInternalConfigurationData);
        when(mockPulsarAdmin.brokers()).thenReturn(mockBrokers);
    }

    private static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
        FileUtils.forceDeleteOnExit(zkTmpDir);
    }

}
