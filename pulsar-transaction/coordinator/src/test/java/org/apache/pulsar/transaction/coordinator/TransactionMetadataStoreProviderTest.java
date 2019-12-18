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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.apache.pulsar.transaction.coordinator.impl.InMemTransactionMetadataStoreProvider;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Unit test different transaction metadata store provider.
 */
public class TransactionMetadataStoreProviderTest {

    @DataProvider(name = "providers")
    public static Object[][] providers() {
        return new Object[][] {
            { InMemTransactionMetadataStoreProvider.class.getName() }
        };
    }

    private final String providerClassName;
    private TransactionMetadataStoreProvider provider;
    private TransactionCoordinatorID tcId;
    private TransactionMetadataStore store;

    @Factory(dataProvider = "providers")
    public TransactionMetadataStoreProviderTest(String providerClassName) throws Exception {
        this.providerClassName = providerClassName;
        this.provider = TransactionMetadataStoreProvider.newProvider(providerClassName);
    }

    @BeforeMethod
    public void setup() throws Exception {
        this.tcId = new TransactionCoordinatorID(1L);
        this.store = this.provider.openStore(tcId, null).get();
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

}
