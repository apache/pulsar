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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;

public class TransactionMetadataStoreServiceTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testAddAndRemoveTransactionMetadataStore() {
        TransactionMetadataStoreService transactionMetadataStoreService = pulsar.getTransactionMetadataStoreService();
        Assert.assertNotNull(transactionMetadataStoreService);

        transactionMetadataStoreService.addTransactionMetadataStore(TransactionCoordinatorID.get(0));
        Assert.assertEquals(transactionMetadataStoreService.getStores().size(), 1);

        transactionMetadataStoreService.removeTransactionMetadataStore(TransactionCoordinatorID.get(0));
        Assert.assertEquals(transactionMetadataStoreService.getStores().size(), 0);
    }

    @Test
    public void testNewTransaction() throws ExecutionException, InterruptedException {
        TransactionMetadataStoreService transactionMetadataStoreService = pulsar.getTransactionMetadataStoreService();
        transactionMetadataStoreService.addTransactionMetadataStore(TransactionCoordinatorID.get(0));
        transactionMetadataStoreService.addTransactionMetadataStore(TransactionCoordinatorID.get(1));
        transactionMetadataStoreService.addTransactionMetadataStore(TransactionCoordinatorID.get(2));
        Assert.assertEquals(transactionMetadataStoreService.getStores().size(), 3);
        TxnID txnID0 = transactionMetadataStoreService.newTransaction(TransactionCoordinatorID.get(0)).get();
        TxnID txnID1 = transactionMetadataStoreService.newTransaction(TransactionCoordinatorID.get(1)).get();
        TxnID txnID2 = transactionMetadataStoreService.newTransaction(TransactionCoordinatorID.get(2)).get();
        Assert.assertEquals(0, txnID0.getMostSigBits());
        Assert.assertEquals(1, txnID1.getMostSigBits());
        Assert.assertEquals(2, txnID2.getMostSigBits());
        transactionMetadataStoreService.removeTransactionMetadataStore(TransactionCoordinatorID.get(0));
        transactionMetadataStoreService.removeTransactionMetadataStore(TransactionCoordinatorID.get(1));
        transactionMetadataStoreService.removeTransactionMetadataStore(TransactionCoordinatorID.get(2));
        Assert.assertEquals(transactionMetadataStoreService.getStores().size(), 0);
    }

    @Test
    public void testAddProducedPartitionToTxn() throws ExecutionException, InterruptedException {
        TransactionMetadataStoreService transactionMetadataStoreService = pulsar.getTransactionMetadataStoreService();
        transactionMetadataStoreService.addTransactionMetadataStore(TransactionCoordinatorID.get(0));
        Assert.assertEquals(transactionMetadataStoreService.getStores().size(), 1);
        TxnID txnID = transactionMetadataStoreService.newTransaction(TransactionCoordinatorID.get(0)).get();
        List<String> partitions = new ArrayList<>();
        partitions.add("ptn-0");
        partitions.add("ptn-1");
        partitions.add("ptn-2");
        transactionMetadataStoreService.addProducedPartitionToTxn(txnID, partitions);
        TxnMeta txn = transactionMetadataStoreService.getTxnMeta(txnID).get();
        assertEquals(txn.status(), TxnStatus.OPEN);
        transactionMetadataStoreService.removeTransactionMetadataStore(TransactionCoordinatorID.get(0));
        Assert.assertEquals(transactionMetadataStoreService.getStores().size(), 0);
    }

    @Test
    public void testAddAckedPartitionToTxn() throws ExecutionException, InterruptedException {
        TransactionMetadataStoreService transactionMetadataStoreService = pulsar.getTransactionMetadataStoreService();
        transactionMetadataStoreService.addTransactionMetadataStore(TransactionCoordinatorID.get(0));
        Assert.assertEquals(transactionMetadataStoreService.getStores().size(), 1);
        TxnID txnID = transactionMetadataStoreService.newTransaction(TransactionCoordinatorID.get(0)).get();
        List<TransactionSubscription> partitions = new ArrayList<>();
        partitions.add(TransactionSubscription.builder().topic("ptn-1").subscription("sub-1").build());
        partitions.add(TransactionSubscription.builder().topic("ptn-2").subscription("sub-1").build());
        partitions.add(TransactionSubscription.builder().topic("ptn-3").subscription("sub-1").build());
        transactionMetadataStoreService.addAckedPartitionToTxn(txnID, partitions);
        TxnMeta txn = transactionMetadataStoreService.getTxnMeta(txnID).get();
        assertEquals(txn.status(), TxnStatus.OPEN);
        transactionMetadataStoreService.removeTransactionMetadataStore(TransactionCoordinatorID.get(0));
        Assert.assertEquals(transactionMetadataStoreService.getStores().size(), 0);
    }
}
