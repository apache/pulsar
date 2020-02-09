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

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.timeout.MLTransactionTimeoutTrackerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

public class MLTransactionTimeoutTrackerTest extends BookKeeperClusterTestCase {

    public MLTransactionTimeoutTrackerTest() {
        super(3);
    }

    @Test
    public void testTimeoutTracker() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory);
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog,
                        new MLTransactionTimeoutTrackerFactory());
        while (true) {
            if (transactionMetadataStore.checkIfReady()) {
                for (int i = 0; i < 1000; i ++) {
                    transactionMetadataStore.newTransactionAsync(5).get();
                }
                transactionMetadataStore.getTxnMetaMap().forEach((txnID, txnMeta) -> {
                    Assert.assertEquals(txnMeta.status(), PulsarApi.TxnStatus.OPEN);
                });
                Thread.sleep(6000L);
                transactionMetadataStore.getTxnMetaMap().forEach((txnID, txnMeta) -> {
                    Assert.assertEquals(txnMeta.status(), PulsarApi.TxnStatus.ABORTING);
                });
                Assert.assertEquals(1000, transactionMetadataStore.getTxnMetaMap().size());
                break;
            }
        }
    }

    @Test
    public void testTimeoutTrackerMultiThreading() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc, factoryConf);
        TransactionCoordinatorID transactionCoordinatorID = new TransactionCoordinatorID(1);
        MLTransactionLogImpl mlTransactionLog = new MLTransactionLogImpl(transactionCoordinatorID, factory);
        MLTransactionMetadataStore transactionMetadataStore =
                new MLTransactionMetadataStore(transactionCoordinatorID, mlTransactionLog,
                        new MLTransactionTimeoutTrackerFactory());

        while(true) {
            if (transactionMetadataStore.checkIfReady()) {
                CountDownLatch countDownLatch = new CountDownLatch(3);

                new Thread(() -> {
                    for (int i = 0; i < 1000; i ++) {
                        try {
                            transactionMetadataStore.newTransactionAsync(1).get();
                        } catch (Exception e) {
                            //no operation
                        }
                    }
                    countDownLatch.countDown();
                }).start();

                new Thread(() -> {
                    for (int i = 0; i < 1000; i ++) {
                        try {
                            transactionMetadataStore.newTransactionAsync(3).get();
                        } catch (Exception e) {
                            //no operation
                        }
                    }
                    countDownLatch.countDown();
                }).start();

                new Thread(() -> {
                    for (int i = 0; i < 1000; i ++) {
                        try {
                            transactionMetadataStore.newTransactionAsync(2).get();
                        } catch (Exception e) {
                            //no operation
                        }
                    }
                    countDownLatch.countDown();
                }).start();
                countDownLatch.await();
                Thread.sleep(4000L);
                transactionMetadataStore.getTxnMetaMap().forEach((txnID, txnMeta) ->{
                    Assert.assertEquals(txnMeta.status(), PulsarApi.TxnStatus.ABORTING);
                });
                Assert.assertEquals(3000, transactionMetadataStore.getTxnMetaMap().size());
                break;
            }
        }
    }
}
