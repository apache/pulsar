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
package org.apache.pulsar.broker.transaction.coordinator;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TransactionMetaStoreAssignmentTest extends TransactionTestBase {

    @Override
    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        setUpBase(3, 16, null, 0);
        pulsarClient.close();
    }

    @Test
    public void testTransactionMetaStoreAssignAndFailover() throws Exception {

        pulsarClient = buildClient();

        checkTransactionCoordinatorNum(16);

        pulsarClient.close();
        PulsarService crashedMetaStore = null;
        for (int i = pulsarServiceList.size() - 1; i >= 0; i--) {
            if (pulsarServiceList.get(i).getTransactionMetadataStoreService().getStores().size() > 0) {
                crashedMetaStore = pulsarServiceList.get(i);
                break;
            }
        }

        Assert.assertNotNull(crashedMetaStore);
        pulsarServiceList.remove(crashedMetaStore);
        crashedMetaStore.close();

        pulsarClient = buildClient();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    int transactionMetaStoreCount2 = pulsarServiceList.stream()
                            .mapToInt(pulsarService -> pulsarService.getTransactionMetadataStoreService().getStores().size())
                            .sum();
                    Assert.assertEquals(transactionMetaStoreCount2, 16);
                });
        pulsarClient.close();
    }

    @Test
    public void testTransactionMetaStoreUnload() throws Exception {

        pulsarClient = buildClient();
        checkTransactionCoordinatorNum(16);

        // close pulsar client will not init tc again
        pulsarClient.close();

        admin.topics().unload(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());

        for (int i = 0; i < 16; i++) {
            final int f = i;
            pulsarServiceList.forEach((pulsarService) -> pulsarService
                    .getTransactionMetadataStoreService()
                    .removeTransactionMetadataStore(TransactionCoordinatorID.get(f)));
        }
        checkTransactionCoordinatorNum(0);
        buildClient();
        checkTransactionCoordinatorNum(16);

        pulsarClient.close();

    }

    private void checkTransactionCoordinatorNum(int number) {
        Awaitility.await()
                .untilAsserted(() -> {
                    int transactionMetaStoreCount = pulsarServiceList.stream()
                            .mapToInt(pulsarService -> pulsarService.getTransactionMetadataStoreService().getStores().size())
                            .sum();
                    Assert.assertEquals(transactionMetaStoreCount, number);
                });
    }

    private PulsarClient buildClient() throws Exception {
        return PulsarClient.builder()
                .serviceUrlProvider(new ServiceUrlProvider() {
                    final AtomicInteger atomicInteger = new AtomicInteger();
                    @Override
                    public void initialize(PulsarClient client) {

                    }

                    @Override
                    public String getServiceUrl() {
                        return pulsarServiceList.get(atomicInteger.getAndIncrement() % pulsarServiceList.size()).getBrokerServiceUrl();
                    }
                })
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();
    }
    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }
}
