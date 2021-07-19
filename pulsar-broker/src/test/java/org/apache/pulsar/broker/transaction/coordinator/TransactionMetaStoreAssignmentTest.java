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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pulsar.broker.PulsarService;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionMetaStoreAssignmentTest extends TransactionMetaStoreTestBase {

    @Test(groups = "broker")
    public void testTransactionMetaStoreAssignAndFailover() throws IOException {

        Awaitility.await()
                .untilAsserted(() -> {
                    int transactionMetaStoreCount = Arrays.stream(pulsarServices)
                            .mapToInt(pulsarService -> pulsarService.getTransactionMetadataStoreService().getStores().size())
                            .sum();
                    Assert.assertEquals(transactionMetaStoreCount, 16);
                });

        PulsarService crashedMetaStore = null;
        for (int i = pulsarServices.length - 1; i >= 0; i--) {
            if (pulsarServices[i].getTransactionMetadataStoreService().getStores().size() > 0) {
                crashedMetaStore = pulsarServices[i];
                break;
            }
        }

        Assert.assertNotNull(crashedMetaStore);
        List<PulsarService> services = new ArrayList<>(pulsarServices.length - 1);
        for (PulsarService pulsarService : pulsarServices) {
            if (pulsarService != crashedMetaStore) {
                services.add(pulsarService);
            }
        }
        pulsarServices = new PulsarService[pulsarServices.length - 1];
        for (int i = 0; i < services.size(); i++) {
            pulsarServices[i] = services.get(i);
        }
        crashedMetaStore.close();

        Awaitility.await()
                .untilAsserted(() -> {
                    int transactionMetaStoreCount2 = Arrays.stream(pulsarServices)
                            .mapToInt(pulsarService -> pulsarService.getTransactionMetadataStoreService().getStores().size())
                            .sum();
                    Assert.assertEquals(transactionMetaStoreCount2, 16);
                });
        transactionCoordinatorClient.close();
    }
}
