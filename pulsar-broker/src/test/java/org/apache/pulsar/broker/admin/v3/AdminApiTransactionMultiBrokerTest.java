/*
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
package org.apache.pulsar.broker.admin.v3;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.policies.data.TransactionBufferInternalStats;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin-isolated")
public class AdminApiTransactionMultiBrokerTest extends TransactionTestBase {

    private static final int NUM_BROKERS = 16;
    private static final int NUM_PARTITIONS = 3;

    @BeforeMethod
    protected void setup() throws Exception {
        setUpBase(NUM_BROKERS, NUM_PARTITIONS, NAMESPACE1 + "/test", 0);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * This test is used to verify the redirect request of `getCoordinatorInternalStats`.
     * <p>
     *     1. Set up 16 broker and create 3 transaction coordinator topic.
     *     2. The 3 transaction coordinator topic will be assigned to these brokers through some kind of
     *     load-balancing strategy. (In current implementations, they tend to be assigned to a broker.)
     *     3. Find a broker x which is not the owner of the transaction coordinator topic.
     *     4. Create a admin connected to broker x, and use the admin to call ` getCoordinatorInternalStats`.
     * </p>
     */
    @Test
    public void testRedirectOfGetCoordinatorInternalStats() throws Exception {
        PulsarAdmin localAdmin = this.admin;
        Map<String, String> map = localAdmin.lookups()
                .lookupPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.toString());

        for (int i = 0; map.containsValue(getPulsarServiceList().get(i).getBrokerServiceUrl()); i++) {
            if (!map.containsValue(getPulsarServiceList().get(i + 1).getBrokerServiceUrl()))
                if (localAdmin != null) {
                    localAdmin.close();
                }
                localAdmin = spy(createNewPulsarAdmin(PulsarAdmin.builder()
                        .serviceHttpUrl(pulsarServiceList.get(i + 1).getWebServiceAddress())));
        }
        if (pulsarClient != null) {
            pulsarClient.shutdown();
        }
        //init tc stores
        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            localAdmin.transactions().getCoordinatorInternalStats(i, false);
        }
        localAdmin.close();
    }

    @Test
    public void testGetTransactionBufferInternalStatsInMultiBroker() throws Exception {
        for (int i = 0; i < super.getBrokerCount(); i++) {
            getPulsarServiceList().get(i).getConfig().setTransactionBufferSegmentedSnapshotEnabled(true);
        }
        String topic1 = NAMESPACE1 +  "/testGetTransactionBufferInternalStatsInMultiBroker";
        assertTrue(admin.namespaces().getBundles(NAMESPACE1).getNumBundles() > 1);
        for (int i = 0; true ; i++) {
            topic1 = topic1 + i;
            admin.topics().createNonPartitionedTopic(topic1);
            String segmentTopicBroker = admin.lookups()
                    .lookupTopic(NAMESPACE1 + "/" + SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS);
            String indexTopicBroker = admin.lookups()
                    .lookupTopic(NAMESPACE1 + "/" + SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_INDEXES);
            if (segmentTopicBroker.equals(indexTopicBroker)) {
                String topicBroker = admin.lookups().lookupTopic(topic1);
                if (!topicBroker.equals(segmentTopicBroker)) {
                    break;
                }
            } else {
                break;
            }
        }
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES).topic(topic1).create();
        TransactionBufferInternalStats stats = admin.transactions()
                .getTransactionBufferInternalStatsAsync(topic1, true).get();
        assertEquals(stats.snapshotType, AbortedTxnProcessor.SnapshotType.Segment.toString());
        assertNull(stats.singleSnapshotSystemTopicInternalStats);
        assertNotNull(stats.segmentInternalStats);
        assertTrue(stats.segmentInternalStats.managedLedgerName
                .contains(SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS));
        assertNotNull(stats.segmentIndexInternalStats);
        assertTrue(stats.segmentIndexInternalStats.managedLedgerName
                .contains(SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_INDEXES));
    }
}
