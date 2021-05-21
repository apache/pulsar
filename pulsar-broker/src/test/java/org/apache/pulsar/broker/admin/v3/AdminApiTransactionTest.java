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
package org.apache.pulsar.broker.admin.v3;

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStatus;
import org.apache.pulsar.packages.management.core.MockedPackagesStorageProvider;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class AdminApiTransactionTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setEnablePackagesManagement(true);
        conf.setPackagesManagementStorageProvider(MockedPackagesStorageProvider.class.getName());
        conf.setTransactionCoordinatorEnabled(true);
        conf.setSystemTopicEnabled(true);
        super.internalSetup();
        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("pulsar", tenantInfo);
        admin.namespaces().createNamespace("pulsar/system", Sets.newHashSet("test"));
        admin.tenants().createTenant("public", tenantInfo);
        admin.namespaces().createNamespace("public/default", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testGetTransactionCoordinatorStatus() throws Exception {
        initTransaction(2);
        getTransaction().commit().get();
        getTransaction().abort().get();
        TransactionCoordinatorStatus transactionCoordinatorStatus =
                admin.transactions().getCoordinatorStatusById(1).get();
        verifyCoordinatorStatus(1L, transactionCoordinatorStatus.coordinatorId,
                transactionCoordinatorStatus.state,
                transactionCoordinatorStatus.sequenceId, transactionCoordinatorStatus.lowWaterMark);

        transactionCoordinatorStatus = admin.transactions().getCoordinatorStatusById(0).get();
        verifyCoordinatorStatus(0L, transactionCoordinatorStatus.coordinatorId,
                transactionCoordinatorStatus.state,
                transactionCoordinatorStatus.sequenceId, transactionCoordinatorStatus.lowWaterMark);
        List<TransactionCoordinatorStatus> list = admin.transactions().getCoordinatorStatusList().get();

        assertEquals(list.size(), 2);

        transactionCoordinatorStatus = list.get(0);
        verifyCoordinatorStatus(0L, transactionCoordinatorStatus.coordinatorId,
                transactionCoordinatorStatus.state,
                transactionCoordinatorStatus.sequenceId, transactionCoordinatorStatus.lowWaterMark);

        transactionCoordinatorStatus = list.get(1);
        verifyCoordinatorStatus(1L, transactionCoordinatorStatus.coordinatorId,
                transactionCoordinatorStatus.state,
                transactionCoordinatorStatus.sequenceId, transactionCoordinatorStatus.lowWaterMark);
    }

    @Test(timeOut = 20000)
    public void testGetCoordinatorInternalStats() throws Exception {
        initTransaction(1);
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();

        ManagedLedgerInternalStats managedLedgerInternalStats = admin.transactions()
                .getCoordinatorInternalStats(0, true).get().managedLedgerInternalStats;
        verifyManagedLegerInternalStats(managedLedgerInternalStats, 26);

        transaction.commit().get();

        managedLedgerInternalStats = admin.transactions()
                .getCoordinatorInternalStats(0, false).get().managedLedgerInternalStats;
        assertNull(managedLedgerInternalStats.ledgers.get(0).metadata);
    }

    private static void verifyCoordinatorStatus(long expectedCoordinatorId, long coordinatorId, String state,
                                                long sequenceId, long lowWaterMark) {
        assertEquals(coordinatorId, expectedCoordinatorId);
        assertEquals(state, "Ready");
        assertEquals(sequenceId, 0);
        assertEquals(lowWaterMark, 0);
    }

    private void initTransaction(int coordinatorSize) throws Exception {
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), coordinatorSize);
        admin.lookups().lookupTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
        Awaitility.await().until(() ->
                pulsar.getTransactionMetadataStoreService().getStores().size() == coordinatorSize);
        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true).build();
    }

    private Transaction getTransaction() throws Exception {
        return pulsarClient.newTransaction()
                .withTransactionTimeout(2, TimeUnit.SECONDS).build().get();
    }

    private static void verifyManagedLegerInternalStats(ManagedLedgerInternalStats managedLedgerInternalStats,
                                                        long totalSize) {
        assertEquals(managedLedgerInternalStats.entriesAddedCounter, 1);
        assertEquals(managedLedgerInternalStats.numberOfEntries, 1);
        assertEquals(managedLedgerInternalStats.totalSize, totalSize);
        assertEquals(managedLedgerInternalStats.currentLedgerEntries, 1);
        assertEquals(managedLedgerInternalStats.currentLedgerSize, totalSize);
        assertNull(managedLedgerInternalStats.lastLedgerCreationFailureTimestamp);
        assertEquals(managedLedgerInternalStats.waitingCursorsCount, 0);
        assertEquals(managedLedgerInternalStats.pendingAddEntriesCount, 0);
        assertNotNull(managedLedgerInternalStats.lastConfirmedEntry);
        assertEquals(managedLedgerInternalStats.ledgers.size(), 1);
        assertNotNull(managedLedgerInternalStats.ledgers.get(0).metadata);
        assertEquals(managedLedgerInternalStats.cursors.size(), 1);
    }
}
