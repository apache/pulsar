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
package org.apache.pulsar.broker.stats;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerMetrics;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ManagedLedgerMetricsTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testManagedLedgerMetrics() throws Exception {
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(pulsar);

        final String addEntryRateKey = "brk_ml_AddEntryMessagesRate";
        List<Metrics> list1 = metrics.generate();
        Assert.assertTrue(list1.isEmpty());

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1")
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        for (Entry<String, ManagedLedgerImpl> ledger : ((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory())
                .getManagedLedgers().entrySet()) {
            ManagedLedgerMBeanImpl stats = (ManagedLedgerMBeanImpl) ledger.getValue().getStats();
            stats.refreshStats(1, TimeUnit.SECONDS);
        }

        List<Metrics> list2 = metrics.generate();
        Assert.assertEquals(list2.get(0).getMetrics().get(addEntryRateKey), 10.0D);

        for (int i = 0; i < 5; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        for (Entry<String, ManagedLedgerImpl> ledger : ((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory())
                .getManagedLedgers().entrySet()) {
            ManagedLedgerMBeanImpl stats = (ManagedLedgerMBeanImpl) ledger.getValue().getStats();
            stats.refreshStats(1, TimeUnit.SECONDS);
        }
        List<Metrics> list3 = metrics.generate();
        Assert.assertEquals(list3.get(0).getMetrics().get(addEntryRateKey), 5.0D);

    }

    @Test
    public void testTransactionTopic() throws Exception {
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 1);
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setMaxEntriesPerLedger(2);
        new MLTransactionLogImpl(TransactionCoordinatorID.get(0),
                pulsar.getManagedLedgerFactory(), managedLedgerConfig)
                .initialize().join();
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(pulsar);
        metrics.generate();
    }

}
