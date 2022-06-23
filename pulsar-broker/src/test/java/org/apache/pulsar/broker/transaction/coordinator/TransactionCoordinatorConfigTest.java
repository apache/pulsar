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

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;
import com.google.common.collect.Sets;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionCoordinatorConfigTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration configuration = getDefaultConf();
        configuration.setTransactionCoordinatorEnabled(true);
        configuration.setMaxActiveTransactionsPerCoordinator(2);
        super.baseSetup(configuration);
        admin.tenants().createTenant("pulsar", new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        pulsar.getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(1));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMaxActiveTxn() throws Exception {
        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .enableTransaction(true).operationTimeout(3, TimeUnit.SECONDS).build();

        // new two txn will not reach max active txns
        Transaction commitTxn =
                pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();
        Transaction abortTxn =
                pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();
        try {
            // new the third txn will timeout, broker will return any response
            pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
        }

        // release active txn
        commitTxn.commit().get();
        abortTxn.abort().get();

        // two txn end, can continue new txn
        pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();
        pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();

        // reach max active txns again
        try {
            // new the third txn will timeout, broker will return any response
            pulsarClient.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
        }
    }
}
