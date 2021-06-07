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
package org.apache.pulsar.broker.transaction;

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;

public class TransactionClientReconnectTest extends TransactionTestBase {

    private static final String RECONNECT_TOPIC = "persistent://public/txn/txn-client-reconnect-test";

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        setBrokerCount(1);
        super.internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace("public/txn", 10);
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createNonPartitionedTopic(RECONNECT_TOPIC);
        admin.topics().createSubscription(RECONNECT_TOPIC, "test", MessageId.latest);
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 1);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void testTransactionClientReconnectTest() throws PulsarClientException, ExecutionException, InterruptedException {

        ((PulsarClientImpl) pulsarClient).getLookup()
                .getPartitionedTopicMetadata(TopicName.TRANSACTION_COORDINATOR_ASSIGN).get();

        Awaitility.await().until(() -> {
            pulsarClient.newTransaction()
                    .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build().get();
            return true;
        });

        TransactionImpl transaction = (TransactionImpl) pulsarClient.newTransaction()
                .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build().get();

        TransactionMetadataStoreService transactionMetadataStoreService =
                getPulsarServiceList().get(0).getTransactionMetadataStoreService();

        transactionMetadataStoreService.removeTransactionMetadataStore(TransactionCoordinatorID.get(0));

        // transaction client will reconnect
        try {
            pulsarClient.newTransaction()
                    .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build().get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.CoordinatorNotFoundException);
        }

        try {
            transaction.registerProducedTopic(RECONNECT_TOPIC).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException);
        }

        try {
            transaction.registerAckedTopic(RECONNECT_TOPIC, "test").get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException);
        }

        try {
            transaction.commit().get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException);
        }

        transactionMetadataStoreService.addTransactionMetadataStore(TransactionCoordinatorID.get(0));

        // wait transaction coordinator init success
        Awaitility.await().until(() -> {
            pulsarClient.newTransaction()
                    .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build().get();
            return true;
        });
        transaction = (TransactionImpl) pulsarClient.newTransaction()
                .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build().get();
        transaction.registerProducedTopic(RECONNECT_TOPIC).get();
        transaction.registerAckedTopic(RECONNECT_TOPIC, "test").get();
        transaction.commit().get();
    }
}
