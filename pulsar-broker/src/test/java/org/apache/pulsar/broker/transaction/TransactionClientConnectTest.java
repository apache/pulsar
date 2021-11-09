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
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TransactionMetaStoreHandler;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;

public class TransactionClientConnectTest extends TransactionTestBase {

    private static final String RECONNECT_TOPIC = "persistent://public/txn/txn-client-reconnect-test";
    private static final int NUM_PARTITIONS = 1;
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
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), NUM_PARTITIONS);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();
        // wait tc init success to ready state
        waitForCoordinatorToBeAvailable(NUM_PARTITIONS);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void testTransactionNewReconnect() throws Exception {
        start();

        // when throw CoordinatorNotFoundException client will reconnect tc
        try {
            pulsarClient.newTransaction()
                    .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build().get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.CoordinatorNotFoundException);
        }
        reconnect();

        fence(getPulsarServiceList().get(0).getTransactionMetadataStoreService());

        // tc fence will remove this tc and reopen
        try {
            pulsarClient.newTransaction()
                    .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build().get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getMessage(),
                    "org.apache.bookkeeper.mledger.ManagedLedgerException$ManagedLedgerFencedException: " +
                            "java.lang.Exception: Attempted to use a fenced managed ledger");
        }

        reconnect();
    }

    @Test
    public void testTransactionAddSubscriptionToTxnAsyncReconnect() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        start();

        try {
            transactionCoordinatorClient.addSubscriptionToTxnAsync(new TxnID(0, 0), "test", "test").get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.CoordinatorNotFoundException);
        }

        reconnect();
        fence(getPulsarServiceList().get(0).getTransactionMetadataStoreService());
        try {
            transactionCoordinatorClient.addSubscriptionToTxnAsync(new TxnID(0, 0), "test", "test").get();
            fail();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TransactionCoordinatorClientException.TransactionNotFoundException) {
                assertEquals(e.getCause().getMessage(), "The transaction with this txdID `(0,0)`not found ");
            } else {
                assertEquals(e.getCause().getMessage(), "java.lang.Exception: Attempted to use a fenced managed ledger");
            }
        }
        reconnect();
    }

    @Test
    public void testTransactionAbortToTxnAsyncReconnect() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        start();

        try {
            transactionCoordinatorClient.abortAsync(new TxnID(0, 0)).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.CoordinatorNotFoundException);
        }

        reconnect();
        fence(getPulsarServiceList().get(0).getTransactionMetadataStoreService());
        try {
            transactionCoordinatorClient.abortAsync(new TxnID(0, 0)).get();
            fail();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TransactionCoordinatorClientException.TransactionNotFoundException) {
                assertEquals(e.getCause().getMessage(), "The transaction with this txdID `(0,0)`not found ");
            } else {
                assertEquals(e.getCause().getMessage(), "java.lang.Exception: Attempted to use a fenced managed ledger");
            }
        }
        reconnect();
    }

    @Test
    public void testTransactionCommitToTxnAsyncReconnect() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        start();

        try {
            transactionCoordinatorClient.commitAsync(new TxnID(0, 0)).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.CoordinatorNotFoundException);
        }

        reconnect();
        fence(getPulsarServiceList().get(0).getTransactionMetadataStoreService());
        try {
            transactionCoordinatorClient.commitAsync(new TxnID(0, 0)).get();
            fail();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TransactionCoordinatorClientException.TransactionNotFoundException) {
                assertEquals(e.getCause().getMessage(), "The transaction with this txdID `(0,0)`not found ");
            } else {
                assertEquals(e.getCause().getMessage(), "java.lang.Exception: Attempted to use a fenced managed ledger");
            }
        }
        reconnect();
    }

    @Test
    public void testTransactionAddPublishPartitionToTxnReconnect() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        start();

        try {
            transactionCoordinatorClient.addPublishPartitionToTxnAsync(new TxnID(0, 0),
                    Collections.singletonList("test")).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.CoordinatorNotFoundException);
        }

        reconnect();
        fence(getPulsarServiceList().get(0).getTransactionMetadataStoreService());
        try {
            transactionCoordinatorClient.addPublishPartitionToTxnAsync(new TxnID(0, 0),
                    Collections.singletonList("test")).get();
            fail();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TransactionCoordinatorClientException.TransactionNotFoundException) {
                assertEquals(e.getCause().getMessage(), "The transaction with this txdID `(0,0)`not found ");
            } else {
                assertEquals(e.getCause().getMessage(), "java.lang.Exception: Attempted to use a fenced managed ledger");
            }
        }
        reconnect();
    }

    @Test
    public void testPulsarClientCloseThenCloseTcClient() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        Field field = TransactionCoordinatorClientImpl.class.getDeclaredField("handlers");
        field.setAccessible(true);
        TransactionMetaStoreHandler[] handlers =
                (TransactionMetaStoreHandler[]) field.get(transactionCoordinatorClient);

        for (TransactionMetaStoreHandler handler : handlers) {
            handler.newTransactionAsync(10, TimeUnit.SECONDS).get();
        }
        pulsarClient.close();
        for (TransactionMetaStoreHandler handler : handlers) {
            Method method = TransactionMetaStoreHandler.class.getMethod("getConnectHandleState");
            method.setAccessible(true);
            assertEquals(method.invoke(handler).toString(), "Closed");
            try {
                handler.newTransactionAsync(10, TimeUnit.SECONDS).get();
            } catch (ExecutionException | InterruptedException e) {
                assertTrue(e.getCause()
                        instanceof TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException);
            }
        }
    }

    public void start() throws Exception {
        // wait transaction coordinator init success
        Awaitility.await().until(() -> {
            try {
                pulsarClient.newTransaction()
                        .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build().get();
            } catch (Exception e) {
                return false;
            }
            return true;
        });
        pulsarClient.newTransaction()
                .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build().get();

        TransactionMetadataStoreService transactionMetadataStoreService =
                getPulsarServiceList().get(0).getTransactionMetadataStoreService();
        // remove transaction metadata store
        transactionMetadataStoreService.removeTransactionMetadataStore(TransactionCoordinatorID.get(0)).get();

    }

    public void fence(TransactionMetadataStoreService transactionMetadataStoreService) throws Exception {
        Field field = ManagedLedgerImpl.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(((MLTransactionMetadataStore) transactionMetadataStoreService.getStores()
                .get(TransactionCoordinatorID.get(0))).getManagedLedger(), ManagedLedgerImpl.State.Fenced);
    }

    public void reconnect() {
        //reconnect
        Awaitility.await().until(() -> {
            try {
                pulsarClient.newTransaction()
                        .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build().get();
            } catch (Exception e) {
                return false;
            }
            return true;
        });
    }
}
