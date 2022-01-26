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
package org.apache.pulsar.client.impl;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Slf4j
public class TransactionClientConnectTest extends TransactionTestBase {

    private static final String RECONNECT_TOPIC = NAMESPACE1 + "/txn-client-reconnect-test";
    private static final int NUM_PARTITIONS = 1;
    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        setUpBase(1, NUM_PARTITIONS, RECONNECT_TOPIC, 0);
        admin.topics().createSubscription(RECONNECT_TOPIC, "test", MessageId.latest);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void testTransactionNewReconnect() throws Exception {
        Callable<CompletableFuture<?>> callable = () -> pulsarClient.newTransaction()
                .withTransactionTimeout(200, TimeUnit.MILLISECONDS).build();
        tryCommandReconnect(callable, callable);
    }

    @Test
    public void testTransactionAddSubscriptionToTxnAsyncReconnect() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        Callable<CompletableFuture<?>> callable = () -> transactionCoordinatorClient
                .addSubscriptionToTxnAsync(new TxnID(0, 0), "test", "test");
        tryCommandReconnect(callable, callable);
    }

    public void tryCommandReconnect(Callable<CompletableFuture<?>> callable1, Callable<CompletableFuture<?>> callable2)
            throws Exception {
        start();
        try {
            callable1.call().get();
        } catch (ExecutionException e) {
            assertFalse(e.getCause() instanceof TransactionCoordinatorClientException.CoordinatorNotFoundException);
            waitToReady();
            callable1.call().get();
        }
        fence(getPulsarServiceList().get(0).getTransactionMetadataStoreService());
        CompletableFuture<?> completableFuture = callable2.call();
        try {
            completableFuture.get(3, TimeUnit.SECONDS);
        } catch (TimeoutException ignore) {
        } catch (ExecutionException e) {
            Assert.assertFalse(e.getCause()
                    instanceof TransactionCoordinatorClientException.CoordinatorNotFoundException);
        }

        unFence(getPulsarServiceList().get(0).getTransactionMetadataStoreService());
        completableFuture.get();
    }

    @Test
    public void testTransactionAbortToTxnAsyncReconnect() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        Callable<CompletableFuture<?>> callable1 = () -> transactionCoordinatorClient.abortAsync(new TxnID(0,
                0));
        Callable<CompletableFuture<?>> callable2 = () -> transactionCoordinatorClient.abortAsync(new TxnID(0,
                1));
        tryCommandReconnect(callable1, callable2);
    }

    @Test
    public void testTransactionCommitToTxnAsyncReconnect() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        Callable<CompletableFuture<?>> callable1 = () -> transactionCoordinatorClient.commitAsync(new TxnID(0,
                0));
        Callable<CompletableFuture<?>> callable2 = () -> transactionCoordinatorClient.commitAsync(new TxnID(0,
                1));
        tryCommandReconnect(callable1, callable2);
    }

    @Test
    public void testTransactionAddPublishPartitionToTxnReconnect() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        Callable<CompletableFuture<?>> callable = () -> transactionCoordinatorClient.addPublishPartitionToTxnAsync(new TxnID(0, 0),
                Collections.singletonList("test"));
        tryCommandReconnect(callable, callable);
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
        for (TransactionMetaStoreHandler handler : handlers) {
            Field stateField = HandlerState.class.getDeclaredField("state");
            stateField.setAccessible(true);
            stateField.set(handler, HandlerState.State.Closed);
        }
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
        pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();
        pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        TransactionMetadataStoreService transactionMetadataStoreService =
                getPulsarServiceList().get(0).getTransactionMetadataStoreService();
        // remove transaction metadap0-ta store
        transactionMetadataStoreService.removeTransactionMetadataStore(TransactionCoordinatorID.get(0)).get();

    }

    public void fence(TransactionMetadataStoreService transactionMetadataStoreService) throws Exception {
        Field field = ManagedLedgerImpl.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(((MLTransactionMetadataStore) transactionMetadataStoreService.getStores()
                .get(TransactionCoordinatorID.get(0))).getManagedLedger(), ManagedLedgerImpl.State.Fenced);
    }
    public void unFence(TransactionMetadataStoreService transactionMetadataStoreService) throws Exception {
        Field field = ManagedLedgerImpl.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(((MLTransactionMetadataStore) transactionMetadataStoreService.getStores()
                .get(TransactionCoordinatorID.get(0))).getManagedLedger(), ManagedLedgerImpl.State.LedgerOpened);
    }

    public void waitToReady() throws Exception{
        TransactionMetadataStoreService transactionMetadataStoreService =
                getPulsarServiceList().get(0).getTransactionMetadataStoreService();
        Class<TransactionMetadataStoreService> transactionMetadataStoreServiceClass =
                TransactionMetadataStoreService.class;
        Field field1 =
                transactionMetadataStoreServiceClass.getDeclaredField("stores");
        field1.setAccessible(true);
        Map<TransactionCoordinatorID, TransactionMetadataStore> stores =
                (Map<TransactionCoordinatorID, TransactionMetadataStore>) field1
                        .get(transactionMetadataStoreService);
        Awaitility.await().until(() -> {
            for (TransactionMetadataStore transactionMetadataStore : stores.values()) {
                Class<TransactionMetadataStoreState> transactionMetadataStoreStateClass =
                        TransactionMetadataStoreState.class;
                Field field = transactionMetadataStoreStateClass.getDeclaredField("state");
                field.setAccessible(true);
                TransactionMetadataStoreState.State state =
                        (TransactionMetadataStoreState.State) field.get(transactionMetadataStore);
                if (!state.equals(TransactionMetadataStoreState.State.Ready)) {
                    return false;
                }
            }
            return true;
        });
    }
}
