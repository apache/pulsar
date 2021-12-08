package org.apache.pulsar.client.impl;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class TransactionRetryTest extends TransactionTestBase {

    private static final int TOPIC_PARTITION = 3;
    private static final String TOPIC_OUTPUT = NAMESPACE1 + "/output";
    private static final String TOPIC_MESSAGE_ACK_TEST = NAMESPACE1 + "/message-ack-test";
    private static final int NUM_PARTITIONS = 16;
    @BeforeMethod
    protected void setup() throws Exception {
        setUpBase(1, NUM_PARTITIONS, TOPIC_OUTPUT, TOPIC_PARTITION);
        admin.topics().createPartitionedTopic(TOPIC_MESSAGE_ACK_TEST, 1);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }


    public void testTryNewTxnAgainWhenTCNotReadyOrConnecting () throws Exception {
        Callable<CompletableFuture<Transaction>> callable = ()
                -> {
            try {
                return pulsarClient
                        .newTransaction()
                        .withTransactionTimeout(5, TimeUnit.SECONDS)
                        .build();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
            return null;
        };
        tryCommandAgainWhenTCNotReadyOrConnecting(callable, callable, this);
    }

    public void testAddPublishPartitionWhenTCNotReadyOrConnecting() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        Transaction transaction1 = pulsarClient
                .newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build()
                .get();
        Transaction transaction2 = pulsarClient
                .newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build()
                .get();
        Callable<CompletableFuture<Void>> callable1 = () -> transactionCoordinatorClient
                .addPublishPartitionToTxnAsync(transaction1.getTxnID(),
                        Collections.singletonList("test"));
        Callable<CompletableFuture<Void>> callable2 = () -> transactionCoordinatorClient
                .addPublishPartitionToTxnAsync(transaction2.getTxnID(),
                        Collections.singletonList("test"));
        tryCommandAgainWhenTCNotReadyOrConnecting(callable1, callable2, this);
    }

    public void testAbortWhenTCNotReadyOrConnecting() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        Transaction transaction1 = pulsarClient
                .newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build()
                .get();
        Transaction transaction2 = pulsarClient
                .newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build()
                .get();
        Callable<CompletableFuture<Void>> callable1 = () -> transactionCoordinatorClient
                .abortAsync(transaction1.getTxnID());
        Callable<CompletableFuture<Void>> callable2 = () -> transactionCoordinatorClient
                .abortAsync(transaction2.getTxnID());
        tryCommandAgainWhenTCNotReadyOrConnecting(callable1, callable2, this);
    }

    public void testCommitWhenTCNotReadyOrConnecting() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        Transaction transaction1 = pulsarClient
                .newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build()
                .get();
        Transaction transaction2 = pulsarClient
                .newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build()
                .get();
        Callable<CompletableFuture<Void>> callable1 = () -> transactionCoordinatorClient
                .commitAsync(transaction1.getTxnID());
        Callable<CompletableFuture<Void>> callable2 = () -> transactionCoordinatorClient
                .commitAsync(transaction2.getTxnID());
        tryCommandAgainWhenTCNotReadyOrConnecting(callable1, callable2, this);
    }

    public void testAddSubscriptionWhenTCNotReadyOrConnecting() throws Exception {
        TransactionCoordinatorClientImpl transactionCoordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();
        Transaction transaction1 = pulsarClient
                .newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build()
                .get();
        Transaction transaction2 = pulsarClient
                .newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS)
                .build()
                .get();
        Callable<CompletableFuture<Void>> callable1 = () -> transactionCoordinatorClient
                .addSubscriptionToTxnAsync(transaction1.getTxnID(), TOPIC_OUTPUT,
                        "test" + RandomUtils.nextLong());
        Callable<CompletableFuture<Void>> callable2 = () -> transactionCoordinatorClient
                .addSubscriptionToTxnAsync(transaction2.getTxnID(), TOPIC_OUTPUT,
                        "test" + RandomUtils.nextLong());

        tryCommandAgainWhenTCNotReadyOrConnecting(callable1, callable2, this);

    }

    public static  <T>  void tryCommandAgainWhenTCNotReadyOrConnecting(Callable<CompletableFuture<T>> callable1,
                                                                       Callable<CompletableFuture<T>> callable2,
                                                                       TransactionRetryTest transactionEndToEndTest)
            throws Exception {
        PulsarClientImpl pulsarClient = (PulsarClientImpl) transactionEndToEndTest.pulsarClient;
        TransactionCoordinatorClientImpl transactionCoordinatorClient =  pulsarClient.getTcClient();
        Class<TransactionCoordinatorClientImpl> transactionCoordinatorClientClass =
                TransactionCoordinatorClientImpl.class;
        Field field = transactionCoordinatorClientClass.getDeclaredField("handlers");
        field.setAccessible(true);
        TransactionMetaStoreHandler[] handlers =
                (TransactionMetaStoreHandler[]) field.get(transactionCoordinatorClient);
        for (HandlerState handlerState : handlers) {
            changeHandlerState(HandlerState.State.Connecting, handlerState);
        }
        CompletableFuture<T> future = callable1.call();
        try {
            future.get(pulsarClient.conf.getOperationTimeoutMs() / 6,
                    TimeUnit.MILLISECONDS);
        } catch (TimeoutException ignored) {
        }
        for (HandlerState handlerState : handlers) {
            changeHandlerState(HandlerState.State.Ready, handlerState);
        }
        future.get(pulsarClient.conf.getOperationTimeoutMs() * 5 / 6,
                TimeUnit.MILLISECONDS);

        TransactionMetadataStoreService transactionMetadataStoreService =
                transactionEndToEndTest.getPulsarServiceList().get(0).getTransactionMetadataStoreService();
        Class<TransactionMetadataStoreService> transactionMetadataStoreServiceClass =
                TransactionMetadataStoreService.class;
        Field field1 =
                transactionMetadataStoreServiceClass.getDeclaredField("stores");
        field1.setAccessible(true);
        Map<TransactionCoordinatorID, TransactionMetadataStore> stores =
                (Map<TransactionCoordinatorID, TransactionMetadataStore>) field1.get(transactionMetadataStoreService);

        for (TransactionMetadataStore transactionMetadataStore : stores.values()) {
            changeMLTransactionMetadataStoreState(TransactionMetadataStoreState.State.Initializing,
                    (MLTransactionMetadataStore) transactionMetadataStore);
        }
        CompletableFuture<T> future1 = callable2.call();
        try {
            future1.get(pulsarClient.conf.getOperationTimeoutMs() / 6,
                    TimeUnit.MILLISECONDS);
        } catch (TimeoutException ignored) {
        }
        for (TransactionMetadataStore transactionMetadataStore : stores.values()) {
            changeMLTransactionMetadataStoreState(TransactionMetadataStoreState.State.Ready,
                    (MLTransactionMetadataStore) transactionMetadataStore);
        }
        future1.get(pulsarClient.conf.getOperationTimeoutMs() * 5 / 6,
                TimeUnit.MILLISECONDS);

    }

    public static void changeMLTransactionMetadataStoreState(TransactionMetadataStoreState.State state,
                                                             TransactionMetadataStoreState metadataStore)
            throws Exception {
        Class<TransactionMetadataStoreState> transactionMetadataStoreStateClass = TransactionMetadataStoreState.class;
        Field field = transactionMetadataStoreStateClass.getDeclaredField("state");
        field.setAccessible(true);
        field.set(metadataStore, state);
    }

    public static void changeHandlerState(HandlerState.State state, HandlerState handlerState) throws Exception{
        Class<HandlerState> handlerStateClass = HandlerState.class;
        Field field = handlerStateClass.getDeclaredField("state");
        field.setAccessible(true);
        field.set(handlerState, state);
    }

}
