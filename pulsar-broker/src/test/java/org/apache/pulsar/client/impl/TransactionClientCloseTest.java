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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.CoordinatorClientStateException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.client.util.MathUtils;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.protocol.Commands;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class TransactionClientCloseTest extends SharedPulsarBaseTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    public void testEndTransactionAfterClientClose() throws Exception {
        @Cleanup
        PulsarClient client = newTransactionClient();
        Transaction txnToCommit = client.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();
        Transaction txnToAbort = client.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();

        client.close();

        // Ending a transaction after its client closed must fail instead of never completing.
        assertFailsWithStateException("commit", txnToCommit.commit());
        assertFailsWithStateException("abort", txnToAbort.abort());
    }

    @Test
    public void testCoordinatorRequestsFailAfterClientClose() throws Exception {
        @Cleanup
        PulsarClient client = newTransactionClient();
        TransactionCoordinatorClient tcClient = client.getTransactionCoordinatorClient();

        client.close();

        assertFailsWithStateException("newTransaction", client.newTransaction().build());
        assertRequestsFail(tcClient);
        assertThat(tcClient.getState()).as("coordinator client state")
                .isEqualTo(TransactionCoordinatorClient.State.CLOSED);
    }

    @Test
    public void testCoordinatorRequestsFailBeforeStart() {
        assertRequestsFail(new TransactionCoordinatorClientImpl(pulsarClient));
    }

    @Test
    public void testNewTransactionWhenClientClosesDuringCoordinatorSelection() throws Exception {
        @Cleanup
        PulsarClient client = newTransactionClient();
        AtomicBoolean closed = new AtomicBoolean();
        CompletableFuture<Transaction> txnFuture;
        // The request found the client ready; the close clears the coordinator handlers while it picks one.
        try (MockedStatic<MathUtils> ignored = mockStatic(MathUtils.class, invocation -> {
            if (invocation.getMethod().getName().equals("signSafeMod") && closed.compareAndSet(false, true)) {
                client.close();
            }
            return invocation.callRealMethod();
        })) {
            txnFuture = client.newTransaction().build();
        }

        assertThat(closed).as("closed while picking the coordinator").isTrue();
        assertFailsWith("newTransaction", txnFuture, AlreadyClosedException.class);
    }

    @Test
    public void testCommitWhenClientClosesBeforeTheRequestIsSent() throws Exception {
        @Cleanup
        PulsarClient client = newTransactionClient();
        Transaction txn = client.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();
        AtomicReference<BaseCommand> endTxnCommand = new AtomicReference<>();
        AtomicReference<ByteBuf> endTxnRequest = new AtomicReference<>();
        CompletableFuture<Void> commitFuture;
        // The commit found the client ready and picked its coordinator handler; the close stops the executor that
        // sends the request while the handler builds it.
        try (MockedStatic<Commands> ignored = mockStatic(Commands.class, invocation -> {
            String method = invocation.getMethod().getName();
            if (method.equals("newEndTxn") && endTxnCommand.get() == null) {
                client.close();
            }
            Object result = invocation.callRealMethod();
            if (method.equals("newEndTxn")) {
                endTxnCommand.compareAndSet(null, (BaseCommand) result);
            } else if (method.equals("serializeWithSize") && invocation.getArgument(0) == endTxnCommand.get()) {
                endTxnRequest.compareAndSet(null, (ByteBuf) result);
            }
            return result;
        })) {
            commitFuture = txn.commit();
        }

        assertFailsWith("commit", commitFuture, AlreadyClosedException.class);
        assertThat(endTxnRequest.get()).as("END_TXN request").isNotNull();
        assertThat(endTxnRequest.get().refCnt()).as("END_TXN request reference count").isZero();
    }

    private PulsarClient newTransactionClient() throws Exception {
        return PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .enableTransaction(true)
                .build();
    }

    private static void assertRequestsFail(TransactionCoordinatorClient tcClient) {
        TxnID txnId = new TxnID(0, 0);
        assertFailsWithStateException("newTransactionAsync", tcClient.newTransactionAsync());
        assertFailsWithStateException("addPublishPartitionToTxnAsync",
                tcClient.addPublishPartitionToTxnAsync(txnId, List.of("topic")));
        assertFailsWithStateException("addSubscriptionToTxnAsync",
                tcClient.addSubscriptionToTxnAsync(txnId, "topic", "subscription"));
        assertFailsWithStateException("commitAsync", tcClient.commitAsync(txnId));
        assertFailsWithStateException("abortAsync", tcClient.abortAsync(txnId));
    }

    private static void assertFailsWithStateException(String operation, CompletableFuture<?> future) {
        assertFailsWith(operation, future, CoordinatorClientStateException.class);
    }

    private static void assertFailsWith(String operation, CompletableFuture<?> future,
                                        Class<? extends Throwable> cause) {
        assertThat(future).as(operation)
                .failsWithin(TIMEOUT)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(cause);
    }
}
