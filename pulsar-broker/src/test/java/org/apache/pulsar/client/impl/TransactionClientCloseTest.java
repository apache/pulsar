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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.client.util.MathUtils;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
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

    @DataProvider
    public Object[][] requestSentBeforeClose() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "requestSentBeforeClose")
    public void testCloseDoesNotWaitForTransactionCallbacks(boolean sentBeforeClose) throws Exception {
        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .enableTransaction(true)
                .ioThreads(1)
                .build();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch executorBusy = new CountDownLatch(1);
        CountDownLatch releaseExecutor = new CountDownLatch(1);
        CompletableFuture<Void> closeFuture = null;
        CompletableFuture<Void> callback = null;
        try {
            Transaction firstTxn = client.newTransaction().build().get(10, TimeUnit.SECONDS);
            Transaction secondTxn = client.newTransaction().build().get(10, TimeUnit.SECONDS);
            List<TransactionMetaStoreHandler> handlers = new ArrayList<>(
                    ((TransactionCoordinatorClientImpl) client.getTransactionCoordinatorClient()).getHandlers());
            int availablePermits = handlers.stream().mapToInt(
                    TransactionMetaStoreHandler::getAvailableRequestPermits).sum();
            List<ByteBuf> sentRequests = new CopyOnWriteArrayList<>();
            // Use real coordinator requests over the real connection. Drop only END_TXN responses so the
            // requests stay in flight until close, as when a coordinator response is delayed or lost.
            for (CompletableFuture<ClientCnx> connection : client.getCnxPool().getConnections()) {
                ClientCnx cnx = connection.get(10, TimeUnit.SECONDS);
                cnx.channel().eventLoop().submit(() -> cnx.channel().pipeline().addBefore(
                        "handler", "hold-transaction-responses", new ChannelDuplexHandler() {
                            @Override
                            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                                    throws Exception {
                                if (msg instanceof ByteBuf buffer) {
                                    ByteBuf frame = buffer.duplicate();
                                    frame.skipBytes(Integer.BYTES);
                                    BaseCommand command = readCommand(frame);
                                    if (command.getType() == BaseCommand.Type.END_TXN) {
                                        sentRequests.add(buffer);
                                    }
                                }
                                super.write(ctx, msg, promise);
                            }

                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                if (msg instanceof ByteBuf buffer
                                        && readCommand(buffer.duplicate()).getType()
                                                == BaseCommand.Type.END_TXN_RESPONSE) {
                                    ReferenceCountUtil.release(msg);
                                } else {
                                    super.channelRead(ctx, msg);
                                }
                            }
                        })).get(10, TimeUnit.SECONDS);
            }
            CompletableFuture<Void> firstFuture = null;
            CompletableFuture<Void> secondFuture = null;
            if (sentBeforeClose) {
                firstFuture = firstTxn.commit();
                secondFuture = secondTxn.abort();
                Awaitility.await().untilAsserted(() -> {
                    assertThat(sentRequests).hasSize(2);
                    assertThat(sentRequests).allSatisfy(request -> assertThat(request.refCnt()).isEqualTo(1));
                });
            }
            // Keep the handler's executor busy until shutdownNow interrupts it. This forces close to run
            // the discarded cleanup tasks on the shutdown thread, without relying on a scheduling race.
            client.getInternalExecutorService().execute(() -> {
                executorBusy.countDown();
                try {
                    releaseExecutor.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(executorBusy.await(10, TimeUnit.SECONDS)).isTrue();
            if (!sentBeforeClose) {
                firstFuture = firstTxn.commit();
                secondFuture = secondTxn.abort();
            }
            assertThat(firstFuture).isNotDone();
            assertThat(secondFuture).isNotDone();
            callback = firstFuture.whenComplete((ignored, error) -> {
                callbackEntered.countDown();
                try {
                    assertThat(releaseCallback.await(30, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            closeFuture = client.closeAsync();

            assertThat(callbackEntered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(closeFuture).as("client close while a transaction callback is blocked; "
                            + "request references: %s, available permits: %s/%s",
                    sentRequests.stream().map(ByteBuf::refCnt).toList(),
                    handlers.stream().mapToInt(TransactionMetaStoreHandler::getAvailableRequestPermits).sum(),
                    availablePermits).succeedsWithin(TIMEOUT);
            assertThat(callback).as("callback is still blocked").isNotDone();
            Class<? extends Throwable> failure = sentBeforeClose ? AlreadyClosedException.class
                    : MetaStoreHandlerNotReadyException.class;
            assertFailsWith("commit", firstFuture, failure);
            assertFailsWith("abort", secondFuture, failure);
            assertThat(handlers).allSatisfy(handler -> assertThat(handler.getPendingRequestsCount()).isZero());
            assertThat(handlers.stream().mapToInt(TransactionMetaStoreHandler::getAvailableRequestPermits).sum())
                    .as("all request permits returned before the callback returns").isEqualTo(availablePermits);
            assertThat(sentRequests).allSatisfy(request -> assertThat(request.refCnt()).isZero());
        } finally {
            releaseCallback.countDown();
            releaseExecutor.countDown();
            if (closeFuture != null) {
                closeFuture.get(30, TimeUnit.SECONDS);
            } else {
                client.close();
            }
            if (callback != null) {
                callback.handle((ignored, error) -> null).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static BaseCommand readCommand(ByteBuf frame) {
        int commandSize = frame.readInt();
        BaseCommand command = new BaseCommand();
        command.parseFrom(frame, commandSize);
        return command;
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
