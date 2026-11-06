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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class TransactionMetaStoreHandlerTest {

    @Test
    public void testStateChangeFailure() throws Exception {
        final PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650").build();
        final CompletableFuture<Void> connectFuture = new CompletableFuture<>();
        final TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                0L, client, "topic", connectFuture);
        final ClientCnx cnx = mock(ClientCnx.class);
        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(19);
        answerConnectRequests(cnx);

        final Channel channel = mock(Channel.class);
        when(cnx.channel()).thenReturn(channel);

        // Set an invalid state so that the state change will fail
        handler.setState(HandlerState.State.Terminated);
        handler.connectionOpened(cnx);

        Awaitility.await().atMost(Duration.ofSeconds(3)).until(connectFuture::isDone);

        assertTrue(connectFuture.isCompletedExceptionally());
        assertEquals(handler.getState(), HandlerState.State.Terminated);
        verify(cnx, times(0)).registerTransactionMetaStoreHandler(anyLong(), any());

        client.close();
    }

    @Test
    public void testFailedResendOnReconnectRemovesPendingRequest() throws Exception {
        @Cleanup
        final PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650").build();
        final CompletableFuture<Void> connectFuture = new CompletableFuture<>();
        final TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                0L, client, "topic", connectFuture);

        // Not connected yet: the request stays queued until the connection is opened.
        final CompletableFuture<TxnID> txnFuture = handler.newTransactionAsync(1, TimeUnit.MINUTES);
        Awaitility.await().untilAsserted(() -> assertEquals(handler.getPendingRequestsCount(), 1));

        final ClientCnx cnx = mock(ClientCnx.class);
        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(19);
        answerConnectRequests(cnx);
        when(cnx.channel()).thenReturn(mock(Channel.class));
        // The handler is closed concurrently after it switched to Ready, but before it resends the pending
        // requests, so resending them fails.
        doAnswer(invocation -> {
            handler.close();
            return null;
        }).when(cnx).registerTransactionMetaStoreHandler(anyLong(), any());

        handler.connectionOpened(cnx).get(10, TimeUnit.SECONDS);

        assertThat(txnFuture).failsWithin(Duration.ofSeconds(10))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException.class);
        // The failed request has been completed and its op recycled, so it must not stay pending.
        assertEquals(handler.getPendingRequestsCount(), 0);
    }

    @Test
    public void testCloseFailsAndReleasesPendingRequests() throws Exception {
        @Cleanup
        final PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650").build();
        final TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                0L, client, "topic", new CompletableFuture<>());
        final List<ByteBuf> sentRequests = connectToSilentCoordinator(handler);
        final CompletableFuture<Void> commitFuture = handler.endTxnAsync(new TxnID(1, 1), TxnAction.COMMIT);
        final ByteBuf request = awaitFirstSentRequest(sentRequests);
        assertThat(request.refCnt()).as("reference held by the pending request").isEqualTo(1);
        assertThat(commitFuture).isNotDone();

        handler.close();

        assertThat(commitFuture).failsWithin(Duration.ofSeconds(10))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarClientException.AlreadyClosedException.class);
        awaitReleased(handler, request);
    }

    @Test
    public void testCloseReleasesCancelledPendingRequests() throws Exception {
        @Cleanup
        final PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650").build();
        final TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                0L, client, "topic", new CompletableFuture<>());
        final List<ByteBuf> sentRequests = connectToSilentCoordinator(handler);
        final CompletableFuture<Void> commitFuture = handler.endTxnAsync(new TxnID(1, 1), TxnAction.COMMIT);
        final ByteBuf request = awaitFirstSentRequest(sentRequests);
        assertThat(request.refCnt()).as("reference held by the pending request").isEqualTo(1);
        // The caller gives up on the commit, whose request stays pending until its response, timeout or close.
        commitFuture.cancel(false);

        handler.close();

        awaitReleased(handler, request);
    }

    @Test
    public void testTimeoutReleasesCancelledPendingRequests() throws Exception {
        @Cleanup
        final PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .operationTimeout(1, TimeUnit.SECONDS)
                .build();
        final TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                0L, client, "topic", new CompletableFuture<>());
        final List<ByteBuf> sentRequests = connectToSilentCoordinator(handler);
        final CompletableFuture<Void> commitFuture = handler.endTxnAsync(new TxnID(1, 1), TxnAction.COMMIT);
        // The caller gives up on the commit right away, long before its request times out.
        commitFuture.cancel(false);
        final ByteBuf request = awaitFirstSentRequest(sentRequests);

        awaitReleased(handler, request);
    }

    @Test
    public void testClientCloseFailsPendingRequestsQueuedBehindBusyExecutor() throws Exception {
        // A single internal executor, so the task below occupies the handler's.
        final PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .ioThreads(1)
                .build();
        final TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                0L, client, "topic", new CompletableFuture<>());
        final List<ByteBuf> sentRequests = connectToSilentCoordinator(handler);
        final CompletableFuture<Void> commitFuture = handler.endTxnAsync(new TxnID(1, 1), TxnAction.COMMIT);
        final ByteBuf request = awaitFirstSentRequest(sentRequests);
        assertThat(request.refCnt()).as("reference held by the pending request").isEqualTo(1);
        // Occupy the executor until the client's shutdown interrupts it, so failing the pending request on close is
        // still queued when the shutdown stops the executor.
        occupyInternalExecutorUntilShutdown(client);

        handler.close();
        client.close();

        assertThat(commitFuture).failsWithin(Duration.ofSeconds(10))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarClientException.AlreadyClosedException.class);
        awaitReleased(handler, request);
    }

    @Test
    public void testClientCloseFailsRequestsQueuedBeforeBeingSent() throws Exception {
        // A single internal executor, so the helper below occupies the handler's.
        final PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .ioThreads(1)
                .build();
        final TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                0L, client, "topic", new CompletableFuture<>());
        final List<ByteBuf> sentRequests = connectToSilentCoordinator(handler);
        final int availablePermits = handler.getAvailableRequestPermits();
        // Occupy the executor until the client's shutdown interrupts it, so the commit below is still queued, neither
        // pending nor sent, when the shutdown stops the executor.
        occupyInternalExecutorUntilShutdown(client);

        final CompletableFuture<Void> commitFuture = handler.endTxnAsync(new TxnID(1, 1), TxnAction.COMMIT);
        handler.close();
        client.close();

        assertThat(commitFuture).failsWithin(Duration.ofSeconds(10))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(TransactionCoordinatorClientException.MetaStoreHandlerNotReadyException.class);
        assertThat(sentRequests).as("sent requests").isEmpty();
        // The op released its permit, and its buffer with it.
        assertThat(handler.getAvailableRequestPermits()).isEqualTo(availablePermits);
        assertThat(handler.getPendingRequestsCount()).isZero();
    }

    // Occupies the client's internal executor until the client's shutdown interrupts it.
    private static void occupyInternalExecutorUntilShutdown(PulsarClientImpl client) throws InterruptedException {
        final CountDownLatch executorBusy = new CountDownLatch(1);
        client.getInternalExecutorService().execute(() -> {
            executorBusy.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(executorBusy.await(10, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * Connects the handler to a mocked connection whose coordinator never answers, and returns the requests the
     * handler sends on it. The reference retained for each write is released, as the connection would once the
     * request is flushed.
     */
    private static List<ByteBuf> connectToSilentCoordinator(TransactionMetaStoreHandler handler) throws Exception {
        final ClientCnx cnx = mock(ClientCnx.class);
        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(19);
        answerConnectRequests(cnx);
        when(cnx.channel()).thenReturn(mock(Channel.class));
        final List<ByteBuf> sentRequests = new CopyOnWriteArrayList<>();
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.writeAndFlush(any(), any())).thenAnswer(invocation -> {
            final ByteBuf request = invocation.getArgument(0);
            request.release();
            sentRequests.add(request);
            return null;
        });
        when(cnx.ctx()).thenReturn(ctx);
        handler.connectionOpened(cnx).get(10, TimeUnit.SECONDS);
        return sentRequests;
    }

    private static void answerConnectRequests(ClientCnx cnx) {
        doAnswer(invocation -> {
            // Release the TC connect request, as the connection would once it is written.
            ReferenceCountUtil.release(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        }).when(cnx).sendRequestWithId(any(), anyLong());
    }

    private static ByteBuf awaitFirstSentRequest(List<ByteBuf> sentRequests) {
        Awaitility.await().untilAsserted(() -> assertThat(sentRequests).hasSize(1));
        return sentRequests.get(0);
    }

    private static void awaitReleased(TransactionMetaStoreHandler handler, ByteBuf request) {
        Awaitility.await().untilAsserted(() -> {
            assertThat(request.refCnt()).as("reference held by the pending request").isZero();
            assertThat(handler.getPendingRequestsCount()).isZero();
        });
    }
}
