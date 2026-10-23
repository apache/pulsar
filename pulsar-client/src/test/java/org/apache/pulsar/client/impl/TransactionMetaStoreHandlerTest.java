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
        final CompletableFuture<ProducerResponse> responseFuture = CompletableFuture.completedFuture(null);
        when(cnx.sendRequestWithId(any(), anyLong())).thenReturn(responseFuture);

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
        when(cnx.sendRequestWithId(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
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

        final ClientCnx cnx = mock(ClientCnx.class);
        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(19);
        doAnswer(invocation -> {
            // Release the TC connect request, as the connection would once it is written.
            ReferenceCountUtil.release(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        }).when(cnx).sendRequestWithId(any(), anyLong());
        when(cnx.channel()).thenReturn(mock(Channel.class));
        // Capture the requests sent to the coordinator, which never answers them, and release the reference
        // retained for the write, as the connection would once the request is flushed.
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

        final CompletableFuture<Void> commitFuture = handler.endTxnAsync(new TxnID(1, 1), TxnAction.COMMIT);
        Awaitility.await().untilAsserted(() -> assertThat(sentRequests).hasSize(1));
        final ByteBuf request = sentRequests.get(0);
        assertThat(commitFuture).isNotDone();
        assertThat(request.refCnt()).as("reference held by the pending request").isEqualTo(1);

        handler.close();

        assertThat(commitFuture).failsWithin(Duration.ofSeconds(10))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarClientException.AlreadyClosedException.class);
        Awaitility.await().untilAsserted(() -> {
            assertThat(request.refCnt()).as("reference held by the pending request").isZero();
            assertThat(handler.getPendingRequestsCount()).isZero();
        });
    }
}
