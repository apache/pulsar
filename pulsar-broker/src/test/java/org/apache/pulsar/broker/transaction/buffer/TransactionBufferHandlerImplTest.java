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
package org.apache.pulsar.broker.transaction.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.TimerTask;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferHandlerImpl;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartitionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscriptionResponse;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionBufferHandlerImplTest {

    @Test
    public void testRequestCredits() throws PulsarServerException {
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(pulsarClient.getCnxPool()).thenReturn(connectionPool);
        PulsarService pulsarService = mock(PulsarService.class);
        NamespaceService namespaceService = mock(NamespaceService.class);
        when(pulsarService.getNamespaceService()).thenReturn(namespaceService);
        when(pulsarService.getClient()).thenReturn(pulsarClient);
        when(namespaceService.getBundleAsync(any())).thenReturn(
                CompletableFuture.completedFuture(mock(NamespaceBundle.class)));
        Optional<NamespaceEphemeralData> opData = Optional.empty();
        when(namespaceService.getOwnerAsync(any())).thenReturn(CompletableFuture.completedFuture(opData));
        when(pulsarClient.getConnection(anyString(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(Pair.of(mock(ClientCnx.class), false)));
        when(pulsarClient.getConnection(anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(ClientCnx.class)));
        TransactionBufferHandlerImpl handler = spy(new TransactionBufferHandlerImpl(pulsarService, null, 1000, 3000));
        doNothing().when(handler).endTxn(any());
        doReturn(CompletableFuture.completedFuture(mock(ClientCnx.class))).when(handler).getClientCnx(anyString());
        for (int i = 0; i < 500; i++) {
            handler.endTxnOnTopic("public/default/t", 1L, 1L, TxnAction.COMMIT, 1L);
        }
        assertEquals(handler.getAvailableRequestCredits(), 500);
        for (int i = 0; i < 500; i++) {
            handler.endTxnOnTopic("public/default/t", 1L, 1L, TxnAction.COMMIT, 1L);
        }
        assertEquals(handler.getAvailableRequestCredits(), 0);
        handler.endTxnOnTopic("public/default/t", 1L, 1L, TxnAction.COMMIT, 1L);
        assertEquals(handler.getPendingRequestsCount(), 1);
        handler.onResponse(null);
        assertEquals(handler.getAvailableRequestCredits(), 0);
        assertEquals(handler.getPendingRequestsCount(), 0);
    }

    @Test
    public void testRequestCreditIsBackWhenRequestCompletes() throws Exception {
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        when(pulsarClient.getCnxPool()).thenReturn(mock(ConnectionPool.class));
        PulsarService pulsarService = mock(PulsarService.class);
        when(pulsarService.getClient()).thenReturn(pulsarClient);
        Channel channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        // Release the command, as writing it would
        when(ctx.writeAndFlush(any(), any())).thenAnswer(invocation -> {
            ReferenceCountUtil.release(invocation.getArgument(0));
            return null;
        });
        ClientCnx clientCnx = mock(ClientCnx.class);
        when(clientCnx.ctx()).thenReturn(ctx);
        HashedWheelTimer timer = mock(HashedWheelTimer.class);
        AtomicReference<TimerTask> timeoutTask = new AtomicReference<>();
        when(timer.newTimeout(any(), anyLong(), any())).thenAnswer(invocation -> {
            timeoutTask.set(invocation.getArgument(0));
            return null;
        });
        TransactionBufferHandlerImpl handler = spy(new TransactionBufferHandlerImpl(pulsarService, timer, 1000, 3000));
        doReturn(CompletableFuture.completedFuture(clientCnx)).when(handler).getClientCnx(anyString());

        // The test thread completes each request, so each callback reads the credits as its request completes.
        // The handler numbers its requests from 0.
        CompletableFuture<Integer> onTopicResponse =
                creditsOnCompletion(handler, handler.endTxnOnTopic("t", 1L, 1L, TxnAction.COMMIT, 1L));
        handler.handleEndTxnOnTopicResponse(0L, new CommandEndTxnOnPartitionResponse().setRequestId(0L));

        CompletableFuture<Integer> onSubscriptionErrorResponse =
                creditsOnCompletion(handler, handler.endTxnOnSubscription("t", "s", 1L, 1L, TxnAction.COMMIT, 1L));
        handler.handleEndTxnOnSubscriptionResponse(1L, new CommandEndTxnOnSubscriptionResponse().setRequestId(1L)
                .setError(ServerError.ServiceNotReady).setMessage("not ready"));

        CompletableFuture<Integer> onTimeout =
                creditsOnCompletion(handler, handler.endTxnOnTopic("t", 1L, 1L, TxnAction.COMMIT, 1L));
        timeoutTask.get().run(null);

        CompletableFuture<ClientCnx> inactiveCnx = new CompletableFuture<>();
        doReturn(inactiveCnx).when(handler).getClientCnx(anyString());
        CompletableFuture<Integer> onInactiveChannel =
                creditsOnCompletion(handler, handler.endTxnOnTopic("t", 1L, 1L, TxnAction.COMMIT, 1L));
        when(channel.isActive()).thenReturn(false);
        inactiveCnx.complete(clientCnx);

        CompletableFuture<ClientCnx> failedCnx = new CompletableFuture<>();
        doReturn(failedCnx).when(handler).getClientCnx(anyString());
        CompletableFuture<Integer> onConnectionFailure =
                creditsOnCompletion(handler, handler.endTxnOnTopic("t", 1L, 1L, TxnAction.COMMIT, 1L));
        failedCnx.completeExceptionally(new PulsarClientException.ConnectException("connection closed"));

        assertThat(List.of(onTopicResponse.get(), onSubscriptionErrorResponse.get(), onTimeout.get(),
                onInactiveChannel.get(), onConnectionFailure.get())).containsExactly(1000, 1000, 1000, 1000, 1000);
    }

    private static CompletableFuture<Integer> creditsOnCompletion(TransactionBufferHandlerImpl handler,
                                                                  CompletableFuture<TxnID> request) {
        return request.handle((__, ___) -> handler.getAvailableRequestCredits());
    }

    @Test
    public void testMinRequestCredits() throws PulsarServerException {
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        when(pulsarClient.getCnxPool()).thenReturn(connectionPool);
        PulsarService pulsarService = mock(PulsarService.class);
        when(pulsarService.getClient()).thenReturn(pulsarClient);
        TransactionBufferHandlerImpl handler = spy(new TransactionBufferHandlerImpl(pulsarService, null, 50, 3000));
        assertEquals(handler.getAvailableRequestCredits(), 100);
    }
}
