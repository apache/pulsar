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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timer;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.SucceededFuture;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.mockito.Mockito;

class ClientTestFixtures {
    public static ScheduledExecutorService SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("ClientTestFixtures-SCHEDULER-%d")
                            .setDaemon(true)
                            .build());

//    static <T> PulsarClientImpl createPulsarClientMock() {
//        return createPulsarClientMock(mock(ExecutorService.class));
//    }

    static <T> PulsarClientImpl createPulsarClientMock(ExecutorProvider executorProvider,
                                                       ExecutorService internalExecutorService) {
        PulsarClientImpl clientMock = mock(PulsarClientImpl.class, Mockito.RETURNS_DEEP_STUBS);

        ClientConfigurationData clientConf = new ClientConfigurationData();
        when(clientMock.getConfiguration()).thenReturn(clientConf);
        when(clientMock.timer()).thenReturn(mock(Timer.class));

        when(clientMock.getInternalExecutorService()).thenReturn(internalExecutorService);
        when(clientMock.externalExecutorProvider()).thenReturn(executorProvider);
        when(clientMock.eventLoopGroup().next()).thenReturn(mock(EventLoop.class));
        when(clientMock.preProcessSchemaBeforeSubscribe(any(), any(), any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(invocation.getArgument(1)));

        return clientMock;
    }

    static <T> PulsarClientImpl createPulsarClientMockWithMockedClientCnx(
            ExecutorProvider executorProvider,
            ExecutorService internalExecutorService) {
        return mockClientCnx(createPulsarClientMock(executorProvider, internalExecutorService));
    }

    static PulsarClientImpl mockClientCnx(PulsarClientImpl clientMock) {
        ClientCnx clientCnxMock = mockClientCnx();
        when(clientMock.getConnection(any())).thenReturn(CompletableFuture.completedFuture(clientCnxMock));
        when(clientMock.getConnection(anyString())).thenReturn(CompletableFuture.completedFuture(clientCnxMock));
        when(clientMock.getConnection(anyString(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(Pair.of(clientCnxMock, false)));
        when(clientMock.getConnection(any(), any(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(clientCnxMock));
        ConnectionPool connectionPoolMock = mock(ConnectionPool.class);
        when(clientMock.getCnxPool()).thenReturn(connectionPoolMock);
        when(connectionPoolMock.getConnection(any(InetSocketAddress.class))).thenReturn(CompletableFuture.completedFuture(clientCnxMock));
        when(connectionPoolMock.getConnection(any(ServiceNameResolver.class))).thenReturn(CompletableFuture.completedFuture(clientCnxMock));
        when(connectionPoolMock.getConnection(any(), any(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(clientCnxMock));
        return clientMock;
    }

    public static ClientCnx mockClientCnx() {
        ClientCnx clientCnxMock = mock(ClientCnx.class, Mockito.RETURNS_DEEP_STUBS);
        ChannelHandlerContext ctx = mockChannelHandlerContext();
        doReturn(ctx).when(clientCnxMock).ctx();
        doAnswer(invocation -> {
            ByteBuf buf = invocation.getArgument(0);
            buf.release();
            return CompletableFuture.completedFuture(mock(ProducerResponse.class));
        }).when(clientCnxMock).sendRequestWithId(any(), anyLong());
        when(clientCnxMock.channel().remoteAddress()).thenReturn(mock(SocketAddress.class));
        return clientCnxMock;
    }

    /**
     * Mock a ChannelHandlerContext where write and writeAndFlush are always successful.
     * This might not be suitable for all tests.
     *
     * @return a mocked ChannelHandlerContext
     */
    public static ChannelHandlerContext mockChannelHandlerContext() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        // return an empty channel mock from ctx.channel()
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);

        // handle write and writeAndFlush methods so that the input message is released

        // create a listener future that is returned from write and writeAndFlush
        // that immediately completes the listener in the calling thread as if it was successful
        ChannelFuture listenerFuture = mock(ChannelFuture.class);
        Future<Void> succeededFuture = createSucceededFuture();
        doAnswer(invocation -> {
            GenericFutureListener<Future<Void>> listener = invocation.getArgument(0);
            listener.operationComplete(succeededFuture);
            return listenerFuture;
        }).when(listenerFuture).addListener(any());

        // handle write and writeAndFlush methods so that the input message is released
        doAnswer(invocation -> {
            Object msg = invocation.getArgument(0);
            ReferenceCountUtil.release(msg);
            return listenerFuture;
        }).when(ctx).write(any(), any());
        doAnswer(invocation -> {
            Object msg = invocation.getArgument(0);
            ReferenceCountUtil.release(msg);
            return listenerFuture;
        }).when(ctx).writeAndFlush(any(), any());
        doAnswer(invocation -> {
            Object msg = invocation.getArgument(0);
            ReferenceCountUtil.release(msg);
            return listenerFuture;
        }).when(ctx).writeAndFlush(any());

        return ctx;
    }

    public static Future<Void> createSucceededFuture() {
        EventExecutor eventExecutor = mockEventExecutor();
        // create a succeeded future that is returned from the listener, listeners will run in the calling thread
        // using the mocked EventExecutor
        SucceededFuture<Void> succeededFuture = new SucceededFuture<>(eventExecutor, null);
        return succeededFuture;
    }

    public static EventExecutor mockEventExecutor() {
        // mock an EventExecutor that runs the listener in the calling thread
        EventExecutor eventExecutor = mock(EventExecutor.class);
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(eventExecutor).execute(any(Runnable.class));
        return eventExecutor;
    }

    static <T> CompletableFuture<T> createDelayedCompletedFuture(T result, int delayMillis) {
        CompletableFuture<T> future = new CompletableFuture<>();
        SCHEDULER.schedule(() -> future.complete(result), delayMillis, TimeUnit.MILLISECONDS);
        return future;
    }

    static <T> CompletableFuture<T> createExceptionFuture(Throwable ex, int delayMillis) {
        CompletableFuture<T> future = new CompletableFuture<>();
        SCHEDULER.schedule(() -> future.completeExceptionally(ex), delayMillis, TimeUnit.MILLISECONDS);
        return future;
    }

    public static ExecutorService createMockedExecutor() {
        return mock(ExecutorService.class);
    }

    public static ExecutorProvider createMockedExecutorProvider() {
        return mock(ExecutorProvider.class);
    }
}
