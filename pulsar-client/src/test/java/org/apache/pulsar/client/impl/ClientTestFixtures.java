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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.util.Timer;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.mockito.Mockito;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClientTestFixtures {
    public static ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor();

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

        return clientMock;
    }

    static <T> PulsarClientImpl createPulsarClientMockWithMockedClientCnx(
            ExecutorProvider executorProvider,
            ExecutorService internalExecutorService) {
        return mockClientCnx(createPulsarClientMock(executorProvider, internalExecutorService));
    }

    static PulsarClientImpl mockClientCnx(PulsarClientImpl clientMock) {
        ClientCnx clientCnxMock = mock(ClientCnx.class, Mockito.RETURNS_DEEP_STUBS);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(clientMock.getCnxPool()).thenReturn(connectionPool);
        when(clientCnxMock.ctx()).thenReturn(mock(ChannelHandlerContext.class));
        when(clientCnxMock.sendRequestWithId(any(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(mock(ProducerResponse.class)));
        when(clientCnxMock.channel().remoteAddress()).thenReturn(mock(SocketAddress.class));
        when(clientMock.getConnection(anyString(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(clientCnxMock));
        when(clientMock.getConnection(anyString())).thenReturn(CompletableFuture.completedFuture(clientCnxMock));
        when(clientMock.getConnection(any(), any(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(clientCnxMock));
        return clientMock;
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
