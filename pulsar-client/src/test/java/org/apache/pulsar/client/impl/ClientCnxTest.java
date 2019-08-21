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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.Test;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

public class ClientCnxTest {

    @Test
    public void testClientCnxTimeout() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        ClientCnx cnx = new ClientCnx(conf, eventLoop);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelFuture listenerFuture = mock(ChannelFuture.class);
        when(listenerFuture.addListener(any())).thenReturn(listenerFuture);
        when(ctx.writeAndFlush(any())).thenReturn(listenerFuture);

        Field ctxField = PulsarHandler.class.getDeclaredField("ctx");
        ctxField.setAccessible(true);
        ctxField.set(cnx, ctx);
        try {
            cnx.newLookup(null, 123).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
        }
    }

    @Test(timeOut = 10000)
    public void testSendTxnRequestWithId() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        ClientCnx clientCnx = new ClientCnx(conf, eventLoop);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelFuture listenerFuture = mock(ChannelFuture.class);
        doReturn(listenerFuture).when(ctx).writeAndFlush(any());
        doReturn(listenerFuture).when(listenerFuture).addListener(any());

        clientCnx.setCtx(ctx);

        clientCnx.sendTxnRequestToTBWithId(Unpooled.EMPTY_BUFFER, 1);

        verify(ctx, times(1)).writeAndFlush(eq(Unpooled.EMPTY_BUFFER));
        verify(listenerFuture, times(1)).addListener(any());
    }

    @Test(timeOut = 10000)
    public void testFailHandleEndTxnOnPartition() {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        ClientCnx clientCnx = new ClientCnx(conf, eventLoop);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelFuture listenerFuture = mock(ChannelFuture.class);
        doReturn(listenerFuture).when(ctx).writeAndFlush(any());
        doReturn(listenerFuture).when(listenerFuture).addListener(any());

        clientCnx.setState(ClientCnx.State.Ready);

        CompletableFuture<Void> future = new CompletableFuture<>();
        clientCnx.getPendingTxnRequest().put(1L, future);
        PulsarApi.CommandEndTxnOnPartitionResponse res =
            PulsarApi.CommandEndTxnOnPartitionResponse.newBuilder().setRequestId(1L).setError(PulsarApi.ServerError.UnknownError).setMessage("Unknown error").build();

        clientCnx.handleEndTxnOnPartitionResponse(res);
        try {
            future.get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException);
            assertEquals(e.getCause().getMessage(), "Unknown error");
        }
        res.recycle();
    }

    @Test(timeOut = 10000)
    public void testSuccessHandleEndTxnOnPartition() {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        ClientCnx clientCnx = new ClientCnx(conf, eventLoop);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelFuture listenerFuture = mock(ChannelFuture.class);
        doReturn(listenerFuture).when(ctx).writeAndFlush(any());
        doReturn(listenerFuture).when(listenerFuture).addListener(any());

        clientCnx.setState(ClientCnx.State.Ready);

        CompletableFuture<Void> future = new CompletableFuture<>();
        clientCnx.getPendingTxnRequest().put(1L, future);
        PulsarApi.CommandEndTxnOnPartitionResponse res =
            PulsarApi.CommandEndTxnOnPartitionResponse.newBuilder()
                                                      .setRequestId(1L)
                                                      .setTxnidLeastBits(1L)
                                                      .setTxnidMostBits(1L).build();

        clientCnx.handleEndTxnOnPartitionResponse(res);
        try {
            future.get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException);
        }
        res.recycle();
    }

}
