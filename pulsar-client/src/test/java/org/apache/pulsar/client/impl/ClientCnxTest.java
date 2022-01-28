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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.BrokerMetadataException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.Test;

public class ClientCnxTest {

    @Test
    public void testClientCnxTimeout() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        conf.setKeepAliveIntervalSeconds(0);
        ClientCnx cnx = new ClientCnx(conf, eventLoop);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        ChannelFuture listenerFuture = mock(ChannelFuture.class);
        when(listenerFuture.addListener(any())).thenReturn(listenerFuture);
        when(ctx.writeAndFlush(any())).thenReturn(listenerFuture);

        cnx.channelActive(ctx);

        try {
            cnx.newLookup(null, 123).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
        }

        eventLoop.shutdownGracefully();
    }

    @Test
    public void testReceiveErrorAtSendConnectFrameState() throws Exception {
        ThreadFactory threadFactory = new DefaultThreadFactory("testReceiveErrorAtSendConnectFrameState");
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        ClientCnx cnx = new ClientCnx(conf, eventLoop);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);

        Field ctxField = PulsarHandler.class.getDeclaredField("ctx");
        ctxField.setAccessible(true);
        ctxField.set(cnx, ctx);

        // set connection as SentConnectFrame
        Field cnxField = ClientCnx.class.getDeclaredField("state");
        cnxField.setAccessible(true);
        cnxField.set(cnx, ClientCnx.State.SentConnectFrame);

        // receive error
        CommandError commandError = new CommandError()
            .setRequestId(-1)
            .setError(ServerError.AuthenticationError)
            .setMessage("authentication was failed");
        try {
            cnx.handleError(commandError);
        } catch (Exception e) {
            fail("should not throw any error");
        }

        eventLoop.shutdownGracefully();
    }

    @Test
    public void testGetLastMessageIdWithError() throws Exception {
        ThreadFactory threadFactory = new DefaultThreadFactory("testReceiveErrorAtSendConnectFrameState");
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, false, threadFactory);
        ClientConfigurationData conf = new ClientConfigurationData();
        ClientCnx cnx = new ClientCnx(conf, eventLoop);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);

        Field ctxField = PulsarHandler.class.getDeclaredField("ctx");
        ctxField.setAccessible(true);
        ctxField.set(cnx, ctx);

        final long requestId = 100;

        // set connection as SentConnectFrame
        Field cnxField = ClientCnx.class.getDeclaredField("state");
        cnxField.setAccessible(true);
        cnxField.set(cnx, ClientCnx.State.SentConnectFrame);

        ChannelFuture listenerFuture = mock(ChannelFuture.class);
        when(listenerFuture.addListener(any())).thenReturn(listenerFuture);
        when(ctx.writeAndFlush(any())).thenReturn(listenerFuture);

        ByteBuf getLastIdCmd = Commands.newGetLastMessageId(5, requestId);
        CompletableFuture<?> future = cnx.sendGetLastMessageId(getLastIdCmd, requestId);

        // receive error
        CommandError commandError = new CommandError()
            .setRequestId(requestId)
            .setError(ServerError.MetadataError)
            .setMessage("failed to read");
        cnx.handleError(commandError);

        try {
            future.get();
            fail("Should have failed");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), BrokerMetadataException.class);
        }

        eventLoop.shutdownGracefully();
    }
}
