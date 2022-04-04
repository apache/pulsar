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
package org.apache.pulsar.proxy.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.mockito.Mockito;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyClientCnxTest {

    private EventLoopGroup eventLoop;

    @BeforeTest
    public void setup() {
        eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("ProxyClientCnxTest"));
    }

    @AfterTest
    public void cleanup() {
        eventLoop.shutdownGracefully();
    }

    @Test
    public void shouldCloseConnection() throws Exception {
        Supplier<CompletableFuture<AuthData>> authDataSupplier = () -> failed(new RuntimeException("Error"));
        ProxyClientCnx proxyClientCnx = createProxyCnx(authDataSupplier);
        ChannelHandlerContext ctx = createCtx();

        proxyClientCnx.channelActive(ctx);

        verify(ctx).close();
    }

    @Test
    public void shouldSendConnectCommand() throws Exception {
        AuthData clientAuthData = generateAuthData();
        Supplier<CompletableFuture<AuthData>> authDataSupplier = () -> CompletableFuture.completedFuture(clientAuthData);
        ProxyClientCnx proxyClientCnx = createProxyCnx(authDataSupplier);
        ChannelHandlerContext ctx = createCtx();

        proxyClientCnx.channelActive(ctx);

        String authMethodName = AuthenticationDisabled.INSTANCE.getAuthMethodName();
        final ByteBuf command = Commands.newConnect(authMethodName, AuthData.of(new byte[0]),
            proxyClientCnx.protocolVersion,
            PulsarVersion.getVersion(), null, proxyClientCnx.clientAuthRole, clientAuthData,
            proxyClientCnx.clientAuthMethod);

        verify(ctx).writeAndFlush(Mockito.eq(command));
    }

    @Test
    public void shouldCloseConnectionIfRefreshTokenError() throws Exception {
        @SuppressWarnings("unchecked")
        Supplier<CompletableFuture<AuthData>> authDataSupplier = mock(Supplier.class);
        ProxyClientCnx proxyClientCnx = createProxyCnx(authDataSupplier);
        ChannelHandlerContext ctx = createCtx();

        AuthData authData = generateAuthData();
        when(authDataSupplier.get()).thenReturn(CompletableFuture.completedFuture(authData));

        proxyClientCnx.channelActive(ctx);
        verify(ctx, never()).close();

        clearInvocations(ctx);

        CommandAuthChallenge command = new CommandAuthChallenge()
            .setProtocolVersion(Commands.getCurrentProtocolVersion());
        command.setChallenge()
            .setAuthData(AuthData.REFRESH_AUTH_DATA.getBytes())
            .setAuthMethodName("token");

        when(authDataSupplier.get()).thenReturn(failed(new RuntimeException("Client auth data error")));
        proxyClientCnx.handleAuthChallenge(command);

        verify(ctx).close();
    }

    @Test
    public void shouldSendRefreshedToken() throws Exception {
        @SuppressWarnings("unchecked")
        Supplier<CompletableFuture<AuthData>> authDataSupplier = mock(Supplier.class);
        ProxyClientCnx proxyClientCnx = createProxyCnx(authDataSupplier);
        ChannelHandlerContext ctx = createCtx();

        AuthData authData = generateAuthData();
        when(authDataSupplier.get()).thenReturn(CompletableFuture.completedFuture(authData));

        proxyClientCnx.channelActive(ctx);
        verify(ctx, never()).close();

        CommandAuthChallenge command = new CommandAuthChallenge()
            .setProtocolVersion(Commands.getCurrentProtocolVersion());
        command.setChallenge()
            .setAuthData(AuthData.REFRESH_AUTH_DATA.getBytes())
            .setAuthMethodName("token");

        clearInvocations(ctx);

        authData = generateAuthData();
        when(authDataSupplier.get()).thenReturn(CompletableFuture.completedFuture(authData));
        proxyClientCnx.handleAuthChallenge(command);

        ByteBuf response = Commands.newAuthResponse(
            proxyClientCnx.clientAuthMethod,
            authData,
            proxyClientCnx.protocolVersion,
            PulsarVersion.getVersion()
        );
        verify(ctx).writeAndFlush(Mockito.eq(response));
    }

    private ChannelHandlerContext createCtx() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        ChannelFuture listenerFuture = mock(ChannelFuture.class);
        when(listenerFuture.addListener(any())).thenReturn(listenerFuture);
        when(ctx.writeAndFlush(any())).thenReturn(listenerFuture);
        return ctx;
    }

    private ProxyClientCnx createProxyCnx(Supplier<CompletableFuture<AuthData>> authDataSupplier) {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setKeepAliveIntervalSeconds(0);

        return new ProxyClientCnx(conf, eventLoop, "client-role", authDataSupplier,
            "auth-method", Commands.getCurrentProtocolVersion(), true
        );
    }

    public AuthData generateAuthData() {
        byte[] bytes = new byte[10];
        new Random().nextBytes(bytes);
        return AuthData.of(bytes);
    }

    public <R> CompletableFuture<R> failed(Throwable error) {
        CompletableFuture<R> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }
}