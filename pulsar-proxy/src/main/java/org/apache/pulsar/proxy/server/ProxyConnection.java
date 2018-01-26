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

import static com.google.common.base.Preconditions.checkArgument;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;

import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.PulsarHandler;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnect;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

/**
 * Handles incoming discovery request from client and sends appropriate response back to client
 *
 */
public class ProxyConnection extends PulsarHandler implements FutureListener<Void> {

    private ProxyService service;
    String clientAuthRole = null;
    private State state;

    private LookupProxyHandler lookupProxyHandler = null;
    private DirectProxyHandler directProxyHandler = null;

    enum State {
        Init,

        // Proxy the lookup requests to a random broker
        // Follow redirects
        ProxyLookupRequests,

        // If we are proxying a connection to a specific broker, we
        // are just forwarding data between the 2 connections, without
        // looking into it
        ProxyConnectionToBroker,

        Closed,
    }

    private static final Gauge activeConnections = Gauge
            .build("pulsar_proxy_active_connections", "Number of connections currently active in the proxy").create()
            .register();

    private static final Counter newConnections = Counter
            .build("pulsar_proxy_new_connections", "Counter of connections being opened in the proxy").create()
            .register();

    public ProxyConnection(ProxyService proxyService) {
        super(30, TimeUnit.SECONDS);
        this.service = proxyService;
        this.state = State.Init;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        activeConnections.inc();
        newConnections.inc();
        LOG.info("[{}] New connection opened", remoteAddress);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        activeConnections.dec();

        if (directProxyHandler != null && directProxyHandler.outboundChannel != null) {
            directProxyHandler.outboundChannel.close();
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        switch (state) {
        case Init:
        case ProxyLookupRequests:
            // Do the regular decoding for the Connected message
            super.channelRead(ctx, msg);
            break;

        case ProxyConnectionToBroker:
            // Pass the buffer to the outbound connection and schedule next read only
            // if we can write on the connection
            directProxyHandler.outboundChannel.writeAndFlush(msg).addListener(this);
            break;

        default:
            break;
        }
    }

    @Override
    public void operationComplete(Future<Void> future) throws Exception {
        // This is invoked when the write operation on the paired connection is completed
        if (future.isSuccess()) {
            ctx.read();
        } else {
            LOG.warn("[{}] Error in writing to inbound channel. Closing", remoteAddress, future.cause());
            directProxyHandler.outboundChannel.close();
        }
    }

    /**
     * handles connect request and sends {@code State.Connected} ack to client
     */
    @Override
    protected void handleConnect(CommandConnect connect) {
        checkArgument(state == State.Init);
        remoteEndpointProtocolVersion = connect.getProtocolVersion();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received CONNECT from {} proxyToBroker={}", remoteAddress,
                    connect.hasProxyToBrokerUrl() ? connect.getProxyToBrokerUrl() : "null");
        }

        // Client need to do some minimal cooperation logic.
        if (remoteEndpointProtocolVersion < PulsarApi.ProtocolVersion.v10_VALUE) {
            LOG.warn("[{}] Client doesn't support connecting through proxy", remoteAddress);
            ctx.close();
            return;
        }

        if (!verifyAuthenticationIfNeeded(connect)) {
            ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, "Failed to authenticate"));
            close();
            return;
        }

        if (connect.hasProxyToBrokerUrl()) {
            // Client already knows which broker to connect. Let's open a connection
            // there and just pass bytes in both directions
            state = State.ProxyConnectionToBroker;
            directProxyHandler = new DirectProxyHandler(service, this, connect.getProxyToBrokerUrl());
        } else {
            // Client is doing a lookup, we can consider the handshake complete and we'll take care of just topics and
            // partitions metadata lookups
            state = State.ProxyLookupRequests;
            lookupProxyHandler = new LookupProxyHandler(service, this);
            ctx.writeAndFlush(Commands.newConnected(connect.getProtocolVersion()));
        }
    }

    @Override
    protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata partitionMetadata) {
        checkArgument(state == State.ProxyLookupRequests);

        lookupProxyHandler.handlePartitionMetadataResponse(partitionMetadata);
    }

    /**
     * handles discovery request from client ands sends next active broker address
     */
    @Override
    protected void handleLookup(CommandLookupTopic lookup) {
        checkArgument(state == State.ProxyLookupRequests);
        lookupProxyHandler.handleLookup(lookup);
    }

    private void close() {
        state = State.Closed;
        ctx.close();
    }

    private boolean verifyAuthenticationIfNeeded(CommandConnect connect) {
        if (!service.getConfiguration().isAuthenticationEnabled()) {
            return true;
        }

        try {
            String authMethod = "none";
            if (connect.hasAuthMethodName()) {
                authMethod = connect.getAuthMethodName();
            } else if (connect.hasAuthMethod()) {
                // Legacy client is passing enum
                authMethod = connect.getAuthMethod().name().substring(10).toLowerCase();
            }
            String authData = connect.getAuthData().toStringUtf8();
            ChannelHandler sslHandler = ctx.channel().pipeline().get("tls");
            SSLSession sslSession = null;
            if (sslHandler != null) {
                sslSession = ((SslHandler) sslHandler).engine().getSession();
            }
            clientAuthRole = service.getAuthenticationService()
                    .authenticate(new AuthenticationDataCommand(authData, remoteAddress, sslSession), authMethod);
            LOG.info("[{}] Client successfully authenticated with {} role {}", remoteAddress, authMethod,
                    clientAuthRole);
            return true;
        } catch (AuthenticationException e) {
            LOG.warn("[{}] Unable to authenticate: {}", remoteAddress, e.getMessage());
            return false;
        }
    }

    @Override
    protected boolean isHandshakeCompleted() {
        return state != State.Init;
    }

    SocketAddress clientAddress() {
        return remoteAddress;
    }

    ChannelHandlerContext ctx() {
        return ctx;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ProxyConnection.class);

}
