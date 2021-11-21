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

import java.util.function.Supplier;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;

import io.netty.handler.codec.haproxy.HAProxyMessage;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarChannelInitializer;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.api.proto.CommandAuthResponse;
import org.apache.pulsar.common.api.proto.CommandConnect;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandGetSchema;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.ServerError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import lombok.Getter;

/**
 * Handles incoming discovery request from client and sends appropriate response back to client
 *
 */
public class ProxyConnection extends PulsarHandler implements FutureListener<Void> {
    // ConnectionPool is used by the proxy to issue lookup requests
    private PulsarClientImpl client;
    private ConnectionPool connectionPool;
    private ProxyService service;
    AuthenticationDataSource authenticationData;
    private State state;
    private final Supplier<SslHandler> sslHandlerSupplier;

    private LookupProxyHandler lookupProxyHandler = null;
    @Getter
    private DirectProxyHandler directProxyHandler = null;
    String clientAuthRole;
    AuthData clientAuthData;
    String clientAuthMethod;

    private String authMethod = "none";
    AuthenticationProvider authenticationProvider;
    AuthenticationState authState;
    private ClientConfigurationData clientConf;
    private boolean hasProxyToBrokerUrl;
    private int protocolVersionToAdvertise;
    private String proxyToBrokerUrl;
    private HAProxyMessage haProxyMessage;

    private static final byte[] EMPTY_CREDENTIALS = new byte[0];

    enum State {
        Init,

        // Connecting between user client and proxy server.
        // Mutual authn needs verify between client and proxy server several times.
        Connecting,

        // Proxy the lookup requests to a random broker
        // Follow redirects
        ProxyLookupRequests,

        // If we are proxying a connection to a specific broker, we
        // are just forwarding data between the 2 connections, without
        // looking into it
        ProxyConnectionToBroker,

        Closed,
    }

    ConnectionPool getConnectionPool() {
        return client.getCnxPool();
    }

    public ProxyConnection(ProxyService proxyService, Supplier<SslHandler> sslHandlerSupplier) {
        super(30, TimeUnit.SECONDS);
        this.service = proxyService;
        this.state = State.Init;
        this.sslHandlerSupplier = sslHandlerSupplier;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        ProxyService.activeConnections.inc();
        if (ProxyService.activeConnections.get() > service.getConfiguration().getMaxConcurrentInboundConnections()) {
            ctx.close();
            ProxyService.rejectedConnections.inc();
            return;
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        ProxyService.activeConnections.dec();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        ProxyService.newConnections.inc();
        service.getClientCnxs().add(this);
        LOG.info("[{}] New connection opened", remoteAddress);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

        if (directProxyHandler != null && directProxyHandler.outboundChannel != null) {
            directProxyHandler.outboundChannel.close();
        }

        if (client != null) {
            client.close();
        }
        service.getClientCnxs().remove(this);
        LOG.info("[{}] Connection closed", remoteAddress);

        if (connectionPool != null) {
            try {
                connectionPool.close();
            } catch (Exception e) {
                LOG.error("Failed to close connection pool {}", e.getMessage(), e);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        LOG.warn("[{}] Got exception {} : {} {}", remoteAddress, cause.getClass().getSimpleName(), cause.getMessage(),
                ClientCnx.isKnownException(cause) ? null : cause);
        ctx.close();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HAProxyMessage) {
            haProxyMessage = (HAProxyMessage) msg;
            return;
        }
        switch (state) {
        case Init:
        case Connecting:
        case ProxyLookupRequests:
            // Do the regular decoding for the Connected message
            super.channelRead(ctx, msg);
            break;

        case ProxyConnectionToBroker:
            // Pass the buffer to the outbound connection and schedule next read
            // only if we can write on the connection
            ProxyService.opsCounter.inc();
            if (msg instanceof ByteBuf) {
                int bytes = ((ByteBuf) msg).readableBytes();
                directProxyHandler.getInboundChannelRequestsRate().recordEvent(bytes);
                ProxyService.bytesCounter.inc(bytes);
            }
            directProxyHandler.outboundChannel.writeAndFlush(msg).addListener(this);
            break;

        default:
            break;
        }
    }

    @Override
    public void operationComplete(Future<Void> future) throws Exception {
        // This is invoked when the write operation on the paired connection is
        // completed
        if (future.isSuccess()) {
            ctx.read();
        } else {
            LOG.warn("[{}] Error in writing to inbound channel. Closing", remoteAddress, future.cause());
            directProxyHandler.outboundChannel.close();
        }
    }

    private void completeConnect() {
        LOG.info("[{}] complete connection, init proxy handler. authenticated with {} role {}, hasProxyToBrokerUrl: {}",
            remoteAddress, authMethod, clientAuthRole, hasProxyToBrokerUrl);
        if (hasProxyToBrokerUrl) {
            // Client already knows which broker to connect. Let's open a
            // connection there and just pass bytes in both directions
            state = State.ProxyConnectionToBroker;
            directProxyHandler = new DirectProxyHandler(service, this, proxyToBrokerUrl,
                protocolVersionToAdvertise, sslHandlerSupplier);
            cancelKeepAliveTask();
        } else {
            // Client is doing a lookup, we can consider the handshake complete
            // and we'll take care of just topics and
            // partitions metadata lookups
            state = State.ProxyLookupRequests;
            lookupProxyHandler = new LookupProxyHandler(service, this);
            ctx.writeAndFlush(Commands.newConnected(protocolVersionToAdvertise));
        }
    }

    private void createClientAndCompleteConnect(AuthData clientData)
        throws PulsarClientException {
        if (service.getConfiguration().isForwardAuthorizationCredentials()) {
            this.clientAuthData = clientData;
            this.clientAuthMethod = authMethod;
        }
        this.client = createClient(clientConf, this.clientAuthData, this.clientAuthMethod, protocolVersionToAdvertise);

        completeConnect();
    }

    // According to auth result, send newConnected or newAuthChallenge command.
    private void doAuthentication(AuthData clientData) throws Exception {
        AuthData brokerData = authState.authenticate(clientData);
        // authentication has completed, will send newConnected command.
        if (authState.isComplete()) {
            clientAuthRole = authState.getAuthRole();
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Client successfully authenticated with {} role {}",
                    remoteAddress, authMethod, clientAuthRole);
            }
            createClientAndCompleteConnect(clientData);
            return;
        }

        // auth not complete, continue auth with client side.
        ctx.writeAndFlush(Commands.newAuthChallenge(authMethod, brokerData, protocolVersionToAdvertise));
        if (LOG.isDebugEnabled()) {
            LOG.debug("[{}] Authentication in progress client by method {}.",
                remoteAddress, authMethod);
        }
        state = State.Connecting;
        return;
    }

    @Override
    protected void handleConnect(CommandConnect connect) {
        checkArgument(state == State.Init);
        this.setRemoteEndpointProtocolVersion(connect.getProtocolVersion());
        this.hasProxyToBrokerUrl = connect.hasProxyToBrokerUrl();
        this.protocolVersionToAdvertise = getProtocolVersionToAdvertise(connect);
        this.proxyToBrokerUrl = connect.hasProxyToBrokerUrl() ? connect.getProxyToBrokerUrl() : "null";

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received CONNECT from {} proxyToBroker={}", remoteAddress, proxyToBrokerUrl);
            LOG.debug(
                "[{}] Protocol version to advertise to broker is {}, clientProtocolVersion={}, proxyProtocolVersion={}",
                remoteAddress, protocolVersionToAdvertise, getRemoteEndpointProtocolVersion(),
                Commands.getCurrentProtocolVersion());
        }

        if (getRemoteEndpointProtocolVersion() < ProtocolVersion.v10.getValue()) {
            LOG.warn("[{}] Client doesn't support connecting through proxy", remoteAddress);
            ctx.close();
            return;
        }

        try {
            // init authn
            this.clientConf = createClientConfiguration();
            int protocolVersion = getProtocolVersionToAdvertise(connect);

            // authn not enabled, complete
            if (!service.getConfiguration().isAuthenticationEnabled()) {
                this.connectionPool = new ProxyConnectionPool(clientConf, service.getWorkerGroup(),
                        () -> new ClientCnx(clientConf, service.getWorkerGroup(), protocolVersion));
                this.client =
                        new PulsarClientImpl(clientConf, service.getWorkerGroup(), connectionPool, service.getTimer());

                completeConnect();
                return;
            }

            AuthData clientData = AuthData.of(connect.hasAuthData() ? connect.getAuthData() : EMPTY_CREDENTIALS);
            if (connect.hasAuthMethodName()) {
                authMethod = connect.getAuthMethodName();
            } else if (connect.hasAuthMethod()) {
                // Legacy client is passing enum
                authMethod = connect.getAuthMethod().name().substring(10).toLowerCase();
            } else {
                authMethod = "none";
            }

            authenticationProvider = service
                .getAuthenticationService()
                .getAuthenticationProvider(authMethod);

            // Not find provider named authMethod. Most used for tests.
            // In AuthenticationDisabled, it will set authMethod "none".
            if (authenticationProvider == null) {
                clientAuthRole = service.getAuthenticationService().getAnonymousUserRole()
                    .orElseThrow(() ->
                        new AuthenticationException("No anonymous role, and no authentication provider configured"));

                createClientAndCompleteConnect(clientData);
                return;
            }

            // init authState and other var
            ChannelHandler sslHandler = ctx.channel().pipeline().get(PulsarChannelInitializer.TLS_HANDLER);
            SSLSession sslSession = null;
            if (sslHandler != null) {
                sslSession = ((SslHandler) sslHandler).engine().getSession();
            }

            authState = authenticationProvider.newAuthState(clientData, remoteAddress, sslSession);
            authenticationData = authState.getAuthDataSource();
            doAuthentication(clientData);
        } catch (Exception e) {
            LOG.warn("[{}] Unable to authenticate: ", remoteAddress, e);
            ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, "Failed to authenticate"));
            close();
            return;
        }
    }

    @Override
    protected void handleAuthResponse(CommandAuthResponse authResponse) {
        checkArgument(state == State.Connecting);
        checkArgument(authResponse.hasResponse());
        checkArgument(authResponse.getResponse().hasAuthData() && authResponse.getResponse().hasAuthMethodName());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received AuthResponse from {}, auth method: {}",
                remoteAddress, authResponse.getResponse().getAuthMethodName());
        }

        try {
            AuthData clientData = AuthData.of(authResponse.getResponse().getAuthData());
            doAuthentication(clientData);
        } catch (Exception e) {
            String msg = "Unable to handleAuthResponse";
            LOG.warn("[{}] {} ", remoteAddress, msg, e);
            ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, msg));
            close();
        }
    }

    @Override
    protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata partitionMetadata) {
        checkArgument(state == State.ProxyLookupRequests);

        lookupProxyHandler.handlePartitionMetadataResponse(partitionMetadata);
    }

    @Override
    protected void handleGetTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace) {
        checkArgument(state == State.ProxyLookupRequests);

        lookupProxyHandler.handleGetTopicsOfNamespace(commandGetTopicsOfNamespace);
    }
    @Override
    protected void handleGetSchema(CommandGetSchema commandGetSchema) {
        checkArgument(state == State.ProxyLookupRequests);

        lookupProxyHandler.handleGetSchema(commandGetSchema);
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
        try {
            if (client != null) {
                client.close();
            }
        } catch (PulsarClientException e) {
            LOG.error("Unable to close pulsar client - {}. Error - {}", client, e.getMessage());
        }
    }

    ClientConfigurationData createClientConfiguration() throws UnsupportedAuthenticationException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(service.getServiceUrl());
        ProxyConfiguration proxyConfig = service.getConfiguration();
        clientConf.setAuthentication(this.getClientAuthentication());
        if (proxyConfig.isTlsEnabledWithBroker()) {
            clientConf.setUseTls(true);
            if (proxyConfig.isBrokerClientTlsEnabledWithKeyStore()) {
                clientConf.setUseKeyStoreTls(true);
                clientConf.setTlsTrustStoreType(proxyConfig.getBrokerClientTlsTrustStoreType());
                clientConf.setTlsTrustStorePath(proxyConfig.getBrokerClientTlsTrustStore());
                clientConf.setTlsTrustStorePassword(proxyConfig.getBrokerClientTlsTrustStorePassword());
            } else {
                clientConf.setTlsTrustCertsFilePath(proxyConfig.getBrokerClientTrustCertsFilePath());
                clientConf.setTlsAllowInsecureConnection(proxyConfig.isTlsAllowInsecureConnection());
            }
        }
        return clientConf;
    }

    private PulsarClientImpl createClient(final ClientConfigurationData clientConf, final AuthData clientAuthData,
            final String clientAuthMethod, final int protocolVersion) throws PulsarClientException {
        this.connectionPool = new ProxyConnectionPool(clientConf, service.getWorkerGroup(),
                () -> new ProxyClientCnx(clientConf, service.getWorkerGroup(), clientAuthRole, clientAuthData,
                        clientAuthMethod, protocolVersion));
        return new PulsarClientImpl(clientConf, service.getWorkerGroup(), connectionPool, service.getTimer());
    }

    private static int getProtocolVersionToAdvertise(CommandConnect connect) {
        return Math.min(connect.getProtocolVersion(), Commands.getCurrentProtocolVersion());
    }

    long newRequestId() {
        return client.newRequestId();
    }

    public Authentication getClientAuthentication() {
        return service.getProxyClientAuthenticationPlugin();
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

    public boolean hasHAProxyMessage() {
        return haProxyMessage != null;
    }

    public HAProxyMessage getHAProxyMessage() {
        return haProxyMessage;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ProxyConnection.class);

}
