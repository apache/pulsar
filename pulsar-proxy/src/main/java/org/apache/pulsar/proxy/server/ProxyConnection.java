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
import static com.google.common.base.Preconditions.checkState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.ssl.SslHandler;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarChannelInitializer;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.internal.PropertiesUtils;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.CommandAuthResponse;
import org.apache.pulsar.common.api.proto.CommandConnect;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandGetSchema;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles incoming discovery request from client and sends appropriate response back to client.
 *
 */
public class ProxyConnection extends PulsarHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ProxyConnection.class);
    // ConnectionPool is used by the proxy to issue lookup requests
    private ConnectionPool connectionPool;
    private final AtomicLong requestIdGenerator =
            new AtomicLong(ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE / 2));
    private final ProxyService service;
    private final DnsAddressResolverGroup dnsAddressResolverGroup;
    AuthenticationDataSource authenticationData;
    private State state;

    private LookupProxyHandler lookupProxyHandler = null;
    @Getter
    private DirectProxyHandler directProxyHandler = null;
    private final BrokerProxyValidator brokerProxyValidator;
    String clientAuthRole;
    AuthData clientAuthData;
    String clientAuthMethod;
    String clientVersion;

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

        // Connecting to the broker
        ProxyConnectingToBroker,

        // If we are proxying a connection to a specific broker, we
        // are just forwarding data between the 2 connections, without
        // looking into it
        ProxyConnectionToBroker,

        Closing,

        Closed,
    }

    ConnectionPool getConnectionPool() {
        return connectionPool;
    }

    public ProxyConnection(ProxyService proxyService, DnsAddressResolverGroup dnsAddressResolverGroup) {
        super(30, TimeUnit.SECONDS);
        this.service = proxyService;
        this.dnsAddressResolverGroup = dnsAddressResolverGroup;
        this.state = State.Init;
        this.brokerProxyValidator = service.getBrokerProxyValidator();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        ProxyService.ACTIVE_CONNECTIONS.inc();
        if (ProxyService.ACTIVE_CONNECTIONS.get() > service.getConfiguration().getMaxConcurrentInboundConnections()) {
            state = State.Closing;
            ctx.close();
            ProxyService.REJECTED_CONNECTIONS.inc();
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        ProxyService.ACTIVE_CONNECTIONS.dec();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        ProxyService.NEW_CONNECTIONS.inc();
        service.getClientCnxs().add(this);
        LOG.info("[{}] New connection opened", remoteAddress);
    }

    @Override
    public synchronized void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

        if (directProxyHandler != null) {
            directProxyHandler.close();
            directProxyHandler = null;
        }

        service.getClientCnxs().remove(this);
        LOG.info("[{}] Connection closed", remoteAddress);

        if (connectionPool != null) {
            try {
                connectionPool.close();
                connectionPool = null;
            } catch (Exception e) {
                LOG.error("Failed to close connection pool {}", e.getMessage(), e);
            }
        }

        state = State.Closed;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        LOG.warn("[{}] Got exception {} : Message: {} State: {}", remoteAddress, cause.getClass().getSimpleName(),
                cause.getMessage(), state,
                ClientCnx.isKnownException(cause) ? null : cause);
        if (state != State.Closed) {
            state = State.Closing;
        }
        if (ctx.channel().isOpen()) {
            ctx.close();
        } else {
            // close connection to broker if that is present
            if (directProxyHandler != null) {
                directProxyHandler.close();
                directProxyHandler = null;
            }
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (directProxyHandler != null && directProxyHandler.outboundChannel != null) {
            // handle backpressure
            // stop/resume reading input from connection between the proxy and the broker
            // when the writability of the connection between the client and the proxy changes
            directProxyHandler.outboundChannel.config().setAutoRead(ctx.channel().isWritable());
        }
        super.channelWritabilityChanged(ctx);
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
            if (directProxyHandler != null) {
                ProxyService.OPS_COUNTER.inc();
                if (msg instanceof ByteBuf) {
                    int bytes = ((ByteBuf) msg).readableBytes();
                    directProxyHandler.getInboundChannelRequestsRate().recordEvent(bytes);
                    ProxyService.BYTES_COUNTER.inc(bytes);
                }
                directProxyHandler.outboundChannel.writeAndFlush(msg)
                        .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            } else {
                LOG.warn("Received message of type {} while connection to broker is missing in state {}. "
                                + "Dropping the input message (readable bytes={}).", msg.getClass(), state,
                        msg instanceof ByteBuf ? ((ByteBuf) msg).readableBytes() : -1);
            }
            break;
        case ProxyConnectingToBroker:
            LOG.warn("Received message of type {} while connecting to broker. "
                            + "Dropping the input message (readable bytes={}).", msg.getClass(),
                    msg instanceof ByteBuf ? ((ByteBuf) msg).readableBytes() : -1);
            break;
        default:
            break;
        }
    }

    private synchronized void completeConnect(AuthData clientData) throws PulsarClientException {
        Supplier<ClientCnx> clientCnxSupplier;
        if (service.getConfiguration().isAuthenticationEnabled()) {
            if (service.getConfiguration().isForwardAuthorizationCredentials()) {
                this.clientAuthData = clientData;
                this.clientAuthMethod = authMethod;
            }
            clientCnxSupplier = () -> new ProxyClientCnx(clientConf, service.getWorkerGroup(), clientAuthRole,
                    clientAuthData, clientAuthMethod, protocolVersionToAdvertise,
                    service.getConfiguration().isForwardAuthorizationCredentials(), this);
        } else {
            clientCnxSupplier = () -> new ClientCnx(clientConf, service.getWorkerGroup(), protocolVersionToAdvertise);
        }

        if (this.connectionPool == null) {
            this.connectionPool = new ConnectionPool(clientConf, service.getWorkerGroup(),
                    clientCnxSupplier,
                    Optional.of(dnsAddressResolverGroup.getResolver(service.getWorkerGroup().next())));
        } else {
            LOG.error("BUG! Connection Pool has already been created for proxy connection to {} state {} role {}",
                    remoteAddress, state, clientAuthRole);
        }

        LOG.info("[{}] complete connection, init proxy handler. authenticated with {} role {}, hasProxyToBrokerUrl: {}",
                remoteAddress, authMethod, clientAuthRole, hasProxyToBrokerUrl);
        if (hasProxyToBrokerUrl) {
            // Optimize proxy connection to fail-fast if the target broker isn't active
            // Pulsar client will retry connecting after a back off timeout
            if (service.getConfiguration().isCheckActiveBrokers()
                    && !isBrokerActive(proxyToBrokerUrl)) {
                state = State.Closing;
                LOG.warn("[{}] Target broker '{}' isn't available. authenticated with {} role {}.",
                        remoteAddress, proxyToBrokerUrl, authMethod, clientAuthRole);
                ctx()
                        .writeAndFlush(
                                Commands.newError(-1, ServerError.ServiceNotReady, "Target broker isn't available."))
                        .addListener(ChannelFutureListener.CLOSE);
                return;
            }

            state = State.ProxyConnectingToBroker;
            brokerProxyValidator.resolveAndCheckTargetAddress(proxyToBrokerUrl)
                    .thenAcceptAsync(this::connectToBroker, ctx.executor())
                    .exceptionally(throwable -> {
                        if (throwable instanceof TargetAddressDeniedException
                                || throwable.getCause() instanceof TargetAddressDeniedException) {
                            TargetAddressDeniedException targetAddressDeniedException =
                                    (TargetAddressDeniedException) (throwable instanceof TargetAddressDeniedException
                                            ? throwable : throwable.getCause());

                            LOG.warn("[{}] Target broker '{}' cannot be validated. {}. authenticated with {} role {}.",
                                    remoteAddress, proxyToBrokerUrl, targetAddressDeniedException.getMessage(),
                                    authMethod, clientAuthRole);
                        } else {
                            LOG.error("[{}] Error validating target broker '{}'. authenticated with {} role {}.",
                                    remoteAddress, proxyToBrokerUrl, authMethod, clientAuthRole, throwable);
                        }
                        ctx()
                                .writeAndFlush(
                                        Commands.newError(-1, ServerError.ServiceNotReady,
                                                "Target broker cannot be validated."))
                                .addListener(ChannelFutureListener.CLOSE);
                        return null;
                    });
        } else {
            // Client is doing a lookup, we can consider the handshake complete
            // and we'll take care of just topics and
            // partitions metadata lookups
            state = State.ProxyLookupRequests;
            lookupProxyHandler = new LookupProxyHandler(service, this);
            ctx.writeAndFlush(Commands.newConnected(protocolVersionToAdvertise))
                    .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        }
    }

    private void handleBrokerConnected(DirectProxyHandler directProxyHandler, CommandConnected connected) {
        checkState(ctx.executor().inEventLoop(), "This method should be called in the event loop");
        if (state == State.ProxyConnectingToBroker && ctx.channel().isOpen() && this.directProxyHandler == null) {
            this.directProxyHandler = directProxyHandler;
            state = State.ProxyConnectionToBroker;
            int maxMessageSize =
                    connected.hasMaxMessageSize() ? connected.getMaxMessageSize() : Commands.INVALID_MAX_MESSAGE_SIZE;
            ctx.writeAndFlush(Commands.newConnected(connected.getProtocolVersion(), maxMessageSize))
                    .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        } else {
            LOG.warn("[{}] Channel is {}. ProxyConnection is in {}. "
                            + "Closing connection to broker '{}'.",
                    remoteAddress, ctx.channel().isOpen() ? "open" : "already closed",
                    state != State.ProxyConnectingToBroker ? "invalid state " + state : "state " + state,
                    proxyToBrokerUrl);
            directProxyHandler.close();
            ctx.close();
        }
    }

    private void connectToBroker(InetSocketAddress brokerAddress) {
        checkState(ctx.executor().inEventLoop(), "This method should be called in the event loop");
        DirectProxyHandler directProxyHandler = new DirectProxyHandler(service, this);
        directProxyHandler.connect(proxyToBrokerUrl, brokerAddress, protocolVersionToAdvertise);
    }

    public void brokerConnected(DirectProxyHandler directProxyHandler, CommandConnected connected) {
        try {
            final CommandConnected finalConnected = new CommandConnected().copyFrom(connected);
            ctx.executor().submit(() -> handleBrokerConnected(directProxyHandler, finalConnected));
        } catch (RejectedExecutionException e) {
            LOG.error("Event loop was already closed. Closing broker connection.", e);
            directProxyHandler.close();
        }
    }

    // According to auth result, send newConnected or newAuthChallenge command.
    private void doAuthentication(AuthData clientData)
            throws Exception {
        AuthData brokerData = authState.authenticate(clientData);
        // authentication has completed, will send newConnected command.
        if (authState.isComplete()) {
            clientAuthRole = authState.getAuthRole();
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Client successfully authenticated with {} role {}",
                        remoteAddress, authMethod, clientAuthRole);
            }

            // First connection
            if (this.connectionPool == null || state == State.Connecting) {
                // authentication has completed, will send newConnected command.
                completeConnect(clientData);
            }
            return;
        }

        // auth not complete, continue auth with client side.
        ctx.writeAndFlush(Commands.newAuthChallenge(authMethod, brokerData, protocolVersionToAdvertise))
                .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        if (LOG.isDebugEnabled()) {
            LOG.debug("[{}] Authentication in progress client by method {}.",
                    remoteAddress, authMethod);
        }
    }

    @Override
    protected void handleConnect(CommandConnect connect) {
        checkArgument(state == State.Init);
        state = State.Connecting;
        this.setRemoteEndpointProtocolVersion(connect.getProtocolVersion());
        this.hasProxyToBrokerUrl = connect.hasProxyToBrokerUrl();
        this.protocolVersionToAdvertise = getProtocolVersionToAdvertise(connect);
        this.proxyToBrokerUrl = connect.hasProxyToBrokerUrl() ? connect.getProxyToBrokerUrl() : "null";
        this.clientVersion = connect.getClientVersion();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received CONNECT from {} proxyToBroker={}", remoteAddress, proxyToBrokerUrl);
            LOG.debug(
                "[{}] Protocol version to advertise to broker is {}, clientProtocolVersion={}, proxyProtocolVersion={}",
                remoteAddress, protocolVersionToAdvertise, getRemoteEndpointProtocolVersion(),
                Commands.getCurrentProtocolVersion());
        }

        if (getRemoteEndpointProtocolVersion() < ProtocolVersion.v10.getValue()) {
            LOG.warn("[{}] Client doesn't support connecting through proxy", remoteAddress);
            state = State.Closing;
            ctx.close();
            return;
        }

        try {
            // init authn
            this.clientConf = createClientConfiguration();

            // authn not enabled, complete
            if (!service.getConfiguration().isAuthenticationEnabled()) {
                completeConnect(null);
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

                completeConnect(clientData);
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
            ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, "Failed to authenticate"))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    protected void handleAuthResponse(CommandAuthResponse authResponse) {
        checkArgument(authResponse.hasResponse());
        checkArgument(authResponse.getResponse().hasAuthData() && authResponse.getResponse().hasAuthMethodName());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received AuthResponse from {}, auth method: {}",
                    remoteAddress, authResponse.getResponse().getAuthMethodName());
        }

        try {
            AuthData clientData = AuthData.of(authResponse.getResponse().getAuthData());
            doAuthentication(clientData);
            if (service.getConfiguration().isForwardAuthorizationCredentials()
                    && connectionPool != null && state == State.ProxyLookupRequests) {
                connectionPool.getConnections().forEach(toBrokerCnxFuture -> {
                    String clientVersion;
                    if (authResponse.hasClientVersion()) {
                        clientVersion = authResponse.getClientVersion();
                    } else {
                        clientVersion = this.clientVersion;
                    }
                    int protocolVersion;
                    if (authResponse.hasProtocolVersion()) {
                        protocolVersion = authResponse.getProtocolVersion();
                    } else {
                        protocolVersion = Commands.getCurrentProtocolVersion();
                    }

                    ByteBuf cmd =
                            Commands.newAuthResponse(clientAuthMethod, clientData, protocolVersion, clientVersion);
                    toBrokerCnxFuture.thenAccept(toBrokerCnx -> toBrokerCnx.ctx().writeAndFlush(cmd)
                                    .addListener(writeFuture -> {
                                        if (writeFuture.isSuccess()) {
                                            if (LOG.isDebugEnabled()) {
                                                LOG.debug("{} authentication is refreshed successfully by {}, "
                                                                + "auth method: {} ",
                                                        toBrokerCnx.ctx().channel(), ctx.channel(), clientAuthMethod);
                                            }
                                        } else {
                                            LOG.error("Failed to forward the auth response "
                                                            + "from the proxy to the broker through the proxy client, "
                                                            + "proxy: {}, proxy client: {}",
                                                    ctx.channel(),
                                                    toBrokerCnx.ctx().channel(),
                                                    writeFuture.cause());
                                            toBrokerCnx.ctx().channel().pipeline()
                                                    .fireExceptionCaught(writeFuture.cause());
                                        }
                                    }))
                            .whenComplete((__, ex) -> {
                                if (ex != null) {
                                    LOG.error("Failed to forward the auth response from the proxy to "
                                                    + "the broker through the proxy client, proxy: {}",
                                            ctx().channel(), ex);
                                }
                            });
                });
            }
        } catch (Exception e) {
            String msg = "Unable to handleAuthResponse";
            LOG.warn("[{}] {} ", remoteAddress, msg, e);
            ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, msg))
                    .addListener(ChannelFutureListener.CLOSE);
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
     * handles discovery request from client ands sends next active broker address.
     */
    @Override
    protected void handleLookup(CommandLookupTopic lookup) {
        checkArgument(state == State.ProxyLookupRequests);
        lookupProxyHandler.handleLookup(lookup);
    }

    ClientConfigurationData createClientConfiguration() {
        ClientConfigurationData initialConf = new ClientConfigurationData();
        ProxyConfiguration proxyConfig = service.getConfiguration();
        initialConf.setServiceUrl(
                proxyConfig.isTlsEnabledWithBroker() ? service.getServiceUrlTls() : service.getServiceUrl());

        // Apply all arbitrary configuration. This must be called before setting any fields annotated as
        // @Secret on the ClientConfigurationData object because of the way they are serialized.
        // See https://github.com/apache/pulsar/issues/8509 for more information.
        Map<String, Object> overrides = PropertiesUtils
                .filterAndMapProperties(proxyConfig.getProperties(), "brokerClient_");
        ClientConfigurationData clientConf = ConfigurationDataUtils
                .loadData(overrides, initialConf, ClientConfigurationData.class);

        clientConf.setAuthentication(this.getClientAuthentication());
        if (proxyConfig.isTlsEnabledWithBroker()) {
            clientConf.setUseTls(true);
            clientConf.setTlsHostnameVerificationEnable(proxyConfig.isTlsHostnameVerificationEnabled());
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

    private static int getProtocolVersionToAdvertise(CommandConnect connect) {
        return Math.min(connect.getProtocolVersion(), Commands.getCurrentProtocolVersion());
    }

    long newRequestId() {
        return requestIdGenerator.getAndIncrement();
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

    private boolean isBrokerActive(String targetBrokerHostPort) {
        for (ServiceLookupData serviceLookupData : getAvailableBrokers()) {
            if (matchesHostAndPort("pulsar://", serviceLookupData.getPulsarServiceUrl(), targetBrokerHostPort)
                    || matchesHostAndPort("pulsar+ssl://", serviceLookupData.getPulsarServiceUrlTls(),
                    targetBrokerHostPort)) {
                return true;
            }
        }
        return false;
    }

    private List<? extends ServiceLookupData> getAvailableBrokers() {
        if (service.getDiscoveryProvider() == null) {
            LOG.warn("Unable to retrieve active brokers. service.getDiscoveryProvider() is null."
                    + "zookeeperServers and configurationStoreServers must be configured in proxy configuration "
                    + "when checkActiveBrokers is enabled.");
            return Collections.emptyList();
        }
        try {
            return service.getDiscoveryProvider().getAvailableBrokers();
        } catch (PulsarServerException e) {
            LOG.error("Unable to get available brokers", e);
            return Collections.emptyList();
        }
    }

    static boolean matchesHostAndPort(String expectedPrefix, String pulsarServiceUrl, String brokerHostPort) {
        return pulsarServiceUrl != null
                && pulsarServiceUrl.length() == expectedPrefix.length() + brokerHostPort.length()
                && pulsarServiceUrl.startsWith(expectedPrefix)
                && pulsarServiceUrl.startsWith(brokerHostPort, expectedPrefix.length());
    }
}
