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
import static org.apache.commons.lang3.StringUtils.isEmpty;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.CharsetUtil;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarDecoder;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.util.NettyClientSslContextRefresher;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectProxyHandler {

    @Getter
    private final Channel inboundChannel;
    private final ProxyConnection proxyConnection;
    @Getter
    Channel outboundChannel;
    @Getter
    private final Rate inboundChannelRequestsRate;
    private final String originalPrincipal;
    private final AuthData clientAuthData;
    private final String clientAuthMethod;
    public static final String TLS_HANDLER = "tls";

    private final Authentication authentication;
    private AuthenticationDataProvider authenticationDataProvider;
    private final ProxyService service;
    private final Runnable onHandshakeCompleteAction;
    private final boolean tlsHostnameVerificationEnabled;
    private final boolean tlsEnabledWithKeyStore;
    private final boolean tlsEnabledWithBroker;
    private final SslContextAutoRefreshBuilder<SslContext> clientSslCtxRefresher;
    private final NettySSLContextAutoRefreshBuilder clientSSLContextAutoRefreshBuilder;

    public DirectProxyHandler(ProxyService service, ProxyConnection proxyConnection) {
        this.service = service;
        this.authentication = proxyConnection.getClientAuthentication();
        this.inboundChannel = proxyConnection.ctx().channel();
        this.proxyConnection = proxyConnection;
        this.inboundChannelRequestsRate = new Rate();
        this.originalPrincipal = proxyConnection.clientAuthRole;
        this.clientAuthData = proxyConnection.clientAuthData;
        this.clientAuthMethod = proxyConnection.clientAuthMethod;
        this.tlsEnabledWithBroker = service.getConfiguration().isTlsEnabledWithBroker();
        this.tlsHostnameVerificationEnabled = service.getConfiguration().isTlsHostnameVerificationEnabled();
        this.tlsEnabledWithKeyStore = service.getConfiguration().isTlsEnabledWithKeyStore();
        this.onHandshakeCompleteAction = proxyConnection::cancelKeepAliveTask;
        ProxyConfiguration config = service.getConfiguration();

        if (tlsEnabledWithBroker) {
            AuthenticationDataProvider authData = null;

            if (!isEmpty(config.getBrokerClientAuthenticationPlugin())) {
                try {
                    authData = AuthenticationFactory.create(config.getBrokerClientAuthenticationPlugin(),
                            config.getBrokerClientAuthenticationParameters()).getAuthData();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }

            if (tlsEnabledWithKeyStore) {
                clientSSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                        config.getBrokerClientSslProvider(),
                        config.isTlsAllowInsecureConnection(),
                        config.getBrokerClientTlsTrustStoreType(),
                        config.getBrokerClientTlsTrustStore(),
                        config.getBrokerClientTlsTrustStorePassword(),
                        config.getBrokerClientTlsCiphers(),
                        config.getBrokerClientTlsProtocols(),
                        config.getTlsCertRefreshCheckDurationSec(),
                        authData);
                clientSslCtxRefresher = null;
            } else {
                SslProvider sslProvider = null;
                if (config.getBrokerClientSslProvider() != null) {
                    sslProvider = SslProvider.valueOf(config.getBrokerClientSslProvider());
                }
                clientSslCtxRefresher = new NettyClientSslContextRefresher(
                        sslProvider,
                        config.isTlsAllowInsecureConnection(),
                        config.getBrokerClientTrustCertsFilePath(),
                        authData,
                        config.getBrokerClientTlsCiphers(),
                        config.getBrokerClientTlsProtocols(),
                        config.getTlsCertRefreshCheckDurationSec()
                );
                clientSSLContextAutoRefreshBuilder = null;
            }
        } else {
            clientSSLContextAutoRefreshBuilder = null;
            clientSslCtxRefresher = null;
        }
    }

    public void connect(String brokerHostAndPort, InetSocketAddress targetBrokerAddress, int protocolVersion) {
        ProxyConfiguration config = service.getConfiguration();

        // Start the connection attempt.
        Bootstrap b = new Bootstrap();
        // Tie the backend connection on the same thread to avoid context
        // switches when passing data between the 2
        // connections
        b.option(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        int brokerProxyConnectTimeoutMs = service.getConfiguration().getBrokerProxyConnectTimeoutMs();
        if (brokerProxyConnectTimeoutMs > 0) {
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, brokerProxyConnectTimeoutMs);
        }
        b.group(inboundChannel.eventLoop())
                .channel(inboundChannel.getClass());

        String remoteHost;
        try {
            remoteHost = parseHost(brokerHostAndPort);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to parse broker host '{}'", inboundChannel, brokerHostAndPort, e);
            inboundChannel.close();
            return;
        }

        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(1024,
                        true));
                if (tlsEnabledWithBroker) {
                    String host = targetBrokerAddress.getHostString();
                    int port = targetBrokerAddress.getPort();
                    SslHandler handler = tlsEnabledWithKeyStore
                            ? new SslHandler(clientSSLContextAutoRefreshBuilder.get().createSSLEngine(host, port))
                            : clientSslCtxRefresher.get().newHandler(ch.alloc(), host, port);
                    if (tlsHostnameVerificationEnabled) {
                        SecurityUtility.configureSSLHandler(handler);
                    }
                    ch.pipeline().addLast(TLS_HANDLER, handler);
                }
                int brokerProxyReadTimeoutMs = service.getConfiguration().getBrokerProxyReadTimeoutMs();
                if (brokerProxyReadTimeoutMs > 0) {
                    ch.pipeline().addLast("readTimeoutHandler",
                            new ReadTimeoutHandler(brokerProxyReadTimeoutMs, TimeUnit.MILLISECONDS));
                }
                ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                    Commands.DEFAULT_MAX_MESSAGE_SIZE + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4));
                ch.pipeline().addLast("proxyOutboundHandler",
                        new ProxyBackendHandler(config, protocolVersion, remoteHost));
            }
        });

        ChannelFuture f = b.connect(targetBrokerAddress);
        outboundChannel = f.channel();
        f.addListener(future -> {
            if (!future.isSuccess()) {
                // Close the connection if the connection attempt has failed.
                log.warn("[{}] Establishing connection to {} ({}) failed. Closing inbound channel.", inboundChannel,
                        targetBrokerAddress, brokerHostAndPort, future.cause());
                inboundChannel.close();
                return;
            }
        });
    }

    private static String parseHost(String brokerPortAndHost) {
        int pos = brokerPortAndHost.lastIndexOf(':');
        if (pos > 0) {
            return brokerPortAndHost.substring(0, pos);
        } else {
            throw new IllegalArgumentException("Illegal broker host:port '" + brokerPortAndHost + "'");
        }
    }

    private void writeHAProxyMessage() {
        if (proxyConnection.hasHAProxyMessage()) {
            outboundChannel.writeAndFlush(encodeProxyProtocolMessage(proxyConnection.getHAProxyMessage()))
                    .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        } else {
            if (inboundChannel.remoteAddress() instanceof InetSocketAddress
                    && outboundChannel.localAddress() instanceof InetSocketAddress) {
                InetSocketAddress clientAddress = (InetSocketAddress) inboundChannel.remoteAddress();
                String sourceAddress = clientAddress.getAddress().getHostAddress();
                int sourcePort = clientAddress.getPort();
                InetSocketAddress proxyAddress = (InetSocketAddress) inboundChannel.remoteAddress();
                String destinationAddress = proxyAddress.getAddress().getHostAddress();
                int destinationPort = proxyAddress.getPort();
                HAProxyMessage msg = new HAProxyMessage(HAProxyProtocolVersion.V1, HAProxyCommand.PROXY,
                        HAProxyProxiedProtocol.TCP4, sourceAddress, destinationAddress, sourcePort,
                        destinationPort);
                outboundChannel.writeAndFlush(encodeProxyProtocolMessage(msg))
                        .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
                msg.release();
            }
        }
    }



    private ByteBuf encodeProxyProtocolMessage(HAProxyMessage msg) {
        // Max length of v1 version proxy protocol message is 108
        ByteBuf out = Unpooled.buffer(108);
        out.writeBytes(TEXT_PREFIX);
        out.writeByte((byte) ' ');
        out.writeCharSequence(msg.proxiedProtocol().name(), CharsetUtil.US_ASCII);
        out.writeByte((byte) ' ');
        out.writeCharSequence(msg.sourceAddress(), CharsetUtil.US_ASCII);
        out.writeByte((byte) ' ');
        out.writeCharSequence(msg.destinationAddress(), CharsetUtil.US_ASCII);
        out.writeByte((byte) ' ');
        out.writeCharSequence(String.valueOf(msg.sourcePort()), CharsetUtil.US_ASCII);
        out.writeByte((byte) ' ');
        out.writeCharSequence(String.valueOf(msg.destinationPort()), CharsetUtil.US_ASCII);
        out.writeByte((byte) '\r');
        out.writeByte((byte) '\n');
        return out;
    }

    static final byte[] TEXT_PREFIX = {
            (byte) 'P',
            (byte) 'R',
            (byte) 'O',
            (byte) 'X',
            (byte) 'Y',
    };

    public void close() {
        if (outboundChannel != null) {
            outboundChannel.close();
        }
    }

    enum BackendState {
        Init, HandshakeCompleted
    }

    public class ProxyBackendHandler extends PulsarDecoder {

        private BackendState state = BackendState.Init;
        private final String remoteHostName;
        protected ChannelHandlerContext ctx;
        private final ProxyConfiguration config;
        private final int protocolVersion;

        public ProxyBackendHandler(ProxyConfiguration config, int protocolVersion, String remoteHostName) {
            this.config = config;
            this.protocolVersion = protocolVersion;
            this.remoteHostName = remoteHostName;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            this.ctx = ctx;

            if (config.isHaProxyProtocolEnabled()) {
                writeHAProxyMessage();
            }

            // Send the Connect command to broker
            authenticationDataProvider = authentication.getAuthData(remoteHostName);
            AuthData authData = authenticationDataProvider.authenticate(AuthData.INIT_AUTH_DATA);
            ByteBuf command;
            command = Commands.newConnect(authentication.getAuthMethodName(), authData, protocolVersion,
                    proxyConnection.clientVersion,
                    null /* target broker */, originalPrincipal, clientAuthData, clientAuthMethod);
            outboundChannel.writeAndFlush(command)
                    .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            // handle backpressure
            // stop/resume reading input from connection between the client and the proxy
            // when the writability of the connection between the proxy and the broker changes
            inboundChannel.config().setAutoRead(ctx.channel().isWritable());
            super.channelWritabilityChanged(ctx);
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            switch (state) {
            case Init:
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Received msg on broker connection: {}", inboundChannel, outboundChannel,
                            msg.getClass());
                }

                // Do the regular decoding for the Connected message
                super.channelRead(ctx, msg);
                break;

            case HandshakeCompleted:
                ProxyService.OPS_COUNTER.inc();
                if (msg instanceof ByteBuf) {
                    ProxyService.BYTES_COUNTER.inc(((ByteBuf) msg).readableBytes());
                }
                inboundChannel.writeAndFlush(msg)
                        .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
                break;

            default:
                break;
            }

        }

        @Override
        protected void handleAuthChallenge(CommandAuthChallenge authChallenge) {
            checkArgument(authChallenge.hasChallenge());
            checkArgument(authChallenge.getChallenge().hasAuthData() && authChallenge.getChallenge().hasAuthData());

            if (Arrays.equals(AuthData.REFRESH_AUTH_DATA_BYTES, authChallenge.getChallenge().getAuthData())) {
                try {
                    authenticationDataProvider = authentication.getAuthData(remoteHostName);
                } catch (PulsarClientException e) {
                    log.error("{} Error when refreshing authentication data provider: {}", ctx.channel(), e);
                    return;
                }
            }

            // mutual authn. If auth not complete, continue auth; if auth complete, complete connectionFuture.
            try {
                AuthData authData = authenticationDataProvider
                    .authenticate(AuthData.of(authChallenge.getChallenge().getAuthData()));

                checkState(!authData.isComplete());

                ByteBuf request = Commands.newAuthResponse(authentication.getAuthMethodName(),
                    authData,
                    this.protocolVersion,
                    PulsarVersion.getVersion());

                if (log.isDebugEnabled()) {
                    log.debug("{} Mutual auth {}", ctx.channel(), authentication.getAuthMethodName());
                }

                outboundChannel.writeAndFlush(request)
                        .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            } catch (Exception e) {
                log.error("Error mutual verify", e);
            }
        }

        @Override
        protected void messageReceived() {
            // no-op
        }

        @Override
        protected void handleConnected(CommandConnected connected) {
            checkArgument(state == BackendState.Init, "Unexpected state %s. BackendState.Init was expected.", state);
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received Connected from broker", inboundChannel, outboundChannel);
            }

            state = BackendState.HandshakeCompleted;

            onHandshakeCompleteAction.run();
            startDirectProxying(connected);

            proxyConnection.brokerConnected(DirectProxyHandler.this, connected);
        }

        private void startDirectProxying(CommandConnected connected) {
            if (service.getProxyLogLevel() == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Removing decoder from pipeline", inboundChannel, outboundChannel);
                }
                // direct tcp proxy
                inboundChannel.pipeline().remove("frameDecoder");
                outboundChannel.pipeline().remove("frameDecoder");
            } else {
                // Enable parsing feature, proxyLogLevel(1 or 2)
                // Add parser handler
                if (connected.hasMaxMessageSize()) {
                    inboundChannel.pipeline()
                            .replace("frameDecoder", "newFrameDecoder",
                                    new LengthFieldBasedFrameDecoder(connected.getMaxMessageSize()
                                            + Commands.MESSAGE_SIZE_FRAME_PADDING,
                                            0, 4, 0, 4));
                    outboundChannel.pipeline().replace("frameDecoder", "newFrameDecoder",
                            new LengthFieldBasedFrameDecoder(
                                    connected.getMaxMessageSize()
                                            + Commands.MESSAGE_SIZE_FRAME_PADDING,
                                    0, 4, 0, 4));

                    inboundChannel.pipeline().addBefore("handler", "inboundParser",
                            new ParserProxyHandler(service, inboundChannel,
                                    ParserProxyHandler.FRONTEND_CONN,
                                    connected.getMaxMessageSize(), outboundChannel.id()));
                    outboundChannel.pipeline().addBefore("proxyOutboundHandler", "outboundParser",
                            new ParserProxyHandler(service, outboundChannel,
                                    ParserProxyHandler.BACKEND_CONN,
                                    connected.getMaxMessageSize(), inboundChannel.id()));
                } else {
                    inboundChannel.pipeline().addBefore("handler", "inboundParser",
                            new ParserProxyHandler(service, inboundChannel,
                                    ParserProxyHandler.FRONTEND_CONN,
                                    Commands.DEFAULT_MAX_MESSAGE_SIZE, outboundChannel.id()));
                    outboundChannel.pipeline().addBefore("proxyOutboundHandler", "outboundParser",
                            new ParserProxyHandler(service, outboundChannel,
                                    ParserProxyHandler.BACKEND_CONN,
                                    Commands.DEFAULT_MAX_MESSAGE_SIZE, inboundChannel.id()));
                }
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            inboundChannel.close();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("[{}] [{}] Caught exception: {}", inboundChannel, outboundChannel, cause.getMessage(), cause);
            ctx.close();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DirectProxyHandler.class);
}
