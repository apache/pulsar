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
package org.apache.pulsar.proxy.server;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.CharsetUtil;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.CustomLog;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.v5.BinaryAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.FeatureFlags;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.protocol.PulsarDecoder;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.tls.impl.TlsContextAcquisition;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.NettyChannelUtil;

@CustomLog
public class DirectProxyHandler {

    @Getter
    private final Channel inboundChannel;
    private final ProxyConnection proxyConnection;
    @Getter
    Channel outboundChannel;
    boolean isTlsOutboundChannel = false;
    @Getter
    private final Rate inboundChannelRequestsRate;
    private final String originalPrincipal;
    private final AuthData clientAuthData;
    private final String clientAuthMethod;
    public static final String TLS_HANDLER = "tls";

    // PIP-478: hard cap on broker challenge rounds within a single binary authentication exchange, mirroring
    // ClientCnx.MAX_AUTH_CHALLENGE_ROUNDS (both in turn mirror HttpAuthenticationDriver.MAX_CHALLENGE_ROUNDS).
    // A broker that answers every CommandAuthResponse with another challenge would otherwise loop against the
    // proxy forever, and each round now also schedules credential work onto a blocking pool.
    static final int MAX_AUTH_CHALLENGE_ROUNDS = 10;

    private final Authentication authentication;
    private final ProxyService service;
    private final Runnable onHandshakeCompleteAction;
    final boolean tlsEnabledWithBroker;

    @SneakyThrows
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
        this.onHandshakeCompleteAction = proxyConnection::cancelKeepAliveTask;
    }

    public void connect(String brokerHostAndPort, InetSocketAddress targetBrokerAddress, int protocolVersion,
                        final FeatureFlags featureFlags) {
        String remoteHost;
        try {
            remoteHost = parseHost(brokerHostAndPort);
        } catch (IllegalArgumentException e) {
            log.warn().attr("channel", inboundChannel)
                    .attr("brokerHost", brokerHostAndPort).exception(e)
                    .log("Failed to parse broker host");
            inboundChannel.close();
            return;
        }
        // PIP-478: ProxyService holds a shared, rotating SslContext for the BROKER_CLIENT purpose; it is read
        // and pinned per connection inside initChannel below (the only broker-client TLS path since PIP-337
        // removal). Reading it at connect time (rather than capturing it here, before the async
        // connect) both narrows the use-after-free window and uses the freshest rotated material.
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

        if (service.proxyZeroCopyModeEnabled && EpollSocketChannel.class.isAssignableFrom(inboundChannel.getClass())) {
            b.option(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
        }

        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(1024,
                        true));
                if (tlsEnabledWithBroker) {
                    String host = targetBrokerAddress.getHostString();
                    int port = targetBrokerAddress.getPort();
                    // PIP-478: build the handler from the shared, rotating client SslContext, pinning it across
                    // newHandler so a concurrent rotation cannot free the native OpenSSL context mid-build
                    // (use-after-free guard). Hostname verification is baked into the context at build time (per
                    // the client TlsPolicy), so it is not re-applied here; host/port drive SNI and the
                    // verification target.
                    ch.pipeline().addLast(TLS_HANDLER, TlsContextAcquisition.withPinnedContext(
                            service::getBrokerClientSslContext, ctx -> ctx.newHandler(ch.alloc(), host, port)));
                }
                int brokerProxyReadTimeoutMs = service.getConfiguration().getBrokerProxyReadTimeoutMs();
                if (brokerProxyReadTimeoutMs > 0) {
                    ch.pipeline().addLast("readTimeoutHandler",
                            new ReadTimeoutHandler(brokerProxyReadTimeoutMs, TimeUnit.MILLISECONDS));
                }
                FrameDecoderUtil.addFrameDecoder(ch.pipeline(), service.getConfiguration().getMaxMessageSize());
                ch.pipeline().addLast("proxyOutboundHandler",
                        (ChannelHandler) new ProxyBackendHandler(config, protocolVersion, remoteHost, featureFlags));
            }
        });

        ChannelFuture f = b.connect(targetBrokerAddress);
        outboundChannel = f.channel();
        f.addListener(future -> {
            if (!future.isSuccess()) {
                // Close the connection if the connection attempt has failed.
                log.warn().attr("channel", inboundChannel)
                        .attr("targetAddress", targetBrokerAddress)
                        .attr("brokerHost", brokerHostAndPort)
                        .exception(future.cause())
                        .log("Establishing connection failed. Closing inbound channel.");
                Channel channel = f.channel();
                if (channel != null) {
                    channel.close();
                }
                inboundChannel.close();
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
            final ByteBuf msg = encodeProxyProtocolMessage(proxyConnection.getHAProxyMessage());
            writeAndFlush(msg);
        } else {
            if (inboundChannel.remoteAddress() instanceof InetSocketAddress
                    && inboundChannel.localAddress() instanceof InetSocketAddress) {
                InetSocketAddress clientAddress = (InetSocketAddress) inboundChannel.remoteAddress();
                String sourceAddress = clientAddress.getAddress().getHostAddress();
                int sourcePort = clientAddress.getPort();
                InetSocketAddress proxyAddress = (InetSocketAddress) inboundChannel.localAddress();
                String destinationAddress = proxyAddress.getAddress().getHostAddress();
                int destinationPort = proxyAddress.getPort();
                HAProxyMessage msg = new HAProxyMessage(HAProxyProtocolVersion.V1, HAProxyCommand.PROXY,
                        HAProxyProxiedProtocol.TCP4, sourceAddress, destinationAddress, sourcePort,
                        destinationPort);
                final ByteBuf encodedMsg = encodeProxyProtocolMessage(msg);
                writeAndFlush(encodedMsg);
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
        private final FeatureFlags featureFlags;
        // PIP-478: the v5 exchange this backend connection authenticates through. Replaced on a broker-pushed
        // REFRESH, which starts a fresh exchange. Only ever touched on this channel's event loop.
        private AuthenticationExchange authExchange;
        // PIP-478: round state. AuthenticationExchange is single-round and non-thread-safe, and serializing
        // its rounds is the caller's obligation; this class is its second caller after ClientCnx. While the
        // frame decoder is still running (state Init) two challenge frames arriving in one read reach
        // handleAuthChallenge in the same event-loop turn, before either resolution has completed, and would
        // otherwise drive the same exchange concurrently. Both fields are touched only on this channel's
        // event loop (channelActive, handleAuthChallenge, and the continuations dispatched there), so they
        // need no synchronization.
        private boolean authRoundInProgress;
        private int authChallengeRounds;

        public ProxyBackendHandler(ProxyConfiguration config, int protocolVersion, String remoteHostName,
                                   FeatureFlags featureFlags) {
            this.config = config;
            this.protocolVersion = protocolVersion;
            this.remoteHostName = remoteHostName;
            this.featureFlags = featureFlags;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            this.ctx = ctx;

            if (config.isHaProxyProtocolEnabled()) {
                writeHAProxyMessage();
            }
            isTlsOutboundChannel = ProxyConnection.isTlsChannel(inboundChannel);

            // Send the Connect command to broker. PIP-478: the credential is resolved through a v5 exchange
            // rather than by calling the v4 plugin here. This method runs on the Netty event loop, and the v4
            // call it used to make is arbitrary plugin code — an OAuth2 token endpoint round trip, an Athenz
            // ZTS fetch, a GSSAPI exchange with the KDC — which stalled every connection multiplexed onto
            // that loop for its duration. The exchange's calls always off-load.
            //
            // The auth method name is read from the v4 plugin here and in handleAuthChallenge below, where
            // ClientCnx instead takes it from the exchange that produced the credential. Both are correct for
            // the proxy — it owns one started plugin, and the bridge's authMethodName() delegates straight to
            // it — and reading it from the plugin does not depend on a round having completed. Deliberate.
            authExchange = service.getProxyClientAuthenticationDriver().newAuthenticationExchange(remoteHostName);
            sendWhenResolved(authExchange.getAuthDataAsync(),
                    authData -> Commands.newConnect(
                            authentication.getAuthMethodName(), authData, protocolVersion,
                            proxyConnection.clientVersion, null /* target broker */,
                            originalPrincipal, clientAuthData, clientAuthMethod, PulsarVersion.getVersion(),
                            featureFlags),
                    "connect");
        }

        /**
         * Send a command built from an asynchronously-resolved credential (PIP-478).
         *
         * <p>The continuation is dispatched onto this channel's event loop, so the command is built and
         * written there whether the credential was already in memory or needed I/O — command ordering on the
         * channel is therefore unchanged from the synchronous version. A failure closes the backend channel:
         * the proxy has no credential to send, and leaving the connection open would wait out the broker's
         * timeout instead of letting the client retry. That covers a failure to build the command too, not
         * just a failure to resolve the credential.
         *
         * <p>This is where an authentication round begins: the round is marked in progress so that a
         * challenge arriving before it completes is dropped rather than re-entering the exchange
         * concurrently.
         *
         * @param resolution     the credential being resolved
         * @param commandBuilder builds the command to send from the resolved credential
         * @param what           what is being authenticated, for logging
         */
        private void sendWhenResolved(CompletableFuture<AuthData> resolution,
                                      Function<AuthData, ByteBuf> commandBuilder, String what) {
            authRoundInProgress = true;
            // The future returned by whenCompleteAsync is intentionally discarded: the continuation handles
            // every outcome itself, and the only way that future fails is ctx.executor() rejecting during
            // event-loop shutdown — at which point the channel is already going away.
            resolution.whenCompleteAsync((authData, throwable) -> {
                authRoundInProgress = false;
                if (throwable != null) {
                    Throwable cause = FutureUtil.unwrapCompletionException(throwable);
                    log.error().attr("channel", ctx.channel()).attr("stage", what).exception(cause)
                            .log("Failed to resolve the proxy's broker-client credential");
                    ctx.close();
                    return;
                }
                if (!ctx.channel().isActive()) {
                    // The backend connection went away while the credential was resolving. Logged so that
                    // "backend connected but never sent CommandConnect" is diagnosable rather than silent.
                    log.debug().attr("channel", ctx.channel()).attr("stage", what)
                            .log("Backend channel closed while the proxy's broker-client credential resolved");
                    return;
                }
                try {
                    writeAndFlush(commandBuilder.apply(authData));
                } catch (Throwable t) {
                    log.error().attr("channel", ctx.channel()).attr("stage", what).exception(t)
                            .log("Failed to send the proxy's broker-client authentication command");
                    ctx.close();
                }
            }, ctx.executor());
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
                log.debug().attr("inbound", inboundChannel)
                        .attr("outbound", outboundChannel)
                        .attr("msgClass", msg.getClass())
                        .log("Received msg on broker connection");

                // Do the regular decoding for the Connected message
                super.channelRead(ctx, msg);
                break;

            case HandshakeCompleted:
                ProxyService.OPS_COUNTER.inc();
                if (msg instanceof ByteBuf) {
                    ProxyService.BYTES_COUNTER.inc(((ByteBuf) msg).readableBytes());
                }
                inboundChannel.writeAndFlush(msg, inboundChannel.voidPromise());

                if (service.proxyZeroCopyModeEnabled && service.proxyLogLevel == 0) {
                    if (!isTlsOutboundChannel && !DirectProxyHandler.this.proxyConnection.isTlsInboundChannel) {
                        if (ctx.pipeline().get("readTimeoutHandler") != null) {
                            ctx.pipeline().remove("readTimeoutHandler");
                        }
                        ProxyConnection.spliceNIC2NIC((EpollSocketChannel) ctx.channel(),
                                        (EpollSocketChannel) inboundChannel, ProxyConnection.SPLICE_BYTES)
                                .addListener(future -> {
                                    if (future.isSuccess()) {
                                        ProxyService.OPS_COUNTER.inc();
                                        ProxyService.BYTES_COUNTER.inc(ProxyConnection.SPLICE_BYTES);
                                    }
                                });
                    }
                }

                break;

            default:
                break;
            }

        }

        @Override
        protected void handleAuthChallenge(CommandAuthChallenge authChallenge) {
            checkArgument(authChallenge.hasChallenge());
            checkArgument(authChallenge.getChallenge().hasAuthData() && authChallenge.getChallenge().hasAuthData());

            // PIP-478 binary routing rule 2: the broker's REFRESH sentinel restarts authentication with a
            // fresh exchange whose getAuthDataAsync() re-produces the current credential, rather than being
            // routed into the conversation it just terminated. Any other challenge is a round of the current
            // exchange, whose state slot carries conversation state across rounds. This mirrors ClientCnx.
            // The REFRESH branch is conformance with that rule rather than a path the broker can reach here:
            // this handler stops decoding once state == HandshakeCompleted, and the broker arms its refresh
            // task with an initial delay of authenticationRefreshCheckSeconds after connect completes, so by
            // the time a REFRESH is pushed it is proxied straight through to the client, which answers it.
            // The proxy's own credential refresh lives in ProxyConnection, not here.
            boolean refresh =
                    Arrays.equals(AuthData.REFRESH_AUTH_DATA_BYTES, authChallenge.getChallenge().getAuthData());
            // PIP-478 (serialize-or-drop): a broker never pipelines challenges — it waits for each
            // CommandAuthResponse — so a challenge arriving while a round is still in flight is anomalous,
            // and servicing it would re-enter the same single-round, non-thread-safe exchange concurrently.
            // Dropping it makes rounds strictly serialized, which is why this class needs none of the
            // generation guarding ClientCnx carries: nothing here can supersede an in-flight round. ClientCnx
            // does need it, because it lets a REFRESH supersede one; the proxy can drop a REFRESH instead,
            // both because it cannot reach this handler and because the broker's refresh check is a
            // scheduleAtFixedRate task that would re-send it on the next tick.
            if (authRoundInProgress) {
                log.debug().attr("channel", ctx.channel())
                        .log("Dropping a broker auth challenge received while an auth round is in progress");
                return;
            }
            // Bound the exchange. A REFRESH opens a fresh exchange, so it resets the counter; any other
            // challenge counts towards the cap.
            if (refresh) {
                authChallengeRounds = 0;
            } else if (++authChallengeRounds > MAX_AUTH_CHALLENGE_ROUNDS) {
                log.error().attr("channel", ctx.channel()).attr("maxChallengeRounds", MAX_AUTH_CHALLENGE_ROUNDS)
                        .log("Binary authentication exceeded the maximum challenge rounds; closing the "
                                + "broker connection");
                ctx.close();
                return;
            }
            CompletableFuture<AuthData> resolution;
            try {
                if (refresh) {
                    authExchange =
                            service.getProxyClientAuthenticationDriver().newAuthenticationExchange(remoteHostName);
                    resolution = authExchange.getAuthDataAsync();
                } else {
                    resolution = authExchange
                            .authenticateAsync(AuthData.of(authChallenge.getChallenge().getAuthData()));
                }
            } catch (Throwable t) {
                // One try/catch so a plugin that throws synchronously fails the connection rather than
                // propagating up the event loop.
                resolution = CompletableFuture.failedFuture(t);
            }

            // mutual authn. If auth not complete, continue auth; if auth complete, complete connectionFuture.
            sendWhenResolved(resolution, authData -> {
                checkState(!authData.isComplete());
                log.debug().attr("channel", ctx.channel())
                        .attr("authMethod", authentication.getAuthMethodName())
                        .log("Mutual auth");
                return Commands.newAuthResponse(authentication.getAuthMethodName(),
                        authData,
                        this.protocolVersion,
                        PulsarVersion.getVersion());
            }, "challenge");
        }

        @Override
        protected void messageReceived(BaseCommand cmd) {
            // no-op
        }

        @Override
        protected void handleConnected(CommandConnected connected) {
            checkArgument(state == BackendState.Init, "Unexpected state %s. BackendState.Init was expected.", state);
            log.debug().attr("inbound", inboundChannel)
                    .attr("outbound", outboundChannel)
                    .log("Received Connected from broker");

            state = BackendState.HandshakeCompleted;

            onHandshakeCompleteAction.run();
            startDirectProxying(connected);

            proxyConnection.brokerConnected(DirectProxyHandler.this, connected);
        }

        private void startDirectProxying(CommandConnected connected) {
            if (service.getProxyLogLevel() == 0) {
                log.debug().attr("inbound", inboundChannel)
                        .attr("outbound", outboundChannel)
                        .log("Removing decoder from pipeline");
                // direct tcp proxy
                FrameDecoderUtil.removeFrameDecoder(inboundChannel.pipeline());
                FrameDecoderUtil.removeFrameDecoder(outboundChannel.pipeline());
            } else {
                // Enable parsing feature, proxyLogLevel(1 or 2)
                // Add parser handler
                ParserProxyHandler.Context parserContext = ParserProxyHandler.createContext();
                if (connected.hasMaxMessageSize()) {
                    FrameDecoderUtil.replaceFrameDecoder(inboundChannel.pipeline(),
                            connected.getMaxMessageSize());
                    FrameDecoderUtil.replaceFrameDecoder(outboundChannel.pipeline(),
                            connected.getMaxMessageSize());
                    inboundChannel.pipeline().addBefore("handler", "inboundParser",
                            new ParserProxyHandler(parserContext, service,
                                    ParserProxyHandler.FRONTEND_CONN,
                                    connected.getMaxMessageSize(), outboundChannel.id()));
                    outboundChannel.pipeline().addBefore("proxyOutboundHandler", "outboundParser",
                            new ParserProxyHandler(parserContext, service,
                                    ParserProxyHandler.BACKEND_CONN,
                                    connected.getMaxMessageSize(), inboundChannel.id()));
                } else {
                    inboundChannel.pipeline().addBefore("handler", "inboundParser",
                            new ParserProxyHandler(parserContext, service,
                                    ParserProxyHandler.FRONTEND_CONN,
                                    Commands.DEFAULT_MAX_MESSAGE_SIZE, outboundChannel.id()));
                    outboundChannel.pipeline().addBefore("proxyOutboundHandler", "outboundParser",
                            new ParserProxyHandler(parserContext, service,
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
            log.warn().attr("inbound", inboundChannel)
                    .attr("outbound", outboundChannel)
                    .exception(cause)
                    .log("Caught exception");
            ctx.close();
        }
    }

    private void writeAndFlush(ByteBuf cmd) {
        NettyChannelUtil.writeAndFlushWithVoidPromise(outboundChannel, cmd);
    }

}
