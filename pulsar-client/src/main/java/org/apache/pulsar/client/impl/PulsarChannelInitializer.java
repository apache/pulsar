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

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.pulsar.client.api.Socks5ProxyScope;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.tls.impl.TlsContextAcquisition;
import org.apache.pulsar.common.tls.impl.TlsSynthesisSpec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.NettyFutureUtil;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsEndpoint;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;

@CustomLog
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final Supplier<ClientCnx> clientCnxSupplier;
    @Getter
    private final boolean tlsEnabled;
    private final InetSocketAddress socks5ProxyAddress;
    private final String socks5ProxyUsername;
    private final String socks5ProxyPassword;
    private final Socks5ProxyScope socks5ProxyScope;
    private final ClientConfigurationData conf;
    // PIP-478 client TLS SPI factory (the only client TLS path since the PIP-337 removal); non-null
    // whenever TLS is enabled. The per-connection SslContext is built off the event loop by the factory
    // (see initTls), which owns rotation, so there is no per-host refresh task.
    // Volatile + swappable (PIP-478): AutoClusterFailover rebuilds the client's factory from the updated
    // ClientConfigurationData and swaps the reference here so NEW connections pick up per-target trust roots /
    // TLS identity; each connection reads it once at initTls time (existing connections keep their factory).
    private volatile PulsarTlsFactory clientTlsFactory;
    // PIP-478: the hostname-verification setting baked into a framework-synthesized Netty context when a
    // custom factory supplies only the JDK SSLContext fallback for CLIENT_DEFAULT.
    private final TlsSynthesisSpec tlsSynthesisSpec;

    public PulsarChannelInitializer(ClientConfigurationData conf, Supplier<ClientCnx> clientCnxSupplier,
                                    ScheduledExecutorService scheduledExecutorService) throws Exception {
        super();
        this.clientCnxSupplier = clientCnxSupplier;
        this.tlsEnabled = conf.isUseTls();
        this.socks5ProxyAddress = conf.getSocks5ProxyAddress();
        this.socks5ProxyUsername = conf.getSocks5ProxyUsername();
        this.socks5ProxyPassword = conf.getSocks5ProxyPassword();
        this.socks5ProxyScope = conf.getSocks5ProxyScope();
        this.conf = conf.clone();
        // The resolved client TLS factory (PIP-478) rides on the (shallow-cloned) conf.
        this.clientTlsFactory = tlsEnabled ? conf.getTlsFactory() : null;
        this.tlsSynthesisSpec = TlsSynthesisSpec.client(conf.isTlsHostnameVerificationEnable());
    }

    /**
     * Swap the client TLS factory NEW connections build their {@code SslContext} from (PIP-478). Called
     * when the client's TLS material/config changes at runtime (AutoClusterFailover rebuilds the factory from
     * the updated configuration). A no-op when TLS is disabled. Connections already established, and any
     * connection currently mid-setup that already read the previous reference, keep the factory they were
     * built with; the caller keeps the superseded factory alive until the client closes.
     *
     * @param factory the rebuilt factory
     */
    void setClientTlsFactory(PulsarTlsFactory factory) {
        if (tlsEnabled) {
            this.clientTlsFactory = factory;
        }
    }

    @VisibleForTesting
    PulsarTlsFactory getClientTlsFactory() {
        return clientTlsFactory;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(1024, true));

        // Setup channel except for the SsHandler for TLS enabled connections
        ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.getEncoder(tlsEnabled));
        FrameDecoderUtil.addFrameDecoder(ch.pipeline(), Commands.DEFAULT_MAX_MESSAGE_SIZE);
        ChannelHandler clientCnx = clientCnxSupplier.get();
        ch.pipeline().addLast("handler", clientCnx);
    }

    /**
     * Initialize TLS for a channel. Should be invoked before the channel is connected to the remote address.
     *
     * @param ch      the channel
     * @param sniHost the value of this argument will be passed as peer host and port when creating the SSLEngine (which
     *                in turn will use these values to set SNI header when doing the TLS handshake). Cannot be
     *                <code>null</code>.
     * @return a {@link CompletableFuture} that completes when the TLS is set up.
     */
    CompletableFuture<Channel> initTls(Channel ch, InetSocketAddress sniHost) {
        if (ch == null) {
            return FutureUtil.failedFuture(new NullPointerException("A channel is required"));
        }
        if (sniHost == null) {
            return FutureUtil.failedFuture(new NullPointerException("A sniHost is required"));
        }
        if (!tlsEnabled) {
            return FutureUtil.failedFuture(new IllegalStateException("TLS is not enabled in client configuration"));
        }
        return initTlsWithFactory(ch, sniHost);
    }

    /**
     * New PIP-478 TLS path: build the connection's {@code SslContext} through the client TLS
     * SPI factory using the one-shot form with the destination endpoint as a hint. The build runs on the
     * factory's blocking executor (off the Netty event loop), so — unlike the legacy path, which builds
     * the context inline in {@code computeIfAbsent} on the event loop — nothing blocks the event loop.
     * The synchronous per-connection work is only the pipeline mutation, hopped back onto the event loop.
     *
     * <p>Hostname verification is baked into the factory-built context (per the client {@code TlsPolicy}),
     * so no per-connection endpoint-identification override is re-applied here; the SNI host/port drive both
     * the SNI header and the verification target. The one-shot handle retains the factory-owned context
     * for the connection's lifetime and is disposed when the channel closes.
     *
     * <p>Design note (folds into the PIP): the PIP's one-shot-per-connection guidance is followed here
     * (rather than the server-style subscription DirectProxyHandler used) because the client's
     * {@code initTls} is already asynchronous — it returns a {@code CompletableFuture} composed into the
     * connect chain before the TCP connect — so the async {@code createInstance} fits naturally and lets
     * the endpoint hint through. The HTTP lookup path, whose AsyncHttpClient {@code SslEngineFactory} is
     * synchronous, instead uses the subscribing form (see {@code HttpClient}).
     */
    private CompletableFuture<Channel> initTlsWithFactory(Channel ch, InetSocketAddress sniHost) {
        // Read the current factory once for this connection; a concurrent AutoClusterFailover swap only affects
        // connections established after it (existing connections and this one keep the factory read here).
        PulsarTlsFactory factory = this.clientTlsFactory;
        return TlsContextAcquisition.acquireNettyContext(factory, TlsPurpose.CLIENT_DEFAULT,
                        new TlsEndpoint(sniHost.getHostName(), sniHost.getPort()), tlsSynthesisSpec)
                .thenCompose(optHandle -> {
                    CompletableFuture<Channel> future = new CompletableFuture<>();
                    try {
                        ch.eventLoop().execute(() -> {
                            TlsHandle<SslContext> handle = null;
                            try {
                                handle = optHandle.orElseThrow(() -> new IllegalStateException(
                                        "Client TLS factory supplied no Netty SslContext for purpose "
                                                + TlsPurpose.CLIENT_DEFAULT));
                                SslHandler handler = handle.get()
                                        .newHandler(ch.alloc(), sniHost.getHostName(), sniHost.getPort());
                                ch.pipeline().addFirst(TLS_HANDLER, handler);
                                // Release the retained one-shot context when this connection closes.
                                TlsHandle<SslContext> acquired = handle;
                                ch.closeFuture().addListener(closeFuture -> acquired.dispose());
                                future.complete(ch);
                            } catch (Throwable t) {
                                // Dispose the retained one-shot context if we acquired it but failed before wiring
                                // the close-future disposal above, else it leaks its Netty ref. Reaching here
                                // means the close-future listener was not registered, so there is no double dispose.
                                if (handle != null) {
                                    handle.dispose();
                                }
                                future.completeExceptionally(t);
                            }
                        });
                    } catch (Throwable rejected) {
                        // A terminated event loop rejects the task synchronously; the runnable never ran, so
                        // the acquired one-shot context must be disposed here or it leaks its Netty ref.
                        optHandle.ifPresent(TlsHandle::dispose);
                        future.completeExceptionally(rejected);
                    }
                    return future;
                });
    }

    CompletableFuture<Channel> initSocks5IfConfig(Channel ch) {
        CompletableFuture<Channel> initSocks5Future = new CompletableFuture<>();
        // Only apply SOCKS5 to the binary protocol path when the scope includes binary connections.
        if (socks5ProxyAddress != null && socks5ProxyScope.appliesToBinary()) {
            ch.eventLoop().execute(() -> {
                try {
                    Socks5ProxyHandler socks5ProxyHandler =
                            new Socks5ProxyHandler(socks5ProxyAddress, socks5ProxyUsername, socks5ProxyPassword);
                    ch.pipeline().addFirst(socks5ProxyHandler.protocol(), socks5ProxyHandler);
                    initSocks5Future.complete(ch);
                } catch (Throwable t) {
                    initSocks5Future.completeExceptionally(t);
                }
            });
        } else {
            initSocks5Future.complete(ch);
        }

        return initSocks5Future;
    }

    /**
     * Sentinel logical address marking a connection that the proxy should pair to any broker it
     * selects (the client sends an empty proxyToBrokerUrl). It is never resolved or dialed — only
     * the physical address (the proxy) is — and is matched by identity.
     */
    static final InetSocketAddress PROXY_TO_ANY_BROKER =
            InetSocketAddress.createUnresolved("proxy-to-any-broker.pulsar.invalid", 0);

    CompletableFuture<Channel> initializeClientCnx(Channel ch,
                                                   InetSocketAddress logicalAddress,
                                                   InetSocketAddress unresolvedPhysicalAddress) {
        return NettyFutureUtil.toCompletableFuture(ch.eventLoop().submit(() -> {
            final ClientCnx cnx = (ClientCnx) ch.pipeline().get("handler");

            if (cnx == null) {
                throw new IllegalStateException("Missing ClientCnx. This should not happen.");
            }

            if (logicalAddress == PROXY_TO_ANY_BROKER) {
                // Pair through the proxy to any broker: send an empty proxyToBrokerUrl so the proxy
                // selects a broker and bridges this connection to it.
                cnx.setProxyToAnyBroker();
            } else if (!logicalAddress.equals(unresolvedPhysicalAddress)) {
                // We are connecting through a proxy. We need to set the target broker in the ClientCnx object so that
                // it can be specified when sending the CommandConnect.
                cnx.setTargetBroker(logicalAddress);
            }

            cnx.setRemoteHostName(unresolvedPhysicalAddress.getHostString());

            return ch;
        }));
    }

}

