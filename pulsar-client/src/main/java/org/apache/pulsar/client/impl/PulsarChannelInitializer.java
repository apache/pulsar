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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ObjectCache;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.netty.NettyFutureUtil;

@Slf4j
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final Supplier<ClientCnx> clientCnxSupplier;
    @Getter
    private final boolean tlsEnabled;
    private final boolean tlsHostnameVerificationEnabled;
    private final boolean tlsEnabledWithKeyStore;
    private final InetSocketAddress socks5ProxyAddress;
    private final String socks5ProxyUsername;
    private final String socks5ProxyPassword;

    private final Supplier<SslContext> sslContextSupplier;
    private NettySSLContextAutoRefreshBuilder nettySSLContextAutoRefreshBuilder;

    private static final long TLS_CERTIFICATE_CACHE_MILLIS = TimeUnit.MINUTES.toMillis(1);

    public PulsarChannelInitializer(ClientConfigurationData conf, Supplier<ClientCnx> clientCnxSupplier)
            throws Exception {
        super();
        this.clientCnxSupplier = clientCnxSupplier;
        this.tlsEnabled = conf.isUseTls();
        this.tlsHostnameVerificationEnabled = conf.isTlsHostnameVerificationEnable();
        this.socks5ProxyAddress = conf.getSocks5ProxyAddress();
        this.socks5ProxyUsername = conf.getSocks5ProxyUsername();
        this.socks5ProxyPassword = conf.getSocks5ProxyPassword();

        this.tlsEnabledWithKeyStore = conf.isUseKeyStoreTls();

        if (tlsEnabled) {
            if (tlsEnabledWithKeyStore) {
                AuthenticationDataProvider authData1 = conf.getAuthentication().getAuthData();
                if (StringUtils.isBlank(conf.getTlsTrustStorePath())) {
                    throw new PulsarClientException("Failed to create TLS context, the tlsTrustStorePath"
                            + " need to be configured if useKeyStoreTls enabled");
                }
                nettySSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                            conf.getSslProvider(),
                            conf.isTlsAllowInsecureConnection(),
                            conf.getTlsTrustStoreType(),
                            conf.getTlsTrustStorePath(),
                            conf.getTlsTrustStorePassword(),
                            conf.getTlsCiphers(),
                            conf.getTlsProtocols(),
                            TLS_CERTIFICATE_CACHE_MILLIS,
                            authData1);
            }

            sslContextSupplier = new ObjectCache<SslContext>(() -> {
                try {
                    SslProvider sslProvider = null;
                    if (conf.getSslProvider() != null) {
                        sslProvider = SslProvider.valueOf(conf.getSslProvider());
                    }

                    // Set client certificate if available
                    AuthenticationDataProvider authData = conf.getAuthentication().getAuthData();
                    if (authData.hasDataForTls()) {
                        return authData.getTlsTrustStoreStream() == null
                                ? SecurityUtility.createNettySslContextForClient(
                                sslProvider,
                                conf.isTlsAllowInsecureConnection(),
                                conf.getTlsTrustCertsFilePath(),
                                authData.getTlsCertificates(),
                                authData.getTlsPrivateKey(),
                                conf.getTlsCiphers(),
                                conf.getTlsProtocols())
                                : SecurityUtility.createNettySslContextForClient(sslProvider,
                                conf.isTlsAllowInsecureConnection(),
                                authData.getTlsTrustStoreStream(),
                                authData.getTlsCertificates(), authData.getTlsPrivateKey(),
                                conf.getTlsCiphers(),
                                conf.getTlsProtocols());
                    } else {
                        return SecurityUtility.createNettySslContextForClient(
                                sslProvider,
                                conf.isTlsAllowInsecureConnection(),
                                conf.getTlsTrustCertsFilePath(),
                                conf.getTlsCiphers(),
                                conf.getTlsProtocols());
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create TLS context", e);
                }
            }, TLS_CERTIFICATE_CACHE_MILLIS, TimeUnit.MILLISECONDS);
        } else {
            sslContextSupplier = null;
        }
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {

        // Setup channel except for the SsHandler for TLS enabled connections

        ch.pipeline().addLast("ByteBufPairEncoder", tlsEnabled ? ByteBufPair.COPYING_ENCODER : ByteBufPair.ENCODER);

        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                Commands.DEFAULT_MAX_MESSAGE_SIZE + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", clientCnxSupplier.get());
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
        Objects.requireNonNull(ch, "A channel is required");
        Objects.requireNonNull(sniHost, "A sniHost is required");
        if (!tlsEnabled) {
            throw new IllegalStateException("TLS is not enabled in client configuration");
        }
        CompletableFuture<Channel> initTlsFuture = new CompletableFuture<>();
        ch.eventLoop().execute(() -> {
            try {
                SslHandler handler = tlsEnabledWithKeyStore
                        ? new SslHandler(nettySSLContextAutoRefreshBuilder.get()
                                .createSSLEngine(sniHost.getHostString(), sniHost.getPort()))
                        : sslContextSupplier.get().newHandler(ch.alloc(), sniHost.getHostString(), sniHost.getPort());

                if (tlsHostnameVerificationEnabled) {
                    SecurityUtility.configureSSLHandler(handler);
                }

                ch.pipeline().addFirst(TLS_HANDLER, handler);
                initTlsFuture.complete(ch);
            } catch (Throwable t) {
                initTlsFuture.completeExceptionally(t);
            }
        });

        return initTlsFuture;
    }

    CompletableFuture<Channel> initSocks5IfConfig(Channel ch) {
        CompletableFuture<Channel> initSocks5Future = new CompletableFuture<>();
        if (socks5ProxyAddress != null) {
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

    CompletableFuture<Channel> initializeClientCnx(Channel ch,
                                                   InetSocketAddress logicalAddress,
                                                   InetSocketAddress unresolvedPhysicalAddress) {
        return NettyFutureUtil.toCompletableFuture(ch.eventLoop().submit(() -> {
            final ClientCnx cnx = (ClientCnx) ch.pipeline().get("handler");

            if (cnx == null) {
                throw new IllegalStateException("Missing ClientCnx. This should not happen.");
            }

            if (!logicalAddress.equals(unresolvedPhysicalAddress)) {
                // We are connecting through a proxy. We need to set the target broker in the ClientCnx object so that
                // it can be specified when sending the CommandConnect.
                cnx.setTargetBroker(logicalAddress);
            }

            cnx.setRemoteHostName(unresolvedPhysicalAddress.getHostString());

            return ch;
        }));
    }
}

