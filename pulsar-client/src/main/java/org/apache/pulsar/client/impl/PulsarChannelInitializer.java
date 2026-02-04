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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.ssl.SslHandler;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.netty.NettyFutureUtil;

@Slf4j
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final Supplier<ClientCnx> clientCnxSupplier;
    @Getter
    private final boolean tlsEnabled;
    private final boolean tlsHostnameVerificationEnabled;
    private final InetSocketAddress socks5ProxyAddress;
    private final String socks5ProxyUsername;
    private final String socks5ProxyPassword;
    private final ClientConfigurationData conf;
    private final Map<String, PulsarSslFactory> pulsarSslFactoryMap;

    private static final long TLS_CERTIFICATE_CACHE_MILLIS = TimeUnit.MINUTES.toMillis(1);

    public PulsarChannelInitializer(ClientConfigurationData conf, Supplier<ClientCnx> clientCnxSupplier,
                                    ScheduledExecutorService scheduledExecutorService) throws Exception {
        super();
        this.clientCnxSupplier = clientCnxSupplier;
        this.tlsEnabled = conf.isUseTls();
        this.tlsHostnameVerificationEnabled = conf.isTlsHostnameVerificationEnable();
        this.socks5ProxyAddress = conf.getSocks5ProxyAddress();
        this.socks5ProxyUsername = conf.getSocks5ProxyUsername();
        this.socks5ProxyPassword = conf.getSocks5ProxyPassword();
        this.conf = conf.clone();
        if (tlsEnabled) {
            this.pulsarSslFactoryMap = new ConcurrentHashMap<>();
            if (scheduledExecutorService != null && conf.getAutoCertRefreshSeconds() > 0) {
                scheduledExecutorService.scheduleWithFixedDelay(() -> this.refreshSslContext(conf),
                        conf.getAutoCertRefreshSeconds(),
                        conf.getAutoCertRefreshSeconds(),
                        TimeUnit.SECONDS);
            }
        } else {
            this.pulsarSslFactoryMap = null;
        }
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
        Objects.requireNonNull(ch, "A channel is required");
        Objects.requireNonNull(sniHost, "A sniHost is required");
        if (!tlsEnabled) {
            throw new IllegalStateException("TLS is not enabled in client configuration");
        }
        CompletableFuture<Channel> initTlsFuture = new CompletableFuture<>();
        ch.eventLoop().execute(() -> {
            try {
                PulsarSslFactory pulsarSslFactory = pulsarSslFactoryMap.computeIfAbsent(sniHost.getHostName(), key -> {
                    try {
                        PulsarSslFactory factory = (PulsarSslFactory) Class.forName(conf.getSslFactoryPlugin())
                                .getConstructor().newInstance();
                        PulsarSslConfiguration sslConfiguration = buildSslConfiguration(conf, key);
                        factory.initialize(sslConfiguration);
                        factory.createInternalSslContext();
                        return factory;
                    } catch (Exception e) {
                        log.error("Unable to initialize and create the ssl context", e);
                        initTlsFuture.completeExceptionally(e);
                        return null;
                    }
                });
                if (pulsarSslFactory == null) {
                    return;
                }
                SslHandler handler = new SslHandler(pulsarSslFactory
                        .createClientSslEngine(ch.alloc(), sniHost.getHostName(), sniHost.getPort()));

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

protected PulsarSslConfiguration buildSslConfiguration(ClientConfigurationData config,
                                                       String host)
            throws PulsarClientException {
        return PulsarSslConfiguration.builder()
                .tlsProvider(config.getSslProvider())
                .tlsKeyStoreType(config.getTlsKeyStoreType())
                .tlsKeyStorePath(config.getTlsKeyStorePath())
                .tlsKeyStorePassword(config.getTlsKeyStorePassword())
                .tlsTrustStoreType(config.getTlsTrustStoreType())
                .tlsTrustStorePath(config.getTlsTrustStorePath())
                .tlsTrustStorePassword(config.getTlsTrustStorePassword())
                .tlsCiphers(config.getTlsCiphers())
                .tlsProtocols(config.getTlsProtocols())
                .tlsTrustCertsFilePath(config.getTlsTrustCertsFilePath())
                .tlsCertificateFilePath(config.getTlsCertificateFilePath())
                .tlsKeyFilePath(config.getTlsKeyFilePath())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(false)
                .tlsEnabledWithKeystore(config.isUseKeyStoreTls())
                .tlsCustomParams(config.getSslFactoryPluginParams())
                .authData(config.getAuthentication().getAuthData(host))
                .serverMode(false)
                .build();
    }

    protected void refreshSslContext(ClientConfigurationData conf) {
        pulsarSslFactoryMap.forEach((key, pulsarSslFactory) -> {
            try {
                try {
                    if (conf.isUseKeyStoreTls()) {
                        pulsarSslFactory.getInternalSslContext();
                    } else {
                        pulsarSslFactory.getInternalNettySslContext();
                    }
                } catch (Exception e) {
                    log.error("SSL Context is not initialized", e);
                    PulsarSslConfiguration sslConfiguration = buildSslConfiguration(conf, key);
                    pulsarSslFactory.initialize(sslConfiguration);
                }
                pulsarSslFactory.update();
            } catch (Exception e) {
                log.error("Failed to refresh SSL context", e);
            }
        });
    }

}

