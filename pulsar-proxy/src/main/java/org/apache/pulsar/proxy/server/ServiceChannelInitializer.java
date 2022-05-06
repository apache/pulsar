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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.OptionalProxyProtocolDecoder;
import org.apache.pulsar.common.util.NettyClientSslContextRefresher;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;

/**
 * Initialize service channel handlers.
 *
 */
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";
    private final ProxyService proxyService;
    private final boolean enableTls;
    private final boolean tlsEnabledWithKeyStore;
    private final int brokerProxyReadTimeoutMs;

    private SslContextAutoRefreshBuilder<SslContext> serverSslCtxRefresher;
    private SslContextAutoRefreshBuilder<SslContext> clientSslCtxRefresher;
    private NettySSLContextAutoRefreshBuilder serverSSLContextAutoRefreshBuilder;
    private NettySSLContextAutoRefreshBuilder clientSSLContextAutoRefreshBuilder;

    public ServiceChannelInitializer(ProxyService proxyService, ProxyConfiguration serviceConfig, boolean enableTls)
            throws Exception {
        super();
        this.proxyService = proxyService;
        this.enableTls = enableTls;
        this.tlsEnabledWithKeyStore = serviceConfig.isTlsEnabledWithKeyStore();
        this.brokerProxyReadTimeoutMs = serviceConfig.getBrokerProxyReadTimeoutMs();

        if (enableTls) {
            if (tlsEnabledWithKeyStore) {
                serverSSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                        serviceConfig.getTlsProvider(),
                        serviceConfig.getTlsKeyStoreType(),
                        serviceConfig.getTlsKeyStore(),
                        serviceConfig.getTlsKeyStorePassword(),
                        serviceConfig.isTlsAllowInsecureConnection(),
                        serviceConfig.getTlsTrustStoreType(),
                        serviceConfig.getTlsTrustStore(),
                        serviceConfig.getTlsTrustStorePassword(),
                        serviceConfig.isTlsRequireTrustedClientCertOnConnect(),
                        serviceConfig.getTlsCiphers(),
                        serviceConfig.getTlsProtocols(),
                        serviceConfig.getTlsCertRefreshCheckDurationSec());
            } else {
                SslProvider sslProvider = null;
                if (serviceConfig.getTlsProvider() != null) {
                    sslProvider = SslProvider.valueOf(serviceConfig.getTlsProvider());
                }
                serverSslCtxRefresher = new NettyServerSslContextBuilder(
                        sslProvider,
                        serviceConfig.isTlsAllowInsecureConnection(),
                        serviceConfig.getTlsTrustCertsFilePath(), serviceConfig.getTlsCertificateFilePath(),
                        serviceConfig.getTlsKeyFilePath(), serviceConfig.getTlsCiphers(),
                        serviceConfig.getTlsProtocols(),
                        serviceConfig.isTlsRequireTrustedClientCertOnConnect(),
                        serviceConfig.getTlsCertRefreshCheckDurationSec());
            }
        } else {
            this.serverSslCtxRefresher = null;
        }

        if (serviceConfig.isTlsEnabledWithBroker()) {
            AuthenticationDataProvider authData = null;

            if (!isEmpty(serviceConfig.getBrokerClientAuthenticationPlugin())) {
                authData = AuthenticationFactory.create(serviceConfig.getBrokerClientAuthenticationPlugin(),
                        serviceConfig.getBrokerClientAuthenticationParameters()).getAuthData();
            }

            if (tlsEnabledWithKeyStore) {
                clientSSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                        serviceConfig.getBrokerClientSslProvider(),
                        serviceConfig.isTlsAllowInsecureConnection(),
                        serviceConfig.getBrokerClientTlsTrustStoreType(),
                        serviceConfig.getBrokerClientTlsTrustStore(),
                        serviceConfig.getBrokerClientTlsTrustStorePassword(),
                        serviceConfig.getBrokerClientTlsCiphers(),
                        serviceConfig.getBrokerClientTlsProtocols(),
                        serviceConfig.getTlsCertRefreshCheckDurationSec(),
                        authData);
            } else {
                SslProvider sslProvider = null;
                if (serviceConfig.getBrokerClientSslProvider() != null) {
                    sslProvider = SslProvider.valueOf(serviceConfig.getBrokerClientSslProvider());
                }
                clientSslCtxRefresher = new NettyClientSslContextRefresher(
                        sslProvider,
                        serviceConfig.isTlsAllowInsecureConnection(),
                        serviceConfig.getBrokerClientTrustCertsFilePath(),
                        authData,
                        serviceConfig.getBrokerClientTlsCiphers(),
                        serviceConfig.getBrokerClientTlsProtocols(),
                        serviceConfig.getTlsCertRefreshCheckDurationSec()
                );
            }
        } else {
            this.clientSslCtxRefresher = null;
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (serverSslCtxRefresher != null && this.enableTls) {
            SslContext sslContext = serverSslCtxRefresher.get();
            if (sslContext != null) {
                ch.pipeline().addLast(TLS_HANDLER, sslContext.newHandler(ch.alloc()));
            }
        } else if (this.tlsEnabledWithKeyStore && serverSSLContextAutoRefreshBuilder != null) {
            ch.pipeline().addLast(TLS_HANDLER,
                    new SslHandler(serverSSLContextAutoRefreshBuilder.get().createSSLEngine()));
        }
        if (brokerProxyReadTimeoutMs > 0) {
            ch.pipeline().addLast("readTimeoutHandler",
                    new ReadTimeoutHandler(brokerProxyReadTimeoutMs, TimeUnit.MILLISECONDS));
        }
        if (proxyService.getConfiguration().isHaProxyProtocolEnabled()) {
            ch.pipeline().addLast(OptionalProxyProtocolDecoder.NAME, new OptionalProxyProtocolDecoder());
        }
        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                Commands.DEFAULT_MAX_MESSAGE_SIZE + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4));

        Supplier<SslHandler> sslHandlerSupplier = null;
        if (clientSslCtxRefresher != null) {
            sslHandlerSupplier = new Supplier<SslHandler>() {
                @Override
                public SslHandler get() {
                    return clientSslCtxRefresher.get().newHandler(ch.alloc());
                }
            };
        } else if (clientSSLContextAutoRefreshBuilder != null) {
            sslHandlerSupplier = new Supplier<SslHandler>() {
                @Override
                public SslHandler get() {
                    return new SslHandler(clientSSLContextAutoRefreshBuilder.get().createSSLEngine());
                }
            };
        }

        ch.pipeline().addLast("handler",
                new ProxyConnection(proxyService, sslHandlerSupplier, proxyService.getDnsAddressResolverGroup()));

    }
}
