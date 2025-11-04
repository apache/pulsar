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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.protocol.OptionalProxyProtocolDecoder;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initialize service channel handlers.
 *
 */
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {
    private static final Logger log = LoggerFactory.getLogger(ServiceChannelInitializer.class);

    public static final String TLS_HANDLER = "tls";
    private final ProxyService proxyService;
    private final boolean enableTls;
    private final boolean tlsEnabledWithKeyStore;
    private final int brokerProxyReadTimeoutMs;
    private final int maxMessageSize;

    private PulsarSslFactory sslFactory;

    public ServiceChannelInitializer(ProxyService proxyService, ProxyConfiguration serviceConfig,
                                     boolean enableTls, ScheduledExecutorService sslContextRefresher)
            throws Exception {
        super();
        this.proxyService = proxyService;
        this.enableTls = enableTls;
        this.tlsEnabledWithKeyStore = serviceConfig.isTlsEnabledWithKeyStore();
        this.brokerProxyReadTimeoutMs = serviceConfig.getBrokerProxyReadTimeoutMs();
        this.maxMessageSize = serviceConfig.getMaxMessageSize();

        if (enableTls) {
            PulsarSslConfiguration sslConfiguration = buildSslConfiguration(serviceConfig);
            this.sslFactory = (PulsarSslFactory) Class.forName(serviceConfig.getSslFactoryPlugin())
                    .getConstructor().newInstance();
            this.sslFactory.initialize(sslConfiguration);
            this.sslFactory.createInternalSslContext();
            if (serviceConfig.getTlsCertRefreshCheckDurationSec() > 0) {
                sslContextRefresher.scheduleWithFixedDelay(this::refreshSslContext,
                        serviceConfig.getTlsCertRefreshCheckDurationSec(),
                        serviceConfig.getTlsCertRefreshCheckDurationSec(), TimeUnit.SECONDS);
            }
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(1024,
                true));
        if (this.enableTls) {
            ch.pipeline().addLast(TLS_HANDLER, new SslHandler(this.sslFactory.createServerSslEngine(ch.alloc())));
        }
        if (brokerProxyReadTimeoutMs > 0) {
            ch.pipeline().addLast("readTimeoutHandler",
                    new ReadTimeoutHandler(brokerProxyReadTimeoutMs, TimeUnit.MILLISECONDS));
        }
        if (proxyService.getConfiguration().isHaProxyProtocolEnabled()) {
            ch.pipeline().addLast(OptionalProxyProtocolDecoder.NAME, new OptionalProxyProtocolDecoder());
        }
        FrameDecoderUtil.addFrameDecoder(ch.pipeline(), maxMessageSize);
        ch.pipeline().addLast("handler", new ProxyConnection(proxyService, proxyService.getDnsAddressResolverGroup()));
    }

    protected PulsarSslConfiguration buildSslConfiguration(ProxyConfiguration config) {
        return PulsarSslConfiguration.builder()
                .tlsProvider(config.getTlsProvider())
                .tlsKeyStoreType(config.getTlsKeyStoreType())
                .tlsKeyStorePath(config.getTlsKeyStore())
                .tlsKeyStorePassword(config.getTlsKeyStorePassword())
                .tlsTrustStoreType(config.getTlsTrustStoreType())
                .tlsTrustStorePath(config.getTlsTrustStore())
                .tlsTrustStorePassword(config.getTlsTrustStorePassword())
                .tlsCiphers(config.getTlsCiphers())
                .tlsProtocols(config.getTlsProtocols())
                .tlsTrustCertsFilePath(config.getTlsTrustCertsFilePath())
                .tlsCertificateFilePath(config.getTlsCertificateFilePath())
                .tlsKeyFilePath(config.getTlsKeyFilePath())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(config.isTlsRequireTrustedClientCertOnConnect())
                .tlsEnabledWithKeystore(config.isTlsEnabledWithKeyStore())
                .tlsCustomParams(config.getSslFactoryPluginParams())
                .authData(null)
                .serverMode(true)
                .build();
    }

    protected void refreshSslContext() {
        try {
            this.sslFactory.update();
        } catch (Exception e) {
            log.error("Failed to refresh SSL context", e);
        }
    }
}
