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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.pulsar.broker.tls.TlsFactorySupport;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.protocol.OptionalProxyProtocolDecoder;
import org.apache.pulsar.common.tls.impl.TlsContextAcquisition;
import org.apache.pulsar.common.tls.impl.TlsSynthesisSpec;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * Initialize service channel handlers.
 *
 */
@CustomLog
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";
    private final ProxyService proxyService;
    private final boolean enableTls;
    private final boolean tlsEnabledWithKeyStore;
    private final int brokerProxyReadTimeoutMs;
    private final int maxMessageSize;

    // PIP-478 TLS SPI factory (the only server TLS path since the PIP-337 removal).
    private PulsarTlsFactory tlsFactory;
    private TlsHandle<SslContext> tlsSubscription;
    private volatile SslContext tlsServerContext;

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
            initializeTlsFactory(serviceConfig, sslContextRefresher);
        }
    }

    // PIP-478: build the PulsarTlsFactory and subscribe to the PROXY purpose; the volatile Netty SslContext
    // holds the latest instance (rotation delivered by the subscription; no cert-refresh task).
    private void initializeTlsFactory(ProxyConfiguration serviceConfig,
                                      ScheduledExecutorService scheduler) throws Exception {
        this.tlsFactory = TlsFactorySupport.createFactory(serviceConfig.getTlsFactoryClassName(), null,
                () -> ProxyTlsFactories.serverFactory(serviceConfig, TlsPurpose.PROXY,
                        serviceConfig.getTlsCiphers(), serviceConfig.getTlsProtocols()));
        try {
            TlsFactoryInitContext initContext = TlsFactorySupport.initContext(
                    TlsFactorySupport.parseFactoryConfig(serviceConfig.getTlsFactoryConfig()),
                    scheduler, scheduler, proxyService.getOpenTelemetry().getOpenTelemetry());
            TlsFactorySupport.initializeBlocking(this.tlsFactory, initContext);
            this.tlsSubscription = TlsContextAcquisition.acquireNettyContext(this.tlsFactory, TlsPurpose.PROXY,
                            TlsSynthesisSpec.server(serviceConfig.isTlsRequireTrustedClientCertOnConnect()),
                            ctx -> this.tlsServerContext = ctx)
                    .get()
                    .orElseThrow(() -> new IllegalStateException(
                            "TLS factory supplied no Netty SslContext for purpose " + TlsPurpose.PROXY));
        } catch (Exception e) {
            // Ctor-throw cleanup: the factory was created (and possibly initialized, its rotation
            // scheduler running) but the subscription failed; close() will not be called on a half-constructed
            // initializer, so release it here. close() is null-safe and idempotent.
            close();
            throw e;
        }
    }

    /** Dispose the PIP-478 TLS factory and its subscription, if any. */
    public void close() {
        TlsHandle<SslContext> subscription = this.tlsSubscription;
        if (subscription != null) {
            this.tlsSubscription = null;
            subscription.dispose();
        }
        PulsarTlsFactory factory = this.tlsFactory;
        if (factory != null) {
            this.tlsFactory = null;
            factory.close();
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(1024,
                true));
        if (this.enableTls) {
            // PIP-478: pin the current (possibly rotated) factory-owned SslContext across newHandler so a
            // concurrent rotation cannot free the native OpenSSL context mid-build (use-after-free guard).
            ch.pipeline().addLast(TLS_HANDLER, TlsContextAcquisition.withPinnedContext(
                    () -> this.tlsServerContext, ctx -> ctx.newHandler(ch.alloc())));
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
}
