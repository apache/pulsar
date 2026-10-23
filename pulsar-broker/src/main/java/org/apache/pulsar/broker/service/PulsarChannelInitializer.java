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
package org.apache.pulsar.broker.service;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContext;
import lombok.Builder;
import lombok.CustomLog;
import lombok.Data;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.tls.DefaultBrokerTlsFactory;
import org.apache.pulsar.broker.tls.TlsFactorySupport;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.protocol.OptionalProxyProtocolDecoder;
import org.apache.pulsar.common.tls.impl.TlsContextAcquisition;
import org.apache.pulsar.common.tls.impl.TlsSynthesisSpec;
import org.apache.pulsar.common.util.netty.PulsarFlowControlHandler;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;

@CustomLog
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final PulsarService pulsar;
    private final String listenerName;
    private final boolean enableTls;
    private final ServiceConfiguration brokerConf;
    // PIP-478 TLS SPI factory (the only server TLS path since PIP-337 removal).
    private PulsarTlsFactory tlsFactory;
    private TlsHandle<SslContext> tlsSubscription;
    private volatile SslContext tlsServerContext;

    /**
     * @param pulsar
     *              An instance of {@link PulsarService}
     * @param opts
     *              Channel options
     */
    public PulsarChannelInitializer(PulsarService pulsar, PulsarChannelOptions opts) throws Exception {
        super();
        this.pulsar = pulsar;
        this.listenerName = opts.getListenerName();
        this.enableTls = opts.isEnableTLS();
        ServiceConfiguration serviceConfig = pulsar.getConfiguration();
        if (this.enableTls) {
            initializeTlsFactory(serviceConfig);
        }
        this.brokerConf = pulsar.getConfiguration();
    }

    // PIP-478: build the PulsarTlsFactory, initialize it, and subscribe to the BROKER purpose. A volatile
    // Netty SslContext holds the latest instance; rotation is delivered by the subscription (no refresh
    // task). The subscription's first delivery happens-before its future completes, so tlsServerContext is
    // set by the time this returns.
    private void initializeTlsFactory(ServiceConfiguration serviceConfig) throws Exception {
        this.tlsFactory = TlsFactorySupport.createFactory(serviceConfig.getTlsFactoryClassName(),
                DefaultBrokerTlsFactory.class,
                () -> DefaultBrokerTlsFactory.fromServiceConfiguration(serviceConfig));
        try {
            TlsFactoryInitContext initContext = TlsFactorySupport.initContext(
                    TlsFactorySupport.parseFactoryConfig(serviceConfig.getTlsFactoryConfig()),
                    pulsar.getExecutor(), pulsar.getExecutor(), pulsar.getOpenTelemetry().getOpenTelemetry());
            TlsFactorySupport.initializeBlocking(this.tlsFactory, initContext);
            this.tlsSubscription = TlsContextAcquisition.acquireNettyContext(this.tlsFactory, TlsPurpose.BROKER,
                            TlsSynthesisSpec.server(serviceConfig.isTlsRequireTrustedClientCertOnConnect()),
                            ctx -> this.tlsServerContext = ctx)
                    .get()
                    .orElseThrow(() -> new IllegalStateException(
                            "TLS factory supplied no Netty SslContext for purpose " + TlsPurpose.BROKER));
        } catch (Exception e) {
            // Ctor-throw cleanup: the factory was created (and possibly initialized, its rotation
            // scheduler running) but the subscription failed; close() will not be called on a half-constructed
            // initializer, so release it here. close() is null-safe and idempotent.
            close();
            throw e;
        }
    }

    /** Dispose the PIP-478 TLS factory and its subscription, if any. Safe to call more than once. */
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
        // disable auto read explicitly so that requests aren't served until auto read is enabled
        // ServerCnx must enable auto read in channelActive after PulsarService is ready to accept incoming requests
        ch.config().setAutoRead(false);
        ch.config().setWriteBufferHighWaterMark(pulsar.getConfig().getPulsarChannelWriteBufferHighWaterMark());
        ch.config().setWriteBufferLowWaterMark(pulsar.getConfig().getPulsarChannelWriteBufferLowWaterMark());
        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(1024, true));
        if (this.enableTls) {
            // PIP-478: build the handler from the current (possibly rotated) factory-owned SslContext, pinning
            // it across newHandler so a concurrent rotation cannot free the native OpenSSL context mid-build
            // (use-after-free guard).
            ch.pipeline().addLast(TLS_HANDLER, TlsContextAcquisition.withPinnedContext(
                    () -> this.tlsServerContext, ctx -> ctx.newHandler(ch.alloc())));
        }
        ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.getEncoder(this.enableTls));

        if (pulsar.getConfiguration().isHaProxyProtocolEnabled()) {
            ch.pipeline().addLast(OptionalProxyProtocolDecoder.NAME, new OptionalProxyProtocolDecoder());
        }
        FrameDecoderUtil.addFrameDecoder(ch.pipeline(), brokerConf.getMaxMessageSize());
        // https://stackoverflow.com/questions/37535482/netty-disabling-auto-read-doesnt-work-for-bytetomessagedecoder
        // Classes such as {@link ByteToMessageDecoder} or {@link MessageToByteEncoder} are free to emit as many events
        // as they like for any given input. so, disabling auto-read on `ByteToMessageDecoder` doesn't work properly and
        // ServerCnx ends up reading higher number of messages and broker can not throttle the messages by disabling
        // auto-read.
        // PulsarFlowControlHandler is used instead of Netty's FlowControlHandler since Netty 4.2.15 changed the
        // behavior to ignore setAutoRead(false) made by a downstream handler while queued messages are being
        // delivered, which breaks throttling of already buffered messages. See PulsarFlowControlHandler javadoc.
        ch.pipeline().addLast("flowController", new PulsarFlowControlHandler());
        // using "ChannelHandler" type to workaround an IntelliJ bug that shows a false positive error
        ChannelHandler cnx = newServerCnx(pulsar, listenerName);
        ch.pipeline().addLast("handler", cnx);
    }

    @VisibleForTesting
    protected ServerCnx newServerCnx(PulsarService pulsar, String listenerName) throws Exception {
        return new ServerCnx(pulsar, listenerName);
    }

    public interface Factory {
        PulsarChannelInitializer newPulsarChannelInitializer(
                PulsarService pulsar, PulsarChannelOptions opts) throws Exception;
    }

    public static final Factory DEFAULT_FACTORY = PulsarChannelInitializer::new;

    @Data
    @Builder
    public static class PulsarChannelOptions {

        /**
         * Indicates whether to enable TLS on the channel.
         */
        private boolean enableTLS;

        /**
         * The name of the listener to associate with the channel (optional).
         */
        private String listenerName;
    }
}
