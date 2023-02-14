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
package org.apache.pulsar.broker.service;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flow.FlowControlHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.OptionalProxyProtocolDecoder;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;

@Slf4j
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final PulsarService pulsar;
    private final String listenerName;
    private final boolean enableTls;
    private final boolean tlsEnabledWithKeyStore;
    private SslContextAutoRefreshBuilder<SslContext> sslCtxRefresher;
    private final ServiceConfiguration brokerConf;
    private NettySSLContextAutoRefreshBuilder nettySSLContextAutoRefreshBuilder;

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
        this.tlsEnabledWithKeyStore = serviceConfig.isTlsEnabledWithKeyStore();
        if (this.enableTls) {
            if (tlsEnabledWithKeyStore) {
                nettySSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
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
                sslCtxRefresher = new NettyServerSslContextBuilder(
                        sslProvider,
                        serviceConfig.isTlsAllowInsecureConnection(),
                        serviceConfig.getTlsTrustCertsFilePath(),
                        serviceConfig.getTlsCertificateFilePath(),
                        serviceConfig.getTlsKeyFilePath(),
                        serviceConfig.getTlsCiphers(),
                        serviceConfig.getTlsProtocols(),
                        serviceConfig.isTlsRequireTrustedClientCertOnConnect(),
                        serviceConfig.getTlsCertRefreshCheckDurationSec());
            }
        } else {
            this.sslCtxRefresher = null;
        }
        this.brokerConf = pulsar.getConfiguration();
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (this.enableTls) {
            if (this.tlsEnabledWithKeyStore) {
                ch.pipeline().addLast(TLS_HANDLER,
                        new SslHandler(nettySSLContextAutoRefreshBuilder.get().createSSLEngine()));
            } else {
                ch.pipeline().addLast(TLS_HANDLER, sslCtxRefresher.get().newHandler(ch.alloc()));
            }
            ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.COPYING_ENCODER);
        } else {
            ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.ENCODER);
        }

        if (pulsar.getConfiguration().isHaProxyProtocolEnabled()) {
            ch.pipeline().addLast(OptionalProxyProtocolDecoder.NAME, new OptionalProxyProtocolDecoder());
        }
        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
            brokerConf.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4));
        // https://stackoverflow.com/questions/37535482/netty-disabling-auto-read-doesnt-work-for-bytetomessagedecoder
        // Classes such as {@link ByteToMessageDecoder} or {@link MessageToByteEncoder} are free to emit as many events
        // as they like for any given input. so, disabling auto-read on `ByteToMessageDecoder` doesn't work properly and
        // ServerCnx ends up reading higher number of messages and broker can not throttle the messages by disabling
        // auto-read.
        ch.pipeline().addLast("flowController", new FlowControlHandler());
        ServerCnx cnx = newServerCnx(pulsar, listenerName);
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
