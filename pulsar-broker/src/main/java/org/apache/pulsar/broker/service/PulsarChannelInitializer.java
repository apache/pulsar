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

import static org.apache.bookkeeper.util.SafeRunnable.safeRun;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.NettySslContextBuilder;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flow.FlowControlHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final PulsarService pulsar;
    private final boolean enableTls;
    private final NettySslContextBuilder sslCtxRefresher;
    private final ServiceConfiguration brokerConf;

    // This cache is used to maintain a list of active connections to iterate over them
    // We keep weak references to have the cache to be auto cleaned up when the connections
    // objects are GCed.
    private final Cache<SocketAddress, PulsarServerCnx> connections = Caffeine.newBuilder()
            .weakKeys()
            .weakValues()
            .build();

    /**
     * @param pulsar
     *              An instance of {@link PulsarService}
     * @param enableTLS
     *              Enable tls or not
     */
    public PulsarChannelInitializer(PulsarService pulsar, boolean enableTLS) throws Exception {
        super();
        this.pulsar = pulsar;
        this.enableTls = enableTLS;
        if (this.enableTls) {
            ServiceConfiguration serviceConfig = pulsar.getConfiguration();
            sslCtxRefresher = new NettySslContextBuilder(serviceConfig.isTlsAllowInsecureConnection(),
                    serviceConfig.getTlsTrustCertsFilePath(), serviceConfig.getTlsCertificateFilePath(),
                    serviceConfig.getTlsKeyFilePath(), serviceConfig.getTlsCiphers(), serviceConfig.getTlsProtocols(),
                    serviceConfig.isTlsRequireTrustedClientCertOnConnect(),
                    serviceConfig.getTlsCertRefreshCheckDurationSec());
        } else {
            this.sslCtxRefresher = null;
        }
        this.brokerConf = pulsar.getConfiguration();

        pulsar.getExecutor().scheduleAtFixedRate(safeRun(this::refreshAuthenticationCredentials),
                pulsar.getConfig().getAuthenticationRefreshCheckSeconds(),
                pulsar.getConfig().getAuthenticationRefreshCheckSeconds(), TimeUnit.SECONDS);
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (this.enableTls) {
            ch.pipeline().addLast(TLS_HANDLER, sslCtxRefresher.get().newHandler(ch.alloc()));
            ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.COPYING_ENCODER);
        } else {
            ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.ENCODER);
        }

        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
            brokerConf.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4));
        // https://stackoverflow.com/questions/37535482/netty-disabling-auto-read-doesnt-work-for-bytetomessagedecoder
        // Classes such as {@link ByteToMessageDecoder} or {@link MessageToByteEncoder} are free to emit as many events
        // as they like for any given input. so, disabling auto-read on `ByteToMessageDecoder` doesn't work properly and
        // PulsarServerCnx ends up reading higher number of messages and broker can not throttle the messages by disabling
        // auto-read.
        ch.pipeline().addLast("flowController", new FlowControlHandler());
        PulsarServerCnx cnx = new PulsarServerCnx(pulsar);
        ch.pipeline().addLast("handler", cnx);

        connections.put(ch.remoteAddress(), cnx);
    }

    private void refreshAuthenticationCredentials() {
        connections.asMap().values().forEach(cnx -> {
            try {
                cnx.refreshAuthenticationCredentials();
            } catch (Throwable t) {
                log.warn("[{}] Failed to refresh auth credentials", cnx.getRemoteAddress());
            }
        });
    }
}
