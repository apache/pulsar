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
package org.apache.pulsar.discovery.service;

import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.NettySslContextBuilder;
import org.apache.pulsar.discovery.service.server.ServiceConfig;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;

/**
 * Initialize service channel handlers.
 *
 */
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";
    private final DiscoveryService discoveryService;
    private final boolean enableTls;
    private final NettySslContextBuilder sslCtxRefresher;

    public ServiceChannelInitializer(DiscoveryService discoveryService, ServiceConfig serviceConfig, boolean e)
            throws Exception {
        super();
        this.discoveryService = discoveryService;
        this.enableTls = e;
        if (this.enableTls) {
            sslCtxRefresher = new NettySslContextBuilder(serviceConfig.isTlsAllowInsecureConnection(),
                    serviceConfig.getTlsTrustCertsFilePath(), serviceConfig.getTlsCertificateFilePath(),
                    serviceConfig.getTlsKeyFilePath(), serviceConfig.getTlsCiphers(), serviceConfig.getTlsProtocols(),
                    serviceConfig.getTlsRequireTrustedClientCertOnConnect(),
                    serviceConfig.getTlsCertRefreshCheckDurationSec());
        } else {
            this.sslCtxRefresher = null;
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (sslCtxRefresher != null && this.enableTls) {
            SslContext sslContext = sslCtxRefresher.get();
            if (sslContext != null) {
                ch.pipeline().addLast(TLS_HANDLER, sslContext.newHandler(ch.alloc()));
            }
        }
        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
            Commands.DEFAULT_MAX_MESSAGE_SIZE + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", new ServerConnection(discoveryService));
    }
}
