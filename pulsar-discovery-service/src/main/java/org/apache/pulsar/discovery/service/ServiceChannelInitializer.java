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

import java.io.File;

import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.api.PulsarLengthFieldFrameDecoder;
import org.apache.pulsar.discovery.service.server.ServiceConfig;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Initialize service channel handlers. 
 *
 */
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";
    private ServiceConfig serviceConfig;
    private DiscoveryService discoveryService;
    private boolean enableTLS;

    public ServiceChannelInitializer(DiscoveryService discoveryService, ServiceConfig serviceConfig, boolean enableTLS) {
        super();
        this.serviceConfig = serviceConfig;
        this.discoveryService = discoveryService;
        this.enableTLS = enableTLS;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (enableTLS) {
            File tlsCert = new File(serviceConfig.getTlsCertificateFilePath());
            File tlsKey = new File(serviceConfig.getTlsKeyFilePath());
            SslContextBuilder builder = SslContextBuilder.forServer(tlsCert, tlsKey);
            if (serviceConfig.isTlsAllowInsecureConnection()) {
                builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else {
                if (serviceConfig.getTlsTrustCertsFilePath().isEmpty()) {
                    // Use system default
                    builder.trustManager((File) null);
                } else {
                    File trustCertCollection = new File(serviceConfig.getTlsTrustCertsFilePath());
                    builder.trustManager(trustCertCollection);
                }
            }
            SslContext sslCtx = builder.clientAuth(ClientAuth.OPTIONAL).build();
            ch.pipeline().addLast(TLS_HANDLER, sslCtx.newHandler(ch.alloc()));
        }
        ch.pipeline().addLast("frameDecoder",
                new PulsarLengthFieldFrameDecoder(PulsarDecoder.MaxFrameSize, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", new ServerConnection(discoveryService));
    }
}
