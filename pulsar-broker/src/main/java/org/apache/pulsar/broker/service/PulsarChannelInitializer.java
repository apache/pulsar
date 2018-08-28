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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.ByteBufPair;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.util.SecurityUtility;

public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final PulsarService pulsar;
    private final boolean enableTLS;

    /**
     *
     * @param brokerService
     */
    public PulsarChannelInitializer(PulsarService pulsar,
            boolean enableTLS) {
        super();
        this.pulsar = pulsar;
        this.enableTLS = enableTLS;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ServiceConfiguration serviceConfig = pulsar.getConfiguration();
        if (enableTLS) {
            SslContext sslCtx = SecurityUtility.createNettySslContextForServer(
                    serviceConfig.isTlsAllowInsecureConnection(), serviceConfig.getTlsTrustCertsFilePath(),
                    serviceConfig.getTlsCertificateFilePath(), serviceConfig.getTlsKeyFilePath(),
                    serviceConfig.getTlsCiphers(), serviceConfig.getTlsProtocols(),
                    serviceConfig.getTlsRequireTrustedClientCertOnConnect());
            ch.pipeline().addLast(TLS_HANDLER, sslCtx.newHandler(ch.alloc()));
            ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.COPYING_ENCODER);
        } else {
            ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.ENCODER);
        }

        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(PulsarDecoder.MaxFrameSize, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", new ServerCnx(pulsar));
    }
}
