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

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.NettySslContextBuilder;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final PulsarService pulsar;
    private final boolean enableTls;
    private final NettySslContextBuilder sslCtxRefresher;
    private final ServiceConfiguration brokerConf;

    /**
     *
     * @param brokerService
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
        ch.pipeline().addLast("handler", new ServerCnx(pulsar));
    }
}
