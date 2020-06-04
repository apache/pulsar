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
package org.apache.pulsar.client.impl;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import lombok.Getter;
import lombok.Setter;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ObjectCache;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;

@Slf4j
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final Supplier<ClientCnx> clientCnxSupplier;
    @Getter
    private final boolean tlsEnabled;
    private final boolean tlsEnabledWithKeyStore;

    private final Supplier<SslContext> sslContextSupplier;
    private NettySSLContextAutoRefreshBuilder nettySSLContextAutoRefreshBuilder;
    @Setter
    private String sniHostName;
    @Setter
    private int sniHostPort;

    private static final long TLS_CERTIFICATE_CACHE_MILLIS = TimeUnit.MINUTES.toMillis(1);

    public PulsarChannelInitializer(ClientConfigurationData conf, Supplier<ClientCnx> clientCnxSupplier)
            throws Exception {
        super();
        this.clientCnxSupplier = clientCnxSupplier;
        this.tlsEnabled = conf.isUseTls();
        this.tlsEnabledWithKeyStore = conf.isUseKeyStoreTls();

        if (tlsEnabled) {
            if (tlsEnabledWithKeyStore) {
                AuthenticationDataProvider authData1 = conf.getAuthentication().getAuthData();

                nettySSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                            conf.getSslProvider(),
                            conf.isTlsAllowInsecureConnection(),
                            conf.getTlsTrustStoreType(),
                            conf.getTlsTrustStorePath(),
                            conf.getTlsTrustStorePassword(),
                            conf.getTlsCiphers(),
                            conf.getTlsProtocols(),
                            TLS_CERTIFICATE_CACHE_MILLIS,
                            authData1);
            }

            sslContextSupplier = new ObjectCache<SslContext>(() -> {
                try {
                    // Set client certificate if available
                    AuthenticationDataProvider authData = conf.getAuthentication().getAuthData();
                    if (authData.hasDataForTls()) {
                        return SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection(),
                                conf.getTlsTrustCertsFilePath(), (X509Certificate[]) authData.getTlsCertificates(),
                                authData.getTlsPrivateKey());
                    } else {
                        return SecurityUtility.createNettySslContextForClient(conf.isTlsAllowInsecureConnection(),
                                conf.getTlsTrustCertsFilePath());
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create TLS context", e);
                }
            }, TLS_CERTIFICATE_CACHE_MILLIS, TimeUnit.MILLISECONDS);
        } else {
            sslContextSupplier = null;
        }
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        if (tlsEnabled) {
            if (tlsEnabledWithKeyStore) {
                ch.pipeline().addLast(TLS_HANDLER,
                        new SslHandler(nettySSLContextAutoRefreshBuilder.get().createSSLEngine()));
			} else {
				SslHandler handler = StringUtils.isNotBlank(sniHostName)
						? sslContextSupplier.get().newHandler(ch.alloc(), sniHostName, sniHostPort)
						: sslContextSupplier.get().newHandler(ch.alloc());
				ch.pipeline().addLast(TLS_HANDLER, handler);
			}
            ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.COPYING_ENCODER);
        } else {
            ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.ENCODER);
        }

        ch.pipeline()
                .addLast("frameDecoder",
                        new LengthFieldBasedFrameDecoder(
                                Commands.DEFAULT_MAX_MESSAGE_SIZE + Commands.MESSAGE_SIZE_FRAME_PADDING,
                                0, 4, 0, 4));
        ch.pipeline().addLast("handler", clientCnxSupplier.get());
    }
}
