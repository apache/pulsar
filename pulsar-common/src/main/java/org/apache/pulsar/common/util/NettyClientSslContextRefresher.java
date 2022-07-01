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
package org.apache.pulsar.common.util;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.Set;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationDataProvider;

/**
 * SSL context builder for Netty Client side.
 */
@Slf4j
public class NettyClientSslContextRefresher extends SslContextAutoRefreshBuilder<SslContext> {
    private volatile SslContext sslNettyContext;
    private final boolean tlsAllowInsecureConnection;
    protected final FileModifiedTimeUpdater tlsTrustCertsFilePath;
    protected final FileModifiedTimeUpdater tlsCertsFilePath;
    protected final FileModifiedTimeUpdater tlsPrivateKeyFilePath;
    private final AuthenticationDataProvider authData;
    private final SslProvider sslProvider;
    private final Set<String> ciphers;
    private final Set<String> protocols;

    public NettyClientSslContextRefresher(SslProvider sslProvider, boolean allowInsecure,
                                          String trustCertsFilePath,
                                          AuthenticationDataProvider authData,
                                          Set<String> ciphers,
                                          Set<String> protocols,
                                          long delayInSeconds) {
        super(delayInSeconds);
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = new FileModifiedTimeUpdater(trustCertsFilePath);
        this.authData = authData;
        this.tlsCertsFilePath = new FileModifiedTimeUpdater(
                authData != null ? authData.getTlsCerificateFilePath() : null);
        this.tlsPrivateKeyFilePath = new FileModifiedTimeUpdater(
                authData != null ? authData.getTlsPrivateKeyFilePath() : null);
        this.sslProvider = sslProvider;
        this.ciphers = ciphers;
        this.protocols = protocols;
    }

    @Override
    public synchronized SslContext update()
            throws SSLException, FileNotFoundException, GeneralSecurityException, IOException {
        if (authData != null && authData.hasDataForTls()) {
            this.sslNettyContext = authData.getTlsTrustStoreStream() == null
                    ? SecurityUtility.createNettySslContextForClient(this.sslProvider, this.tlsAllowInsecureConnection,
                    tlsTrustCertsFilePath.getFileName(), (X509Certificate[]) authData.getTlsCertificates(),
                    authData.getTlsPrivateKey(), this.ciphers, this.protocols)
                    : SecurityUtility.createNettySslContextForClient(this.sslProvider, this.tlsAllowInsecureConnection,
                    authData.getTlsTrustStoreStream(), (X509Certificate[]) authData.getTlsCertificates(),
                    authData.getTlsPrivateKey(), this.ciphers, this.protocols);
        } else {
            this.sslNettyContext =
                    SecurityUtility.createNettySslContextForClient(this.sslProvider, this.tlsAllowInsecureConnection,
                            this.tlsTrustCertsFilePath.getFileName(), this.ciphers, this.protocols);
        }
        return this.sslNettyContext;
    }

    @Override
    public SslContext getSslContext() {
        return this.sslNettyContext;
    }

    @Override
    public boolean needUpdate() {
        return tlsTrustCertsFilePath.checkAndRefresh() || tlsCertsFilePath.checkAndRefresh()
                || tlsPrivateKeyFilePath.checkAndRefresh();

    }
}
