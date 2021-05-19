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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationDataProvider;

/**
 * SSL context builder for Netty Client side.
 */
@Slf4j
public class NettyClientSslContextRefresher extends SslContextAutoRefreshBuilder<SslContext> {
    private volatile SslContext sslNettyContext;
    private boolean tlsAllowInsecureConnection;
    protected final FileModifiedTimeUpdater tlsTrustCertsFilePath;
    private AuthenticationDataProvider authData;

    public NettyClientSslContextRefresher(boolean allowInsecure,
                                          String trustCertsFilePath,
                                          AuthenticationDataProvider authData,
                                          long delayInSeconds)
            throws IOException, GeneralSecurityException {
        super(delayInSeconds);
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = new FileModifiedTimeUpdater(trustCertsFilePath);
        this.authData = authData;
    }

    @Override
    public synchronized SslContext update()
            throws SSLException, FileNotFoundException, GeneralSecurityException, IOException {
        if (authData != null && authData.hasDataForTls()) {
            this.sslNettyContext = authData.getTlsTrustStoreStream() == null
                    ? SecurityUtility.createNettySslContextForClient(this.tlsAllowInsecureConnection,
                            tlsTrustCertsFilePath.getFileName(), (X509Certificate[]) authData.getTlsCertificates(),
                            authData.getTlsPrivateKey())
                    : SecurityUtility.createNettySslContextForClient(this.tlsAllowInsecureConnection,
                            authData.getTlsTrustStoreStream(), (X509Certificate[]) authData.getTlsCertificates(),
                            authData.getTlsPrivateKey());
        } else {
            this.sslNettyContext = SecurityUtility.createNettySslContextForClient(this.tlsAllowInsecureConnection,
                    this.tlsTrustCertsFilePath.getFileName());
        }
        return this.sslNettyContext;
    }

    @Override
    public SslContext getSslContext() {
        return this.sslNettyContext;
    }

    @Override
    public boolean needUpdate() {
        return  tlsTrustCertsFilePath.checkAndRefresh();
    }
}
