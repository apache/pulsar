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
import java.util.Set;
import javax.net.ssl.SSLException;

/**
 * SSL context builder for Netty Server side.
 */
public class NettyServerSslContextBuilder extends SslContextAutoRefreshBuilder<SslContext> {
    private volatile SslContext sslNettyContext;

    protected final boolean tlsAllowInsecureConnection;
    protected final FileModifiedTimeUpdater tlsTrustCertsFilePath, tlsCertificateFilePath, tlsKeyFilePath;
    protected final Set<String> tlsCiphers;
    protected final Set<String> tlsProtocols;
    protected final boolean tlsRequireTrustedClientCertOnConnect;

    public NettyServerSslContextBuilder(boolean allowInsecure, String trustCertsFilePath, String certificateFilePath,
                                        String keyFilePath, Set<String> ciphers, Set<String> protocols,
                                        boolean requireTrustedClientCertOnConnect,
                                        long delayInSeconds) {
        super(delayInSeconds);
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = new FileModifiedTimeUpdater(trustCertsFilePath);
        this.tlsCertificateFilePath = new FileModifiedTimeUpdater(certificateFilePath);
        this.tlsKeyFilePath = new FileModifiedTimeUpdater(keyFilePath);
        this.tlsCiphers = ciphers;
        this.tlsProtocols = protocols;
        this.tlsRequireTrustedClientCertOnConnect = requireTrustedClientCertOnConnect;
    }

    @Override
    public synchronized SslContext update()
        throws SSLException, FileNotFoundException, GeneralSecurityException, IOException {
        this.sslNettyContext = SecurityUtility.createNettySslContextForServer(tlsAllowInsecureConnection,
                tlsTrustCertsFilePath.getFileName(), tlsCertificateFilePath.getFileName(), tlsKeyFilePath.getFileName(),
                tlsCiphers, tlsProtocols, tlsRequireTrustedClientCertOnConnect);
        return this.sslNettyContext;
    }

    @Override
    public SslContext getSslContext() {
        return this.sslNettyContext;
    }

    @Override
    public boolean needUpdate() {
        return  tlsTrustCertsFilePath.checkAndRefresh()
                || tlsCertificateFilePath.checkAndRefresh()
                || tlsKeyFilePath.checkAndRefresh();
    }
}
