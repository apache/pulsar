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

import java.security.GeneralSecurityException;
import javax.net.ssl.SSLContext;

@SuppressWarnings("checkstyle:JavadocType")
public class DefaultSslContextBuilder extends SslContextAutoRefreshBuilder<SSLContext> {
    private volatile SSLContext sslContext;

    protected final boolean tlsAllowInsecureConnection;
    protected final FileModifiedTimeUpdater tlsTrustCertsFilePath, tlsCertificateFilePath, tlsKeyFilePath;
    protected final boolean tlsRequireTrustedClientCertOnConnect;
    private final String providerName;

    public DefaultSslContextBuilder(boolean allowInsecure, String trustCertsFilePath, String certificateFilePath,
                                    String keyFilePath, boolean requireTrustedClientCertOnConnect,
                                    long certRefreshInSec) {
        super(certRefreshInSec);
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = new FileModifiedTimeUpdater(trustCertsFilePath);
        this.tlsCertificateFilePath = new FileModifiedTimeUpdater(certificateFilePath);
        this.tlsKeyFilePath = new FileModifiedTimeUpdater(keyFilePath);
        this.tlsRequireTrustedClientCertOnConnect = requireTrustedClientCertOnConnect;
        this.providerName = null;
    }

    public DefaultSslContextBuilder(boolean allowInsecure, String trustCertsFilePath, String certificateFilePath,
                                    String keyFilePath, boolean requireTrustedClientCertOnConnect,
                                    long certRefreshInSec, String providerName) {
        super(certRefreshInSec);
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = new FileModifiedTimeUpdater(trustCertsFilePath);
        this.tlsCertificateFilePath = new FileModifiedTimeUpdater(certificateFilePath);
        this.tlsKeyFilePath = new FileModifiedTimeUpdater(keyFilePath);
        this.tlsRequireTrustedClientCertOnConnect = requireTrustedClientCertOnConnect;
        this.providerName = providerName;
    }

    @Override
    public synchronized SSLContext update() throws GeneralSecurityException {
        this.sslContext = SecurityUtility.createSslContext(tlsAllowInsecureConnection,
                tlsTrustCertsFilePath.getFileName(), tlsCertificateFilePath.getFileName(),
                tlsKeyFilePath.getFileName(), this.providerName);
        return this.sslContext;
    }

    @Override
    public SSLContext getSslContext() {
        return this.sslContext;
    }

    @Override
    public boolean needUpdate() {
        return  tlsTrustCertsFilePath.checkAndRefresh()
                || tlsCertificateFilePath.checkAndRefresh()
                || tlsKeyFilePath.checkAndRefresh();
    }
}
