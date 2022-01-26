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
package org.apache.pulsar.common.util.keystoretls;

import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLContext;
import org.apache.pulsar.common.util.FileModifiedTimeUpdater;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;

/**
 * Similar to `DefaultSslContextBuilder`, which build `javax.net.ssl.SSLContext` for web service.
 */
public class NetSslContextBuilder extends SslContextAutoRefreshBuilder<SSLContext> {
    private volatile SSLContext sslContext;

    protected final boolean tlsAllowInsecureConnection;
    protected final boolean tlsRequireTrustedClientCertOnConnect;

    protected final String tlsProvider;
    protected final String tlsKeyStoreType;
    protected final String tlsKeyStorePassword;
    protected final FileModifiedTimeUpdater tlsKeyStore;
    protected final String tlsTrustStoreType;
    protected final String tlsTrustStorePassword;
    protected final FileModifiedTimeUpdater tlsTrustStore;

    public NetSslContextBuilder(String sslProviderString,
                                String keyStoreTypeString,
                                String keyStore,
                                String keyStorePasswordPath,
                                boolean allowInsecureConnection,
                                String trustStoreTypeString,
                                String trustStore,
                                String trustStorePasswordPath,
                                boolean requireTrustedClientCertOnConnect,
                                long certRefreshInSec) {
        super(certRefreshInSec);

        this.tlsAllowInsecureConnection = allowInsecureConnection;
        this.tlsProvider = sslProviderString;
        this.tlsKeyStoreType = keyStoreTypeString;
        this.tlsKeyStore = new FileModifiedTimeUpdater(keyStore);
        this.tlsKeyStorePassword = keyStorePasswordPath;

        this.tlsTrustStoreType = trustStoreTypeString;
        this.tlsTrustStore = new FileModifiedTimeUpdater(trustStore);
        this.tlsTrustStorePassword = trustStorePasswordPath;

        this.tlsRequireTrustedClientCertOnConnect = requireTrustedClientCertOnConnect;
    }

    @Override
    public synchronized SSLContext update()
            throws GeneralSecurityException, IOException {
        this.sslContext = KeyStoreSSLContext.createServerSslContext(tlsProvider,
                tlsKeyStoreType, tlsKeyStore.getFileName(), tlsKeyStorePassword,
                tlsAllowInsecureConnection,
                tlsTrustStoreType, tlsTrustStore.getFileName(), tlsTrustStorePassword,
                tlsRequireTrustedClientCertOnConnect);
        return this.sslContext;
    }

    @Override
    public SSLContext getSslContext() {
        return this.sslContext;
    }

    @Override
    public boolean needUpdate() {
        return  tlsKeyStore.checkAndRefresh()
                || tlsTrustStore.checkAndRefresh();
    }
}
