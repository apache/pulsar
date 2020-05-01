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
import java.util.Set;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.common.util.FileModifiedTimeUpdater;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;

/**
 * SSL context builder for Netty.
 */
public class NettySSLContextAutoRefreshBuilder extends SslContextAutoRefreshBuilder<KeyStoreSSLContext> {
    private volatile KeyStoreSSLContext keyStoreSSLContext;

    protected final boolean tlsAllowInsecureConnection;
    protected final Set<String> tlsCiphers;
    protected final Set<String> tlsProtocols;
    protected boolean tlsRequireTrustedClientCertOnConnect;

    protected final String tlsProvider;
    protected final String tlsTrustStoreType;
    protected final String tlsTrustStorePassword;
    protected final FileModifiedTimeUpdater tlsTrustStore;

    // client context not need keystore at start time, keyStore is passed in by authData.
    protected String tlsKeyStoreType;
    protected String tlsKeyStorePassword;
    protected FileModifiedTimeUpdater tlsKeyStore;

    protected AuthenticationDataProvider authData;
    protected final boolean isServer;

    // for server
    public NettySSLContextAutoRefreshBuilder(String sslProviderString,
                                             String keyStoreTypeString,
                                             String keyStore,
                                             String keyStorePassword,
                                             boolean allowInsecureConnection,
                                             String trustStoreTypeString,
                                             String trustStore,
                                             String trustStorePassword,
                                             boolean requireTrustedClientCertOnConnect,
                                             Set<String> ciphers,
                                             Set<String> protocols,
                                             long certRefreshInSec) {
        super(certRefreshInSec);

        this.tlsAllowInsecureConnection = allowInsecureConnection;
        this.tlsProvider = sslProviderString;

        this.tlsKeyStoreType = keyStoreTypeString;
        this.tlsKeyStore = new FileModifiedTimeUpdater(keyStore);
        this.tlsKeyStorePassword = keyStorePassword;

        this.tlsTrustStoreType = trustStoreTypeString;
        this.tlsTrustStore = new FileModifiedTimeUpdater(trustStore);
        this.tlsTrustStorePassword = trustStorePassword;

        this.tlsRequireTrustedClientCertOnConnect = requireTrustedClientCertOnConnect;
        this.tlsCiphers = ciphers;
        this.tlsProtocols = protocols;

        this.isServer = true;
    }

    // for client
    public NettySSLContextAutoRefreshBuilder(String sslProviderString,
                                             boolean allowInsecureConnection,
                                             String trustStoreTypeString,
                                             String trustStore,
                                             String trustStorePassword,
                                             Set<String> ciphers,
                                             Set<String> protocols,
                                             long certRefreshInSec,
                                             AuthenticationDataProvider authData) {
        super(certRefreshInSec);

        this.tlsAllowInsecureConnection = allowInsecureConnection;
        this.tlsProvider = sslProviderString;

        this.authData = authData;

        this.tlsTrustStoreType = trustStoreTypeString;
        this.tlsTrustStore = new FileModifiedTimeUpdater(trustStore);
        this.tlsTrustStorePassword = trustStorePassword;

        this.tlsCiphers = ciphers;
        this.tlsProtocols = protocols;

        this.isServer = false;
    }

    @Override
    public synchronized KeyStoreSSLContext update() throws GeneralSecurityException, IOException {
        if (isServer) {
            this.keyStoreSSLContext = KeyStoreSSLContext.createServerKeyStoreSslContext(tlsProvider,
                    tlsKeyStoreType, tlsKeyStore.getFileName(), tlsKeyStorePassword,
                    tlsAllowInsecureConnection,
                    tlsTrustStoreType, tlsTrustStore.getFileName(), tlsTrustStorePassword,
                    tlsRequireTrustedClientCertOnConnect, tlsCiphers, tlsProtocols);
        } else {
            KeyStoreParams authParams = authData.getTlsKeyStoreParams();
            this.keyStoreSSLContext = KeyStoreSSLContext.createClientKeyStoreSslContext(tlsProvider,
                    authParams != null ? authParams.getKeyStoreType() : null,
                    authParams != null ? authParams.getKeyStorePath() : null,
                    authParams != null ? authParams.getKeyStorePassword() : null,
                    tlsAllowInsecureConnection,
                    tlsTrustStoreType, tlsTrustStore.getFileName(), tlsTrustStorePassword,
                    tlsCiphers, tlsProtocols);
        }
        return this.keyStoreSSLContext;
    }

    @Override
    public KeyStoreSSLContext getSslContext() {
        return this.keyStoreSSLContext;
    }

    @Override
    public boolean needUpdate() {
        return  (tlsKeyStore != null && tlsKeyStore.checkAndRefresh())
                || (tlsTrustStore != null && tlsTrustStore.checkAndRefresh());
    }
}
