/*
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

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.common.util.keystoretls.KeyStoreSSLContext;

/**
 * Default Implementation of {@link PulsarSslFactory}. This factory loads file based certificates to create SSLContext
 * and SSL Engines. This class is not thread safe. It has been integrated into the pulsar code base as a single writer,
 * multiple readers pattern.
 */
@NotThreadSafe
public class DefaultPulsarSslFactory implements PulsarSslFactory {

    private PulsarSslConfiguration config;
    private final AtomicReference<SSLContext> internalSslContext = new AtomicReference<>();
    private final AtomicReference<SslContext> internalNettySslContext = new AtomicReference<>();

    protected FileModifiedTimeUpdater tlsKeyStore;
    protected FileModifiedTimeUpdater tlsTrustStore;
    protected FileModifiedTimeUpdater tlsTrustCertsFilePath;
    protected FileModifiedTimeUpdater tlsCertificateFilePath;
    protected FileModifiedTimeUpdater tlsKeyFilePath;
    protected AuthenticationDataProvider authData;
    protected boolean isTlsTrustStoreStreamProvided;
    protected final String[] defaultSslEnabledProtocols = {"TLSv1.3", "TLSv1.2"};
    protected String tlsKeystoreType;
    protected String tlsKeystorePath;
    protected String tlsKeystorePassword;

    /**
     * Initializes the DefaultPulsarSslFactory.
     *
     * @param config {@link PulsarSslConfiguration} object required for initialization.
     *
     */
    @Override
    public void initialize(PulsarSslConfiguration config) {
        this.config = config;
        AuthenticationDataProvider authData = this.config.getAuthData();
        if (this.config.isTlsEnabledWithKeystore()) {
            if (authData != null && authData.hasDataForTls()) {
                KeyStoreParams authParams = authData.getTlsKeyStoreParams();
                if (authParams != null) {
                    this.tlsKeystoreType = authParams.getKeyStoreType();
                    this.tlsKeystorePath = authParams.getKeyStorePath();
                    this.tlsKeystorePassword = authParams.getKeyStorePassword();
                }
            }
            if (this.tlsKeystoreType == null) {
                this.tlsKeystoreType = this.config.getTlsKeyStoreType();
            }
            if (this.tlsKeystorePath == null) {
                this.tlsKeystorePath = this.config.getTlsKeyStorePath();
            }
            if (this.tlsKeystorePassword == null) {
                this.tlsKeystorePassword = this.config.getTlsKeyStorePassword();
            }
            this.tlsKeyStore = new FileModifiedTimeUpdater(this.tlsKeystorePath);
            this.tlsTrustStore = new FileModifiedTimeUpdater(this.config.getTlsTrustStorePath());
        } else {
            if (authData != null && authData.hasDataForTls()) {
                if (authData.getTlsTrustStoreStream() != null) {
                    this.isTlsTrustStoreStreamProvided = true;
                } else {
                    this.tlsTrustCertsFilePath = new FileModifiedTimeUpdater(this.config.getTlsTrustCertsFilePath());
                }
                this.authData = authData;
            } else {
                this.tlsCertificateFilePath = new FileModifiedTimeUpdater(this.config.getTlsCertificateFilePath());
                this.tlsTrustCertsFilePath = new FileModifiedTimeUpdater(this.config.getTlsTrustCertsFilePath());
                this.tlsKeyFilePath = new FileModifiedTimeUpdater(this.config.getTlsKeyFilePath());
            }
        }
    }

    /**
     * Creates a Client {@link SSLEngine} utilizing the peer hostname, peer port and {@link PulsarSslConfiguration}
     * object provided during initialization.
     *
     * @param peerHost the name of the peer host
     * @param peerPort the port number of the peer
     * @return {@link SSLEngine}
     */
    @Override
    public SSLEngine createClientSslEngine(ByteBufAllocator buf, String peerHost, int peerPort) {
        return createSSLEngine(buf, peerHost, peerPort, NetworkMode.CLIENT);
    }

    /**
     * Creates a Server {@link SSLEngine} utilizing the {@link PulsarSslConfiguration} object provided during
     * initialization.
     *
     * @return {@link SSLEngine}
     */
    @Override
    public SSLEngine createServerSslEngine(ByteBufAllocator buf) {
        return createSSLEngine(buf, "", 0, NetworkMode.SERVER);
    }

    /**
     * Returns a boolean value based on if the underlying certificate files have been modified since it was last read.
     *
     * @return {@code true} if the underlying certificates have been modified indicating that
     * the SSL Context should be refreshed.
     */
    @Override
    public boolean needsUpdate() {
        if (this.config.isTlsEnabledWithKeystore()) {
            return  (this.tlsKeyStore != null && this.tlsKeyStore.checkAndRefresh())
                    || (this.tlsTrustStore != null && this.tlsTrustStore.checkAndRefresh());
        } else {
            if (this.authData != null && this.authData.hasDataForTls()) {
                return true;
            } else {
                return this.tlsTrustCertsFilePath.checkAndRefresh() || this.tlsCertificateFilePath.checkAndRefresh()
                        || this.tlsKeyFilePath.checkAndRefresh();
            }
        }
    }

    /**
     * Creates a {@link SSLContext} object and saves it internally.
     *
     * @throws Exception If there were any issues generating the {@link SSLContext}
     */
    @Override
    public void createInternalSslContext() throws Exception {
        if (this.config.isTlsEnabledWithKeystore()) {
            this.internalSslContext.set(buildKeystoreSslContext(this.config.isServerMode()));
        } else {
            if (this.config.isHttps()) {
                this.internalSslContext.set(buildSslContext());
            } else {
                this.internalNettySslContext.set(buildNettySslContext());
            }
        }
    }


    /**
     * Get the internally stored {@link SSLContext}.
     *
     * @return {@link SSLContext}
     * @throws RuntimeException if the {@link SSLContext} object has not yet been initialized.
     */
    @Override
    public SSLContext getInternalSslContext() {
        if (this.internalSslContext.get() == null) {
            throw new RuntimeException("Internal SSL context is not initialized. "
                    + "Please call createInternalSslContext() first.");
        }
        return this.internalSslContext.get();
    }

    /**
     * Get the internally stored {@link SslContext}.
     *
     * @return {@link SslContext}
     * @throws RuntimeException if the {@link SslContext} object has not yet been initialized.
     */
    public SslContext getInternalNettySslContext() {
        if (this.internalNettySslContext.get() == null) {
            throw new RuntimeException("Internal SSL context is not initialized. "
                    + "Please call createInternalSslContext() first.");
        }
        return this.internalNettySslContext.get();
    }

    private SSLContext buildKeystoreSslContext(boolean isServerMode) throws GeneralSecurityException, IOException {
        KeyStoreSSLContext keyStoreSSLContext;
        if (isServerMode) {
            keyStoreSSLContext = KeyStoreSSLContext.createServerKeyStoreSslContext(this.config.getTlsProvider(),
                    this.tlsKeystoreType, this.tlsKeyStore.getFileName(),
                    this.tlsKeystorePassword, this.config.isAllowInsecureConnection(),
                    this.config.getTlsTrustStoreType(), this.tlsTrustStore.getFileName(),
                    this.config.getTlsTrustStorePassword(), this.config.isRequireTrustedClientCertOnConnect(),
                    this.config.getTlsCiphers(), this.config.getTlsProtocols());
        } else {
            keyStoreSSLContext = KeyStoreSSLContext.createClientKeyStoreSslContext(this.config.getTlsProvider(),
                    this.tlsKeystoreType, this.tlsKeyStore.getFileName(),
                    this.tlsKeystorePassword, this.config.isAllowInsecureConnection(),
                    this.config.getTlsTrustStoreType(), this.tlsTrustStore.getFileName(),
                    this.config.getTlsTrustStorePassword(), this.config.getTlsCiphers(),
                    this.config.getTlsProtocols());
        }
        return keyStoreSSLContext.createSSLContext();
    }

    private SSLContext buildSslContext() throws GeneralSecurityException {
        if (this.authData != null && this.authData.hasDataForTls()) {
            if (this.isTlsTrustStoreStreamProvided) {
                return SecurityUtility.createSslContext(this.config.isAllowInsecureConnection(),
                        SecurityUtility.loadCertificatesFromPemStream(this.authData.getTlsTrustStoreStream()),
                        this.authData.getTlsCertificates(),
                        this.authData.getTlsPrivateKey(),
                        this.config.getTlsProvider());
            } else {
                if (this.authData.getTlsCertificates() != null) {
                    return SecurityUtility.createSslContext(this.config.isAllowInsecureConnection(),
                            SecurityUtility.loadCertificatesFromPemFile(this.tlsTrustCertsFilePath.getFileName()),
                            this.authData.getTlsCertificates(),
                            this.authData.getTlsPrivateKey(),
                            this.config.getTlsProvider());
                } else {
                    return SecurityUtility.createSslContext(this.config.isAllowInsecureConnection(),
                            this.tlsTrustCertsFilePath.getFileName(),
                            this.authData.getTlsCertificateFilePath(),
                            this.authData.getTlsPrivateKeyFilePath(),
                            this.config.getTlsProvider()
                            );
                }
            }
        } else {
            return SecurityUtility.createSslContext(this.config.isAllowInsecureConnection(),
                    this.tlsTrustCertsFilePath.getFileName(),
                    this.tlsCertificateFilePath.getFileName(),
                    this.tlsKeyFilePath.getFileName(),
                    this.config.getTlsProvider());
        }
    }

    private SslContext buildNettySslContext() throws GeneralSecurityException, IOException {
        SslProvider sslProvider = null;
        if (StringUtils.isNotBlank(this.config.getTlsProvider())) {
            sslProvider = SslProvider.valueOf(this.config.getTlsProvider());
        }
        if (this.authData != null && this.authData.hasDataForTls()) {
            if (this.isTlsTrustStoreStreamProvided) {
                return SecurityUtility.createNettySslContextForClient(sslProvider,
                        this.config.isAllowInsecureConnection(),
                        this.authData.getTlsTrustStoreStream(),
                        this.authData.getTlsCertificates(),
                        this.authData.getTlsPrivateKey(),
                        this.config.getTlsCiphers(),
                        this.config.getTlsProtocols());
            } else {
                if (this.authData.getTlsCertificates() != null) {
                    return SecurityUtility.createNettySslContextForClient(sslProvider,
                            this.config.isAllowInsecureConnection(),
                            this.tlsTrustCertsFilePath.getFileName(),
                            this.authData.getTlsCertificates(),
                            this.authData.getTlsPrivateKey(),
                            this.config.getTlsCiphers(),
                            this.config.getTlsProtocols());
                } else {
                    return SecurityUtility.createNettySslContextForClient(sslProvider,
                            this.config.isAllowInsecureConnection(),
                            this.tlsTrustCertsFilePath.getFileName(),
                            this.authData.getTlsCertificateFilePath(),
                            this.authData.getTlsPrivateKeyFilePath(),
                            this.config.getTlsCiphers(),
                            this.config.getTlsProtocols());
                }
            }
        } else {
            if (this.config.isServerMode()) {
                return SecurityUtility.createNettySslContextForServer(sslProvider,
                        this.config.isAllowInsecureConnection(),
                        this.tlsTrustCertsFilePath.getFileName(),
                        this.tlsCertificateFilePath.getFileName(),
                        this.tlsKeyFilePath.getFileName(),
                        this.config.getTlsCiphers(),
                        this.config.getTlsProtocols(),
                        this.config.isRequireTrustedClientCertOnConnect());
            } else {
                return SecurityUtility.createNettySslContextForClient(sslProvider,
                        this.config.isAllowInsecureConnection(),
                        this.tlsTrustCertsFilePath.getFileName(),
                        this.tlsCertificateFilePath.getFileName(),
                        this.tlsKeyFilePath.getFileName(),
                        this.config.getTlsCiphers(),
                        this.config.getTlsProtocols());
            }
        }
    }

    private SSLEngine createSSLEngine(ByteBufAllocator buf, String peerHost, int peerPort, NetworkMode mode) {
        SSLEngine sslEngine;
        SSLParameters sslParams;
        SSLContext sslContext = this.internalSslContext.get();
        SslContext nettySslContext = this.internalNettySslContext.get();
        validateSslContext(sslContext, nettySslContext);
        if (mode == NetworkMode.CLIENT) {
            if (sslContext != null) {
                sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
            } else {
                sslEngine = nettySslContext.newEngine(buf, peerHost, peerPort);
            }
            sslEngine.setUseClientMode(true);
            sslParams = sslEngine.getSSLParameters();
        } else {
            if (sslContext != null) {
                sslEngine = sslContext.createSSLEngine();
            } else {
                sslEngine = nettySslContext.newEngine(buf);
            }
            sslEngine.setUseClientMode(false);
            sslParams = sslEngine.getSSLParameters();
            if (this.config.isRequireTrustedClientCertOnConnect()) {
                sslParams.setNeedClientAuth(true);
            } else {
                sslParams.setWantClientAuth(true);
            }
        }
        if (this.config.getTlsProtocols() != null && !this.config.getTlsProtocols().isEmpty()) {
            sslParams.setProtocols(this.config.getTlsProtocols().toArray(new String[0]));
        } else {
            sslParams.setProtocols(defaultSslEnabledProtocols);
        }
        if (this.config.getTlsCiphers() != null && !this.config.getTlsCiphers().isEmpty()) {
            sslParams.setCipherSuites(this.config.getTlsCiphers().toArray(new String[0]));
        }
        sslEngine.setSSLParameters(sslParams);
        return sslEngine;
    }

    private void validateSslContext(SSLContext sslContext, SslContext nettySslContext) {
        if (sslContext == null && nettySslContext == null) {
            throw new RuntimeException("Internal SSL context is not initialized. "
                    + "Please call createInternalSslContext() first.");
        }
    }

    /**
     * Clean any resources that may have been created.
     * @throws Exception if any resources failed to be cleaned.
     */
    @Override
    public void close() throws Exception {
        // noop
    }

    private enum NetworkMode {
        CLIENT, SERVER
    }
}