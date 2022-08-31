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

import static org.apache.pulsar.common.util.SecurityUtility.getProvider;
import com.google.common.base.Strings;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.SecurityUtility;

/**
 * KeyStoreSSLContext that mainly wrap a SSLContext to provide SSL context for both webservice and netty.
 */
@Slf4j
public class KeyStoreSSLContext {
    public static final String DEFAULT_KEYSTORE_TYPE = "JKS";
    public static final String DEFAULT_SSL_PROTOCOL = "TLS";
    public static final String DEFAULT_SSL_ENABLED_PROTOCOLS = "TLSv1.3,TLSv1.2";
    public static final String DEFAULT_SSL_KEYMANGER_ALGORITHM = KeyManagerFactory.getDefaultAlgorithm();
    public static final String DEFAULT_SSL_TRUSTMANAGER_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm();

    public static final Provider BC_PROVIDER = getProvider();

    /**
     * Connection Mode for TLS.
     */
    public enum Mode {
        CLIENT,
        SERVER
    }

    @Getter
    private final Mode mode;

    private final String sslProviderString;
    private final String keyStoreTypeString;
    private final String keyStorePath;
    private final String keyStorePassword;
    private final boolean allowInsecureConnection;
    private final String trustStoreTypeString;
    private final String trustStorePath;
    private final String trustStorePassword;
    private final boolean needClientAuth;
    private final Set<String> ciphers;
    private final Set<String> protocols;
    private SSLContext sslContext;

    private final String protocol = DEFAULT_SSL_PROTOCOL;
    private final String kmfAlgorithm = DEFAULT_SSL_KEYMANGER_ALGORITHM;
    private final String tmfAlgorithm = DEFAULT_SSL_TRUSTMANAGER_ALGORITHM;

    // only init vars, before using it, need to call createSSLContext to create ssl context.
    public KeyStoreSSLContext(Mode mode,
                              String sslProviderString,
                              String keyStoreTypeString,
                              String keyStorePath,
                              String keyStorePassword,
                              boolean allowInsecureConnection,
                              String trustStoreTypeString,
                              String trustStorePath,
                              String trustStorePassword,
                              boolean requireTrustedClientCertOnConnect,
                              Set<String> ciphers,
                              Set<String> protocols) {
        this.mode = mode;
        this.sslProviderString = sslProviderString;
        this.keyStoreTypeString = Strings.isNullOrEmpty(keyStoreTypeString)
                ? DEFAULT_KEYSTORE_TYPE
                : keyStoreTypeString;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStoreTypeString = Strings.isNullOrEmpty(trustStoreTypeString)
                ? DEFAULT_KEYSTORE_TYPE
                : trustStoreTypeString;
        this.trustStorePath = trustStorePath;
        if (trustStorePassword == null) {
            this.trustStorePassword = "";
        } else {
            this.trustStorePassword = trustStorePassword;
        }
        this.needClientAuth = requireTrustedClientCertOnConnect;

        if (protocols != null && protocols.size() > 0) {
            this.protocols = protocols;
        } else {
            this.protocols = new HashSet<>(Arrays.asList(DEFAULT_SSL_ENABLED_PROTOCOLS.split("\\s*,\\s*")));
        }

        if (ciphers != null && ciphers.size() > 0) {
            this.ciphers = ciphers;
        } else {
            this.ciphers = null;
        }

        this.allowInsecureConnection = allowInsecureConnection;
    }

    public SSLContext createSSLContext() throws GeneralSecurityException, IOException {
        SSLContext sslContext;

        Provider provider = SecurityUtility.resolveProvider(sslProviderString);
        if (provider != null) {
            sslContext = SSLContext.getInstance(protocol, provider);
        } else {
            sslContext = SSLContext.getInstance(protocol);
        }

        // key store
        KeyManager[] keyManagers = null;
        if (!Strings.isNullOrEmpty(keyStorePath)) {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(kmfAlgorithm);
            KeyStore keyStore = KeyStore.getInstance(keyStoreTypeString);
            char[] passwordChars = keyStorePassword.toCharArray();
            try (FileInputStream inputStream = new FileInputStream(keyStorePath)) {
                keyStore.load(inputStream, passwordChars);
            }
            keyManagerFactory.init(keyStore, passwordChars);
            keyManagers = keyManagerFactory.getKeyManagers();
        }

        // trust store
        TrustManagerFactory trustManagerFactory;
        if (this.allowInsecureConnection) {
            trustManagerFactory = InsecureTrustManagerFactory.INSTANCE;
        } else {
            trustManagerFactory = provider != null
                    ? TrustManagerFactory.getInstance(tmfAlgorithm, provider)
                    : TrustManagerFactory.getInstance(tmfAlgorithm);
            KeyStore trustStore = KeyStore.getInstance(trustStoreTypeString);
            char[] passwordChars = trustStorePassword.toCharArray();
            try (FileInputStream inputStream = new FileInputStream(trustStorePath)) {
                trustStore.load(inputStream, passwordChars);
            }
            trustManagerFactory.init(trustStore);
        }

        // init
        sslContext.init(keyManagers, SecurityUtility
                        .processConscryptTrustManagers(trustManagerFactory.getTrustManagers()),
                new SecureRandom());
        this.sslContext = sslContext;
        return sslContext;
    }

    public SSLContext getSslContext() {
        if (sslContext == null) {
            throw new IllegalStateException("createSSLContext hasn't been called.");
        }
        return sslContext;
    }

    public SSLEngine createSSLEngine() {
        return configureSSLEngine(getSslContext().createSSLEngine());
    }

    public SSLEngine createSSLEngine(String peerHost, int peerPort) {
        return configureSSLEngine(getSslContext().createSSLEngine(peerHost, peerPort));
    }

    private SSLEngine configureSSLEngine(SSLEngine sslEngine) {
        sslEngine.setEnabledProtocols(protocols.toArray(new String[0]));
        if (this.ciphers != null) {
            sslEngine.setEnabledCipherSuites(this.ciphers.toArray(new String[0]));
        }

        if (this.mode == Mode.SERVER) {
            sslEngine.setNeedClientAuth(this.needClientAuth);
            sslEngine.setUseClientMode(false);
        } else {
            sslEngine.setUseClientMode(true);
        }
        return sslEngine;
    }

    public static KeyStoreSSLContext createClientKeyStoreSslContext(String sslProviderString,
                                                            String keyStoreTypeString,
                                                            String keyStorePath,
                                                            String keyStorePassword,
                                                            boolean allowInsecureConnection,
                                                            String trustStoreTypeString,
                                                            String trustStorePath,
                                                            String trustStorePassword,
                                                            Set<String> ciphers,
                                                            Set<String> protocols)
            throws GeneralSecurityException, IOException {
        KeyStoreSSLContext keyStoreSSLContext = new KeyStoreSSLContext(Mode.CLIENT,
                sslProviderString,
                keyStoreTypeString,
                keyStorePath,
                keyStorePassword,
                allowInsecureConnection,
                trustStoreTypeString,
                trustStorePath,
                trustStorePassword,
                false,
                ciphers,
                protocols);

        keyStoreSSLContext.createSSLContext();
        return keyStoreSSLContext;
    }


    public static KeyStoreSSLContext createServerKeyStoreSslContext(String sslProviderString,
                                                    String keyStoreTypeString,
                                                    String keyStorePath,
                                                    String keyStorePassword,
                                                    boolean allowInsecureConnection,
                                                    String trustStoreTypeString,
                                                    String trustStorePath,
                                                    String trustStorePassword,
                                                    boolean requireTrustedClientCertOnConnect,
                                                    Set<String> ciphers,
                                                    Set<String> protocols)
            throws GeneralSecurityException, IOException {
        KeyStoreSSLContext keyStoreSSLContext = new KeyStoreSSLContext(Mode.SERVER,
                sslProviderString,
                keyStoreTypeString,
                keyStorePath,
                keyStorePassword,
                allowInsecureConnection,
                trustStoreTypeString,
                trustStorePath,
                trustStorePassword,
                requireTrustedClientCertOnConnect,
                ciphers,
                protocols);

        keyStoreSSLContext.createSSLContext();
        return keyStoreSSLContext;
    }

    // the web server only use this method to get SSLContext, it won't use this to configure engine
    // no need ciphers and protocols
    public static SSLContext createServerSslContext(String sslProviderString,
                                                    String keyStoreTypeString,
                                                    String keyStorePath,
                                                    String keyStorePassword,
                                                    boolean allowInsecureConnection,
                                                    String trustStoreTypeString,
                                                    String trustStorePath,
                                                    String trustStorePassword,
                                                    boolean requireTrustedClientCertOnConnect)
            throws GeneralSecurityException, IOException {

        return createServerKeyStoreSslContext(
                sslProviderString,
                keyStoreTypeString,
                keyStorePath,
                keyStorePassword,
                allowInsecureConnection,
                trustStoreTypeString,
                trustStorePath,
                trustStorePassword,
                requireTrustedClientCertOnConnect,
                null,
                null).getSslContext();
    }

    // for web client
    public static SSLContext createClientSslContext(String sslProviderString,
                                                    String keyStoreTypeString,
                                                    String keyStorePath,
                                                    String keyStorePassword,
                                                    boolean allowInsecureConnection,
                                                    String trustStoreTypeString,
                                                    String trustStorePath,
                                                    String trustStorePassword,
                                                    Set<String> ciphers,
                                                    Set<String> protocol)
            throws GeneralSecurityException, IOException {
        KeyStoreSSLContext keyStoreSSLContext = new KeyStoreSSLContext(Mode.CLIENT,
                sslProviderString,
                keyStoreTypeString,
                keyStorePath,
                keyStorePassword,
                allowInsecureConnection,
                trustStoreTypeString,
                trustStorePath,
                trustStorePassword,
                false,
                ciphers,
                protocol);

        return keyStoreSSLContext.createSSLContext();
    }

    // for web client
    public static SSLContext createClientSslContext(String keyStoreTypeString,
                                                    String keyStorePath,
                                                    String keyStorePassword,
                                                    String trustStoreTypeString,
                                                    String trustStorePath,
                                                    String trustStorePassword)
            throws GeneralSecurityException, IOException {
        KeyStoreSSLContext keyStoreSSLContext = new KeyStoreSSLContext(Mode.CLIENT,
                null,
                keyStoreTypeString,
                keyStorePath,
                keyStorePassword,
                false,
                trustStoreTypeString,
                trustStorePath,
                trustStorePassword,
                false,
                null,
                null);

        return keyStoreSSLContext.createSSLContext();
    }
}
