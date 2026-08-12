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
package org.apache.pulsar.common.util.tls;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.pulsar.common.util.KeyStoreHolder;

/**
 * Assembles JDK {@link SSLContext} instances from loaded certificate/key material, resolving the security
 * provider through {@link JcaProviders} and parsing PEM inputs through {@link PemReader}. This is the JDK
 * {@code SSLContext} assembly primitive extracted from the {@code SecurityUtility} grab-bag (PIP-478);
 * the default file-based TLS factory ({@code TlsContexts}) builds its JDK fallback context here, and TLS
 * tests reuse it to construct JDK contexts from PEM material.
 */
public final class JdkSslContexts {

    private JdkSslContexts() {
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertificates,
                                              String providerName)
            throws GeneralSecurityException {
        return createSslContext(allowInsecureConnection, trustCertificates, null, null, providerName);
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, String trustCertsFilePath,
            String certFilePath, String keyFilePath, String providerName) throws GeneralSecurityException {
        return createSslContext(allowInsecureConnection, trustCertsFilePath, certFilePath, keyFilePath, providerName,
                null);
    }

    /**
     * Load PEM material through a pinned JCA provider (PIP-478 {@code jcaProvider}) and assemble a JDK
     * {@link SSLContext} with it.
     *
     * @param allowInsecureConnection whether to trust all certificates (insecure)
     * @param trustCertsFilePath      the PEM trust certificates path
     * @param certFilePath            the PEM certificate path
     * @param keyFilePath             the PEM private key path
     * @param providerName            the JSSE provider name, or {@code null} for the platform default
     * @param jcaProvider             the pinned JCA (material) provider, or {@code null} for the JVM search order
     * @return the assembled JDK {@link SSLContext}
     * @throws GeneralSecurityException if the material cannot be loaded or the context cannot be assembled
     */
    public static SSLContext createSslContext(boolean allowInsecureConnection, String trustCertsFilePath,
            String certFilePath, String keyFilePath, String providerName, Provider jcaProvider)
            throws GeneralSecurityException {
        X509Certificate[] trustCertificates = PemReader.loadCertificatesFromPemFile(trustCertsFilePath, jcaProvider);
        X509Certificate[] certificates = PemReader.loadCertificatesFromPemFile(certFilePath, jcaProvider);
        PrivateKey privateKey = PemReader.loadPrivateKeyFromPemFile(keyFilePath, jcaProvider);
        return createSslContextWithProvider(allowInsecureConnection, trustCertificates, certificates, privateKey,
                JcaProviders.resolveProvider(providerName), jcaProvider);
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertficates,
                                              Certificate[] certificates, PrivateKey privateKey)
            throws GeneralSecurityException {
        return createSslContext(allowInsecureConnection, trustCertficates, certificates, privateKey,
                (String) null);
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertficates,
                                              Certificate[] certificates, PrivateKey privateKey, String providerName)
            throws GeneralSecurityException {
        return createSslContextWithProvider(allowInsecureConnection, trustCertficates, certificates, privateKey,
                JcaProviders.resolveProvider(providerName));
    }

    /**
     * Assemble a JDK {@link SSLContext} backed by an already-resolved {@link Provider} (or the platform default
     * when {@code provider} is {@code null}). This is the PIP-478 {@code jsseProvider} entry point: the caller
     * resolves the named JSSE (SSLContext) provider (fail-loud) via
     * {@link JcaProviders#resolveNamedProvider(String)} and passes it here, so the JDK-engine web/Jetty and
     * fallback paths honor a pinned FIPS JSSE provider (e.g. BCJSSE, backed by BCFIPS as its crypto provider).
     *
     * @param allowInsecureConnection whether to trust all certificates (insecure)
     * @param trustCertficates        the trusted CA certificates (may be null/empty)
     * @param certificates            the key-cert chain (may be null when no client/server cert)
     * @param privateKey              the private key (may be null)
     * @param provider                the resolved crypto provider, or {@code null} for the platform default
     * @return the assembled JDK {@link SSLContext}
     * @throws GeneralSecurityException if the context cannot be assembled
     */
    public static SSLContext createSslContextWithProvider(boolean allowInsecureConnection,
                                                          Certificate[] trustCertficates, Certificate[] certificates,
                                                          PrivateKey privateKey, Provider provider)
            throws GeneralSecurityException {
        return createSslContextWithProvider(allowInsecureConnection, trustCertficates, certificates, privateKey,
                provider, null);
    }

    /**
     * As {@link #createSslContextWithProvider(boolean, Certificate[], Certificate[], PrivateKey, Provider)},
     * additionally pinning the JCA (material) provider used for the in-memory carrier keystores.
     *
     * @param allowInsecureConnection whether to trust all certificates (insecure)
     * @param trustCertficates        the trusted CA certificates (may be null/empty)
     * @param certificates            the key-cert chain (may be null when no client/server cert)
     * @param privateKey              the private key (may be null)
     * @param provider                the resolved JSSE provider, or {@code null} for the platform default
     * @param jcaProvider             the pinned JCA (material) provider, or {@code null} for the JVM search order
     * @return the assembled JDK {@link SSLContext}
     * @throws GeneralSecurityException if the context cannot be assembled
     */
    public static SSLContext createSslContextWithProvider(boolean allowInsecureConnection,
                                                          Certificate[] trustCertficates, Certificate[] certificates,
                                                          PrivateKey privateKey, Provider provider,
                                                          Provider jcaProvider)
            throws GeneralSecurityException {
        KeyManager[] keyManagers = setupKeyManager(privateKey, certificates, provider, jcaProvider);
        return assembleSslContext(allowInsecureConnection, trustCertficates, keyManagers, provider, jcaProvider);
    }

    /**
     * Assemble a JDK {@link SSLContext} from already-built {@link KeyManager}s (the keystore multi-alias path):
     * the caller has built the {@code KeyManager}s from a whole keystore via {@link #createKeyManagerFactory}
     * so JSSE can select an alias by the peer's requested key type / acceptable issuers, rather than being
     * pinned to a single private key. The trust side is set up exactly as the single-key overload.
     *
     * @param allowInsecureConnection whether to trust all certificates (insecure)
     * @param trustCertficates        the trusted CA certificates (may be null/empty)
     * @param keyManagers             the pre-built key managers (may be null when no client/server identity)
     * @param provider                the resolved JSSE provider, or {@code null} for the platform default
     * @return the assembled JDK {@link SSLContext}
     * @throws GeneralSecurityException if the context cannot be assembled
     */
    public static SSLContext createSslContextWithProvider(boolean allowInsecureConnection,
                                                          Certificate[] trustCertficates, KeyManager[] keyManagers,
                                                          Provider provider)
            throws GeneralSecurityException {
        return assembleSslContext(allowInsecureConnection, trustCertficates, keyManagers, provider, null);
    }

    /**
     * As {@link #createSslContextWithProvider(boolean, Certificate[], KeyManager[], Provider)}, additionally
     * pinning the JCA (material) provider used for the in-memory trust carrier keystore.
     *
     * @param allowInsecureConnection whether to trust all certificates (insecure)
     * @param trustCertficates        the trusted CA certificates (may be null/empty)
     * @param keyManagers             the pre-built key managers (may be null when no client/server identity)
     * @param provider                the resolved JSSE provider, or {@code null} for the platform default
     * @param jcaProvider             the pinned JCA (material) provider, or {@code null} for the JVM search order
     * @return the assembled JDK {@link SSLContext}
     * @throws GeneralSecurityException if the context cannot be assembled
     */
    public static SSLContext createSslContextWithProvider(boolean allowInsecureConnection,
                                                          Certificate[] trustCertficates, KeyManager[] keyManagers,
                                                          Provider provider, Provider jcaProvider)
            throws GeneralSecurityException {
        return assembleSslContext(allowInsecureConnection, trustCertficates, keyManagers, provider, jcaProvider);
    }

    private static SSLContext assembleSslContext(boolean allowInsecureConnection, Certificate[] trustCertficates,
                                                 KeyManager[] keyManagers, Provider provider, Provider jcaProvider)
            throws GeneralSecurityException {
        TrustManager[] trustManagers =
                createTrustManagers(trustCertficates, allowInsecureConnection, provider, jcaProvider);
        SSLContext sslCtx = provider != null ? SSLContext.getInstance("TLS", provider)
                : SSLContext.getInstance("TLS");
        // With a pinned JSSE provider, pass null so the SSLContext uses that provider's own SecureRandom: a
        // `new SecureRandom()` resolves through the JVM search order and would seed a FIPS context from a
        // non-validated module. With no pin, keep the historical explicit instance.
        sslCtx.init(keyManagers, trustManagers, provider != null ? null : new SecureRandom());
        return sslCtx;
    }

    /**
     * Build a {@link KeyManagerFactory} from an initialized {@link KeyStore}, negotiating the algorithm against
     * a pinned {@code jsseProvider}. Preferring the provider's factory keeps a configured FIPS JSSE provider
     * (e.g. BCJSSE) backing the private-key side; the algorithm must be negotiated because BCJSSE registers
     * X.509 (with X509/PKIX aliases) but NOT the JDK's default "SunX509", and a provider with no
     * {@code KeyManagerFactory} service at all (e.g. Conscrypt) falls back to the platform default factory —
     * the pinned provider still supplies the {@code SSLContext}, which consumes standard X509KeyManagers.
     *
     * <p>Because the whole keystore is handed to the factory, the resulting {@code KeyManager}s expose every
     * alias, so JSSE selects an identity by the peer's requested key type / acceptable issuers instead of being
     * pinned to a single entry.
     *
     * @param keyStore    the initialized keystore holding one or more key entries
     * @param keyPassword the password the key entries are stored under
     * @param provider    the resolved JSSE provider, or {@code null} for the platform default
     * @return the initialized {@link KeyManagerFactory}
     * @throws GeneralSecurityException if the factory cannot be built or initialized
     */
    public static KeyManagerFactory createKeyManagerFactory(KeyStore keyStore, char[] keyPassword, Provider provider)
            throws GeneralSecurityException {
        KeyManagerFactory kmf;
        if (provider != null) {
            String algorithm = supportedAlgorithm(provider, "KeyManagerFactory",
                    KeyManagerFactory.getDefaultAlgorithm(), "PKIX");
            kmf = algorithm != null ? KeyManagerFactory.getInstance(algorithm, provider)
                    : KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        } else {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        }
        kmf.init(keyStore, keyPassword);
        return kmf;
    }

    private static KeyManager[] setupKeyManager(PrivateKey privateKey, Certificate[] certificates, Provider provider,
                                                Provider jcaProvider)
            throws GeneralSecurityException {
        KeyManagerFactory kmf = createKeyManagerFactory(privateKey, certificates, provider, jcaProvider);
        return kmf == null ? null : kmf.getKeyManagers();
    }

    /**
     * Build a {@link KeyManagerFactory} over a single PEM identity (private key plus its certificate chain),
     * carrying it into JSSE through an in-memory keystore created by the pinned JCA provider and a
     * {@code KeyManagerFactory} from the pinned JSSE provider.
     *
     * <p>Exposed so the Netty context builders can pin both provider axes too. Handing Netty the raw
     * {@code PrivateKey}/{@code X509Certificate[]} instead makes it build the carrier {@code KeyStore} and the
     * {@code KeyManagerFactory} itself through the JVM provider search order ({@code SslContext
     * .buildKeyManagerFactory} calls the no-provider {@code getInstance} forms), which silently defeats a
     * {@code jsseProvider}/{@code jcaProvider} pin — {@code sslContextProvider} only pins the {@code SSLContext}
     * itself.
     *
     * @param privateKey   the identity's private key, or {@code null} when there is no identity
     * @param certificates the identity's certificate chain, or {@code null} when there is no identity
     * @param provider     the resolved JSSE provider, or {@code null} for the platform default
     * @param jcaProvider  the pinned JCA (material) provider, or {@code null} for the JVM search order
     * @return the initialized factory, or {@code null} when no identity was given
     * @throws GeneralSecurityException if the carrier or the factory cannot be built
     */
    public static KeyManagerFactory createKeyManagerFactory(PrivateKey privateKey, Certificate[] certificates,
                                                            Provider provider, Provider jcaProvider)
            throws GeneralSecurityException {
        if (certificates == null || privateKey == null) {
            return null;
        }
        KeyStoreHolder ksh = new KeyStoreHolder(jcaProvider);
        ksh.setPrivateKey("private", privateKey, certificates);
        // The carrier's entry password is generated per holder (never empty): a FIPS provider in approved-only
        // mode rejects an empty password-based-KDF password on the PKCS12/BCFKS carrier.
        char[] entryPassword = ksh.getEntryPassword();
        try {
            return createKeyManagerFactory(ksh.getKeyStore(), entryPassword, provider);
        } finally {
            Arrays.fill(entryPassword, '\0');
        }
    }

    /**
     * Build the {@link TrustManager}s for a set of trust anchors, honoring both provider axes: the carrier
     * keystore holding the anchors comes from the pinned JCA provider and the {@code TrustManagerFactory} from
     * the pinned JSSE provider (algorithm-negotiated as on the key side). Also applies the Conscrypt
     * hostname-verifier propagation workaround.
     *
     * <p>Exposed for the same reason as {@link #createKeyManagerFactory(PrivateKey, Certificate[], Provider,
     * Provider)}: {@code SslContextBuilder.trustManager(X509Certificate...)} would have Netty build the
     * factory through the JVM provider search order, defeating the pin.
     *
     * @param trustCertificates       the trust anchors; {@code null}/empty means the platform default trust
     * @param allowInsecureConnection whether to trust all certificates (insecure)
     * @param provider                the resolved JSSE provider, or {@code null} for the platform default
     * @param jcaProvider             the pinned JCA (material) provider, or {@code null} for the JVM search order
     * @return the trust managers
     * @throws GeneralSecurityException if the factory cannot be built or initialized
     */
    public static TrustManager[] createTrustManagers(Certificate[] trustCertificates,
                                                     boolean allowInsecureConnection, Provider provider,
                                                     Provider jcaProvider) throws GeneralSecurityException {
        if (allowInsecureConnection) {
            return InsecureTrustManagerFactory.INSTANCE.getTrustManagers();
        }
        // Same algorithm negotiation as the key-manager side: prefer the pinned provider's
        // TrustManagerFactory (BCJSSE registers PKIX, the platform default), fall back to the platform
        // factory for a provider that offers none (e.g. Conscrypt).
        TrustManagerFactory tmf;
        if (provider != null) {
            String algorithm = supportedAlgorithm(provider, "TrustManagerFactory",
                    TrustManagerFactory.getDefaultAlgorithm(), "PKIX");
            tmf = algorithm != null ? TrustManagerFactory.getInstance(algorithm, provider)
                    : TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        } else {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        }

        if (trustCertificates == null || trustCertificates.length == 0) {
            tmf.init((KeyStore) null);
        } else {
            KeyStoreHolder ksh = new KeyStoreHolder(jcaProvider);
            for (int i = 0; i < trustCertificates.length; i++) {
                ksh.setCertificate("trust" + i, trustCertificates[i]);
            }
            tmf.init(ksh.getKeyStore());
        }

        return tmf.getTrustManagers();
    }

    /**
     * Select an algorithm the provider actually implements for a JCA service type: the platform default
     * algorithm when the provider registers it (directly or through an alias), else the given fallback,
     * else {@code null} when the provider offers no such service at all.
     */
    static String supportedAlgorithm(Provider provider, String serviceType, String defaultAlgorithm,
            String fallbackAlgorithm) {
        if (provider.getService(serviceType, defaultAlgorithm) != null) {
            return defaultAlgorithm;
        }
        if (provider.getService(serviceType, fallbackAlgorithm) != null) {
            return fallbackAlgorithm;
        }
        return null;
    }

}
