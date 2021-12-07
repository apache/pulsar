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

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.tls.TlsHostnameVerifier;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Helper class for the security domain.
 */
@Slf4j
public class SecurityUtility {

    public static final Provider BC_PROVIDER = getProvider();
    public static final String BC_FIPS_PROVIDER_CLASS = "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider";
    public static final String BC_NON_FIPS_PROVIDER_CLASS = "org.bouncycastle.jce.provider.BouncyCastleProvider";
    public static final String CONSCRYPT_PROVIDER_CLASS = "org.conscrypt.OpenSSLProvider";
    public static final Provider CONSCRYPT_PROVIDER = loadConscryptProvider();

    // Security.getProvider("BC") / Security.getProvider("BCFIPS").
    // also used to get Factories. e.g. CertificateFactory.getInstance("X.509", "BCFIPS")
    public static final String BC_FIPS = "BCFIPS";
    public static final String BC = "BC";

    public static boolean isBCFIPS() {
        return BC_PROVIDER.getClass().getCanonicalName().equals(BC_FIPS_PROVIDER_CLASS);
    }

    /**
     * Get Bouncy Castle provider, and call Security.addProvider(provider) if success.
     *  1. try get from classpath.
     *  2. try get from Nar.
     */
    public static Provider getProvider() {
        boolean isProviderInstalled =
                Security.getProvider(BC) != null || Security.getProvider(BC_FIPS) != null;

        if (isProviderInstalled) {
            Provider provider = Security.getProvider(BC) != null
                    ? Security.getProvider(BC)
                    : Security.getProvider(BC_FIPS);
            if (log.isDebugEnabled()) {
                log.debug("Already instantiated Bouncy Castle provider {}", provider.getName());
            }
            return provider;
        }

        // Not installed, try load from class path
        try {
            return getBCProviderFromClassPath();
        } catch (Exception e) {
            log.warn("Not able to get Bouncy Castle provider for both FIPS and Non-FIPS from class path:", e);
            throw new RuntimeException(e);
        }
    }

    private static Provider loadConscryptProvider() {
        Class<?> conscryptClazz;

        try {
            conscryptClazz = Class.forName("org.conscrypt.Conscrypt");
            conscryptClazz.getMethod("checkAvailability").invoke(null);
        } catch (Throwable e) {
            log.warn("Conscrypt isn't available. Using JDK default security provider.", e);
            return null;
        }

        Provider provider;
        try {
            provider = (Provider) Class.forName(CONSCRYPT_PROVIDER_CLASS).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            log.warn("Unable to get security provider for class {}", CONSCRYPT_PROVIDER_CLASS, e);
            return null;
        }

        // Configure Conscrypt's default hostname verifier to use Pulsar's TlsHostnameVerifier which
        // is more relaxed than the Conscrypt HostnameVerifier checking for RFC 2818 conformity.
        //
        // Certificates used in Pulsar docs and examples aren't strictly RFC 2818 compliant since they use the
        // deprecated way of specifying the hostname in the CN field of the subject DN of the certificate.
        // RFC 2818 recommends the use of SAN (subjectAltName) extension for specifying the hostname in the dNSName
        // field of the subjectAltName extension.
        //
        // Conscrypt's default HostnameVerifier has dropped support for the deprecated method of specifying the hostname
        // in the CN field. Pulsar's TlsHostnameVerifier continues to support the CN field.
        //
        // more details of Conscrypt's hostname verification:
        // https://github.com/google/conscrypt/blob/master/IMPLEMENTATION_NOTES.md#hostname-verification
        // there's a bug in Conscrypt while setting a custom HostnameVerifier,
        // https://github.com/google/conscrypt/issues/1015 and therefore this solution alone
        // isn't sufficient to configure Conscrypt's hostname verifier. The method processConscryptTrustManager
        // contains the workaround.
        try {
            HostnameVerifier hostnameVerifier = new TlsHostnameVerifier();
            Object wrappedHostnameVerifier = conscryptClazz
                    .getMethod("wrapHostnameVerifier",
                            new Class[]{HostnameVerifier.class}).invoke(null, hostnameVerifier);
            Method setDefaultHostnameVerifierMethod =
                    conscryptClazz
                            .getMethod("setDefaultHostnameVerifier",
                                    new Class[]{Class.forName("org.conscrypt.ConscryptHostnameVerifier")});
            setDefaultHostnameVerifierMethod.invoke(null, wrappedHostnameVerifier);
        } catch (Exception e) {
            log.warn("Unable to set default hostname verifier for Conscrypt", e);
        }

        Security.addProvider(provider);
        if (log.isDebugEnabled()) {
            log.debug("Added security provider '{}' from class {}", provider.getName(), CONSCRYPT_PROVIDER_CLASS);
        }
        return provider;
    }

    /**
     * Get Bouncy Castle provider from classpath, and call Security.addProvider.
     * Throw Exception if failed.
     */
    public static Provider getBCProviderFromClassPath() throws Exception {
        Class clazz;
        try {
            // prefer non FIPS, for backward compatibility concern.
            clazz = Class.forName(BC_NON_FIPS_PROVIDER_CLASS);
        } catch (ClassNotFoundException cnf) {
            log.warn("Not able to get Bouncy Castle provider: {}, try to get FIPS provider {}",
                    BC_NON_FIPS_PROVIDER_CLASS, BC_FIPS_PROVIDER_CLASS);
            // attempt to use the FIPS provider.
            clazz = Class.forName(BC_FIPS_PROVIDER_CLASS);
        }

        Provider provider = (Provider) clazz.getDeclaredConstructor().newInstance();
        Security.addProvider(provider);
        if (log.isDebugEnabled()) {
            log.debug("Found and Instantiated Bouncy Castle provider in classpath {}", provider.getName());
        }
        return provider;
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertificates)
            throws GeneralSecurityException {
        return createSslContext(allowInsecureConnection, trustCertificates, (Certificate[]) null, (PrivateKey) null);
    }

    public static SslContext createNettySslContextForClient(boolean allowInsecureConnection, String trustCertsFilePath)
            throws GeneralSecurityException, SSLException, FileNotFoundException, IOException {
        return createNettySslContextForClient(allowInsecureConnection, trustCertsFilePath, (Certificate[]) null,
                (PrivateKey) null);
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, String trustCertsFilePath,
            String certFilePath, String keyFilePath) throws GeneralSecurityException {
        X509Certificate[] trustCertificates = loadCertificatesFromPemFile(trustCertsFilePath);
        X509Certificate[] certificates = loadCertificatesFromPemFile(certFilePath);
        PrivateKey privateKey = loadPrivateKeyFromPemFile(keyFilePath);
        return createSslContext(allowInsecureConnection, trustCertificates, certificates, privateKey);
    }

    /**
     * Creates {@link SslContext} with capability to do auto-cert refresh.
     * @param allowInsecureConnection
     * @param trustCertsFilePath
     * @param certFilePath
     * @param keyFilePath
     * @param sslContextAlgorithm
     * @param refreshDurationSec
     * @param executor
     * @return
     * @throws GeneralSecurityException
     * @throws SSLException
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static SslContext createAutoRefreshSslContextForClient(boolean allowInsecureConnection,
            String trustCertsFilePath, String certFilePath, String keyFilePath, String sslContextAlgorithm,
            int refreshDurationSec, ScheduledExecutorService executor)
            throws GeneralSecurityException, SSLException, FileNotFoundException, IOException {
        KeyManagerProxy keyManager = new KeyManagerProxy(certFilePath, keyFilePath, refreshDurationSec, executor);
        SslContextBuilder sslContexBuilder = SslContextBuilder.forClient();
        sslContexBuilder.keyManager(keyManager);
        if (allowInsecureConnection) {
            sslContexBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        } else {
            TrustManagerProxy trustManager = new TrustManagerProxy(trustCertsFilePath, refreshDurationSec, executor);
            sslContexBuilder.trustManager(trustManager);
        }
        return sslContexBuilder.build();
    }

    public static SslContext createNettySslContextForClient(boolean allowInsecureConnection, String trustCertsFilePath,
            String certFilePath, String keyFilePath)
            throws GeneralSecurityException, SSLException, FileNotFoundException, IOException {
        X509Certificate[] certificates = loadCertificatesFromPemFile(certFilePath);
        PrivateKey privateKey = loadPrivateKeyFromPemFile(keyFilePath);
        return createNettySslContextForClient(allowInsecureConnection, trustCertsFilePath, certificates, privateKey);
    }

    public static SslContext createNettySslContextForClient(boolean allowInsecureConnection, String trustCertsFilePath,
            Certificate[] certificates, PrivateKey privateKey)
            throws GeneralSecurityException, SSLException, FileNotFoundException, IOException {

        if (StringUtils.isNotBlank(trustCertsFilePath)) {
            try (FileInputStream trustCertsStream = new FileInputStream(trustCertsFilePath)) {
                return createNettySslContextForClient(allowInsecureConnection, trustCertsStream, certificates,
                        privateKey);
            }
        } else {
            return createNettySslContextForClient(allowInsecureConnection, (InputStream) null, certificates,
                    privateKey);
        }
    }

    public static SslContext createNettySslContextForClient(boolean allowInsecureConnection,
            InputStream trustCertsStream, Certificate[] certificates, PrivateKey privateKey)
            throws GeneralSecurityException, SSLException, FileNotFoundException, IOException {
        SslContextBuilder builder = SslContextBuilder.forClient();
        setupTrustCerts(builder, allowInsecureConnection, trustCertsStream);
        setupKeyManager(builder, privateKey, (X509Certificate[]) certificates);
        return builder.build();
    }

    public static SslContext createNettySslContextForServer(boolean allowInsecureConnection, String trustCertsFilePath,
            String certFilePath, String keyFilePath, Set<String> ciphers, Set<String> protocols,
            boolean requireTrustedClientCertOnConnect)
            throws GeneralSecurityException, SSLException, FileNotFoundException, IOException {
        X509Certificate[] certificates = loadCertificatesFromPemFile(certFilePath);
        PrivateKey privateKey = loadPrivateKeyFromPemFile(keyFilePath);

        SslContextBuilder builder = SslContextBuilder.forServer(privateKey, (X509Certificate[]) certificates);
        setupCiphers(builder, ciphers);
        setupProtocols(builder, protocols);
        if (StringUtils.isNotBlank(trustCertsFilePath)) {
            try (FileInputStream trustCertsStream = new FileInputStream(trustCertsFilePath)) {
                setupTrustCerts(builder, allowInsecureConnection, trustCertsStream);
            }
        } else {
            setupTrustCerts(builder, allowInsecureConnection, null);
        }
        setupKeyManager(builder, privateKey, certificates);
        setupClientAuthentication(builder, requireTrustedClientCertOnConnect);
        return builder.build();
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertficates,
            Certificate[] certificates, PrivateKey privateKey) throws GeneralSecurityException {
        KeyStoreHolder ksh = new KeyStoreHolder();
        TrustManager[] trustManagers = null;
        KeyManager[] keyManagers = null;

        trustManagers = setupTrustCerts(ksh, allowInsecureConnection, trustCertficates, CONSCRYPT_PROVIDER);
        keyManagers = setupKeyManager(ksh, privateKey, certificates);

        SSLContext sslCtx = CONSCRYPT_PROVIDER != null ? SSLContext.getInstance("TLS", CONSCRYPT_PROVIDER)
                : SSLContext.getInstance("TLS");
        sslCtx.init(keyManagers, trustManagers, new SecureRandom());
        sslCtx.getDefaultSSLParameters();
        return sslCtx;
    }

    private static KeyManager[] setupKeyManager(KeyStoreHolder ksh, PrivateKey privateKey, Certificate[] certificates)
            throws KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException {
        KeyManager[] keyManagers = null;
        if (certificates != null && privateKey != null) {
            ksh.setPrivateKey("private", privateKey, certificates);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ksh.getKeyStore(), "".toCharArray());
            keyManagers = kmf.getKeyManagers();
        }
        return keyManagers;
    }

    private static TrustManager[] setupTrustCerts(KeyStoreHolder ksh, boolean allowInsecureConnection,
                                                  Certificate[] trustCertficates, Provider securityProvider)
            throws NoSuchAlgorithmException, KeyStoreException {
        TrustManager[] trustManagers;
        if (allowInsecureConnection) {
            trustManagers = InsecureTrustManagerFactory.INSTANCE.getTrustManagers();
        } else {
            TrustManagerFactory tmf = securityProvider != null
                    ? TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm(), securityProvider)
                    : TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

            if (trustCertficates == null || trustCertficates.length == 0) {
                tmf.init((KeyStore) null);
            } else {
                for (int i = 0; i < trustCertficates.length; i++) {
                    ksh.setCertificate("trust" + i, trustCertficates[i]);
                }
                tmf.init(ksh.getKeyStore());
            }

            trustManagers = tmf.getTrustManagers();

            for (TrustManager trustManager : trustManagers) {
                processConscryptTrustManager(trustManager);
            }
        }
        return trustManagers;
    }

    /***
     * Conscrypt TrustManager instances will be configured to use the Pulsar {@link TlsHostnameVerifier}
     * class.
     * This method is used as a workaround for https://github.com/google/conscrypt/issues/1015
     * when Conscrypt / OpenSSL is used as the TLS security provider.
     *
     * @param trustManagers the array of TrustManager instances to process.
     * @return same instance passed as parameter
     */
    @InterfaceAudience.Private
    public static TrustManager[] processConscryptTrustManagers(TrustManager[] trustManagers) {
        for (TrustManager trustManager : trustManagers) {
            processConscryptTrustManager(trustManager);
        }
        return trustManagers;
    }

    // workaround https://github.com/google/conscrypt/issues/1015
    private static void processConscryptTrustManager(TrustManager trustManager) {
        if (trustManager.getClass().getName().equals("org.conscrypt.TrustManagerImpl")) {
            try {
                Class<?> conscryptClazz = Class.forName("org.conscrypt.Conscrypt");
                Object hostnameVerifier = conscryptClazz.getMethod("getHostnameVerifier",
                        new Class[]{TrustManager.class}).invoke(null, trustManager);
                if (hostnameVerifier == null) {
                    Object defaultHostnameVerifier = conscryptClazz.getMethod("getDefaultHostnameVerifier",
                            new Class[]{TrustManager.class}).invoke(null, trustManager);
                    if (defaultHostnameVerifier != null) {
                        conscryptClazz.getMethod("setHostnameVerifier", new Class[]{
                                TrustManager.class,
                                Class.forName("org.conscrypt.ConscryptHostnameVerifier")
                        }).invoke(null, trustManager, defaultHostnameVerifier);
                    }
                }
            } catch (ReflectiveOperationException e) {
                log.warn("Unable to set hostname verifier for Conscrypt TrustManager implementation", e);
            }
        }
    }

    public static X509Certificate[] loadCertificatesFromPemFile(String certFilePath) throws KeyManagementException {
        X509Certificate[] certificates = null;

        if (certFilePath == null || certFilePath.isEmpty()) {
            return certificates;
        }

        try (FileInputStream input = new FileInputStream(certFilePath)) {
            certificates = loadCertificatesFromPemStream(input);
        } catch (GeneralSecurityException | IOException e) {
            throw new KeyManagementException("Certificate loading error", e);
        }

        return certificates;
    }

    public static X509Certificate[] loadCertificatesFromPemStream(InputStream inStream) throws KeyManagementException  {
        if (inStream == null) {
            return null;
        }
        CertificateFactory cf;
        try {
            if (inStream.markSupported()) {
                inStream.reset();
            }
            cf = CertificateFactory.getInstance("X.509");
            Collection<X509Certificate> collection = (Collection<X509Certificate>) cf.generateCertificates(inStream);
            return collection.toArray(new X509Certificate[collection.size()]);
        } catch (CertificateException | IOException e) {
            throw new KeyManagementException("Certificate loading error", e);
        }
    }

    public static PrivateKey loadPrivateKeyFromPemFile(String keyFilePath) throws KeyManagementException {
        if (keyFilePath == null || keyFilePath.isEmpty()) {
            return null;
        }

        PrivateKey privateKey;

        try (FileInputStream input = new FileInputStream(keyFilePath)) {
            privateKey = loadPrivateKeyFromPemStream(input);
        } catch (IOException e) {
            throw new KeyManagementException("Private key loading error", e);
        }

        return privateKey;
    }

    public static PrivateKey loadPrivateKeyFromPemStream(InputStream inStream) throws KeyManagementException {
        if (inStream == null) {
            return null;
        }

        PrivateKey privateKey;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8))) {
            if (inStream.markSupported()) {
                inStream.reset();
            }
            StringBuilder sb = new StringBuilder();
            String currentLine = null;

            // Jump to the first line after -----BEGIN [RSA] PRIVATE KEY-----
            while ((currentLine = reader.readLine()) != null && !currentLine.startsWith("-----BEGIN")) {
                reader.readLine();
            }

            // Stop (and skip) at the last line that has, say, -----END [RSA] PRIVATE KEY-----
            while ((currentLine = reader.readLine()) != null && !currentLine.startsWith("-----END")) {
                sb.append(currentLine);
            }

            KeyFactory kf = KeyFactory.getInstance("RSA");
            KeySpec keySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(sb.toString()));
            privateKey = kf.generatePrivate(keySpec);
        } catch (GeneralSecurityException | IOException e) {
            throw new KeyManagementException("Private key loading error", e);
        }

        return privateKey;
    }

    private static void setupTrustCerts(SslContextBuilder builder, boolean allowInsecureConnection,
            InputStream trustCertsStream) throws IOException, FileNotFoundException {
        if (allowInsecureConnection) {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        } else {
            if (trustCertsStream != null) {
                builder.trustManager(trustCertsStream);
            } else {
                builder.trustManager((File) null);
            }
        }
    }

    private static void setupKeyManager(SslContextBuilder builder, PrivateKey privateKey,
            X509Certificate[] certificates) {
        builder.keyManager(privateKey, (X509Certificate[]) certificates);
    }

    private static void setupCiphers(SslContextBuilder builder, Set<String> ciphers) {
        if (ciphers != null && ciphers.size() > 0) {
            builder.ciphers(ciphers);
        }
    }

    private static void setupProtocols(SslContextBuilder builder, Set<String> protocols) {
        if (protocols != null && protocols.size() > 0) {
            builder.protocols(protocols.toArray(new String[protocols.size()]));
        }
    }

    private static void setupClientAuthentication(SslContextBuilder builder,
        boolean requireTrustedClientCertOnConnect) {
        if (requireTrustedClientCertOnConnect) {
            builder.clientAuth(ClientAuth.REQUIRE);
        } else {
            builder.clientAuth(ClientAuth.OPTIONAL);
        }
    }

    public static SslContextFactory createSslContextFactory(boolean tlsAllowInsecureConnection,
            String tlsTrustCertsFilePath, String tlsCertificateFilePath, String tlsKeyFilePath,
            boolean tlsRequireTrustedClientCertOnConnect, boolean autoRefresh, long certRefreshInSec)
            throws GeneralSecurityException, SSLException, FileNotFoundException, IOException {
        SslContextFactory sslCtxFactory = null;
        if (autoRefresh) {
            sslCtxFactory = new SslContextFactoryWithAutoRefresh(tlsAllowInsecureConnection, tlsTrustCertsFilePath,
                tlsCertificateFilePath, tlsKeyFilePath, tlsRequireTrustedClientCertOnConnect, 0);
        } else {
            sslCtxFactory = new SslContextFactory();
            SSLContext sslCtx = createSslContext(tlsAllowInsecureConnection, tlsTrustCertsFilePath,
                tlsCertificateFilePath, tlsKeyFilePath);
            sslCtxFactory.setSslContext(sslCtx);
        }
        if (tlsRequireTrustedClientCertOnConnect) {
            sslCtxFactory.setNeedClientAuth(true);
        } else {
            sslCtxFactory.setWantClientAuth(true);
        }
        sslCtxFactory.setTrustAll(true);
        return sslCtxFactory;
    }

    /**
     * {@link SslContextFactory} that auto-refresh SSLContext.
     */
    static class SslContextFactoryWithAutoRefresh extends SslContextFactory {

        private final DefaultSslContextBuilder sslCtxRefresher;

        public SslContextFactoryWithAutoRefresh(boolean tlsAllowInsecureConnection, String tlsTrustCertsFilePath,
                String tlsCertificateFilePath, String tlsKeyFilePath, boolean tlsRequireTrustedClientCertOnConnect,
                long certRefreshInSec)
                throws SSLException, FileNotFoundException, GeneralSecurityException, IOException {
            super();
            sslCtxRefresher = new DefaultSslContextBuilder(tlsAllowInsecureConnection, tlsTrustCertsFilePath,
                    tlsCertificateFilePath, tlsKeyFilePath, tlsRequireTrustedClientCertOnConnect, certRefreshInSec);
            if (CONSCRYPT_PROVIDER != null) {
                setProvider(CONSCRYPT_PROVIDER.getName());
            }
        }

        @Override
        public SSLContext getSslContext() {
            return sslCtxRefresher.get();
        }
    }
}
