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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.eclipse.jetty.util.ssl.SslContextFactory;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class SecurityUtility {

    static {
        // Fixes loading PKCS8Key file: https://stackoverflow.com/a/18912362
        java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
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
        SslContextBuilder builder = SslContextBuilder.forClient();
        setupTrustCerts(builder, allowInsecureConnection, trustCertsFilePath);
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
        setupTrustCerts(builder, allowInsecureConnection, trustCertsFilePath);
        setupKeyManager(builder, privateKey, certificates);
        setupClientAuthentication(builder, requireTrustedClientCertOnConnect);
        return builder.build();
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertficates,
            Certificate[] certificates, PrivateKey privateKey) throws GeneralSecurityException {
        KeyStoreHolder ksh = new KeyStoreHolder();
        TrustManager[] trustManagers = null;
        KeyManager[] keyManagers = null;

        trustManagers = setupTrustCerts(ksh, allowInsecureConnection, trustCertficates);
        keyManagers = setupKeyManager(ksh, privateKey, certificates);

        SSLContext sslCtx = SSLContext.getInstance("TLS");
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
            Certificate[] trustCertficates) throws NoSuchAlgorithmException, KeyStoreException {
        TrustManager[] trustManagers;
        if (allowInsecureConnection) {
            trustManagers = InsecureTrustManagerFactory.INSTANCE.getTrustManagers();
        } else {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

            if (trustCertficates == null || trustCertficates.length == 0) {
                tmf.init((KeyStore) null);
            } else {
                for (int i = 0; i < trustCertficates.length; i++) {
                    ksh.setCertificate("trust" + i, trustCertficates[i]);
                }
                tmf.init(ksh.getKeyStore());
            }

            trustManagers = tmf.getTrustManagers();
        }
        return trustManagers;
    }

    public static X509Certificate[] loadCertificatesFromPemFile(String certFilePath) throws KeyManagementException {
        X509Certificate[] certificates = null;

        if (certFilePath == null || certFilePath.isEmpty()) {
            return certificates;
        }

        try (FileInputStream input = new FileInputStream(certFilePath)) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Collection<X509Certificate> collection = (Collection<X509Certificate>) cf.generateCertificates(input);
            certificates = collection.toArray(new X509Certificate[collection.size()]);
        } catch (GeneralSecurityException | IOException e) {
            throw new KeyManagementException("Certificate loading error", e);
        }

        return certificates;
    }

    public static PrivateKey loadPrivateKeyFromPemFile(String keyFilePath) throws KeyManagementException {
        PrivateKey privateKey = null;

        if (keyFilePath == null || keyFilePath.isEmpty()) {
            return privateKey;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(keyFilePath))) {
            StringBuilder sb = new StringBuilder();
            String previousLine = "";
            String currentLine = null;

            // Skip the first line (-----BEGIN RSA PRIVATE KEY-----)
            reader.readLine();
            while ((currentLine = reader.readLine()) != null) {
                sb.append(previousLine);
                previousLine = currentLine;
            }
            // Skip the last line (-----END RSA PRIVATE KEY-----)

            KeyFactory kf = KeyFactory.getInstance("RSA");
            KeySpec keySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(sb.toString()));
            privateKey = kf.generatePrivate(keySpec);
        } catch (GeneralSecurityException | IOException e) {
            throw new KeyManagementException("Private key loading error", e);
        }

        return privateKey;
    }

    private static void setupTrustCerts(SslContextBuilder builder, boolean allowInsecureConnection,
            String trustCertsFilePath) throws IOException, FileNotFoundException {
        if (allowInsecureConnection) {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        } else {
            if (trustCertsFilePath != null && trustCertsFilePath.length() != 0) {
                try (FileInputStream input = new FileInputStream(trustCertsFilePath)) {
                    builder.trustManager(input);
                }
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

    private static void setupClientAuthentication(SslContextBuilder builder, boolean requireTrustedClientCertOnConnect) {
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
     * {@link SslContextFactory} that auto-refresh SSLContext
     *
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
        }

        @Override
        public SSLContext getSslContext() {
            return sslCtxRefresher.get();
        }
    }
}
