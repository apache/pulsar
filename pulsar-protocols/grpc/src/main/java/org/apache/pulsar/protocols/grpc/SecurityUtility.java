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
package org.apache.pulsar.protocols.grpc;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import javax.net.ssl.SSLException;
import java.io.*;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.Set;

/**
 * Helper class for the security domain.
 */
public class SecurityUtility {

    static {
        // Fixes loading PKCS8Key file: https://stackoverflow.com/a/18912362
        java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    public static SslContext createNettySslContextForClient(boolean allowInsecureConnection, String trustCertsFilePath)
            throws GeneralSecurityException, SSLException, FileNotFoundException, IOException {
        return createNettySslContextForClient(allowInsecureConnection, trustCertsFilePath, (Certificate[]) null,
                (PrivateKey) null);
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
        SslContextBuilder builder = GrpcSslContexts.forClient();
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

        SslContextBuilder builder = GrpcSslContexts.forServer(new FileInputStream(certFilePath), new FileInputStream(keyFilePath));
        setupCiphers(builder, ciphers);
        setupProtocols(builder, protocols);
        setupTrustCerts(builder, allowInsecureConnection, trustCertsFilePath);
        setupKeyManager(builder, privateKey, certificates);
        setupClientAuthentication(builder, requireTrustedClientCertOnConnect);
        return builder.build();
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

    private static void setupClientAuthentication(SslContextBuilder builder,
            boolean requireTrustedClientCertOnConnect) {
        if (requireTrustedClientCertOnConnect) {
            builder.clientAuth(ClientAuth.REQUIRE);
        } else {
            builder.clientAuth(ClientAuth.OPTIONAL);
        }
    }

}
