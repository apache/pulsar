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

import java.io.*;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;

import javax.net.ssl.*;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class SecurityUtility {

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertificates)
            throws GeneralSecurityException {
        return createSslContext(allowInsecureConnection, trustCertificates, (Certificate[]) null, (PrivateKey) null);
    }

    public static SslContext createNettySslContext(boolean allowInsecureConnection, String trustCertsFilePath)
            throws GeneralSecurityException, SSLException, FileNotFoundException {
        return createNettySslContext(allowInsecureConnection, trustCertsFilePath, (Certificate[]) null, (PrivateKey) null);
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, String trustCertsFilePath,
            String certFilePath, String keyFilePath) throws GeneralSecurityException {
        X509Certificate[] trustCertificates = loadCertificatesFromPemFile(trustCertsFilePath);
        X509Certificate[] certificates = loadCertificatesFromPemFile(certFilePath);
        PrivateKey privateKey = loadPrivateKeyFromPemFile(keyFilePath);
        return createSslContext(allowInsecureConnection, trustCertificates, certificates, privateKey);
    }

    public static SslContext createNettySslContext(boolean allowInsecureConnection, String trustCertsFilePath,
                                              String certFilePath, String keyFilePath) throws GeneralSecurityException, SSLException, FileNotFoundException {
        X509Certificate[] certificates = loadCertificatesFromPemFile(certFilePath);
        PrivateKey privateKey = loadPrivateKeyFromPemFile(keyFilePath);
        return createNettySslContext(allowInsecureConnection, trustCertsFilePath, certificates, privateKey);
    }

    public static SslContext createNettySslContext(boolean allowInsecureConnection, String trustCertsFilePath,
                                                   Certificate[] certificates, PrivateKey privateKey) throws GeneralSecurityException, SSLException, FileNotFoundException {
        SslContextBuilder builder = SslContextBuilder.forClient();
        if (allowInsecureConnection) {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        } else {
            if (trustCertsFilePath != null && trustCertsFilePath.length() != 0) {
                builder.trustManager(new FileInputStream(trustCertsFilePath));
            }
        }
        builder.keyManager(privateKey, (X509Certificate[]) certificates);
        return builder.build();
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertficates,
            Certificate[] certificates, PrivateKey privateKey) throws GeneralSecurityException {
        KeyStoreHolder ksh = new KeyStoreHolder();
        TrustManager[] trustManagers = null;
        KeyManager[] keyManagers = null;

        // Set trusted certificate
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

        // Set private key and certificate
        if (certificates != null && privateKey != null) {
            ksh.setPrivateKey("private", privateKey, certificates);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ksh.getKeyStore(), "".toCharArray());
            keyManagers = kmf.getKeyManagers();
        }

        SSLContext sslCtx = SSLContext.getInstance("TLS");
        sslCtx.init(keyManagers, trustManagers, new SecureRandom());
        return sslCtx;
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

}
