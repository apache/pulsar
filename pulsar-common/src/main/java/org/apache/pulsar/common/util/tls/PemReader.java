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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;

/**
 * Parses PEM-encoded X.509 certificates and PKCS#8 private keys from files and streams. This is the single
 * shared PEM-parsing primitive extracted from the {@code SecurityUtility} grab-bag (PIP-478): it holds
 * no TLS-context assembly or security-provider concerns, only the file/stream-to-material parsing that several
 * callers (the file-based TLS material sources and {@code AuthenticationDataTls}) reuse.
 *
 * <p>Every entry point has an overload taking a pinned JCA {@link Provider} (the PIP-478 {@code jcaProvider}):
 * with {@code null} — and in the no-provider overloads — the JVM provider search order is used, exactly as
 * before; with a provider the {@code CertificateFactory}/{@code KeyFactory} engines that manufacture the
 * {@code X509Certificate}/{@code PrivateKey} objects come from that provider, so a FIPS deployment parses its
 * material inside the validated module.
 */
@CustomLog
public final class PemReader {

    private static final List<String> KEY_FACTORY_ALGORITHMS = List.of("RSA", "EC");

    private PemReader() {
    }

    public static X509Certificate[] loadCertificatesFromPemFile(String certFilePath) throws KeyManagementException {
        return loadCertificatesFromPemFile(certFilePath, null);
    }

    /**
     * Load PEM certificates, manufacturing them with a pinned JCA provider.
     *
     * @param certFilePath the PEM file path
     * @param jcaProvider  the pinned JCA provider, or {@code null} for the JVM provider search order
     * @return the loaded certificates, or {@code null} when no path was given
     * @throws KeyManagementException if the certificates cannot be loaded
     */
    public static X509Certificate[] loadCertificatesFromPemFile(String certFilePath, Provider jcaProvider)
            throws KeyManagementException {
        X509Certificate[] certificates = null;

        if (certFilePath == null || certFilePath.isEmpty()) {
            return certificates;
        }

        try (FileInputStream input = new FileInputStream(certFilePath)) {
            certificates = loadCertificatesFromPemStream(input, jcaProvider);
        } catch (GeneralSecurityException | IOException e) {
            throw new KeyManagementException("Certificate loading error", e);
        }

        return certificates;
    }

    public static X509Certificate[] loadCertificatesFromPemStream(InputStream inStream) throws KeyManagementException  {
        return loadCertificatesFromPemStream(inStream, null);
    }

    /**
     * Load PEM certificates from a stream, manufacturing them with a pinned JCA provider.
     *
     * @param inStream    the PEM stream
     * @param jcaProvider the pinned JCA provider, or {@code null} for the JVM provider search order
     * @return the loaded certificates, or {@code null} when no stream was given
     * @throws KeyManagementException if the certificates cannot be loaded
     */
    public static X509Certificate[] loadCertificatesFromPemStream(InputStream inStream, Provider jcaProvider)
            throws KeyManagementException  {
        if (inStream == null) {
            return null;
        }
        CertificateFactory cf;
        try {
            if (inStream.markSupported()) {
                inStream.reset();
            }
            cf = JcaKeyStores.certificateFactory("X.509", jcaProvider);
            @SuppressWarnings("unchecked") // CertificateFactory.getInstance("X.509") returns X509Certificate instances
            Collection<X509Certificate> collection = (Collection<X509Certificate>) cf.generateCertificates(inStream);
            return collection.toArray(new X509Certificate[collection.size()]);
        } catch (CertificateException | IOException e) {
            throw new KeyManagementException("Certificate loading error", e);
        }
    }

    public static PrivateKey loadPrivateKeyFromPemFile(String keyFilePath) throws KeyManagementException {
        return loadPrivateKeyFromPemFile(keyFilePath, null);
    }

    /**
     * Load a PKCS#8 PEM private key, manufacturing the key object with a pinned JCA provider.
     *
     * @param keyFilePath the PEM file path
     * @param jcaProvider the pinned JCA provider, or {@code null} for the JVM provider search order
     * @return the loaded private key, or {@code null} when no path was given
     * @throws KeyManagementException if the key cannot be loaded
     */
    public static PrivateKey loadPrivateKeyFromPemFile(String keyFilePath, Provider jcaProvider)
            throws KeyManagementException {
        if (keyFilePath == null || keyFilePath.isEmpty()) {
            return null;
        }

        PrivateKey privateKey;

        try (FileInputStream input = new FileInputStream(keyFilePath)) {
            privateKey = loadPrivateKeyFromPemStream(input, jcaProvider);
        } catch (IOException e) {
            throw new KeyManagementException("Private key loading error", e);
        }

        return privateKey;
    }

    public static PrivateKey loadPrivateKeyFromPemStream(InputStream inStream) throws KeyManagementException {
        return loadPrivateKeyFromPemStream(inStream, null);
    }

    /**
     * Load a PKCS#8 PEM private key from a stream, manufacturing the key object with a pinned JCA provider.
     *
     * <p>The existing per-algorithm loop degrades naturally: an algorithm the pinned provider does not supply
     * is skipped like an algorithm that does not match the key, and the same loud "algorithm is not supported"
     * error is thrown when none of them works.
     *
     * @param inStream    the PEM stream
     * @param jcaProvider the pinned JCA provider, or {@code null} for the JVM provider search order
     * @return the loaded private key, or {@code null} when no stream was given
     * @throws KeyManagementException if the key cannot be loaded
     */
    public static PrivateKey loadPrivateKeyFromPemStream(InputStream inStream, Provider jcaProvider)
            throws KeyManagementException {
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

            // Jump to the first line after -----BEGIN [RSA] PRIVATE KEY-----.
            // One line is consumed per iteration: the extracted-from version discarded a second line in the
            // loop body, so a file whose BEGIN line sat at an ODD offset (one leading comment, blank line, or
            // openssl "Bag Attributes" header) had that BEGIN line swallowed, ran the scan to EOF, and failed
            // with the misleading "private key algorithm is not supported" below.
            while ((currentLine = reader.readLine()) != null && !currentLine.startsWith("-----BEGIN")) {
                // skip preamble
            }

            // Stop (and skip) at the last line that has, say, -----END [RSA] PRIVATE KEY-----
            while ((currentLine = reader.readLine()) != null && !currentLine.startsWith("-----END")) {
                sb.append(currentLine);
            }
            final KeySpec keySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(sb.toString()));
            final List<String> failedAlgorithm = new ArrayList<>(KEY_FACTORY_ALGORITHMS.size());
            for (String algorithm : KEY_FACTORY_ALGORITHMS) {
                try {
                    KeyFactory keyFactory = jcaProvider != null ? KeyFactory.getInstance(algorithm, jcaProvider)
                            : KeyFactory.getInstance(algorithm);
                    PrivateKey key = keyFactory.generatePrivate(keySpec);
                    log.debug().attr("algorithm", algorithm).attr("provider", keyFactory.getProvider().getName())
                            .log("Loaded PEM private key");
                    return key;
                } catch (InvalidKeySpecException | NoSuchAlgorithmException ex) {
                    failedAlgorithm.add(algorithm);
                }
            }
            throw new KeyManagementException("The private key algorithm is not supported. attempted: "
                    + StringUtils.join(failedAlgorithm, ","));
        } catch (IOException e) {
            throw new KeyManagementException("Private key loading error", e);
        }

    }
}
