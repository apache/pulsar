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

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
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
 * Parses PEM-encoded X.509 certificates and PKCS#8, PKCS#1 RSA or SEC1 EC private keys from files and streams.
 *
 * <p>SEC1 EC keys require Bouncy Castle bcpkix and its matching dependencies on the class path.
 *
 * <p>Every entry point has an overload taking a pinned JCA {@link Provider}. With {@code null}, or in the
 * no-provider overloads, the JVM provider search order is used. With a provider, the
 * {@code CertificateFactory} and {@code KeyFactory} engines that create the certificates and private keys
 * come from that provider.
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
     * Load a PKCS#8, PKCS#1 RSA or SEC1 EC PEM private key, manufacturing the key object with a pinned JCA provider.
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
     * Load a PKCS#8, PKCS#1 RSA or SEC1 EC PEM private key from a stream, manufacturing the key object
     * with a pinned JCA provider.
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

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8))) {
            if (inStream.markSupported()) {
                inStream.reset();
            }
            StringBuilder sb = new StringBuilder();
            String currentLine = null;

            // Skip preamble lines and allow whitespace around PEM boundaries.
            while ((currentLine = reader.readLine()) != null) {
                currentLine = currentLine.strip();
                if (currentLine.startsWith("-----BEGIN")) {
                    break;
                }
            }

            boolean sec1Ec = "-----BEGIN EC PRIVATE KEY-----".equals(currentLine);
            boolean pkcs1Rsa = "-----BEGIN RSA PRIVATE KEY-----".equals(currentLine);

            // BufferedReader handles LF, CRLF and CR. Ignore whitespace in the Base64 body (RFC 7468 section 2).
            while ((currentLine = reader.readLine()) != null) {
                if (currentLine.strip().startsWith("-----END")) {
                    break;
                }
                for (int i = 0; i < currentLine.length(); i++) {
                    char c = currentLine.charAt(i);
                    if (c != ' ' && (c < '\t' || c > '\r')) {
                        sb.append(c);
                    }
                }
            }
            byte[] encodedKey;
            if (sec1Ec) {
                encodedKey = convertEcPrivateKey(sb.toString(), PemReader.class.getClassLoader());
            } else {
                encodedKey = Base64.getDecoder().decode(sb.toString());
                if (pkcs1Rsa) {
                    encodedKey = wrapRsaPkcs1Key(encodedKey);
                }
            }
            final KeySpec keySpec = new PKCS8EncodedKeySpec(encodedKey);
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

    /**
     * Use optional Bouncy Castle PKIX classes only for SEC1 decoding. The resulting PKCS#8 bytes still
     * pass through the selected JCA provider; parsing does not register or select a BC provider.
     */
    @VisibleForTesting
    static byte[] convertEcPrivateKey(String base64, ClassLoader classLoader) throws KeyManagementException {
        try {
            Class<?> parserClass = Class.forName("org.bouncycastle.openssl.PEMParser", true, classLoader);
            Class<?> keyPairClass = Class.forName("org.bouncycastle.openssl.PEMKeyPair", true, classLoader);
            Class<?> keyInfoClass = Class.forName("org.bouncycastle.asn1.pkcs.PrivateKeyInfo", true, classLoader);
            String pem = "-----BEGIN EC PRIVATE KEY-----\n" + base64 + "\n-----END EC PRIVATE KEY-----\n";
            try (Reader parser = (Reader) parserClass.getConstructor(Reader.class).newInstance(new StringReader(pem))) {
                Object keyPair = parserClass.getMethod("readObject").invoke(parser);
                if (!keyPairClass.isInstance(keyPair)) {
                    throw new KeyManagementException("Invalid BEGIN EC PRIVATE KEY file: expected an EC key pair");
                }
                Object keyInfo = keyPairClass.getMethod("getPrivateKeyInfo").invoke(keyPair);
                return (byte[]) keyInfoClass.getMethod("getEncoded").invoke(keyInfo);
            }
        } catch (ClassNotFoundException | LinkageError e) {
            throw new KeyManagementException("Bouncy Castle bcpkix and its matching dependencies must be on the "
                    + "class path to parse BEGIN EC PRIVATE KEY files, or convert the key to PKCS#8", e);
        } catch (InvocationTargetException e) {
            throw new KeyManagementException("Failed to parse BEGIN EC PRIVATE KEY file", e.getCause());
        } catch (ReflectiveOperationException | IOException e) {
            throw new KeyManagementException("Failed to parse BEGIN EC PRIVATE KEY file", e);
        }
    }

    /**
     * PKCS#8 PrivateKeyInfo contains a version, an AlgorithmIdentifier and the PKCS#1 key in an OCTET STRING.
     * Only add the envelope here; key parsing and validation remain with the selected JCA provider.
     *
     * @see <a href="https://www.rfc-editor.org/rfc/rfc5208.html#section-5">RFC 5208 section 5: PrivateKeyInfo</a>
     * @see <a href="https://www.rfc-editor.org/rfc/rfc8017.html#appendix-A.1">RFC 8017 appendix A.1:
     *      rsaEncryption identifier and NULL parameters</a>
     */
    private static byte[] wrapRsaPkcs1Key(byte[] pkcs1Key) {
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        content.writeBytes(new byte[]{0x02, 0x01, 0x00}); // version 0
        // rsaEncryption (1.2.840.113549.1.1.1), with NULL parameters.
        content.writeBytes(new byte[]{0x30, 0x0d, 0x06, 0x09, 0x2a, (byte) 0x86, 0x48,
                (byte) 0x86, (byte) 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00});
        content.writeBytes(encodeDerValue(0x04, pkcs1Key));
        return encodeDerValue(0x30, content.toByteArray());
    }

    private static byte[] encodeDerValue(int tag, byte[] value) {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        encoded.write(tag);
        if (value.length < 128) {
            encoded.write(value.length);
        } else {
            int lengthBytes = (Integer.SIZE - Integer.numberOfLeadingZeros(value.length) + 7) / 8;
            encoded.write(0x80 | lengthBytes);
            for (int shift = (lengthBytes - 1) * 8; shift >= 0; shift -= 8) {
                encoded.write(value.length >>> shift);
            }
        }
        encoded.writeBytes(value);
        return encoded.toByteArray();
    }
}
