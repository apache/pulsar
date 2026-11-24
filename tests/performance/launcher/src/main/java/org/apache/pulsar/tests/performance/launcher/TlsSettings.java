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
package org.apache.pulsar.tests.performance.launcher;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * A scenario's {@code cluster.tls} section: the workloads connect to the brokers with TLS, so that a run measures the
 * cost of the handshakes and of the encryption, which the brokers' and the clients' I/O threads do.
 *
 * <pre>
 * cluster:
 *   tls:
 *     enabled: true
 *     keyAlgorithm: RSA
 * </pre>
 *
 * <p>The launcher creates a certificate authority for each run, which signs a certificate of the brokers whose
 * subject alternative names are the brokers' host and container names, so that the clients verify the brokers'
 * names as they do by default. The brokers open their TLS port beside the plain one, which the proxy, the
 * geo-replication and the launcher's admin requests keep using; only the gateways and the applications connect with
 * TLS. The clients don't authenticate with a certificate of their own.
 *
 * @param enabled whether the workloads connect with TLS
 * @param keyAlgorithm the keys' algorithm: {@code RSA}, the default, 2048-bit RSA keys, or {@code EC}, ECDSA keys on
 *     the P-256 curve, whose handshakes cost less CPU
 */
record TlsSettings(boolean enabled, String keyAlgorithm) {
    static final TlsSettings DISABLED = new TlsSettings(false, null);
    static final String ENABLED = "enabled";
    static final String KEY_ALGORITHM = "keyAlgorithm";
    static final String RSA = "RSA";
    static final String EC = "EC";
    private static final List<String> KEYS = List.of(ENABLED, KEY_ALGORITHM);
    private static final List<String> KEY_ALGORITHMS = List.of(RSA, EC);
    /** Where the brokers and the workloads find the run's certificates, see {@link #writeCertificates}. */
    static final String MOUNT = "/pulsar/performance-tls";
    static final String CA_CERTIFICATE = "ca.cert.pem";
    static final String BROKER_CERTIFICATE = "broker.cert.pem";
    static final String BROKER_KEY = "broker.key-pk8.pem";
    /** The certificates' validity starts this long before the run, in case the containers' clock is behind. */
    private static final Duration CLOCK_SKEW = Duration.ofDays(1);
    private static final Duration VALIDITY = Duration.ofDays(30);

    /** Reads the {@code cluster.tls} section, which may be missing. */
    static TlsSettings read(JsonNode tls) {
        String path = "cluster.tls";
        if (tls.isMissingNode() || tls.isNull()) {
            return DISABLED;
        }
        if (!tls.isObject()) {
            throw new IllegalArgumentException(path + " must be a mapping with " + KEYS);
        }
        tls.fieldNames().forEachRemaining(field -> {
            if (!KEYS.contains(field)) {
                throw new IllegalArgumentException(path + "." + field + " isn't a setting; " + path + " has " + KEYS);
            }
        });
        JsonNode enabled = tls.path(ENABLED);
        if (!enabled.isMissingNode() && !enabled.isNull() && !enabled.isBoolean()
                && !List.of("true", "false").contains(enabled.asText())) {
            throw new IllegalArgumentException(path + "." + ENABLED + " must be true or false, not " + enabled);
        }
        JsonNode keyAlgorithm = tls.path(KEY_ALGORITHM);
        String algorithm = keyAlgorithm.isMissingNode() || keyAlgorithm.isNull() ? RSA : keyAlgorithm.asText();
        if (!KEY_ALGORITHMS.contains(algorithm)) {
            throw new IllegalArgumentException(path + "." + KEY_ALGORITHM + " must be one of " + KEY_ALGORITHMS
                    + ", not " + keyAlgorithm);
        }
        // An environment override sets the value as a string
        return new TlsSettings(enabled.asBoolean(false) || "true".equals(enabled.asText()), algorithm);
    }

    /** The workloads' service URL's scheme. */
    String scheme() {
        return enabled ? "pulsar+ssl://" : "pulsar://";
    }

    /** The brokers' port that the workloads connect to. */
    int brokerPort() {
        return enabled ? PulsarContainer.BROKER_PORT_TLS : PulsarContainer.BROKER_PORT;
    }

    /** The CA certificate that the workloads trust, in their containers; null without TLS. */
    String trustCertsFilePath() {
        return enabled ? MOUNT + "/" + CA_CERTIFICATE : null;
    }

    /**
     * The brokers' environment: {@code env}, and with TLS the settings of the TLS port and of the certificates, which
     * those of {@code env} replace.
     */
    Map<String, String> withBrokerTlsSettings(Map<String, String> env) {
        if (!enabled) {
            return env;
        }
        Map<String, String> brokerEnv = new LinkedHashMap<>();
        brokerEnv.put("brokerServicePortTls", Integer.toString(PulsarContainer.BROKER_PORT_TLS));
        brokerEnv.put("tlsCertificateFilePath", MOUNT + "/" + BROKER_CERTIFICATE);
        brokerEnv.put("tlsKeyFilePath", MOUNT + "/" + BROKER_KEY);
        brokerEnv.put("tlsTrustCertsFilePath", MOUNT + "/" + CA_CERTIFICATE);
        brokerEnv.putAll(env);
        return brokerEnv;
    }

    /**
     * Writes a new CA certificate, and the brokers' certificate and key that it signs, into {@code directory}, which
     * the brokers and the workloads mount at {@link #MOUNT}. The files are readable by every user, since the
     * containers' users aren't the launcher's; the key protects nothing but the run, a test cluster.
     *
     * @param hostNames the brokers' names that the clients connect to, which the certificate names
     */
    void writeCertificates(Path directory, Collection<String> hostNames) throws IOException {
        try {
            Files.createDirectories(directory);
            setPermissions(directory, "rwxr-xr-x");
            Instant now = Instant.now();
            Date notBefore = Date.from(now.minus(CLOCK_SKEW));
            Date notAfter = Date.from(now.plus(VALIDITY));
            SecureRandom random = new SecureRandom();
            JcaX509ExtensionUtils extensions = new JcaX509ExtensionUtils();

            KeyPair caKeys = keyPair();
            X500Name caName = new X500Name("CN=Pulsar performance test CA");
            X509v3CertificateBuilder ca = new JcaX509v3CertificateBuilder(caName, new BigInteger(64, random),
                    notBefore, notAfter, caName, caKeys.getPublic())
                    .addExtension(Extension.basicConstraints, true, new BasicConstraints(true))
                    .addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign))
                    .addExtension(Extension.subjectKeyIdentifier, false,
                            extensions.createSubjectKeyIdentifier(caKeys.getPublic()));
            X509CertificateHolder caCertificate = sign(ca, caKeys);

            KeyPair brokerKeys = keyPair();
            GeneralName[] names = hostNames.stream().distinct()
                    .map(name -> new GeneralName(GeneralName.dNSName, name)).toArray(GeneralName[]::new);
            X509v3CertificateBuilder broker = new JcaX509v3CertificateBuilder(caName, new BigInteger(64, random),
                    notBefore, notAfter, new X500Name("CN=pulsar-broker"), brokerKeys.getPublic())
                    .addExtension(Extension.basicConstraints, true, new BasicConstraints(false))
                    .addExtension(Extension.keyUsage, true, new KeyUsage(RSA.equals(keyAlgorithm)
                            ? KeyUsage.digitalSignature | KeyUsage.keyEncipherment : KeyUsage.digitalSignature))
                    .addExtension(Extension.extendedKeyUsage, false,
                            new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth))
                    .addExtension(Extension.subjectAlternativeName, false, new GeneralNames(names))
                    .addExtension(Extension.subjectKeyIdentifier, false,
                            extensions.createSubjectKeyIdentifier(brokerKeys.getPublic()))
                    .addExtension(Extension.authorityKeyIdentifier, false,
                            extensions.createAuthorityKeyIdentifier(caKeys.getPublic()));
            X509CertificateHolder brokerCertificate = sign(broker, caKeys);

            writePem(directory.resolve(CA_CERTIFICATE), "CERTIFICATE", caCertificate.getEncoded());
            writePem(directory.resolve(BROKER_CERTIFICATE), "CERTIFICATE", brokerCertificate.getEncoded());
            // JCA encodes private keys as PKCS #8, which the broker reads
            writePem(directory.resolve(BROKER_KEY), "PRIVATE KEY", brokerKeys.getPrivate().getEncoded());
        } catch (GeneralSecurityException | OperatorCreationException | CertIOException e) {
            throw new IOException("Couldn't create the run's TLS certificates", e);
        }
    }

    private KeyPair keyPair() throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance(keyAlgorithm);
        if (RSA.equals(keyAlgorithm)) {
            generator.initialize(2048);
        } else {
            generator.initialize(new ECGenParameterSpec("secp256r1"));
        }
        return generator.generateKeyPair();
    }

    private X509CertificateHolder sign(X509v3CertificateBuilder certificate, KeyPair issuerKeys)
            throws OperatorCreationException {
        return certificate.build(new JcaContentSignerBuilder(RSA.equals(keyAlgorithm) ? "SHA256withRSA"
                : "SHA256withECDSA").build(issuerKeys.getPrivate()));
    }

    private static void writePem(Path file, String type, byte[] der) throws IOException {
        String pem = "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                + "\n-----END " + type + "-----\n";
        Files.writeString(file, pem, StandardCharsets.US_ASCII);
        setPermissions(file, "rw-r--r--");
    }

    private static void setPermissions(Path path, String permissions) throws IOException {
        try {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(permissions));
        } catch (UnsupportedOperationException e) {
            // A file system without POSIX permissions, such as on Windows, has no other users to let in
        }
    }
}
