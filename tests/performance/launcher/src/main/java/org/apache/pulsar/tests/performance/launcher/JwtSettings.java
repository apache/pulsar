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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.AuthenticationFactory;

/**
 * A scenario's {@code cluster.jwt} section: the brokers authenticate every client with a JSON Web Token, as Pulsar's
 * token authentication does, so that a run measures what authentication costs, such as verifying the token of each
 * connection that a client opens.
 *
 * <pre>
 * cluster:
 *   jwt:
 *     enabled: true
 *     signatureAlgorithm: RS256
 * </pre>
 *
 * <p>The launcher creates the keys and the tokens for each run, as the Pulsar Helm chart's JWT secret job does with
 * {@code pulsar tokens}: a key pair, or a secret key, and the token of each role that it signs. The gateways and the
 * applications authenticate as {@value #WORKLOAD_ROLE}; the brokers' own clients, such as geo-replication's, and the
 * launcher's admin requests as {@value #ADMIN_ROLE}, the super-user. The brokers don't authorize the roles, so any
 * authenticated client may produce and consume.
 *
 * @param enabled whether the brokers authenticate the clients
 * @param signatureAlgorithm how the tokens are signed: {@code RS256}, the default, with a 2048-bit RSA key pair, whose
 *     public key the brokers verify the tokens with, or {@code HS256}, with a secret key that the brokers share
 */
record JwtSettings(boolean enabled, String signatureAlgorithm) {
    static final JwtSettings DISABLED = new JwtSettings(false, null);
    static final String ENABLED = "enabled";
    static final String SIGNATURE_ALGORITHM = "signatureAlgorithm";
    static final String RS256 = "RS256";
    static final String HS256 = "HS256";
    private static final List<String> KEYS = List.of(ENABLED, SIGNATURE_ALGORITHM);
    private static final List<String> SIGNATURE_ALGORITHMS = List.of(RS256, HS256);
    /** Where the brokers and the workloads find the run's keys and tokens, see {@link #writeKeysAndTokens}. */
    static final String MOUNT = "/pulsar/performance-jwt";
    static final String PUBLIC_KEY = "public.key";
    static final String PRIVATE_KEY = "private.key";
    static final String SECRET_KEY = "secret.key";
    static final String ADMIN_ROLE = "admin";
    static final String WORKLOAD_ROLE = "iot-workload";
    static final String TOKEN_SUFFIX = ".token";
    private static final String TOKEN_PROVIDER = "org.apache.pulsar.broker.authentication.AuthenticationProviderToken";
    static final String TOKEN_AUTHENTICATION = "org.apache.pulsar.client.impl.auth.AuthenticationToken";

    /** Reads the {@code cluster.jwt} section, which may be missing. */
    static JwtSettings read(JsonNode jwt) {
        String path = "cluster.jwt";
        if (jwt.isMissingNode() || jwt.isNull()) {
            return DISABLED;
        }
        if (!jwt.isObject()) {
            throw new IllegalArgumentException(path + " must be a mapping with " + KEYS);
        }
        jwt.fieldNames().forEachRemaining(field -> {
            if (!KEYS.contains(field)) {
                throw new IllegalArgumentException(path + "." + field + " isn't a setting; " + path + " has " + KEYS);
            }
        });
        JsonNode enabled = jwt.path(ENABLED);
        if (!enabled.isMissingNode() && !enabled.isNull() && !enabled.isBoolean()
                && !List.of("true", "false").contains(enabled.asText())) {
            throw new IllegalArgumentException(path + "." + ENABLED + " must be true or false, not " + enabled);
        }
        JsonNode signatureAlgorithm = jwt.path(SIGNATURE_ALGORITHM);
        String algorithm = signatureAlgorithm.isMissingNode() || signatureAlgorithm.isNull() ? RS256
                : signatureAlgorithm.asText();
        if (!SIGNATURE_ALGORITHMS.contains(algorithm)) {
            throw new IllegalArgumentException(path + "." + SIGNATURE_ALGORITHM + " must be one of "
                    + SIGNATURE_ALGORITHMS + ", not " + signatureAlgorithm);
        }
        // An environment override sets the value as a string
        return new JwtSettings(enabled.asBoolean(false) || "true".equals(enabled.asText()), algorithm);
    }

    /** The token that the workloads authenticate with, in their containers; null without JWT. */
    String workloadTokenFilePath() {
        return enabled ? MOUNT + "/" + WORKLOAD_ROLE + TOKEN_SUFFIX : null;
    }

    /**
     * The brokers' environment: {@code env}, and with JWT the settings of token authentication and of the brokers'
     * own clients' token, which those of {@code env} replace.
     */
    Map<String, String> withBrokerJwtSettings(Map<String, String> env) {
        if (!enabled) {
            return env;
        }
        Map<String, String> brokerEnv = new LinkedHashMap<>();
        brokerEnv.put("authenticationEnabled", "true");
        brokerEnv.put("authenticationProviders", TOKEN_PROVIDER);
        if (RS256.equals(signatureAlgorithm)) {
            // RS256 is the brokers' default tokenPublicAlg, which conf/broker.conf doesn't have
            brokerEnv.put("tokenPublicKey", "file://" + MOUNT + "/" + PUBLIC_KEY);
        } else {
            brokerEnv.put("tokenSecretKey", "file://" + MOUNT + "/" + SECRET_KEY);
        }
        brokerEnv.put("superUserRoles", ADMIN_ROLE);
        brokerEnv.put("brokerClientAuthenticationPlugin", TOKEN_AUTHENTICATION);
        brokerEnv.put("brokerClientAuthenticationParameters", "file://" + MOUNT + "/" + ADMIN_ROLE + TOKEN_SUFFIX);
        brokerEnv.putAll(env);
        return brokerEnv;
    }

    /**
     * Writes new keys, and the tokens of {@link #ADMIN_ROLE} and {@link #WORKLOAD_ROLE} that they sign, into
     * {@code directory}, which the brokers and the workloads mount at {@link #MOUNT}, in the formats of
     * {@code pulsar tokens}: the keys' raw bytes, the public key as X.509 and the private key as PKCS #8. The files are
     * readable by every user, since the containers' users aren't the launcher's; the keys protect nothing but the run,
     * a test cluster.
     *
     * @return the admin token, which the launcher's admin requests authenticate with
     */
    String writeKeysAndTokens(Path directory) throws IOException {
        try {
            Files.createDirectories(directory);
            setPermissions(directory, "rwxr-xr-x");
            Key signingKey;
            if (RS256.equals(signatureAlgorithm)) {
                KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
                generator.initialize(2048);
                KeyPair keyPair = generator.generateKeyPair();
                write(directory.resolve(PUBLIC_KEY), keyPair.getPublic().getEncoded());
                write(directory.resolve(PRIVATE_KEY), keyPair.getPrivate().getEncoded());
                signingKey = keyPair.getPrivate();
            } else {
                // 256 bits, which HS256 requires at least
                byte[] secret = new byte[32];
                new SecureRandom().nextBytes(secret);
                write(directory.resolve(SECRET_KEY), secret);
                signingKey = new SecretKeySpec(secret, "HmacSHA256");
            }
            String adminToken = token(signingKey, ADMIN_ROLE);
            write(directory.resolve(ADMIN_ROLE + TOKEN_SUFFIX), adminToken.getBytes(StandardCharsets.US_ASCII));
            write(directory.resolve(WORKLOAD_ROLE + TOKEN_SUFFIX),
                    token(signingKey, WORKLOAD_ROLE).getBytes(StandardCharsets.US_ASCII));
            return adminToken;
        } catch (GeneralSecurityException e) {
            throw new IOException("Couldn't create the run's JWT keys and tokens", e);
        }
    }

    /** A token of {@code subject} without an expiry, as {@code pulsar tokens create} makes by default. */
    private String token(Key signingKey, String subject) throws GeneralSecurityException {
        ObjectMapper mapper = new ObjectMapper();
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String unsigned = encoder.encodeToString(mapper.createObjectNode().put("alg", signatureAlgorithm).toString()
                .getBytes(StandardCharsets.UTF_8)) + "."
                + encoder.encodeToString(mapper.createObjectNode().put("sub", subject).toString()
                .getBytes(StandardCharsets.UTF_8));
        byte[] signature;
        if (signingKey instanceof PrivateKey privateKey) {
            Signature signer = Signature.getInstance("SHA256withRSA");
            signer.initSign(privateKey);
            signer.update(unsigned.getBytes(StandardCharsets.US_ASCII));
            signature = signer.sign();
        } else {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(signingKey);
            signature = mac.doFinal(unsigned.getBytes(StandardCharsets.US_ASCII));
        }
        return unsigned + "." + encoder.encodeToString(signature);
    }

    /**
     * A builder of an admin client of {@code serviceHttpUrl} that authenticates with {@code adminToken}, or without
     * authentication when it is null.
     */
    static PulsarAdminBuilder adminBuilder(String serviceHttpUrl, String adminToken) {
        PulsarAdminBuilder builder = PulsarAdmin.builder().serviceHttpUrl(serviceHttpUrl);
        if (adminToken != null) {
            builder.authentication(AuthenticationFactory.token(adminToken));
        }
        return builder;
    }

    private static void write(Path file, byte[] content) throws IOException {
        Files.write(file, content);
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
