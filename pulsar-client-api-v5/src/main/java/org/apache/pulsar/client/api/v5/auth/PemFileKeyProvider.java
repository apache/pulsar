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
package org.apache.pulsar.client.api.v5.auth;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Batteries-included key provider that loads PEM-encoded keys from local files.
 *
 * <p>The same instance can serve as both {@link PublicKeyProvider} and
 * {@link PrivateKeyProvider} — register public keys for the producer side and
 * private keys for the consumer side. In typical setups each side instantiates
 * its own provider, configured only with the keys it actually needs.
 *
 * <p>For more complex sources (KMS, Vault, ...) implement {@link PublicKeyProvider}
 * or {@link PrivateKeyProvider} directly.
 *
 * <pre>{@code
 * var keys = PemFileKeyProvider.builder()
 *         .publicKey("orders-v1", Path.of("/etc/keys/orders-pub.pem"))
 *         .privateKey("orders-v1", Path.of("/etc/keys/orders-priv.pem"))
 *         .build();
 *
 * client.newProducer(Schema.string())
 *       .topic("orders")
 *       .encryptionPolicy(ProducerEncryptionPolicy.builder()
 *               .publicKeyProvider(keys)
 *               .keyName("orders-v1")
 *               .build())
 *       .create();
 * }</pre>
 */
public final class PemFileKeyProvider implements CryptoKeyProvider {

    private final Map<String, Path> publicKeys;
    private final Map<String, Path> privateKeys;

    private PemFileKeyProvider(Map<String, Path> publicKeys, Map<String, Path> privateKeys) {
        this.publicKeys = Map.copyOf(publicKeys);
        this.privateKeys = Map.copyOf(privateKeys);
    }

    @Override
    public CompletableFuture<EncryptionKey> getPublicKey(String keyName) {
        return loadKey(keyName, publicKeys, "public");
    }

    @Override
    public CompletableFuture<EncryptionKey> getPrivateKey(String keyName, Map<String, String> metadata) {
        return loadKey(keyName, privateKeys, "private");
    }

    private static CompletableFuture<EncryptionKey> loadKey(String keyName,
                                                            Map<String, Path> keys,
                                                            String role) {
        Path path = keys.get(keyName);
        if (path == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "no " + role + " key registered for name: " + keyName));
        }
        try {
            byte[] bytes = Files.readAllBytes(path);
            return CompletableFuture.completedFuture(EncryptionKey.of(bytes));
        } catch (IOException e) {
            return CompletableFuture.failedFuture(new IOException(
                    "failed to read " + role + " key '" + keyName + "' from " + path, e));
        }
    }

    /**
     * @return a new builder for constructing a {@link PemFileKeyProvider}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link PemFileKeyProvider}.
     */
    public static final class Builder {
        private final Map<String, Path> publicKeys = new HashMap<>();
        private final Map<String, Path> privateKeys = new HashMap<>();

        private Builder() {
        }

        /**
         * Register a public key file under the given name. Producer-side use.
         *
         * @param keyName the key identifier the producer will reference
         * @param path    path to the PEM-encoded public key file
         * @return this builder
         */
        public Builder publicKey(String keyName, Path path) {
            publicKeys.put(keyName, path);
            return this;
        }

        /**
         * Register a private key file under the given name. Consumer-side use.
         *
         * @param keyName the key identifier the producer used to encrypt
         * @param path    path to the PEM-encoded private key file
         * @return this builder
         */
        public Builder privateKey(String keyName, Path path) {
            privateKeys.put(keyName, path);
            return this;
        }

        /**
         * @return a new {@link PemFileKeyProvider} instance
         */
        public PemFileKeyProvider build() {
            return new PemFileKeyProvider(publicKeys, privateKeys);
        }
    }
}
