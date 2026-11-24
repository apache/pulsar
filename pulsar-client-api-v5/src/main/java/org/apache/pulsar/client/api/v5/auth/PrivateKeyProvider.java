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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Consumer-side SPI: load a private key by name for end-to-end message decryption.
 *
 * <p>The provider is consulted on every encrypted message the consumer receives.
 * The signature is asynchronous so an implementation backed by a remote KMS
 * (Vault, AWS KMS, GCP KMS, ...) can fetch keys without blocking the client's IO
 * thread. For local key stores, return a completed future.
 *
 * <p>For a simple file-based provider, see {@link PemFileKeyProvider}.
 */
public interface PrivateKeyProvider {

    /**
     * Look up the private key for the given name.
     *
     * <p>{@code metadata} carries any hints the producer attached when the message
     * was encrypted — typically a key version or rotation marker. Implementations
     * that don't rotate keys can ignore it.
     *
     * @param keyName  the key identifier the producer used to encrypt
     * @param metadata producer-supplied hints about the key (never {@code null};
     *                 empty when the producer didn't attach any)
     * @return a future completing with the private key, or completing exceptionally
     *         if the key cannot be found or loaded
     */
    CompletableFuture<EncryptionKey> getPrivateKey(String keyName, Map<String, String> metadata);
}
