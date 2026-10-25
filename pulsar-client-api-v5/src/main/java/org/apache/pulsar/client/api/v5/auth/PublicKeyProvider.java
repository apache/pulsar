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

import java.util.concurrent.CompletableFuture;

/**
 * Producer-side SPI: load a public key by name for end-to-end message encryption.
 *
 * <p>The provider is consulted at producer creation time and on every key rotation.
 * The signature is asynchronous so an implementation backed by a remote KMS
 * (Vault, AWS KMS, GCP KMS, ...) can fetch keys without blocking the client's IO
 * thread. For local key stores, return a completed future.
 *
 * <p>For a simple file-based provider, see {@link PemFileKeyProvider}.
 */
public interface PublicKeyProvider {

    /**
     * Look up the public key with the given name.
     *
     * @param keyName the key identifier as configured on the producer
     * @return a future completing with the public key, or completing exceptionally
     *         if the key cannot be found or loaded
     */
    CompletableFuture<EncryptionKey> getPublicKey(String keyName);
}
