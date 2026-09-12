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
import java.util.Objects;

/**
 * A single encryption / decryption key returned by a {@link PublicKeyProvider} or
 * {@link PrivateKeyProvider}.
 *
 * <p>The producer-side flow returns just the key bytes. The consumer-side flow may
 * include {@link #metadata()} that the producer attached when the message was
 * encrypted (e.g. a key version) — the {@link PrivateKeyProvider} can use it to
 * pick the right private key when keys have been rotated.
 *
 * <p>Use {@link #of(byte[])} when there's no metadata, or {@link #of(byte[], Map)}
 * to attach producer-side hints alongside the bytes.
 */
public final class EncryptionKey {

    private final byte[] key;
    private final Map<String, String> metadata;

    private EncryptionKey(byte[] key, Map<String, String> metadata) {
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    /**
     * @return the raw key bytes
     */
    public byte[] key() {
        return key;
    }

    /**
     * @return key-value metadata associated with the key (never {@code null};
     *         empty when the producer did not attach any)
     */
    public Map<String, String> metadata() {
        return metadata;
    }

    /**
     * Create an {@link EncryptionKey} with no metadata.
     *
     * @param key the raw key bytes
     * @return a new {@link EncryptionKey}
     */
    public static EncryptionKey of(byte[] key) {
        return new EncryptionKey(key, Map.of());
    }

    /**
     * Create an {@link EncryptionKey} with associated metadata.
     *
     * @param key      the raw key bytes
     * @param metadata key-value metadata to attach to the key
     * @return a new {@link EncryptionKey}
     */
    public static EncryptionKey of(byte[] key, Map<String, String> metadata) {
        return new EncryptionKey(key, metadata);
    }
}
