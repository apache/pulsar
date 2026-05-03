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
package org.apache.pulsar.client.api.v5.config;

import java.util.List;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.client.api.v5.auth.CryptoFailureAction;
import org.apache.pulsar.client.api.v5.auth.CryptoKeyReader;

/**
 * End-to-end encryption configuration for producers and consumers.
 *
 * <p>For producers, supply a {@link CryptoKeyReader} and one or more encryption key names —
 * use {@link #forProducer(CryptoKeyReader, String...)} as the typical entry point.
 * For consumers/readers, supply a {@link CryptoKeyReader} and a {@link CryptoFailureAction} —
 * use {@link #forConsumer(CryptoKeyReader, CryptoFailureAction)}.
 *
 * <p>{@link #builder()} is available for callers that need to tune both sides explicitly.
 */
@EqualsAndHashCode
@ToString
public final class EncryptionPolicy {

    private final CryptoKeyReader keyReader;
    private final List<String> keyNames;
    private final CryptoFailureAction failureAction;

    private EncryptionPolicy(CryptoKeyReader keyReader, List<String> keyNames,
                             CryptoFailureAction failureAction) {
        Objects.requireNonNull(keyReader, "keyReader must not be null");
        if (keyNames == null) {
            keyNames = List.of();
        }
        if (failureAction == null) {
            failureAction = CryptoFailureAction.FAIL;
        }
        this.keyReader = keyReader;
        this.keyNames = List.copyOf(keyNames);
        this.failureAction = failureAction;
    }

    /**
     * @return the crypto key reader for loading encryption/decryption keys
     */
    public CryptoKeyReader keyReader() {
        return keyReader;
    }

    /**
     * @return the producer-side encryption key names (empty list for consumer/reader)
     */
    public List<String> keyNames() {
        return keyNames;
    }

    /**
     * @return the action to take when encryption or decryption fails
     */
    public CryptoFailureAction failureAction() {
        return failureAction;
    }

    /**
     * Create an encryption policy for producers.
     *
     * @param keyReader the crypto key reader for loading encryption keys
     * @param keyNames  one or more encryption key names to use
     * @return an {@link EncryptionPolicy} configured for producer-side encryption
     */
    public static EncryptionPolicy forProducer(CryptoKeyReader keyReader, String... keyNames) {
        return new EncryptionPolicy(keyReader, List.of(keyNames), CryptoFailureAction.FAIL);
    }

    /**
     * Create an encryption policy for consumers/readers.
     *
     * @param keyReader     the crypto key reader for loading decryption keys
     * @param failureAction the action to take when decryption fails
     * @return an {@link EncryptionPolicy} configured for consumer-side decryption
     */
    public static EncryptionPolicy forConsumer(CryptoKeyReader keyReader, CryptoFailureAction failureAction) {
        return new EncryptionPolicy(keyReader, List.of(), failureAction);
    }

    /**
     * @return a new builder for constructing an {@link EncryptionPolicy}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link EncryptionPolicy}.
     */
    public static final class Builder {
        private CryptoKeyReader keyReader;
        private List<String> keyNames = List.of();
        private CryptoFailureAction failureAction = CryptoFailureAction.FAIL;

        private Builder() {
        }

        /**
         * Crypto key reader used to load encryption/decryption keys. Required.
         *
         * @param keyReader the key reader
         * @return this builder
         */
        public Builder keyReader(CryptoKeyReader keyReader) {
            this.keyReader = keyReader;
            return this;
        }

        /**
         * Producer-side encryption key names. Leave empty (default) for consumer-side use.
         *
         * @param keyNames the key names
         * @return this builder
         */
        public Builder keyNames(String... keyNames) {
            this.keyNames = List.of(keyNames);
            return this;
        }

        /**
         * Action to take when encryption (producer) or decryption (consumer) fails.
         * Default is {@link CryptoFailureAction#FAIL}.
         *
         * @param failureAction the failure action
         * @return this builder
         */
        public Builder failureAction(CryptoFailureAction failureAction) {
            this.failureAction = failureAction;
            return this;
        }

        /**
         * @return a new {@link EncryptionPolicy} instance
         */
        public EncryptionPolicy build() {
            return new EncryptionPolicy(keyReader, keyNames, failureAction);
        }
    }
}
