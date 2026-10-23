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
import org.apache.pulsar.client.api.v5.auth.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.auth.PublicKeyProvider;

/**
 * Producer-side end-to-end encryption configuration.
 *
 * <p>Construct via {@link #builder()}. Required: a {@link PublicKeyProvider} and at
 * least one key name.
 */
@EqualsAndHashCode
@ToString
public final class ProducerEncryptionPolicy {

    private final PublicKeyProvider publicKeyProvider;
    private final List<String> keyNames;
    private final ProducerCryptoFailureAction failureAction;

    private ProducerEncryptionPolicy(PublicKeyProvider publicKeyProvider,
                                     List<String> keyNames,
                                     ProducerCryptoFailureAction failureAction) {
        Objects.requireNonNull(publicKeyProvider, "publicKeyProvider must not be null");
        Objects.requireNonNull(keyNames, "keyNames must not be null");
        if (keyNames.isEmpty()) {
            throw new IllegalArgumentException("at least one key name must be configured");
        }
        Objects.requireNonNull(failureAction, "failureAction must not be null");
        this.publicKeyProvider = publicKeyProvider;
        this.keyNames = List.copyOf(keyNames);
        this.failureAction = failureAction;
    }

    /**
     * @return the provider used to load public keys for encryption
     */
    public PublicKeyProvider publicKeyProvider() {
        return publicKeyProvider;
    }

    /**
     * @return the configured key names; the producer encrypts each message's data
     *         key with every public key listed here
     */
    public List<String> keyNames() {
        return keyNames;
    }

    /**
     * @return the action the producer takes when encryption fails
     */
    public ProducerCryptoFailureAction failureAction() {
        return failureAction;
    }

    /**
     * @return a new builder for constructing a {@link ProducerEncryptionPolicy}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link ProducerEncryptionPolicy}.
     */
    public static final class Builder {
        private PublicKeyProvider publicKeyProvider;
        private List<String> keyNames = List.of();
        private ProducerCryptoFailureAction failureAction = ProducerCryptoFailureAction.FAIL;

        private Builder() {
        }

        /**
         * Provider used to load public keys. Required.
         *
         * @param publicKeyProvider the public-key provider
         * @return this builder
         */
        public Builder publicKeyProvider(PublicKeyProvider publicKeyProvider) {
            this.publicKeyProvider = publicKeyProvider;
            return this;
        }

        /**
         * Single key name shortcut — equivalent to {@code keyNames(List.of(name))}.
         *
         * @param keyName the key name
         * @return this builder
         */
        public Builder keyName(String keyName) {
            this.keyNames = List.of(keyName);
            return this;
        }

        /**
         * Multiple key names. The producer encrypts each message's data key with
         * every public key listed here, so any consumer with one of the matching
         * private keys can decrypt.
         *
         * @param keyNames one or more key names
         * @return this builder
         */
        public Builder keyNames(String... keyNames) {
            this.keyNames = List.of(keyNames);
            return this;
        }

        /**
         * Action to take when encryption fails. Default: {@link ProducerCryptoFailureAction#FAIL}.
         *
         * @param failureAction the failure action
         * @return this builder
         */
        public Builder failureAction(ProducerCryptoFailureAction failureAction) {
            this.failureAction = failureAction;
            return this;
        }

        /**
         * @return a new {@link ProducerEncryptionPolicy} instance
         */
        public ProducerEncryptionPolicy build() {
            return new ProducerEncryptionPolicy(publicKeyProvider, keyNames, failureAction);
        }
    }
}
