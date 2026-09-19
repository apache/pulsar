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

import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.client.api.v5.auth.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.auth.PrivateKeyProvider;

/**
 * Consumer-side end-to-end decryption configuration.
 *
 * <p>Construct via {@link #builder()}. The {@link PrivateKeyProvider} is required
 * when {@link #failureAction()} is {@link ConsumerCryptoFailureAction#FAIL} (the
 * default — strict mode); for {@link ConsumerCryptoFailureAction#DISCARD} or
 * {@link ConsumerCryptoFailureAction#CONSUME} the provider may be omitted, in
 * which case the consumer just relies on the failure action to decide what to do
 * with encrypted messages it can't decrypt.
 */
@EqualsAndHashCode
@ToString
public final class ConsumerEncryptionPolicy {

    private final PrivateKeyProvider privateKeyProvider;
    private final ConsumerCryptoFailureAction failureAction;

    private ConsumerEncryptionPolicy(PrivateKeyProvider privateKeyProvider,
                                     ConsumerCryptoFailureAction failureAction) {
        Objects.requireNonNull(failureAction, "failureAction must not be null");
        if (failureAction == ConsumerCryptoFailureAction.FAIL && privateKeyProvider == null) {
            throw new IllegalArgumentException(
                    "privateKeyProvider must be set when failureAction is FAIL");
        }
        this.privateKeyProvider = privateKeyProvider;
        this.failureAction = failureAction;
    }

    /**
     * @return the provider used to load private keys for decryption, or {@code null}
     *         when the consumer doesn't decrypt and falls back to the failure action
     *         (DISCARD or CONSUME)
     */
    public PrivateKeyProvider privateKeyProvider() {
        return privateKeyProvider;
    }

    /**
     * @return the action the consumer takes when decryption fails
     */
    public ConsumerCryptoFailureAction failureAction() {
        return failureAction;
    }

    /**
     * @return a new builder for constructing a {@link ConsumerEncryptionPolicy}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link ConsumerEncryptionPolicy}.
     */
    public static final class Builder {
        private PrivateKeyProvider privateKeyProvider;
        private ConsumerCryptoFailureAction failureAction = ConsumerCryptoFailureAction.FAIL;

        private Builder() {
        }

        /**
         * Provider used to load private keys. Required.
         *
         * @param privateKeyProvider the private-key provider
         * @return this builder
         */
        public Builder privateKeyProvider(PrivateKeyProvider privateKeyProvider) {
            this.privateKeyProvider = privateKeyProvider;
            return this;
        }

        /**
         * Action to take when decryption fails. Default: {@link ConsumerCryptoFailureAction#FAIL}.
         *
         * @param failureAction the failure action
         * @return this builder
         */
        public Builder failureAction(ConsumerCryptoFailureAction failureAction) {
            this.failureAction = failureAction;
            return this;
        }

        /**
         * @return a new {@link ConsumerEncryptionPolicy} instance
         */
        public ConsumerEncryptionPolicy build() {
            return new ConsumerEncryptionPolicy(privateKeyProvider, failureAction);
        }
    }
}
