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

import java.time.Duration;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Transaction configuration for the Pulsar client.
 *
 * <p>Construct via {@link #builder()}.
 */
@EqualsAndHashCode
@ToString
public final class TransactionPolicy {

    private final Duration timeout;

    private TransactionPolicy(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        this.timeout = timeout;
    }

    /**
     * @return transaction timeout — if the transaction is not committed or aborted within this duration,
     *         the broker automatically aborts it
     */
    public Duration timeout() {
        return timeout;
    }

    /**
     * @return a new builder for constructing a {@link TransactionPolicy}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link TransactionPolicy}.
     */
    public static final class Builder {
        private Duration timeout;

        private Builder() {
        }

        /**
         * Transaction timeout. Required. The broker auto-aborts transactions that
         * neither commit nor abort within this duration.
         *
         * @param timeout the transaction timeout
         * @return this builder
         */
        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * @return a new {@link TransactionPolicy} instance
         */
        public TransactionPolicy build() {
            return new TransactionPolicy(timeout);
        }
    }
}
