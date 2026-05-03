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
 * Backoff configuration for broker reconnection attempts.
 *
 * <p>The delay for attempt {@code n} is {@code min(initialInterval * multiplier^(n-1), maxInterval)}.
 *
 * <p>Use {@link #fixed(Duration, Duration)} or {@link #exponential(Duration, Duration)} for
 * the common cases, or {@link #builder()} to configure all knobs explicitly.
 */
@EqualsAndHashCode
@ToString
public final class BackoffPolicy {

    private final Duration initialInterval;
    private final Duration maxInterval;
    private final double multiplier;

    private BackoffPolicy(Duration initialInterval, Duration maxInterval, double multiplier) {
        Objects.requireNonNull(initialInterval, "initialInterval must not be null");
        Objects.requireNonNull(maxInterval, "maxInterval must not be null");
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
        this.initialInterval = initialInterval;
        this.maxInterval = maxInterval;
        this.multiplier = multiplier;
    }

    /**
     * @return the delay before the first reconnection attempt
     */
    public Duration initialInterval() {
        return initialInterval;
    }

    /**
     * @return the maximum delay between reconnection attempts
     */
    public Duration maxInterval() {
        return maxInterval;
    }

    /**
     * @return the multiplier applied after each attempt
     */
    public double multiplier() {
        return multiplier;
    }

    /**
     * Create a fixed backoff (no increase between retries).
     *
     * @param initialInterval the constant delay between reconnection attempts
     * @param maxInterval     the maximum delay between reconnection attempts
     * @return a {@link BackoffPolicy} with a multiplier of 1.0
     */
    public static BackoffPolicy fixed(Duration initialInterval, Duration maxInterval) {
        return new BackoffPolicy(initialInterval, maxInterval, 1.0);
    }

    /**
     * Create an exponential backoff with the given bounds and a default multiplier of 2.
     *
     * @param initialInterval the delay before the first reconnection attempt
     * @param maxInterval     the maximum delay between reconnection attempts
     * @return a {@link BackoffPolicy} with a multiplier of 2.0
     */
    public static BackoffPolicy exponential(Duration initialInterval, Duration maxInterval) {
        return new BackoffPolicy(initialInterval, maxInterval, 2.0);
    }

    /**
     * @return a new builder for constructing a {@link BackoffPolicy}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link BackoffPolicy}.
     */
    public static final class Builder {
        private Duration initialInterval;
        private Duration maxInterval;
        private double multiplier = 2.0;

        private Builder() {
        }

        /**
         * Delay before the first reconnection attempt. Required.
         *
         * @param initialInterval the initial backoff delay
         * @return this builder
         */
        public Builder initialInterval(Duration initialInterval) {
            this.initialInterval = initialInterval;
            return this;
        }

        /**
         * Upper bound on the backoff delay. Required.
         *
         * @param maxInterval the maximum backoff delay
         * @return this builder
         */
        public Builder maxInterval(Duration maxInterval) {
            this.maxInterval = maxInterval;
            return this;
        }

        /**
         * Multiplier applied to the previous delay on each retry. Must be {@code >= 1.0}.
         * Default is {@code 2.0} (exponential backoff). Use {@code 1.0} for fixed backoff.
         *
         * @param multiplier the per-attempt multiplier
         * @return this builder
         */
        public Builder multiplier(double multiplier) {
            this.multiplier = multiplier;
            return this;
        }

        /**
         * @return a new {@link BackoffPolicy} instance
         */
        public BackoffPolicy build() {
            return new BackoffPolicy(initialInterval, maxInterval, multiplier);
        }
    }
}
