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
 * <p>The base delay for attempt {@code n} is {@code min(initialInterval * multiplier^(n-1), maxInterval)}.
 * A symmetric random jitter of {@code ±jitterPercent/2} is applied to each delay (including the
 * first one) to spread out concurrent retries.
 *
 * <p>Use {@link #fixed(Duration, Duration)} or {@link #exponential(Duration, Duration)} for
 * the common cases, or {@link #builder()} to configure all knobs explicitly.
 */
@EqualsAndHashCode
@ToString
public final class BackoffPolicy {

    /** Default jitter percentage applied when not explicitly specified. */
    public static final double DEFAULT_JITTER_PERCENT = 10.0;

    private final Duration initialInterval;
    private final Duration maxInterval;
    private final double multiplier;
    private final double jitterPercent;

    private BackoffPolicy(Duration initialInterval, Duration maxInterval, double multiplier, double jitterPercent) {
        Objects.requireNonNull(initialInterval, "initialInterval must not be null");
        Objects.requireNonNull(maxInterval, "maxInterval must not be null");
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
        if (jitterPercent < 0 || jitterPercent > 100) {
            throw new IllegalArgumentException("jitterPercent must be in [0, 100]");
        }
        this.initialInterval = initialInterval;
        this.maxInterval = maxInterval;
        this.multiplier = multiplier;
        this.jitterPercent = jitterPercent;
    }

    /**
     * @return the base delay before the first reconnection attempt
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
     * @return the symmetric jitter percentage applied to each delay; {@code 0} means no jitter
     */
    public double jitterPercent() {
        return jitterPercent;
    }

    /**
     * Create a fixed backoff (no increase between retries) with the default jitter.
     *
     * @param initialInterval the constant base delay between reconnection attempts
     * @param maxInterval     the maximum delay between reconnection attempts
     * @return a {@link BackoffPolicy} with a multiplier of 1.0 and the default jitter
     */
    public static BackoffPolicy fixed(Duration initialInterval, Duration maxInterval) {
        return new BackoffPolicy(initialInterval, maxInterval, 1.0, DEFAULT_JITTER_PERCENT);
    }

    /**
     * Create an exponential backoff with the given bounds, a default multiplier of 2 and the
     * default jitter.
     *
     * @param initialInterval the base delay before the first reconnection attempt
     * @param maxInterval     the maximum delay between reconnection attempts
     * @return a {@link BackoffPolicy} with a multiplier of 2.0 and the default jitter
     */
    public static BackoffPolicy exponential(Duration initialInterval, Duration maxInterval) {
        return new BackoffPolicy(initialInterval, maxInterval, 2.0, DEFAULT_JITTER_PERCENT);
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
        private double jitterPercent = DEFAULT_JITTER_PERCENT;

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
         * Symmetric jitter percentage applied to each returned delay. The actual jitter is the
         * base delay multiplied by a uniform random factor in
         * {@code [1 - jitterPercent/200, 1 + jitterPercent/200)}. Default is {@code 10.0}; set to
         * {@code 0} to disable jitter.
         *
         * @param jitterPercent the jitter percentage, must be in {@code [0, 100]}
         * @return this builder
         */
        public Builder jitterPercent(double jitterPercent) {
            this.jitterPercent = jitterPercent;
            return this;
        }

        /**
         * @return a new {@link BackoffPolicy} instance
         */
        public BackoffPolicy build() {
            return new BackoffPolicy(initialInterval, maxInterval, multiplier, jitterPercent);
        }
    }
}
