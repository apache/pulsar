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

/**
 * Backoff configuration for broker reconnection attempts.
 *
 * <p>The base delay for attempt {@code n} is {@code min(initialInterval * multiplier^(n-1), maxInterval)}.
 * A symmetric random jitter of {@code ±jitterPercent/2} is applied to each delay (including the
 * first one) to spread out concurrent retries.
 *
 * @param initialInterval the delay before the first reconnection attempt
 * @param maxInterval     the maximum delay between reconnection attempts
 * @param multiplier      the multiplier applied after each attempt
 * @param jitterPercent   the symmetric jitter percentage applied to each delay; {@code 0} disables jitter
 */
public record BackoffPolicy(
        Duration initialInterval,
        Duration maxInterval,
        double multiplier,
        double jitterPercent
) {
    /** Default jitter percentage applied when not explicitly specified. */
    public static final double DEFAULT_JITTER_PERCENT = 10.0;

    public BackoffPolicy {
        Objects.requireNonNull(initialInterval, "initialInterval must not be null");
        Objects.requireNonNull(maxInterval, "maxInterval must not be null");
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
        if (jitterPercent < 0 || jitterPercent > 100) {
            throw new IllegalArgumentException("jitterPercent must be in [0, 100]");
        }
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
     * Create an exponential backoff with a custom multiplier and the default jitter.
     *
     * @param initialInterval the base delay before the first reconnection attempt
     * @param maxInterval     the maximum delay between reconnection attempts
     * @param multiplier      the multiplier applied after each attempt, must be &gt;= 1.0
     * @return a {@link BackoffPolicy} with the specified parameters and the default jitter
     */
    public static BackoffPolicy exponential(Duration initialInterval, Duration maxInterval, double multiplier) {
        return new BackoffPolicy(initialInterval, maxInterval, multiplier, DEFAULT_JITTER_PERCENT);
    }

    /**
     * Returns a copy of this policy with the given jitter percentage. The actual jitter applied to
     * each returned delay is symmetric around the base value: a uniform random factor in
     * {@code [1 - jitterPercent/200, 1 + jitterPercent/200)}.
     *
     * @param jitterPercent the jitter percentage to apply, must be in {@code [0, 100]}; {@code 0} disables jitter
     * @return a new {@link BackoffPolicy} with the configured jitter
     * @throws IllegalArgumentException if {@code jitterPercent} is outside {@code [0, 100]}
     */
    public BackoffPolicy withJitter(double jitterPercent) {
        return new BackoffPolicy(initialInterval, maxInterval, multiplier, jitterPercent);
    }
}
