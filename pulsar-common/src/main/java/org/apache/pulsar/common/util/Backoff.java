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
package org.apache.pulsar.common.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import lombok.Getter;

/**
 * Exponential backoff with mandatory stop.
 *
 * <p>Delays start at {@code initialDelay} and double on every call to {@link #next()}, up to
 * {@code maxBackoff}. A symmetric random jitter of {@code ±jitterPercent/2} is applied to every
 * returned value (including the first one) to avoid thundering-herd retries.
 *
 * <p>If a {@code mandatoryStop} duration is configured, the backoff tracks wall-clock time from the
 * first {@link #next()} call. Once the elapsed time plus the next delay would exceed the mandatory
 * stop, the delay is truncated so that the total does not exceed it, and {@link #isMandatoryStopMade()}
 * returns {@code true}. After the mandatory stop, backoff continues to grow normally.
 *
 * <p>Use {@link #reset()} to restart the sequence from the initial delay.
 *
 * <pre>{@code
 * Backoff backoff = Backoff.builder()
 *         .initialDelay(Duration.ofMillis(100))
 *         .maxBackoff(Duration.ofMinutes(1))
 *         .mandatoryStop(Duration.ofSeconds(30))
 *         .jitterPercent(10.0)
 *         .build();
 *
 * Duration delay = backoff.next();
 * }</pre>
 */
public class Backoff {
    private static final Duration DEFAULT_INITIAL_DELAY = Duration.ofMillis(100);
    private static final Duration DEFAULT_MAX_BACKOFF_INTERVAL = Duration.ofMinutes(1);
    private static final double DEFAULT_JITTER_PERCENT = 10.0;
    private static final Random random = new Random();

    @Getter
    private final Duration initial;
    @Getter
    private final Duration max;
    @Getter
    private final Duration mandatoryStop;
    @Getter
    private final double jitterPercent;
    private final Clock clock;

    private Duration next;
    @Getter
    private Instant firstBackoffTime;
    private boolean firstBackoffTimeSet;
    @Getter
    private boolean mandatoryStopMade;

    private Backoff(Duration initial, Duration max, Duration mandatoryStop, double jitterPercent, Clock clock) {
        this.initial = initial;
        this.max = max;
        this.mandatoryStop = mandatoryStop;
        this.jitterPercent = jitterPercent;
        this.next = initial;
        this.clock = clock;
        this.firstBackoffTime = Instant.EPOCH;
        this.firstBackoffTimeSet = false;
        if (initial.isZero() && max.isZero() && mandatoryStop.isZero()) {
            this.mandatoryStopMade = true;
        }
    }

    /**
     * Creates a {@link Backoff} with the default configuration (initial delay 100 ms, max 1 min,
     * no mandatory stop).
     *
     * @return a new Backoff with default settings
     */
    public static Backoff create() {
        return new Builder().build();
    }

    /**
     * Creates a new {@link Builder} with default settings.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the next backoff delay, advancing the internal state.
     *
     * <p>The underlying delay starts at the initial delay and doubles on each call up to the max
     * backoff. A symmetric jitter of {@code ±jitterPercent/2} is applied on every call (including
     * the first one) to spread out concurrent retries; the returned value may therefore be slightly
     * below the initial delay or slightly above the max backoff.
     *
     * @return the delay to wait before the next retry attempt
     */
    public Duration next() {
        Duration current = this.next;
        if (current.compareTo(max) < 0) {
            Duration doubled = this.next.multipliedBy(2);
            this.next = doubled.compareTo(this.max) < 0 ? doubled : this.max;
        }

        // Check for mandatory stop
        if (!mandatoryStopMade) {
            Instant now = clock.instant();
            Duration timeElapsedSinceFirstBackoff = Duration.ZERO;
            if (!firstBackoffTimeSet) {
                firstBackoffTime = now;
                firstBackoffTimeSet = true;
            } else {
                timeElapsedSinceFirstBackoff = Duration.between(firstBackoffTime, now);
            }

            if (timeElapsedSinceFirstBackoff.plus(current).compareTo(mandatoryStop) > 0) {
                Duration remaining = mandatoryStop.minus(timeElapsedSinceFirstBackoff);
                current = remaining.compareTo(initial) > 0 ? remaining : initial;
                mandatoryStopMade = true;
            }
        }

        long currentMillis = current.toMillis();
        if (jitterPercent > 0 && currentMillis > 0) {
            // Apply a symmetric jitter of ±jitterPercent/2 around the current delay.
            double factor = 1.0 + (random.nextDouble() - 0.5) * (jitterPercent / 100.0);
            currentMillis = Math.max(0L, Math.round(currentMillis * factor));
        }
        return Duration.ofMillis(currentMillis);
    }

    /**
     * Halves the next delay (but never below the initial delay).
     * Useful after a partially successful operation to converge faster.
     */
    public void reduceToHalf() {
        if (next.compareTo(initial) > 0) {
            Duration half = next.dividedBy(2);
            this.next = half.compareTo(initial) > 0 ? half : initial;
        }
    }

    /**
     * Resets the backoff to its initial state so the next call to {@link #next()} returns the
     * initial delay again. Also resets the mandatory-stop tracking.
     */
    public void reset() {
        this.next = this.initial;
        this.firstBackoffTime = Instant.EPOCH;
        this.firstBackoffTimeSet = false;
        this.mandatoryStopMade = initial.isZero() && max.isZero() && mandatoryStop.isZero();
    }

    /**
     * Builder for {@link Backoff}.
     *
     * <p>Defaults: initial delay 100 ms, max backoff 1 min, no mandatory stop, 10% jitter.
     */
    public static class Builder {
        private Duration initialDelay = DEFAULT_INITIAL_DELAY;
        private Duration maxBackoff = DEFAULT_MAX_BACKOFF_INTERVAL;
        private Duration mandatoryStop = Duration.ZERO;
        private double jitterPercent = DEFAULT_JITTER_PERCENT;
        private Clock clock = Clock.systemDefaultZone();

        /**
         * Sets the initial (smallest) backoff delay. Defaults to 100 ms.
         *
         * @param initialDelay the initial delay
         * @return this builder
         */
        public Builder initialDelay(Duration initialDelay) {
            this.initialDelay = initialDelay;
            return this;
        }

        /**
         * Sets the upper bound for the backoff delay. Defaults to 1 min.
         *
         * @param maxBackoff the maximum delay
         * @return this builder
         */
        public Builder maxBackoff(Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
            return this;
        }

        /**
         * Sets the mandatory-stop deadline measured from the first {@link Backoff#next()} call.
         * Once wall-clock time exceeds this duration the current delay is truncated and
         * {@link Backoff#isMandatoryStopMade()} returns {@code true}. Defaults to zero (disabled).
         *
         * @param mandatoryStop the mandatory stop duration
         * @return this builder
         */
        public Builder mandatoryStop(Duration mandatoryStop) {
            this.mandatoryStop = mandatoryStop;
            return this;
        }

        /**
         * Sets the jitter percentage applied to each returned delay. The actual jitter is symmetric:
         * the returned value is multiplied by a uniform random factor in
         * {@code [1 - jitterPercent/200, 1 + jitterPercent/200)}. Defaults to 10. Set to 0 to disable
         * jitter.
         *
         * @param jitterPercent the jitter percentage, must be in {@code [0, 100]}
         * @return this builder
         * @throws IllegalArgumentException if {@code jitterPercent} is outside {@code [0, 100]}
         */
        public Builder jitterPercent(double jitterPercent) {
            if (jitterPercent < 0 || jitterPercent > 100) {
                throw new IllegalArgumentException("jitterPercent must be in [0, 100]");
            }
            this.jitterPercent = jitterPercent;
            return this;
        }

        Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Builds a new {@link Backoff} instance with the configured parameters.
         *
         * @return a new Backoff
         */
        public Backoff build() {
            return new Backoff(initialDelay, maxBackoff, mandatoryStop, jitterPercent, clock);
        }
    }
}
