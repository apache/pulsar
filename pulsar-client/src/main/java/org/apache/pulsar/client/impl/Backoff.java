/**
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
package org.apache.pulsar.client.impl;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.Getter;

/**
 * Thread-safe utility to provide an exponential backoff.
 *
 * All variables are in {@link TimeUnit#MILLISECONDS}, by default.
 */
public class Backoff {
    public static final long DEFAULT_INTERVAL_IN_NANOSECONDS = TimeUnit.MILLISECONDS.toNanos(100);
    public static final long MAX_BACKOFF_INTERVAL_NANOSECONDS = TimeUnit.SECONDS.toNanos(30);
    private final long initial;
    @Getter
    private final long max;
    private final Clock clock;
    private final long mandatoryStop;

    // Mutable state for this class. Only updated in synchronized blocks on this class, so they don't need to be
    // marked as volatile.
    private long next = 0;
    private long firstBackoffTimeInMillis = 0;
    private boolean mandatoryStopMade = false;

    private static final Random random = new Random();

    /**
     * Backoff constructor.
     *
     * We do not set {@link #next} or any of this class's other mutable fields in the constructor to ensure safe
     * initialization.
     *
     * @param initial - the starting delay that will be returned for the first call of {@link #next()} as well as the
     *                first call to {@link #next()} after {@link #reset()}. Must be positive.
     * @param unitInitial - the {@link TimeUnit} for the initial argument.
     * @param max - the maximum delay that will be returned by {@link #next()}. Must be positive.
     * @param unitMax - the {@link TimeUnit} for the max argument.
     * @param mandatoryStop - a time, from now, that this class will make sure to "retry", even though it likely will
     *                      result in a break from the exponential backoff sequence. If the value is less than
     *                      the argument for initial, this mandatoryStop will be ignored. This value essentially
     *                      triggers an early result of the {@link #next()} method. It will not add an extra call in the
     *                      exponential backoff.
     * @param unitMandatoryStop - the {@link TimeUnit} for the mandatoryStop argument.
     * @param clock - the {@link Clock} to use for determining the current millis.
     */
    Backoff(long initial, TimeUnit unitInitial, long max, TimeUnit unitMax, long mandatoryStop,
            TimeUnit unitMandatoryStop, Clock clock) {
        if (initial <= 0) {
            throw new IllegalArgumentException("Illegal initial time");
        }
        if (max <= 0) {
            throw new IllegalArgumentException("Illegal max retry time");
        }
        this.initial = unitInitial.toMillis(initial);
        this.max = unitMax.toMillis(max);
        this.mandatoryStop = unitMandatoryStop.toMillis(mandatoryStop);
        this.clock = clock;
    }

    public Backoff(long initial, TimeUnit unitInitial, long max, TimeUnit unitMax, long mandatoryStop,
                   TimeUnit unitMandatoryStop) {
        this(initial, unitInitial, max, unitMax, mandatoryStop, unitMandatoryStop, Clock.systemDefaultZone());
    }

    /**
     * Get the next length of time to back off based on the current state of this {@link Backoff}.
     * @return the next delay, in milliseconds
     */
    public synchronized long next() {
        if (this.next == 0) {
            this.next = this.initial;
        }
        long current = this.next;
        if (current < max) {
            this.next = Math.min(this.next * 2, this.max);
        }

        // Check for mandatory stop
        if (!mandatoryStopMade) {
            long now = clock.millis();
            long timeElapsedSinceFirstBackoff = 0;
            if (initial == current) {
                firstBackoffTimeInMillis = now;
            } else {
                timeElapsedSinceFirstBackoff = now - firstBackoffTimeInMillis;
            }

            if (timeElapsedSinceFirstBackoff + current > mandatoryStop) {
                current = Math.max(initial, mandatoryStop - timeElapsedSinceFirstBackoff);
                mandatoryStopMade = true;
            }
        }

        // Randomly decrease the timeout up to 10% to avoid simultaneous retries
        // If current < 10 then current/10 < 1 and we get an exception from Random saying "Bound must be positive"
        if (current > 10) {
            current -= random.nextInt((int) current / 10);
        }
        return Math.max(initial, current);
    }

    /**
     * Reduce the next backoff to half of its current value. If the result is less than {@link #initial}, set
     * {@link #next} to {@link #initial}.
     */
    public synchronized void reduceToHalf() {
        if (next > initial) {
            this.next = Math.max(this.next / 2, this.initial);
        }
    }

    /**
     * Reset this {@link Backoff} instance to its initial state.
     */
    public synchronized void reset() {
        this.next = this.initial;
        this.mandatoryStopMade = false;
    }

    public synchronized boolean isMandatoryStopMade() {
        return mandatoryStopMade;
    }

    @VisibleForTesting
    synchronized long getFirstBackoffTimeInMillis() {
        return firstBackoffTimeInMillis;
    }

    public static boolean shouldBackoff(long initialTimestamp, TimeUnit unitInitial, int failedAttempts,
                                        long defaultInterval, long maxBackoffInterval) {
        long initialTimestampInNano = unitInitial.toNanos(initialTimestamp);
        long currentTime = System.nanoTime();
        long interval = defaultInterval;
        for (int i = 1; i < failedAttempts; i++) {
            interval = interval * 2;
            if (interval > maxBackoffInterval) {
                interval = maxBackoffInterval;
                break;
            }
        }

        // if the current time is less than the time at which next retry should occur, we should backoff
        return currentTime < (initialTimestampInNano + interval);
    }

    public static boolean shouldBackoff(long initialTimestamp, TimeUnit unitInitial, int failedAttempts) {
        return Backoff.shouldBackoff(initialTimestamp, unitInitial, failedAttempts,
                                     DEFAULT_INTERVAL_IN_NANOSECONDS, MAX_BACKOFF_INTERVAL_NANOSECONDS);
    }
}
