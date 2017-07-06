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

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Backoff {
    private static final long DEFAULT_INTERVAL_IN_NANOSECONDS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long MAX_BACKOFF_INTERVAL_NANOSECONDS = TimeUnit.SECONDS.toNanos(30);
    private final long initial;
    private final long max;
    private long next;

    private static final Random random = new Random();

    public Backoff(long initial, TimeUnit unitInitial, long max, TimeUnit unitMax) {
        this.initial = unitInitial.toMillis(initial);
        this.max = unitMax.toMillis(max);
        this.next = this.initial;
    }

    public long next() {
        long current = this.next;
        if (current < max) {
            this.next = Math.min(this.next * 2, this.max);
        }

        // Randomly increase the timeout up to 25% to avoid simultaneous retries
        current += random.nextInt((int) current / 4);
        return current;
    }

    public void reduceToHalf() {
        if (next > initial) {
            this.next = Math.max(this.next / 2, this.initial);
        }
    }

    public void reset() {
        this.next = this.initial;
    }

    public static boolean shouldBackoff(long initialTimestamp, TimeUnit unitInitial, int failedAttempts) {
        long initialTimestampInNano = unitInitial.toNanos(initialTimestamp);
        long currentTime = System.nanoTime();
        long interval = DEFAULT_INTERVAL_IN_NANOSECONDS;
        for (int i = 1; i < failedAttempts; i++) {
            interval = interval * 2;
            if (interval > MAX_BACKOFF_INTERVAL_NANOSECONDS) {
                interval = MAX_BACKOFF_INTERVAL_NANOSECONDS;
                break;
            }
        }

        // if the current time is less than the time at which next retry should occur, we should backoff
        return currentTime < (initialTimestampInNano + interval);
    }
}
