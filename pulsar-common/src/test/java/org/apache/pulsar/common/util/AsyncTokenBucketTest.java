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

import static org.testng.Assert.assertEquals;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AsyncTokenBucketTest {
    private AtomicLong manualClockSource;
    private LongSupplier clockSource;

    private AsyncTokenBucket asyncTokenBucket;

    @BeforeMethod
    public void setup() {
        manualClockSource = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        clockSource = manualClockSource::get;
    }


    private void incrementSeconds(int seconds) {
        manualClockSource.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
        asyncTokenBucket.updateTokens();
    }

    private void incrementMillis(long millis) {
        manualClockSource.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }

    @Test
    void shouldAddTokensWithConfiguredRate() {
        asyncTokenBucket =
                AsyncTokenBucket.builder().capacity(100).rate(10).initialTokens(0).clockSource(clockSource).build();
        incrementSeconds(5);
        assertEquals(50, asyncTokenBucket.tokens(true));
        incrementSeconds(1);
        assertEquals(60, asyncTokenBucket.tokens(true));
        incrementSeconds(4);
        assertEquals(100, asyncTokenBucket.tokens(true));

        // No matter how long the period is, tokens do not go above capacity
        incrementSeconds(5);
        assertEquals(100, asyncTokenBucket.tokens(true));

        // Consume all and verify none available and then wait 1 period and check replenished
        asyncTokenBucket.consumeTokens(100);
        assertEquals(0, asyncTokenBucket.tokens(true));
        incrementSeconds(1);
        assertEquals(10, asyncTokenBucket.tokens(true));
    }

    @Test
    void shouldCalculatePauseCorrectly() {
        asyncTokenBucket =
                AsyncTokenBucket.builder().capacity(100).rate(10).initialTokens(0).clockSource(clockSource).build();
        incrementSeconds(5);
        asyncTokenBucket.consumeTokens(100);
        assertEquals(-50, asyncTokenBucket.tokens(true));
        assertEquals(6, TimeUnit.NANOSECONDS.toSeconds(asyncTokenBucket.calculatePause(true)));
    }

    @Test
    void shouldSupportFractionsWhenUpdatingTokens() {
        asyncTokenBucket =
                AsyncTokenBucket.builder().capacity(100).rate(10).initialTokens(0).clockSource(clockSource).build();
        incrementMillis(100);
        assertEquals(1, asyncTokenBucket.tokens(true));
    }

    @Test
    void shouldSupportFractionsAndRetainLeftoverWhenUpdatingTokens() {
        asyncTokenBucket =
                AsyncTokenBucket.builder().capacity(100).rate(10).initialTokens(0).clockSource(clockSource).build();
        for (int i = 0; i < 150; i++) {
            incrementMillis(1);
        }
        assertEquals(1, asyncTokenBucket.tokens(true));
        incrementMillis(150);
        assertEquals(3, asyncTokenBucket.tokens(true));
    }

}