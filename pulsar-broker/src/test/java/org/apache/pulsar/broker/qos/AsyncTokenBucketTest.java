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

package org.apache.pulsar.broker.qos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.data.Offset;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AsyncTokenBucketTest {
    private AtomicLong manualClockSource;
    private MonotonicSnapshotClock clockSource;

    private AsyncTokenBucket asyncTokenBucket;

    @BeforeMethod
    public void setup() {
        manualClockSource = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        clockSource = requestSnapshot -> manualClockSource.get();
    }


    private void incrementSeconds(int seconds) {
        manualClockSource.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
    }

    private void incrementMillis(long millis) {
        manualClockSource.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }

    @Test
    void shouldAddTokensWithConfiguredRate() {
        asyncTokenBucket =
                AsyncTokenBucket.builder().capacity(100).rate(10).initialTokens(0).clock(clockSource).build();
        incrementSeconds(5);
        assertEquals(asyncTokenBucket.getTokens(), 50);
        incrementSeconds(1);
        assertEquals(asyncTokenBucket.getTokens(), 60);
        incrementSeconds(4);
        assertEquals(asyncTokenBucket.getTokens(), 100);

        // No matter how long the period is, tokens do not go above capacity
        incrementSeconds(5);
        assertEquals(asyncTokenBucket.getTokens(), 100);

        // Consume all and verify none available and then wait 1 period and check replenished
        asyncTokenBucket.consumeTokens(100);
        assertEquals(asyncTokenBucket.tokens(true), 0);
        incrementSeconds(1);
        assertEquals(asyncTokenBucket.getTokens(), 10);
    }

    @Test
    void shouldCalculatePauseCorrectly() {
        asyncTokenBucket =
                AsyncTokenBucket.builder().capacity(100).rate(10).initialTokens(0).clock(clockSource)
                        .build();
        incrementSeconds(5);
        asyncTokenBucket.consumeTokens(100);
        assertEquals(asyncTokenBucket.getTokens(), -50);
        assertEquals(TimeUnit.NANOSECONDS.toMillis(asyncTokenBucket.calculateThrottlingDuration()), 5100);
    }

    @Test
    void shouldSupportFractionsWhenUpdatingTokens() {
        asyncTokenBucket =
                AsyncTokenBucket.builder().capacity(100).rate(10).initialTokens(0).clock(clockSource).build();
        incrementMillis(100);
        assertEquals(asyncTokenBucket.getTokens(), 1);
    }

    @Test
    void shouldSupportFractionsAndRetainLeftoverWhenUpdatingTokens() {
        asyncTokenBucket =
                AsyncTokenBucket.builder().capacity(100).rate(10).initialTokens(0).clock(clockSource).build();
        for (int i = 0; i < 150; i++) {
            incrementMillis(1);
        }
        assertEquals(asyncTokenBucket.getTokens(), 1);
        incrementMillis(150);
        assertEquals(asyncTokenBucket.getTokens(), 3);
    }

    @Test
    void shouldSupportFractionsAndRetainLeftoverWhenUpdatingTokens2() {
        asyncTokenBucket =
                AsyncTokenBucket.builder().capacity(100).rate(1).initialTokens(0).clock(clockSource).build();
        for (int i = 0; i < 150; i++) {
            incrementMillis(1);
            assertEquals(asyncTokenBucket.tokens((i + 1) % 31 == 0), 0);
        }
        incrementMillis(150);
        assertEquals(asyncTokenBucket.tokens(true), 0);
        incrementMillis(699);
        assertEquals(asyncTokenBucket.tokens(true), 0);
        incrementMillis(2);
        assertEquals(asyncTokenBucket.tokens(true), 1);
        incrementMillis(999);
        assertEquals(asyncTokenBucket.tokens(true), 2);
    }

    @Test
    void shouldHandleNegativeBalanceWithEventuallyConsistentTokenUpdates() {
        asyncTokenBucket =
                AsyncTokenBucket.builder()
                        // intentionally pick a coarse resolution
                        .resolutionNanos(TimeUnit.SECONDS.toNanos(51))
                        .capacity(100).rate(10).initialTokens(0).clock(clockSource).build();
        // assert that the token balance is 0 initially
        assertThat(asyncTokenBucket.tokens(true)).isEqualTo(0);

        // consume tokens without exceeding the rate
        for (int i = 0; i < 10000; i++) {
            asyncTokenBucket.consumeTokens(500);
            incrementSeconds(50);
        }

        // let 9 seconds pass
        incrementSeconds(9);

        // there should be 90 tokens available
        assertThat(asyncTokenBucket.tokens(true)).isEqualTo(90);
    }

    @Test
    void shouldNotExceedTokenBucketSizeWithNegativeTokens() {
        asyncTokenBucket =
                AsyncTokenBucket.builder()
                        // intentionally pick a coarse resolution
                        .resolutionNanos(TimeUnit.SECONDS.toNanos(51))
                        .capacity(100).rate(10).initialTokens(0).clock(clockSource).build();
        // assert that the token balance is 0 initially
        assertThat(asyncTokenBucket.tokens(true)).isEqualTo(0);

        // consume tokens without exceeding the rate
        for (int i = 0; i < 100; i++) {
            asyncTokenBucket.consumeTokens(600);
            incrementSeconds(50);
            // let tokens accumulate back to 0 every 10 seconds
            if ((i + 1) % 10 == 0) {
                incrementSeconds(100);
            }
        }

        // let 9 seconds pass
        incrementSeconds(9);

        // there should be 90 tokens available
        assertThat(asyncTokenBucket.tokens(true)).isEqualTo(90);
    }

    @Test
    void shouldAccuratelyCalculateTokensWhenTimeIsLaggingBehindInInconsistentUpdates() {
        clockSource = requestSnapshot -> {
          if (requestSnapshot) {
              return manualClockSource.get();
          } else {
              // let the clock lag behind
              return manualClockSource.get() - TimeUnit.SECONDS.toNanos(52);
          }
        };
        incrementSeconds(1);
        asyncTokenBucket =
                AsyncTokenBucket.builder().resolutionNanos(TimeUnit.SECONDS.toNanos(51))
                        .capacity(100).rate(10).initialTokens(100).clock(clockSource).build();
        assertThat(asyncTokenBucket.tokens(true)).isEqualTo(100);

        // consume tokens without exceeding the rate
        for (int i = 0; i < 10000; i++) {
            asyncTokenBucket.consumeTokens(500);
            incrementSeconds(i == 0 ? 40 : 50);
        }

        // let 9 seconds pass
        incrementSeconds(9);

        // there should be 90 tokens available
        assertThat(asyncTokenBucket.tokens(true)).isEqualTo(90);
    }

    @Test
    void shouldTolerateInstableClockSourceWhenUpdatingTokens() {
        AtomicLong offset = new AtomicLong(0);
        long resolutionNanos = TimeUnit.MILLISECONDS.toNanos(100);
        DefaultMonotonicSnapshotClock monotonicSnapshotClock =
                new DefaultMonotonicSnapshotClock(resolutionNanos,
                        () -> offset.get() + manualClockSource.get());
        long initialTokens = 500L;
        asyncTokenBucket =
                AsyncTokenBucket.builder().resolutionNanos(resolutionNanos)
                        .capacity(100000).rate(1000).initialTokens(initialTokens).clock(monotonicSnapshotClock).build();
        Random random = new Random(0);
        int randomOffsetCount = 0;
        for (int i = 0; i < 100000; i++) {
            // increment the clock by 1ms, since rate is 1000 tokens/s, this should make 1 token available
            incrementMillis(1);
            if (i % 39 == 0) {
                // randomly offset the clock source
                // update the tokens consistently before and after offsetting the clock source
                asyncTokenBucket.tokens(true);
                offset.set((random.nextBoolean() ? -1L : 1L) * random.nextLong(0L, 100L) * resolutionNanos);
                asyncTokenBucket.tokens(true);
                randomOffsetCount++;
            }
            // consume 1 token
            asyncTokenBucket.consumeTokens(1);
        }
        assertThat(asyncTokenBucket.tokens(true))
                // since the rate is 1/ms and the test increments the clock by 1ms and consumes 1 token in each
                // iteration, the tokens should be greater than or equal to the initial tokens
                .isGreaterThanOrEqualTo(initialTokens)
                // tolerate difference in added tokens since when clock leaps forward or backwards, the clock
                // is assumed to have moved forward by the resolutionNanos
                .isCloseTo(initialTokens, Offset.offset(3L * randomOffsetCount));
    }
}