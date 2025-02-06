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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.data.Offset;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class DefaultMonotonicSnapshotClockTest {
    @DataProvider
    private static Object[] booleanValues() {
        return new Object[]{ true, false };
    }

    @Test(dataProvider = "booleanValues")
    void testClockHandlesTimeLeapsBackwardsOrForward(boolean requestSnapshot) throws InterruptedException {
        long snapshotIntervalMillis = 5;
        AtomicLong offsetValue = new AtomicLong(0);
        @Cleanup
        DefaultMonotonicSnapshotClock clock =
                new DefaultMonotonicSnapshotClock(Duration.ofMillis(snapshotIntervalMillis).toNanos(),
                        () -> System.nanoTime() + offsetValue.get());

        long previousTick = -1;
        boolean leapDirection = true;
        for (int i = 0; i < 100; i++) {
            long tick = clock.getTickNanos(requestSnapshot);
            log.info("i = {}, tick = {}", i, tick);
            if ((i + 1) % 3 == 0) {
                leapDirection = !leapDirection;
                log.info("Time leap 5 minutes {}", leapDirection ? "forward" : "backwards");
                // make the clock leap 5 minute forward or backwards
                offsetValue.set((leapDirection ? 1L : -1L) * Duration.ofMinutes(5).toNanos());
                Thread.sleep(2 * snapshotIntervalMillis);
            } else {
                Thread.sleep(snapshotIntervalMillis);
            }
            try {
                var assertion = assertThat(tick)
                        .describedAs("i = %d, tick = %d, previousTick = %d", i, tick, previousTick);
                if (requestSnapshot) {
                    assertion = assertion.isGreaterThan(previousTick);
                } else {
                    assertion = assertion.isGreaterThanOrEqualTo(previousTick);
                }
                assertion.isCloseTo(previousTick,
                                Offset.offset(5 * TimeUnit.MILLISECONDS.toNanos(snapshotIntervalMillis)));
            } catch (AssertionError e) {
                if (i < 3) {
                    // ignore the assertion errors in first 3 rounds since classloading of AssertJ makes the timings
                    // flaky
                } else {
                    throw e;
                }
            }
            previousTick = tick;
        }
    }

    @Test
    void testRequestUpdate() throws InterruptedException {
        @Cleanup
        DefaultMonotonicSnapshotClock clock =
                new DefaultMonotonicSnapshotClock(Duration.ofSeconds(5).toNanos(), System::nanoTime);
        long tick1 = clock.getTickNanos(false);
        long tick2 = clock.getTickNanos(true);
        assertThat(tick2).isGreaterThan(tick1);
    }

    @Test
    void testRequestingSnapshotAfterClosed() throws InterruptedException {
        DefaultMonotonicSnapshotClock clock =
                new DefaultMonotonicSnapshotClock(Duration.ofSeconds(5).toNanos(), System::nanoTime);
        clock.close();
        long tick1 = clock.getTickNanos(true);
        Thread.sleep(10);
        long tick2 = clock.getTickNanos(true);
        assertThat(tick2).isGreaterThan(tick1);
    }

    @Test
    void testConstructorValidation() {
        assertThatThrownBy(() -> new DefaultMonotonicSnapshotClock(0, System::nanoTime))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("snapshotIntervalNanos must be at least 1 millisecond");
        assertThatThrownBy(() -> new DefaultMonotonicSnapshotClock(-1, System::nanoTime))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("snapshotIntervalNanos must be at least 1 millisecond");
        assertThatThrownBy(() -> new DefaultMonotonicSnapshotClock(TimeUnit.MILLISECONDS.toNanos(1), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("clockSource must not be null");
    }

    @Test
    void testFailureHandlingInClockSource() {
        @Cleanup
        DefaultMonotonicSnapshotClock clock =
                new DefaultMonotonicSnapshotClock(Duration.ofSeconds(5).toNanos(), () -> {
                    throw new RuntimeException("Test clock failure");
                });
        // the exception should be propagated
        assertThatThrownBy(() -> clock.getTickNanos(true))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Test clock failure");
    }
}