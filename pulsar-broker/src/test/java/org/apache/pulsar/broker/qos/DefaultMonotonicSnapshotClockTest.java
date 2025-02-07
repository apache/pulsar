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
    void testClockHandlesTimeLeapsBackwards(boolean requestSnapshot) throws InterruptedException {
        long snapshotIntervalMillis = 5;
        AtomicLong clockValue = new AtomicLong(1);
        @Cleanup
        DefaultMonotonicSnapshotClock clock =
                new DefaultMonotonicSnapshotClock(Duration.ofMillis(snapshotIntervalMillis).toNanos(),
                        clockValue::get);


        long previousTick = -1;
        boolean leapDirection = true;
        for (int i = 0; i < 10000; i++) {
            clockValue.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
            long tick = clock.getTickNanos(requestSnapshot);
            //log.info("i = {}, tick = {}", i, tick);
            if ((i + 1) % 5 == 0) {
                leapDirection = !leapDirection;
                //log.info("Time leap 5 minutes backwards");
                clockValue.addAndGet(-Duration.ofMinutes(5).toNanos());
            }
            if (previousTick != -1) {
                assertThat(tick)
                        .describedAs("i = %d, tick = %d, previousTick = %d", i, tick, previousTick)
                        .isGreaterThanOrEqualTo(previousTick)
                        .isCloseTo(previousTick,
                                // then snapshot is requested, the time difference between the two ticks is accurate
                                // otherwise allow time difference at most 4 times the snapshot interval since the
                                // clock is updated periodically by a background thread
                                Offset.offset(TimeUnit.MILLISECONDS.toNanos(
                                        requestSnapshot ? 1 : 4 * snapshotIntervalMillis)));
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

    @Test
    void testLeapDetectionIndepently() {
        AtomicLong clockValue = new AtomicLong(0);
        AtomicLong tickValue = new AtomicLong(0);
        long expectedTickValue = 0;
        long snapshotIntervalNanos = TimeUnit.MILLISECONDS.toNanos(1);
        DefaultMonotonicSnapshotClock.MonotonicLeapDetectingTickUpdater updater =
                new DefaultMonotonicSnapshotClock.MonotonicLeapDetectingTickUpdater(clockValue::get, tickValue::set,
                        snapshotIntervalNanos);

        updater.update(true);

        // advance the clock
        clockValue.addAndGet(snapshotIntervalNanos);
        expectedTickValue += snapshotIntervalNanos;
        updater.update(true);
        assertThat(tickValue.get()).isEqualTo(expectedTickValue);

        // simulate a leap backwards in time
        clockValue.addAndGet(-10 * snapshotIntervalNanos);
        expectedTickValue += snapshotIntervalNanos;
        updater.update(true);
        assertThat(tickValue.get()).isEqualTo(expectedTickValue);

        // advance the clock
        clockValue.addAndGet(snapshotIntervalNanos);
        expectedTickValue += snapshotIntervalNanos;
        updater.update(true);
        assertThat(tickValue.get()).isEqualTo(expectedTickValue);

        // simulate a leap backwards in time, without waiting a full snapshot interval
        clockValue.addAndGet(-10 * snapshotIntervalNanos);
        updater.update(false);
        assertThat(tickValue.get()).isEqualTo(expectedTickValue);

        // advance the clock
        clockValue.addAndGet(snapshotIntervalNanos);
        expectedTickValue += snapshotIntervalNanos;
        updater.update(true);
        assertThat(tickValue.get()).isEqualTo(expectedTickValue);

        // simulate a small leap backwards in time which isn't detected, without waiting a full snapshot interval
        clockValue.addAndGet(-1 * snapshotIntervalNanos);
        updater.update(false);
        assertThat(tickValue.get()).isEqualTo(expectedTickValue);
        // clock doesn't advance for one snapshot interval
        clockValue.addAndGet(snapshotIntervalNanos);
        updater.update(false);
        assertThat(tickValue.get()).isEqualTo(expectedTickValue);
        // now the clock should advance again
        clockValue.addAndGet(snapshotIntervalNanos);
        expectedTickValue += snapshotIntervalNanos;
        updater.update(false);
        assertThat(tickValue.get()).isEqualTo(expectedTickValue);

        // simulate a leap forward
        clockValue.addAndGet(10 * snapshotIntervalNanos);
        // no special handling for leap forward
        expectedTickValue += 10 * snapshotIntervalNanos;
        updater.update(true);
        assertThat(tickValue.get()).isEqualTo(expectedTickValue);
    }
}