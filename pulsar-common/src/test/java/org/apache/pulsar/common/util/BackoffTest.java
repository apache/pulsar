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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BackoffTest {
    boolean withinTenPercentAndDecrementTimer(Backoff backoff, long t2) {
        long t1 = backoff.next().toMillis();
        return (t1 >= t2 * 0.9 && t1 <= t2);
    }

    boolean checkExactAndDecrementTimer(Backoff backoff, long t2) {
        long t1 = backoff.next().toMillis();
        return t1 == t2;
    }

    @Test
    public void mandatoryStopTestNegativeTest() {
        Backoff backoff = Backoff.builder()
                .initialDelay(Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(60))
                .mandatoryStop(Duration.ofMillis(1900))
                .build();
        assertEquals(backoff.next().toMillis(), 100);
        backoff.next(); // 200
        backoff.next(); // 400
        backoff.next(); // 800
        assertFalse(withinTenPercentAndDecrementTimer(backoff, 400));
    }

    @Test
    public void firstBackoffTimerTest() {
        Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.instant())
            .thenReturn(Instant.ofEpochMilli(0))
            .thenReturn(Instant.ofEpochMilli(300));

        Backoff backoff = Backoff.builder()
            .initialDelay(Duration.ofMillis(100))
            .maxBackoff(Duration.ofSeconds(60))
            .mandatoryStop(Duration.ofMillis(1900))
            .clock(mockClock)
            .build();

        assertEquals(backoff.next().toMillis(), 100);

        Instant firstBackOffTime = backoff.getFirstBackoffTime();
        backoff.reset();
        assertEquals(backoff.next().toMillis(), 100);
        long diffBackOffTime = Duration.between(firstBackOffTime, backoff.getFirstBackoffTime()).toMillis();
        assertEquals(diffBackOffTime, 300);
    }

    @Test
    public void basicTest() {
        Clock mockClock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());
        Backoff backoff = Backoff.builder()
                .initialDelay(Duration.ofMillis(5))
                .maxBackoff(Duration.ofSeconds(60))
                .mandatoryStop(Duration.ofSeconds(60))
                .clock(mockClock)
                .build();
        assertTrue(checkExactAndDecrementTimer(backoff, 5));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 10));
        backoff.reset();
        assertTrue(checkExactAndDecrementTimer(backoff, 5));
    }

    @Test
    public void maxTest() {
        Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.instant())
            .thenReturn(Instant.ofEpochMilli(0))
            .thenReturn(Instant.ofEpochMilli(10))
            .thenReturn(Instant.ofEpochMilli(20))
            .thenReturn(Instant.ofEpochMilli(40));

        Backoff backoff = Backoff.builder()
            .initialDelay(Duration.ofMillis(5))
            .maxBackoff(Duration.ofMillis(20))
            .mandatoryStop(Duration.ofMillis(20))
            .clock(mockClock)
            .build();

        assertTrue(checkExactAndDecrementTimer(backoff, 5));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 10));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 5));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 20));
    }

    @Test
    public void mandatoryStopTest() {
        Clock mockClock = Mockito.mock(Clock.class);

        Backoff backoff = Backoff.builder()
            .initialDelay(Duration.ofMillis(100))
            .maxBackoff(Duration.ofSeconds(60))
            .mandatoryStop(Duration.ofMillis(1900))
            .clock(mockClock)
            .build();

        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(0));
        assertTrue(checkExactAndDecrementTimer(backoff, 100));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(100));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 200));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(300));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(700));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 800));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(1500));

        // would have been 1600 w/o the mandatory stop
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        assertTrue(backoff.isMandatoryStopMade());
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(1900));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 3200));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(3200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 6400));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(3200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 12800));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(6400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 25600));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(12800));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 51200));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(25600));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 60000));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(51200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 60000));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(60000));

        backoff.reset();
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(0));
        assertTrue(checkExactAndDecrementTimer(backoff, 100));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(100));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 200));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(300));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(700));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 800));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(1500));
        // would have been 1600 w/o the mandatory stop
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));

        backoff.reset();
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(0));
        assertTrue(checkExactAndDecrementTimer(backoff, 100));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(100));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 200));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(300));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(700));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 800));

        backoff.reset();
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(0));
        assertTrue(checkExactAndDecrementTimer(backoff, 100));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(100));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 200));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(300));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        Mockito.when(mockClock.instant()).thenReturn(Instant.ofEpochMilli(700));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 800));
    }

}
