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
package org.apache.pulsar.io.kinesis;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BackoffTest {

    @Test
    public void mandatoryStopShouldWorkWithConstantBackoff() {
        Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.millis())
                .thenReturn(0L)
                .thenReturn(400L);

        Backoff backoff = new Backoff(
                100, TimeUnit.MILLISECONDS,
                100, TimeUnit.MILLISECONDS,
                300, TimeUnit.MILLISECONDS,
                mockClock
        );

        // first call starts the mandatory-stop timer
        backoff.next();
        long firstBackoffTime = backoff.getFirstBackoffTimeInMillis();

        // elapsed wall-clock time should exceed mandatory stop
        backoff.next();
        assertEquals(backoff.getFirstBackoffTimeInMillis(), firstBackoffTime,
                "firstBackoffTimeInMillis should not be updated after the first call to next()");
        assertTrue(backoff.isMandatoryStopMade(),
                "mandatory stop should be reached even when initial == max (constant backoff)");
    }

    @Test
    public void reduceToHalfShouldNotResetMandatoryStopTimer() {
        Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.millis())
                .thenReturn(0L)
                .thenReturn(200L);

        Backoff backoff = new Backoff(
                100, TimeUnit.MILLISECONDS,
                400, TimeUnit.MILLISECONDS,
                250, TimeUnit.MILLISECONDS,
                mockClock
        );

        // first call starts the mandatory-stop timer (next becomes 200)
        backoff.next();
        long firstBackoffTime = backoff.getFirstBackoffTimeInMillis();

        // This can bring the next delay back to initial, but it should not reset firstBackoffTimeInMillis.
        backoff.reduceToHalf();

        backoff.next();
        assertEquals(backoff.getFirstBackoffTimeInMillis(), firstBackoffTime,
                "reduceToHalf should not reset firstBackoffTimeInMillis");
        assertTrue(backoff.isMandatoryStopMade(),
                "mandatory stop should be reached based on wall-clock time since the first call to next()");
    }

    @Test
    public void resetShouldStartNewMandatoryStopCycle() {
        Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.millis())
                .thenReturn(0L)
                .thenReturn(400L)
                .thenReturn(1000L)
                .thenReturn(1400L);

        Backoff backoff = new Backoff(
                100, TimeUnit.MILLISECONDS,
                100, TimeUnit.MILLISECONDS,
                300, TimeUnit.MILLISECONDS,
                mockClock
        );

        // Cycle 1: reach mandatory stop
        backoff.next();
        long firstCycleStart = backoff.getFirstBackoffTimeInMillis();
        backoff.next();
        assertTrue(backoff.isMandatoryStopMade(), "mandatory stop should be reached in cycle-1");

        // Reset should clear mandatory-stop state so a new cycle can start.
        backoff.reset();
        assertFalse(backoff.isMandatoryStopMade(), "reset should clear mandatoryStopMade");
        assertEquals(backoff.getFirstBackoffTimeInMillis(), 0, "reset should clear firstBackoffTimeInMillis");

        // Cycle 2: should start timing again and be able to reach mandatory stop again.
        backoff.next();
        long secondCycleStart = backoff.getFirstBackoffTimeInMillis();
        assertEquals(secondCycleStart, 1000, "reset should start a new mandatory-stop timing window");
        assertEquals(secondCycleStart - firstCycleStart, 1000, "reset should not reuse the old timing window");

        backoff.next();
        assertTrue(backoff.isMandatoryStopMade(), "mandatory stop should be reached in cycle-2");
    }
}

