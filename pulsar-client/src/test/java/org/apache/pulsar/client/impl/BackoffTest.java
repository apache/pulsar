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

import static org.testng.Assert.*;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.Backoff;
import org.testng.annotations.Test;

public class BackoffTest {
    boolean withinTenPercentAndDecrementTimer(Backoff backoff, long t2) {
        long t1 = backoff.next();
        backoff.firstBackoffTimeInMillis -= t2;
        return (t1 >= t2 * 0.9 && t1 <= t2);
    }

    boolean checkExactAndDecrementTimer(Backoff backoff, long t2) {
        long t1 = backoff.next();
        backoff.firstBackoffTimeInMillis -= t2;
        return t1 == t2;
    }
    @Test
    public void shouldBackoffTest() {
        long currentTimestamp = System.nanoTime();
        Backoff testBackoff = new Backoff(currentTimestamp, TimeUnit.NANOSECONDS, 100, TimeUnit.MICROSECONDS, 0,
                TimeUnit.NANOSECONDS);
        // gives false
        assertTrue(!testBackoff.shouldBackoff(0L, TimeUnit.NANOSECONDS, 0));
        currentTimestamp = System.nanoTime();
        // gives true
        assertTrue(testBackoff.shouldBackoff(currentTimestamp, TimeUnit.NANOSECONDS, 100));
    }

    @Test
    public void mandatoryStopTestNegativeTest() {
        Backoff backoff = new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, 1900, TimeUnit.MILLISECONDS);
        assertEquals(backoff.next(), 100);
        backoff.next(); // 200
        backoff.next(); // 400
        backoff.next(); // 800
        assertFalse(withinTenPercentAndDecrementTimer(backoff, 400));
    }
    
    @Test
    public void firstBackoffTimerTest() throws InterruptedException {
        Backoff backoff = new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, 1900, TimeUnit.MILLISECONDS);
        assertEquals(backoff.next(), 100);
        long firstBackOffTime = backoff.firstBackoffTimeInMillis;
        Thread.sleep(300);
        long diffBackOffTime = backoff.firstBackoffTimeInMillis - firstBackOffTime;
        assertEquals(diffBackOffTime, 0);
        
        backoff.reset();
        assertEquals(backoff.next(), 100);
        diffBackOffTime = backoff.firstBackoffTimeInMillis - firstBackOffTime;
        assertTrue(diffBackOffTime >= 300 && diffBackOffTime < 310);
    }
    
    @Test
    public void basicTest() {
        Backoff backoff = new Backoff(5, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, 60, TimeUnit.SECONDS);
        assertTrue(checkExactAndDecrementTimer(backoff, 5));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 10));
        backoff.reset();
        assertTrue(checkExactAndDecrementTimer(backoff, 5));
    }

    @Test
    public void maxTest() {
        Backoff backoff = new Backoff(5, TimeUnit.MILLISECONDS, 20, TimeUnit.MILLISECONDS, 20, TimeUnit.MILLISECONDS);
        assertTrue(checkExactAndDecrementTimer(backoff, 5));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 10));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 5));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 20));
    }

    @Test
    public void mandatoryStopTest() {
        Backoff backoff = new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, 1900, TimeUnit.MILLISECONDS);
        assertTrue(checkExactAndDecrementTimer(backoff, 100));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 800));
        // would have been 1600 w/o the mandatory stop 
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 3200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 6400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 12800));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 25600));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 51200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 60000));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 60000));
        backoff.reset();
        assertTrue(checkExactAndDecrementTimer(backoff, 100));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 800));
        // would have been 1600 w/o the mandatory stop
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));

        backoff.reset();
        assertTrue(checkExactAndDecrementTimer(backoff, 100));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 800));

        backoff.reset();
        assertTrue(checkExactAndDecrementTimer(backoff, 100));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 800));
    }

    public void ignoringMandatoryStopTest() {
        Backoff backoff = new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, 0, TimeUnit.MILLISECONDS);
        assertTrue(checkExactAndDecrementTimer(backoff, 100));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 800));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 1600));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 3200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 6400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 12800));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 25600));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 51200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 60000));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 60000));

        backoff.reset();
        assertTrue(checkExactAndDecrementTimer(backoff, 100));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 800));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 1600));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 3200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 6400));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 12800));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 25600));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 51200));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 60000));
        assertTrue(withinTenPercentAndDecrementTimer(backoff, 60000));
    }
}
