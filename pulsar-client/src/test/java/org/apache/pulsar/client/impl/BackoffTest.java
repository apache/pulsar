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
    boolean withinTenPercent(long t1, long t2) {
        return (t1 >= t2 * 0.9 && t1 <= t2);
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
    public void basicTest() {
        Backoff backoff = new Backoff(5, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, 60, TimeUnit.SECONDS);
        assertEquals(backoff.next(), 5);
        assertTrue(withinTenPercent(backoff.next(), 10));
        backoff.reset();
        assertEquals(backoff.next(), 5);
    }

    @Test
    public void maxTest() {
        Backoff backoff = new Backoff(5, TimeUnit.MILLISECONDS, 20, TimeUnit.MILLISECONDS, 20, TimeUnit.MILLISECONDS);
        assertEquals(backoff.next(), 5);
        assertTrue(withinTenPercent(backoff.next(), 10));
        assertTrue(withinTenPercent(backoff.next(), 5));
        assertTrue(withinTenPercent(backoff.next(), 20));
    }

    @Test
    public void mandatoryStopTest() {
        Backoff backoff = new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, 1900, TimeUnit.MILLISECONDS);
        assertEquals(backoff.next(), 100);
        assertTrue(withinTenPercent(backoff.next(), 200));
        assertTrue(withinTenPercent(backoff.next(), 400));
        assertTrue(withinTenPercent(backoff.next(), 800));
        // would have been 1600 w/o the mandatory stop 
        assertTrue(withinTenPercent(backoff.next(), 400));
        assertTrue(withinTenPercent(backoff.next(), 3200));
        assertTrue(withinTenPercent(backoff.next(), 6400));
        assertTrue(withinTenPercent(backoff.next(), 12800));
        assertTrue(withinTenPercent(backoff.next(), 25600));
        assertTrue(withinTenPercent(backoff.next(), 51200));
        assertTrue(withinTenPercent(backoff.next(), 60000));
        assertTrue(withinTenPercent(backoff.next(), 60000));
        backoff.reset();
        assertEquals(backoff.next(), 100);
        assertTrue(withinTenPercent(backoff.next(), 200));
        assertTrue(withinTenPercent(backoff.next(), 400));
        assertTrue(withinTenPercent(backoff.next(), 800));
        // would have been 1600 w/o the mandatory stop
        assertTrue(withinTenPercent(backoff.next(), 400));

        backoff.reset();
        assertEquals(backoff.next(), 100);
        assertTrue(withinTenPercent(backoff.next(), 200));
        assertTrue(withinTenPercent(backoff.next(), 400));
        assertTrue(withinTenPercent(backoff.next(), 800));

        backoff.reset();
        assertEquals(backoff.next(), 100);
        assertTrue(withinTenPercent(backoff.next(), 200));
        assertTrue(withinTenPercent(backoff.next(), 400));
        assertTrue(withinTenPercent(backoff.next(), 800));
    }

    public void ignoringMandatoryStopTest() {
        Backoff backoff = new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, 0, TimeUnit.MILLISECONDS);
        assertEquals(backoff.next(), 100);
        assertTrue(withinTenPercent(backoff.next(), 200));
        assertTrue(withinTenPercent(backoff.next(), 400));
        assertTrue(withinTenPercent(backoff.next(), 800));
        assertTrue(withinTenPercent(backoff.next(), 1600));
        assertTrue(withinTenPercent(backoff.next(), 3200));
        assertTrue(withinTenPercent(backoff.next(), 6400));
        assertTrue(withinTenPercent(backoff.next(), 12800));
        assertTrue(withinTenPercent(backoff.next(), 25600));
        assertTrue(withinTenPercent(backoff.next(), 51200));
        assertTrue(withinTenPercent(backoff.next(), 60000));
        assertTrue(withinTenPercent(backoff.next(), 60000));

        backoff.reset();
        assertEquals(backoff.next(), 100);
        assertTrue(withinTenPercent(backoff.next(), 200));
        assertTrue(withinTenPercent(backoff.next(), 400));
        assertTrue(withinTenPercent(backoff.next(), 800));
        assertTrue(withinTenPercent(backoff.next(), 1600));
        assertTrue(withinTenPercent(backoff.next(), 3200));
        assertTrue(withinTenPercent(backoff.next(), 6400));
        assertTrue(withinTenPercent(backoff.next(), 12800));
        assertTrue(withinTenPercent(backoff.next(), 25600));
        assertTrue(withinTenPercent(backoff.next(), 51200));
        assertTrue(withinTenPercent(backoff.next(), 60000));
        assertTrue(withinTenPercent(backoff.next(), 60000));
    }
}
