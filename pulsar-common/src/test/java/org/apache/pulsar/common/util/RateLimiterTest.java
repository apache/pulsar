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
import static org.testng.Assert.fail;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class RateLimiterTest {

    @Test
    public void testInvalidRenewTime() {
        try {
            RateLimiter.builder().permits(0).rateTime(100).timeUnit(TimeUnit.SECONDS).build();
            fail("should have thrown exception: invalid rate, must be > 0");
        } catch (IllegalArgumentException ie) {
            // Ok
        }

        try {
            RateLimiter.builder().permits(10).rateTime(0).timeUnit(TimeUnit.SECONDS).build();
            fail("should have thrown exception: invalid rateTime, must be > 0");
        } catch (IllegalArgumentException ie) {
            // Ok
        }
    }

    @Test
    public void testClose() throws Exception {
        RateLimiter rate = RateLimiter.builder().permits(1).rateTime(1000).timeUnit(TimeUnit.MILLISECONDS).build();
        assertFalse(rate.isClosed());
        rate.close();
        assertTrue(rate.isClosed());
        try {
            rate.acquire();
            fail("should have failed, executor is already closed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void testAcquireBlock() throws Exception {
        final long rateTimeMSec = 1000;
        RateLimiter rate = RateLimiter.builder().permits(1).rateTime(rateTimeMSec).timeUnit(TimeUnit.MILLISECONDS)
                .build();
        rate.acquire();
        assertEquals(rate.getAvailablePermits(), 0);
        long start = System.currentTimeMillis();
        rate.acquire();
        long end = System.currentTimeMillis();
        assertTrue((end - start) > rateTimeMSec / 2);
        rate.close();
    }

    @Test
    public void testDispatchRate() {
        RateLimiter rateLimiter = new RateLimiter(Executors.newSingleThreadScheduledExecutor(), 1, 1, TimeUnit.SECONDS, () -> 1L, false, null);
        try {
            rateLimiter.acquire(1);
            System.out.println("Acquired initial permit");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Thread was interrupted");
        }

        boolean result = rateLimiter.tryAcquire(1);
        System.out.println("Tried to acquire permit: " + result);

        assertTrue(result, "Expected to acquire permit");
    }
}

