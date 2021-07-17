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
package org.apache.pulsar.common.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.testng.annotations.Test;

public class LeakyBucketRateLimiterTest {

    @Test
    public void testInvalidRenewTime() {
        try {
            new LeakyBucketRateLimiter(null, 0, 100, TimeUnit.SECONDS, null);
            fail("should have thrown exception: invalid rate, must be > 0");
        } catch (IllegalArgumentException ie) {
            // Ok
        }

        try {
            new LeakyBucketRateLimiter(null, 10, 0, TimeUnit.SECONDS, null);
            fail("should have thrown exception: invalid rateTime, must be > 0");
        } catch (IllegalArgumentException ie) {
            // Ok
        }
    }

    @Test
    public void testClose() throws Exception {
        LeakyBucketRateLimiter rate = new LeakyBucketRateLimiter(null, 1, 1000, TimeUnit.MILLISECONDS, null);
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
        LeakyBucketRateLimiter rate = new LeakyBucketRateLimiter(null, 1, rateTimeMSec,
                TimeUnit.MILLISECONDS, null);
        rate.acquire();
        assertEquals(rate.getAvailablePermits(), 0);
        long start = System.currentTimeMillis();
        rate.acquire();
        long end = System.currentTimeMillis();
        // no permits are available: need to wait on acquire
        assertTrue((end - start) > rateTimeMSec / 2);
        rate.close();
    }

    @Test
    public void testAcquire() throws Exception {
        final long rateTimeMSec = 1000;
        final int permits = 100;
        LeakyBucketRateLimiter rate = new LeakyBucketRateLimiter(null, permits, rateTimeMSec,
                TimeUnit.MILLISECONDS, null);
        long start = System.currentTimeMillis();
        for (int i = 0; i < permits; i++) {
            rate.acquire();
        }
        long end = System.currentTimeMillis();
        assertTrue((end - start) < rateTimeMSec);
        assertEquals(rate.getAvailablePermits(), 0);
        rate.close();
    }

    @Test
    public void testMultipleAcquire() throws Exception {
        final long rateTimeMSec = 1000;
        final int permits = 100;
        final int acquirePermits = 50;
        LeakyBucketRateLimiter rate = new LeakyBucketRateLimiter(null, permits, rateTimeMSec,
                TimeUnit.MILLISECONDS, null);
        long start = System.currentTimeMillis();
        for (int i = 0; i < permits / acquirePermits; i++) {
            rate.acquire(acquirePermits);
        }
        long end = System.currentTimeMillis();
        assertTrue((end - start) < rateTimeMSec);
        assertEquals(rate.getAvailablePermits(), 0);
        rate.close();
    }

    @Test
    public void testTryAcquireNoPermits() {
        final long rateTimeMSec = 1000;
        LeakyBucketRateLimiter rate = new LeakyBucketRateLimiter(null, 1, rateTimeMSec,
                TimeUnit.MILLISECONDS, null);

        //If the bucket goes full while acquiring we want it to return false
        assertFalse(rate.tryAcquire());
        assertFalse(rate.tryAcquire());
        assertEquals(rate.getAvailablePermits(), 0);
        rate.close();
    }

    @Test
    public void testTryAcquire() {
        final long rateTimeMSec = 1000;
        final int permits = 100;
        LeakyBucketRateLimiter rate = new LeakyBucketRateLimiter(null, permits, rateTimeMSec,
                TimeUnit.MILLISECONDS, null);
        for (int i = 0; i < permits; i++) {
            rate.tryAcquire();
        }
        assertEquals(rate.getAvailablePermits(), 0);
        rate.close();
    }

    @Test
    public void testMultipleTryAcquire() {
        final long rateTimeMSec = 1000;
        final int permits = 100;
        final int acquirePermits = 50;
        LeakyBucketRateLimiter rate = new LeakyBucketRateLimiter(null, permits, rateTimeMSec,
                TimeUnit.MILLISECONDS, null);
        for (int i = 0; i < permits / acquirePermits; i++) {
            rate.tryAcquire(acquirePermits);
        }
        assertEquals(rate.getAvailablePermits(), 0);
        rate.close();
    }

    @Test
    public void testRateLimiterWithPermitUpdater() throws Exception {
        long permits = 10;
        long rateTime = 1;
        long newUpdatedRateLimit = 100L;
        Supplier<Long> permitUpdater = () -> newUpdatedRateLimit;
        LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(null, permits, 1, TimeUnit.SECONDS, permitUpdater);
        limiter.acquire();
        Thread.sleep(rateTime * 3 * 1000);
        assertEquals(limiter.getAvailablePermits(), newUpdatedRateLimit);
    }
}
