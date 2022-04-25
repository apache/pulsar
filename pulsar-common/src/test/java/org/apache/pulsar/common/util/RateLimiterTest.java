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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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
        // no permits are available: need to wait on acquire
        assertTrue((end - start) > rateTimeMSec / 2);
        rate.close();
    }

    @Test
    public void testAcquire() throws Exception {
        final long rateTimeMSec = 1000;
        final int permits = 100;
        RateLimiter rate = RateLimiter.builder().permits(permits).rateTime(rateTimeMSec).timeUnit(TimeUnit.MILLISECONDS)
                .build();
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
        RateLimiter rate = RateLimiter.builder().permits(permits).rateTime(rateTimeMSec).timeUnit(TimeUnit.MILLISECONDS)
                .build();
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
        RateLimiter rate = RateLimiter.builder().permits(1).rateTime(rateTimeMSec).timeUnit(TimeUnit.MILLISECONDS)
                .build();
        assertTrue(rate.tryAcquire());
        assertFalse(rate.tryAcquire());
        assertEquals(rate.getAvailablePermits(), 0);
        rate.close();
    }

    @Test
    public void testTryAcquire() {
        final long rateTimeMSec = 1000;
        final int permits = 100;
        RateLimiter rate = RateLimiter.builder().permits(permits).rateTime(rateTimeMSec).timeUnit(TimeUnit.MILLISECONDS)
                .build();
        for (int i = 0; i < permits; i++) {
            rate.tryAcquire();
        }
        assertEquals(rate.getAvailablePermits(), 0);
        rate.close();
    }

    @Test
    public void testTryAcquireMoreThanPermits() {
        final long rateTimeMSec = 1000;
        RateLimiter rate = RateLimiter.builder().permits(3).rateTime(rateTimeMSec).timeUnit(TimeUnit.MILLISECONDS)
                .build();
        assertTrue(rate.tryAcquire(2));
        assertEquals(rate.getAvailablePermits(), 1);

        //try to acquire failed, not decrease availablePermits.
        assertFalse(rate.tryAcquire(2));
        assertEquals(rate.getAvailablePermits(), 1);

        assertTrue(rate.tryAcquire(1));
        assertEquals(rate.getAvailablePermits(), 0);

        rate.close();
    }

    @Test
    public void testMultipleTryAcquire() {
        final long rateTimeMSec = 1000;
        final int permits = 100;
        final int acquirePermits = 50;
        RateLimiter rate = RateLimiter.builder().permits(permits).rateTime(rateTimeMSec).timeUnit(TimeUnit.MILLISECONDS)
                .build();
        for (int i = 0; i < permits / acquirePermits; i++) {
            rate.tryAcquire(acquirePermits);
        }
        assertEquals(rate.getAvailablePermits(), 0);
        rate.close();
    }

    @Test
    public void testResetRate() throws Exception {
        final long rateTimeMSec = 1000;
        final int permits = 100;
        RateLimiter rate = RateLimiter.builder().permits(permits).rateTime(rateTimeMSec).timeUnit(TimeUnit.MILLISECONDS)
                .build();
        rate.tryAcquire(permits);
        assertEquals(rate.getAvailablePermits(), 0);
        // check after a rate-time: permits must be renewed
        Thread.sleep(rateTimeMSec * 2);
        assertEquals(rate.getAvailablePermits(), permits);

        // change rate-time from 1sec to 5sec
        rate.setRate(permits, 5 * rateTimeMSec, TimeUnit.MILLISECONDS, null);
        assertEquals(rate.getAvailablePermits(), 100);
        assertTrue(rate.tryAcquire(permits));
        assertEquals(rate.getAvailablePermits(), 0);
        // check after a rate-time: permits can't be renewed
        Thread.sleep(rateTimeMSec);
        assertEquals(rate.getAvailablePermits(), 0);

        rate.close();
    }

    @Test
    public void testDispatchRate() throws Exception {
        final long rateTimeMSec = 1000;
        final int permits = 100;
        RateLimiter rate = RateLimiter.builder().permits(permits).rateTime(rateTimeMSec).timeUnit(TimeUnit.MILLISECONDS)
                .isDispatchOrPrecisePublishRateLimiter(true)
                .build();
        rate.tryAcquire(100);
        rate.tryAcquire(100);
        rate.tryAcquire(100);
        assertEquals(rate.getAvailablePermits(), 0);

        Thread.sleep(rateTimeMSec * 2);
        // check after two rate-time: acquiredPermits is 100
        assertEquals(rate.getAvailablePermits(), 0);

        Thread.sleep(rateTimeMSec);
        // check after three rate-time: acquiredPermits is 0
        assertTrue(rate.getAvailablePermits() > 0);

        rate.close();
    }

    @Test
    public void testRateLimiterWithPermitUpdater() throws Exception {
        long permits = 10;
        long rateTime = 1;
        long newUpdatedRateLimit = 100L;
        Supplier<Long> permitUpdater = () -> newUpdatedRateLimit;
        RateLimiter limiter = RateLimiter.builder().permits(permits).rateTime(1).timeUnit(TimeUnit.SECONDS)
                .permitUpdater(permitUpdater)
                .build();
        limiter.acquire();
        Thread.sleep(rateTime * 3 * 1000);
        assertEquals(limiter.getAvailablePermits(), newUpdatedRateLimit);
    }

    @Test
    public void testRateLimiterWithFunction() {
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        long permits = 10;
        long rateTime = 1;
        int reNewTime = 3;
        RateLimitFunction rateLimitFunction = atomicInteger::incrementAndGet;
        RateLimiter rateLimiter = RateLimiter.builder().permits(permits).rateTime(rateTime).timeUnit(TimeUnit.SECONDS)
                .rateLimitFunction(rateLimitFunction)
                .build();
        for (int i = 0; i < reNewTime; i++) {
            rateLimiter.renew();
        }
        assertEquals(reNewTime, atomicInteger.get());
    }

}
