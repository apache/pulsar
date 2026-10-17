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
package org.apache.pulsar.utils;

import static org.testng.Assert.assertEquals;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

/**
 * Comprehensive test suite for TimedSingleThreadRateLimiter class.
 */
public class TimedSingleThreadRateLimiterTest {

    @Test
    public void testConstructorAndGetters() {
        int rate = 100;
        long period = 5;
        TimeUnit unit = TimeUnit.SECONDS;
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(rate, period, unit);
        assertEquals(limiter.getRate(), rate);
        assertEquals(limiter.getPeriodAtMs(), unit.toMillis(period));
        assertEquals(limiter.getRemaining(), rate); // Initially should have all permits
    }

    @Test
    public void testConstructorWithDifferentTimeUnits() {
        // Test with milliseconds
        TimedSingleThreadRateLimiter limiterMs = new TimedSingleThreadRateLimiter(50, 1000, TimeUnit.MILLISECONDS);
        assertEquals(limiterMs.getPeriodAtMs(), 1000);
        // Test with seconds
        TimedSingleThreadRateLimiter limiterSec = new TimedSingleThreadRateLimiter(50, 2, TimeUnit.SECONDS);
        assertEquals(limiterSec.getPeriodAtMs(), 2000);
        // Test with minutes
        TimedSingleThreadRateLimiter limiterMin = new TimedSingleThreadRateLimiter(50, 1, TimeUnit.MINUTES);
        assertEquals(limiterMin.getPeriodAtMs(), 60000);
    }

    @Test
    public void testBasicAcquire() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(100, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Test acquiring single permit
        int acquired = limiter.acquire(1);
        assertEquals(acquired, 1);
        assertEquals(limiter.getRemaining(), 99);
        // Test acquiring multiple permits
        acquired = limiter.acquire(10);
        assertEquals(acquired, 10);
        assertEquals(limiter.getRemaining(), 89);
    }

    @Test
    public void testAcquireMoreThanRemaining() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(10, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Acquire most permits
        int acquired = limiter.acquire(8);
        assertEquals(acquired, 8);
        assertEquals(limiter.getRemaining(), 2);
        // Try to acquire more than remaining
        acquired = limiter.acquire(5);
        assertEquals(acquired, 2); // Should only get remaining permits
        assertEquals(limiter.getRemaining(), 0);
    }

    @Test
    public void testAcquireWhenNoPermitsRemaining() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(5, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Exhaust all permits
        limiter.acquire(5);
        assertEquals(limiter.getRemaining(), 0);
        // Try to acquire when no permits left
        int acquired = limiter.acquire(3);
        assertEquals(acquired, 0);
        assertEquals(limiter.getRemaining(), 0);
    }

    @Test
    public void testAcquireZeroPermits() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(10, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        int acquired = limiter.acquire(0);
        assertEquals(acquired, 0);
        assertEquals(limiter.getRemaining(), 10); // Should remain unchanged
    }

    @Test
    public void testPermitRenewalAfterPeriod() throws InterruptedException {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(10, 100, TimeUnit.MILLISECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Exhaust all permits
        limiter.acquire(10);
        assertEquals(limiter.getRemaining(), 0);
        // Wait for period to pass
        Thread.sleep(150);
        // Acquire should trigger renewal
        int acquired = limiter.acquire(5);
        assertEquals(acquired, 5);
        assertEquals(limiter.getRemaining(), 5);
    }

    @Test
    public void testNoRenewalBeforePeriodExpires() throws InterruptedException {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(10, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Exhaust all permits
        limiter.acquire(10);
        assertEquals(limiter.getRemaining(), 0);
        // Should not renew yet
        int acquired = limiter.acquire(5);
        assertEquals(acquired, 0);
        assertEquals(limiter.getRemaining(), 0);
    }

    @Test
    public void testTimingOpen() throws Exception {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(10, 1, TimeUnit.SECONDS);
        // Set timing to open for 500ms
        limiter.timingOpen(500, TimeUnit.MILLISECONDS);
        // During open period.
        int acquired = limiter.acquire(15);
        assertEquals(acquired, 10);
        assertEquals(limiter.getRemaining(), 0);
        // Closed.
        Thread.sleep(1000);
        int acquired2 = limiter.acquire(1000);
        assertEquals(acquired2, 1000);
    }

    @Test
    public void testTimingOpenWithZeroDuration() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(10, 1, TimeUnit.SECONDS);
        // Set timing to open for 0 duration.
        limiter.timingOpen(0, TimeUnit.MILLISECONDS);
        // Closed.
        int acquired = limiter.acquire(7000);
        assertEquals(acquired, 7000);
    }

    @Test
    public void testHighRateAcquisition() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(1000, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Acquire permits in chunks
        int totalAcquired = 0;
        for (int i = 0; i < 10; i++) {
            totalAcquired += limiter.acquire(100);
        }
        assertEquals(totalAcquired, 1000);
        assertEquals(limiter.getRemaining(), 0);
    }

    @Test
    public void testLowRateAcquisition() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(3, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Acquire all permits one by one
        assertEquals(limiter.acquire(1), 1);
        assertEquals(limiter.getRemaining(), 2);
        assertEquals(limiter.acquire(1), 1);
        assertEquals(limiter.getRemaining(), 1);
        assertEquals(limiter.acquire(1), 1);
        assertEquals(limiter.getRemaining(), 0);
        // No more permits available
        assertEquals(limiter.acquire(1), 0);
        assertEquals(limiter.getRemaining(), 0);
    }

    @Test
    public void testRenewalWithPartialAcquisition() throws InterruptedException {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(10, 100, TimeUnit.MILLISECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Acquire some permits
        limiter.acquire(6);
        assertEquals(limiter.getRemaining(), 4);
        // Wait for renewal
        Thread.sleep(150);
        // After renewal, should have full rate again
        int acquired = limiter.acquire(8);
        assertEquals(acquired, 8);
        assertEquals(limiter.getRemaining(), 2);
    }

    @Test
    public void testConcurrentBehaviorSimulation() throws InterruptedException {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(20, 100, TimeUnit.MILLISECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Simulate rapid acquisitions
        int totalAcquired = 0;
        for (int i = 0; i < 5; i++) {
            totalAcquired += limiter.acquire(5);
        }
        assertEquals(totalAcquired, 20);
        assertEquals(limiter.getRemaining(), 0);
        // Wait for renewal
        Thread.sleep(150);
        // Should be able to acquire again
        int newAcquired = limiter.acquire(10);
        assertEquals(newAcquired, 10);
        assertEquals(limiter.getRemaining(), 10);
    }

    @Test
    public void testVeryShortPeriod() throws InterruptedException {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(5, 10, TimeUnit.MILLISECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Exhaust permits
        limiter.acquire(5);
        assertEquals(limiter.getRemaining(), 0);
        // Wait for very short period
        Thread.sleep(20);
        // Should renew quickly
        int acquired = limiter.acquire(3);
        assertEquals(acquired, 3);
        assertEquals(limiter.getRemaining(), 2);
    }

    @Test
    public void testVeryLongPeriod() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(10, 1, TimeUnit.HOURS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        assertEquals(limiter.getPeriodAtMs(), TimeUnit.HOURS.toMillis(1));
        // Acquire some permits
        int acquired = limiter.acquire(7);
        assertEquals(acquired, 7);
        assertEquals(limiter.getRemaining(), 3);
        // Even after a short wait, should not renew (period is 1 hour)
        int acquired2 = limiter.acquire(5);
        assertEquals(acquired2, 3); // Only remaining permits
        assertEquals(limiter.getRemaining(), 0);
    }

    @Test
    public void testSinglePermitRate() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(1, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        assertEquals(limiter.getRate(), 1);
        assertEquals(limiter.getRemaining(), 1);
        // Acquire the only permit
        int acquired = limiter.acquire(1);
        assertEquals(acquired, 1);
        assertEquals(limiter.getRemaining(), 0);
        // Try to acquire more
        acquired = limiter.acquire(1);
        assertEquals(acquired, 0);
        assertEquals(limiter.getRemaining(), 0);
    }

    @Test
    public void testLargePermitRequest() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(10, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Request much more than available
        int acquired = limiter.acquire(1000);
        assertEquals(acquired, 10); // Should get all available permits
        assertEquals(limiter.getRemaining(), 0);
    }

    @Test
    public void testNegativePermitRequest() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(10, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Request negative permits (edge case)
        int acquired = limiter.acquire(-5);
        // The implementation doesn't explicitly handle negative permits
        // This test documents the current behavior
        assertEquals(acquired, 0); // Should not return negative
        assertEquals(limiter.getRemaining(), 10); // Remaining should not go negative
    }

    @Test
    public void testMultipleRenewalCycles() throws InterruptedException {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(5, 50, TimeUnit.MILLISECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // First cycle
        limiter.acquire(5);
        assertEquals(limiter.getRemaining(), 0);
        // Wait for first renewal
        Thread.sleep(60);
        limiter.acquire(3);
        assertEquals(limiter.getRemaining(), 2);
        // Wait for second renewal
        Thread.sleep(60);
        int acquired = limiter.acquire(4);
        assertEquals(acquired, 4);
        assertEquals(limiter.getRemaining(), 1);
        // Wait for third renewal
        Thread.sleep(60);
        acquired = limiter.acquire(5);
        assertEquals(acquired, 5);
        assertEquals(limiter.getRemaining(), 0);
    }

    @Test
    public void testRapidAcquisitionPattern() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(100, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Simulate rapid small acquisitions
        int totalAcquired = 0;
        for (int i = 0; i < 50; i++) {
            totalAcquired += limiter.acquire(2);
        }
        assertEquals(totalAcquired, 100);
        assertEquals(limiter.getRemaining(), 0);
    }

    @Test
    public void testBurstAcquisitionPattern() {
        TimedSingleThreadRateLimiter limiter = new TimedSingleThreadRateLimiter(50, 1, TimeUnit.SECONDS);
        limiter.timingOpen(10, TimeUnit.SECONDS);
        // Large burst acquisition
        int acquired1 = limiter.acquire(30);
        assertEquals(acquired1, 30);
        assertEquals(limiter.getRemaining(), 20);
        // Another burst
        int acquired2 = limiter.acquire(25);
        assertEquals(acquired2, 20); // Only remaining permits
        assertEquals(limiter.getRemaining(), 0);
    }
}
