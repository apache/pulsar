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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerReplicationMemoryLimiterTest {

    @Test
    public void testDisabledByDefault() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        assertEquals(limiter.getMaxBytes(), 0);
        assertEquals(limiter.acquireUpTo(1024 * 1024 * 1024, null), 1024 * 1024 * 1024);
        assertEquals(limiter.getInflightBytes(), 0);
    }

    @Test
    public void testAcquireAndRelease() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        limiter.setMaxBytes(100);

        assertEquals(limiter.acquireUpTo(60, null), 60);
        assertEquals(limiter.getInflightBytes(), 60);

        assertEquals(limiter.acquireUpTo(40, null), 40);
        assertEquals(limiter.getInflightBytes(), 100);

        limiter.release(40);
        assertEquals(limiter.getInflightBytes(), 60);

        limiter.release(60);
        assertEquals(limiter.getInflightBytes(), 0);
    }

    @Test
    public void testAcquirePartial() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        limiter.setMaxBytes(100);

        assertEquals(limiter.acquireUpTo(80, null), 80);
        assertEquals(limiter.getInflightBytes(), 80);

        // Only 20 available, request 50 -> get 20
        assertEquals(limiter.acquireUpTo(50, null), 20);
        assertEquals(limiter.getInflightBytes(), 100);
    }

    @Test
    public void testAcquireZeroEnqueuesRetry() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        limiter.setMaxBytes(100);

        assertEquals(limiter.acquireUpTo(100, null), 100);

        AtomicBoolean retryExecuted = new AtomicBoolean(false);
        assertEquals(limiter.acquireUpTo(10, () -> retryExecuted.set(true)), 0);
        assertEquals(limiter.getInflightBytes(), 100);
        assertEquals(limiter.getPendingTaskCount(), 1);

        limiter.release(50);
        assertEquals(limiter.getInflightBytes(), 50);

        Awaitility.await().untilTrue(retryExecuted);
        assertEquals(limiter.getPendingTaskCount(), 0);
    }

    @Test
    public void testPartialAcquireDoesNotEnqueueRetry() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        limiter.setMaxBytes(100);

        assertEquals(limiter.acquireUpTo(80, null), 80);

        // Request 50, only 20 available -> get 20, no pending task
        assertEquals(limiter.acquireUpTo(50, null), 20);
        assertEquals(limiter.getInflightBytes(), 100);
        assertEquals(limiter.getPendingTaskCount(), 0);
    }

    @Test
    public void testLimitReached() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        limiter.setMaxBytes(100);

        assertEquals(limiter.acquireUpTo(80, null), 80);
        assertFalse(limiter.isLimitReached());

        assertEquals(limiter.acquireUpTo(50, null), 20);
        assertTrue(limiter.isLimitReached());

        assertEquals(limiter.acquireUpTo(10, null), 0);

        limiter.release(30);
        assertFalse(limiter.isLimitReached());
    }

    @Test
    public void testCalibrateCorrection() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        limiter.setMaxBytes(100);

        assertEquals(limiter.acquireUpTo(50, null), 50);
        assertEquals(limiter.getInflightBytes(), 50);

        limiter.calibrate(50, 30);
        assertEquals(limiter.getInflightBytes(), 30);
    }

    @Test
    public void testCalibrateTriggersDrain() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        limiter.setMaxBytes(100);

        assertEquals(limiter.acquireUpTo(100, null), 100);

        AtomicBoolean retry = new AtomicBoolean(false);
        assertEquals(limiter.acquireUpTo(20, () -> retry.set(true)), 0);

        limiter.calibrate(100, 50);
        assertEquals(limiter.getInflightBytes(), 50);

        Awaitility.await().untilTrue(retry);
    }

    @Test
    public void testReleaseEstimated() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        limiter.setMaxBytes(100);

        assertEquals(limiter.acquireUpTo(60, null), 60);
        assertEquals(limiter.getInflightBytes(), 60);

        limiter.releaseEstimated(60);
        assertEquals(limiter.getInflightBytes(), 0);
    }

    @Test
    public void testDynamicMaxBytesIncreaseTriggersDrain() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        limiter.setMaxBytes(100);

        assertEquals(limiter.acquireUpTo(100, null), 100);
        AtomicBoolean retry = new AtomicBoolean(false);
        assertEquals(limiter.acquireUpTo(20, () -> retry.set(true)), 0);
        assertEquals(limiter.getPendingTaskCount(), 1);

        limiter.setMaxBytes(200);
        Awaitility.await().untilTrue(retry);
        assertEquals(limiter.getPendingTaskCount(), 0);
    }

    @Test
    public void testDynamicMaxBytesDecrease() {
        BrokerReplicationMemoryLimiter limiter =
                new BrokerReplicationMemoryLimiter(Executors.newSingleThreadExecutor());
        limiter.setMaxBytes(200);

        assertEquals(limiter.acquireUpTo(150, null), 150);
        limiter.setMaxBytes(100);

        AtomicBoolean retry = new AtomicBoolean(false);
        assertEquals(limiter.acquireUpTo(10, () -> retry.set(true)), 0);
        assertEquals(limiter.getPendingTaskCount(), 1);
    }
}
