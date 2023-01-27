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
package org.apache.bookkeeper.mledger.impl.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class InflightReadsLimiterTest {

    @Test
    public void testDisabled() throws Exception {

        InflightReadsLimiter limiter = new InflightReadsLimiter(0);
        assertTrue(limiter.isDisabled());

        limiter = new InflightReadsLimiter(-1);
        assertTrue(limiter.isDisabled());

        limiter = new InflightReadsLimiter(1);
        assertFalse(limiter.isDisabled());
    }

    @Test
    public void testBasicAcquireRelease() throws Exception {
        InflightReadsLimiter limiter = new InflightReadsLimiter(100);
        assertEquals(100, limiter.getRemainingBytes());
        InflightReadsLimiter.Handle handle = limiter.acquire(100, null);
        assertEquals(0, limiter.getRemainingBytes());
        assertTrue(handle.success);
        assertEquals(handle.acquiredPermits, 100);
        assertEquals(1, handle.trials);
        limiter.release(handle);
        assertEquals(100, limiter.getRemainingBytes());
    }

    @Test
    public void testNotEnoughPermits() throws Exception {
        InflightReadsLimiter limiter = new InflightReadsLimiter(100);
        assertEquals(100, limiter.getRemainingBytes());
        InflightReadsLimiter.Handle handle = limiter.acquire(100, null);
        assertEquals(0, limiter.getRemainingBytes());
        assertTrue(handle.success);
        assertEquals(handle.acquiredPermits, 100);
        assertEquals(1, handle.trials);

        InflightReadsLimiter.Handle handle2 = limiter.acquire(100, null);
        assertEquals(0, limiter.getRemainingBytes());
        assertFalse(handle2.success);
        assertEquals(handle2.acquiredPermits, 0);
        assertEquals(1, handle2.trials);

        limiter.release(handle);
        assertEquals(100, limiter.getRemainingBytes());

        handle2 = limiter.acquire(100, handle2);
        assertEquals(0, limiter.getRemainingBytes());
        assertTrue(handle2.success);
        assertEquals(handle2.acquiredPermits, 100);
        assertEquals(2, handle2.trials);

        limiter.release(handle2);
        assertEquals(100, limiter.getRemainingBytes());

    }

    @Test
    public void testPartialAcquire() throws Exception {
        InflightReadsLimiter limiter = new InflightReadsLimiter(100);
        assertEquals(100, limiter.getRemainingBytes());

        InflightReadsLimiter.Handle handle = limiter.acquire(30, null);
        assertEquals(70, limiter.getRemainingBytes());
        assertTrue(handle.success);
        assertEquals(handle.acquiredPermits, 30);
        assertEquals(1, handle.trials);

        InflightReadsLimiter.Handle handle2 = limiter.acquire(100, null);
        assertEquals(0, limiter.getRemainingBytes());
        assertFalse(handle2.success);
        assertEquals(handle2.acquiredPermits, 70);
        assertEquals(1, handle2.trials);

        limiter.release(handle);

        handle2 = limiter.acquire(100, handle2);
        assertEquals(0, limiter.getRemainingBytes());
        assertTrue(handle2.success);
        assertEquals(handle2.acquiredPermits, 100);
        assertEquals(2, handle2.trials);

        limiter.release(handle2);
        assertEquals(100, limiter.getRemainingBytes());

    }

    @Test
    public void testTooManyTrials() throws Exception {
        InflightReadsLimiter limiter = new InflightReadsLimiter(100);
        assertEquals(100, limiter.getRemainingBytes());

        InflightReadsLimiter.Handle handle = limiter.acquire(30, null);
        assertEquals(70, limiter.getRemainingBytes());
        assertTrue(handle.success);
        assertEquals(handle.acquiredPermits, 30);
        assertEquals(1, handle.trials);

        InflightReadsLimiter.Handle handle2 = limiter.acquire(100, null);
        assertEquals(0, limiter.getRemainingBytes());
        assertFalse(handle2.success);
        assertEquals(handle2.acquiredPermits, 70);
        assertEquals(1, handle2.trials);

        handle2 = limiter.acquire(100, handle2);
        assertEquals(0, limiter.getRemainingBytes());
        assertFalse(handle2.success);
        assertEquals(handle2.acquiredPermits, 70);
        assertEquals(2, handle2.trials);

        handle2 = limiter.acquire(100, handle2);
        assertEquals(0, limiter.getRemainingBytes());
        assertFalse(handle2.success);
        assertEquals(handle2.acquiredPermits, 70);
        assertEquals(3, handle2.trials);

        handle2 = limiter.acquire(100, handle2);
        assertEquals(0, limiter.getRemainingBytes());
        assertFalse(handle2.success);
        assertEquals(handle2.acquiredPermits, 70);
        assertEquals(4, handle2.trials);

        // too many trials, start from scratch
        handle2 = limiter.acquire(100, handle2);
        assertEquals(70, limiter.getRemainingBytes());
        assertFalse(handle2.success);
        assertEquals(handle2.acquiredPermits, 0);
        assertEquals(1, handle2.trials);

        limiter.release(handle);

        handle2 = limiter.acquire(100, handle2);
        assertEquals(0, limiter.getRemainingBytes());
        assertTrue(handle2.success);
        assertEquals(handle2.acquiredPermits, 100);
        assertEquals(2, handle2.trials);

        limiter.release(handle2);
        assertEquals(100, limiter.getRemainingBytes());

    }

}
