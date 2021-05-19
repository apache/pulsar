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
package org.apache.pulsar.io.elasticsearch;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RandomExponentialRetryTests {

    int failureCount = 0;

    @BeforeMethod
    void resetCount() {
        failureCount = 0;
    }

    Integer testFunction(int numRetries) throws IOException {
        if (failureCount < numRetries) {
            failureCount++;
            throw new IOException();
        }
        return failureCount;
    }

    @Test
    public void testExponentialWait() {
        RandomExponentialRetry backoffRetry = new RandomExponentialRetry(5);
        assertEquals(backoffRetry.waitInMs(0, 100), 100L);
        assertEquals(backoffRetry.waitInMs(1, 100), 200L);
        assertEquals(backoffRetry.waitInMs(2, 100), 400L);
        assertEquals(backoffRetry.waitInMs(3, 100), 800L);
        assertEquals(backoffRetry.waitInMs(4, 100), 1600L);
        assertEquals(backoffRetry.waitInMs(5, 100), 3200L);
        assertEquals(backoffRetry.waitInMs(6, 100), 5000L);
    }

    @Test
    public void callWithNoRetries() throws Exception {
        MockTime mockTime = new MockTime();
        RandomExponentialRetry backoffRetry = new RandomExponentialRetry();
        assertEquals(0, (int)backoffRetry.retry( () -> testFunction(0), 3, 100, "NoRetries", mockTime));
        assertEquals(0L, mockTime.totalMs.get());
        assertEquals(0L, mockTime.sleeps.size());
    }

    @Test(expectedExceptions = { IOException.class })
    public void callWithExhaustedRetries() throws Exception {
        MockTime mockTime = new MockTime();
        RandomExponentialRetry backoffRetry = new RandomExponentialRetry();
        assertEquals(4, (int)backoffRetry.retry( () -> testFunction(4), 3, 100, "ExhautstedRetries", mockTime));
    }

    @Test
    public void callWithSomeRetries() throws Exception {
        int N = 10;
        MockTime mockTime = new MockTime();
        RandomExponentialRetry backoffRetry = new RandomExponentialRetry();
        assertEquals(N, (int)backoffRetry.retry( () -> testFunction(N), N+1, 100, "SomeRetries", mockTime));
        assertEquals(N, mockTime.sleeps.size());
        for(int i = 0; i < N; i++) {
            assertTrue(mockTime.sleeps.get(i) <=  backoffRetry.waitInMs(i, 100));
        }
        System.out.println("sleeps="+mockTime.sleeps);
    }

    static class MockTime extends RandomExponentialRetry.Time {
        public final AtomicLong totalMs = new AtomicLong(0L);
        public final List<Long> sleeps = new ArrayList<>();
        public void sleep(long ms) {
            totalMs.addAndGet(ms);
            sleeps.add(ms);
        }
    }
}
