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
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testng.annotations.Test;

@Test(groups = "utils")
public class LatencyTracerTest {

    @Test
    public void testMulti() {
        final var tracer = new LatencyTracer(10_000_000, 3);
        tracer.trace("A", 30_000_000);
        tracer.trace("B", 70_000_000);
        tracer.trace("C", 80_000_000);
        assertEquals(tracer.latencyString(), "total: 70 ms, A: 20 ms, B: 40 ms, C: 10 ms");
        assertEquals(tracer.latencyInMillis(), 70);
    }

    @Test
    public void testEmpty() {
        final var tracer = new LatencyTracer(10_000_000, 0);
        assertEquals(tracer.latencyString(), "total: 0 ms");
        assertEquals(tracer.latencyInMillis(), 0);
    }

    @Test
    public void testSingle() {
        final var tracer = new LatencyTracer(10_000_000, 1);
        tracer.trace("X", 100_000_000);
        assertEquals(tracer.latencyString(), "total: 90 ms, X: 90 ms");
        assertEquals(tracer.latencyInMillis(), 90);
    }

    @Test
    public void testZeroMs() {
        final var tracer = new LatencyTracer(0, 1);
        tracer.trace("A", 999_999);
        tracer.trace("B", 2_000_000);
        tracer.trace("C", 2_100_000);
        assertEquals(tracer.latencyString(), "total: 2 ms, A: 999 us, B: 1 ms, C: 100 us");
        assertEquals(tracer.latencyInMillis(), 2);
    }

    @Test
    public void testTraceFuture() throws Exception {
        final var tracer = new LatencyTracer(System.nanoTime(), 1);
        final var future = CompletableFuture.completedFuture(100);
        assertSame(tracer.trace("A", future), future);
        assertEquals(tracer.latencyString(), "total: 0 ms");

        final var future2 = new CompletableFuture<Integer>();
        CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS).execute(() -> future2.complete(1));
        final var tracedFuture = tracer.trace("A", future2);
        assertNotSame(tracedFuture, future2);
        assertEquals(tracedFuture.get(), 1);
        String s = tracer.latencyString();
        Matcher m = Pattern.compile("total: (\\d+) ms, A: (\\d+) ms").matcher(s);
        assertTrue(m.matches());
        assertTrue(Long.parseLong(m.group(1)) >= 500);
        assertEquals(m.group(2), m.group(1));
    }
}
