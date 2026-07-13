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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testng.annotations.Test;

@Test(groups = "utils")
public class LatencyTracerTest {

    private static LatencyTracer.NanoTimeSupplier testNanoTimeSupplier(long... nanoTimes) {
        return new LatencyTracer.NanoTimeSupplier() {
            final LinkedList<Long> nanoTimesQueue = new LinkedList<>();

            {
                for (long nanoTime : nanoTimes) {
                    nanoTimesQueue.add(nanoTime);
                }
            }

            @Override
            public long getNanos() {
                final var nanos = nanoTimesQueue.poll();
                assertNotNull(nanos);
                return nanos;
            }
        };
    }

    @Test
    public void testMulti() {
        final var tracer = new LatencyTracer(new LinkedList<>(), testNanoTimeSupplier(10_000_000L, 30_000_000L,
                70_000_000L, 80_000_000L));
        tracer.trace("A");
        tracer.trace("B");
        assertEquals(tracer.latencyString(), "total: 70 ms, A: 20 ms, B: 40 ms, done: 10 ms");
        assertEquals(tracer.latencyInMillis(), 70);
    }

    @Test
    public void testEmpty() {
        final var tracer = new LatencyTracer(new LinkedList<>(), testNanoTimeSupplier(0L, 20_000_000L));
        assertEquals(tracer.latencyString(), "total: 20 ms");
        assertEquals(tracer.latencyInMillis(), 20);
    }

    @Test
    public void testZeroMs() {
        final var tracer = new LatencyTracer(new LinkedList<>(), testNanoTimeSupplier(0L, 999_999L, 2_000_000L,
                2_100_000L));
        tracer.trace("A");
        tracer.trace("B");
        assertEquals(tracer.latencyString(), "total: 2 ms, A: 999 us, B: 1 ms, done: 100 us");
        assertEquals(tracer.latencyInMillis(), 2);
    }

    @Test
    public void testTraceFuture() throws Exception {
        final var tracer = new LatencyTracer(new LinkedList<>(), System::nanoTime);
        final var future = CompletableFuture.completedFuture(100);
        assertSame(tracer.trace("A", future), future);
        final var latency = tracer.latencyString();
        assertTrue(Pattern.compile("total: \\d+ ms").matcher(latency).matches(), latency);

        final var tracer2 = new LatencyTracer(new LinkedList<>(), System::nanoTime);
        final var future2 = new CompletableFuture<Integer>();
        CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS).execute(() -> future2.complete(1));
        final var tracedFuture = tracer2.trace("A", future2);
        assertNotSame(tracedFuture, future2);
        assertEquals(tracedFuture.get(), 1);
        final var latency2 = tracer2.latencyString();
        Matcher m = Pattern.compile("total: \\d+ ms, A: (\\d+) ms, done: \\d+ [mu]s").matcher(latency2);
        assertTrue(m.matches(), latency2);
        assertTrue(Long.parseLong(m.group(1)) >= 500, latency2);
    }
}
