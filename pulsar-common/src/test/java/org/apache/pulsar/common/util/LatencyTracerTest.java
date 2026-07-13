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
import static org.testng.Assert.fail;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
        var snapshot = tracer.getLatency();
        assertEquals(snapshot.description(), "total: 60 ms, A: 20 ms, B: 40 ms");
        assertEquals(snapshot.elapsedInMillis(), 60);

        tracer.trace("C");
        snapshot = tracer.getLatency();
        assertEquals(snapshot.description(), "total: 70 ms, A: 20 ms, B: 40 ms, C: 10 ms");
        assertEquals(snapshot.elapsedInMillis(), 70);
    }

    @Test
    public void testEmpty() {
        final var tracer = new LatencyTracer(new LinkedList<>(), testNanoTimeSupplier(0L, 20_000_000L));
        final var snapshot = tracer.getLatency();
        assertEquals(snapshot.description(), "total: 0 ms");
        assertEquals(snapshot.elapsedInMillis(), 0);
    }

    @Test
    public void testZeroMs() {
        final var tracer = new LatencyTracer(new LinkedList<>(), testNanoTimeSupplier(0L, 999_999L, 2_000_000L,
                2_100_000L));
        tracer.trace("A");
        tracer.trace("B");
        tracer.trace("C");
        final var snapshot = tracer.getLatency();
        assertEquals(snapshot.description(), "total: 2 ms, A: 999 us, B: 1 ms, C: 100 us");
        assertEquals(snapshot.elapsedInMillis(), 2);
    }

    @Test
    public void testTraceFuture() throws Exception {
        final var tracer = new LatencyTracer(new LinkedList<>(), System::nanoTime);
        final var future = CompletableFuture.completedFuture(100);
        assertSame(tracer.trace("A", future), future);
        final var latency = tracer.getLatency().description();
        assertTrue(Pattern.compile("total: \\d+ ms").matcher(latency).matches(), latency);

        final var future2 = new CompletableFuture<Integer>();
        CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS).execute(() -> future2.complete(1));
        final var tracedFuture = tracer.trace("B", future2);
        assertNotSame(tracedFuture, future2);
        assertEquals(tracedFuture.get(), 1);
        final var snapshot = tracer.getLatency();
        Matcher m = Pattern.compile("total: \\d+ ms, B: (\\d+) ms").matcher(snapshot.description());
        assertTrue(m.matches(), snapshot.description());
        assertEquals(Long.parseLong(m.group(1)), snapshot.elapsedInMillis(), snapshot.description());
        assertTrue(snapshot.elapsedInMillis() >= 500, snapshot.description());
    }

    @Test
    public void testTraceFailedFuture() throws Exception {
        final var tracer = new LatencyTracer(new LinkedList<>(), testNanoTimeSupplier(1L));

        final var future = new CompletableFuture<Void>();
        CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS).execute(() -> future.completeExceptionally(
                new RuntimeException("failure")));

        try {
            tracer.trace("A", future).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals(e.getCause().getMessage(), "failure");
        }
    }
}
