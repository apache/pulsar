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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testng.annotations.Test;

@Test(groups = "utils")
public class LatencyTracerTest {

    private static LatencyTracer.NanoTimeSupplier testNanoTimeSupplier(long... nanoTimes) {
        return new LatencyTracer.NanoTimeSupplier() {
            final Deque<Long> nanoTimesQueue = new ArrayDeque<>();

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

    private static LatencyTracer testTracer(long... nanoTimes) {
        return new LatencyTracer(testNanoTimeSupplier(nanoTimes));
    }

    private static void assertLatencyDescription(String actual, String details, String state) {
        String pattern = "state: ".concat(Pattern.quote(state)).concat(", start timestamp: \\d+, end timestamp: \\d+, ")
                .concat(Pattern.quote(details));
        assertTrue(Pattern.compile(pattern).matcher(actual).matches(), actual);
    }

    @Test
    public void testMulti() {
        final var tracer = testTracer(10_000_000L, 10_000_000L, 30_000_000L, 30_000_000L, 70_000_000L,
                70_000_000L, 80_000_000L);
        tracer.finishTrace(tracer.startTrace("A"), null);
        tracer.finishTrace(tracer.startTrace("B"), null);
        var snapshot = tracer.getSnapshot();
        assertLatencyDescription(snapshot.description(), "total: 60 ms, A: 20 ms, B: 40 ms", "succeed");
        assertEquals(snapshot.elapsedInMillis(), 60);
        assertTrue(snapshot.completed());
        assertTrue(snapshot.success());

        tracer.finishTrace(tracer.startTrace("C"), null);
        snapshot = tracer.getSnapshot();
        assertLatencyDescription(snapshot.description(), "total: 70 ms, A: 20 ms, B: 40 ms, C: 10 ms", "succeed");
        assertEquals(snapshot.elapsedInMillis(), 70);
    }

    @Test
    public void testEmpty() {
        final var tracer = testTracer(0L, 20_000_000L);
        final var snapshot = tracer.getSnapshot();
        assertLatencyDescription(snapshot.description(), "total: 0 ms", "succeed");
        assertEquals(snapshot.elapsedInMillis(), 0);
    }

    @Test
    public void testPendingTracePoint() {
        final var tracer = testTracer(0L, 20_000_000L, 30_000_000L);
        tracer.startTrace("A");

        final var snapshot = tracer.getSnapshot();
        assertTrue(Pattern.compile("state: still in progress, start timestamp: \\d+, end timestamp: \\d+, total: 30 ms, pending steps: A")
                .matcher(snapshot.description()).matches());
        assertFalse(snapshot.completed());
        assertFalse(snapshot.success());
    }

    @Test
    public void testClosedTracerIgnoresTraceRequests() {
        final var tracer = testTracer(0L, 1L);
        tracer.startTrace("A");
        tracer.close();

        assertTrue(tracer.isClosed());
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        assertSame(tracer.trace("B", future), future);
        assertEquals(tracer.startTrace("C"), null);
        assertEquals(tracer.getTracePoints().size(), 1);
    }

    @Test
    public void testTracePointsMeasureOwnDurationWhenCompletedOutOfOrder() {
        final var tracer = testTracer(0L, 1_000_000L, 2_000_000L, 12_000_000L, 22_000_000L);
        final var tracePointA = tracer.startTrace("A");
        final var tracePointB = tracer.startTrace("B");
        tracer.finishTrace(tracePointB, null);
        tracer.finishTrace(tracePointA, null);

        final var snapshot = tracer.getSnapshot();
        assertLatencyDescription(snapshot.description(), "total: 22 ms, A: 21 ms, B: 10 ms", "succeed");
    }

    @Test
    public void testCloseIgnoresNewTraceRequestsAndCompletesExistingTracePoint() {
        final var tracer = testTracer(0L, 1_000_000L, 2_000_000L);
        final var tracePoint = tracer.startTrace("A");

        tracer.close();
        tracer.finishTrace(tracePoint, new RuntimeException("failure"));

        assertEquals(tracer.getTracePoints().size(), 1);
        assertLatencyDescription(tracer.getSnapshot().description(), "total: 2 ms, A: 1 ms, failure reason: A: failure",
                "failure");
    }

    @Test
    public void testRunAfterPendingActionsComplete() {
        final var tracer = testTracer(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        final var invocations = new AtomicInteger();
        final var tracePointA = tracer.startTrace("A");
        final var tracePointB = tracer.startTrace("B");
        tracer.runAfterPendingActionsComplete(invocations::incrementAndGet);

        tracer.finishTrace(tracePointA, null);
        assertEquals(invocations.get(), 0);

        final var tracePointC = tracer.startTrace("C");
        tracer.finishTrace(tracePointB, null);
        assertEquals(invocations.get(), 0);

        tracer.finishTrace(tracePointC, null);
        assertEquals(invocations.get(), 1);
    }

    @Test
    public void testRunAfterPendingActionsCompleteContinuesAfterRunnableFailure() {
        final var tracer = testTracer(0L, 1L, 2L);
        final var invocations = new AtomicInteger();
        final var tracePointA = tracer.startTrace("A");
        tracer.runAfterPendingActionsComplete(() -> {
            throw new RuntimeException("failure");
        });
        tracer.runAfterPendingActionsComplete(invocations::incrementAndGet);

        tracer.finishTrace(tracePointA, null);
        assertEquals(invocations.get(), 1);
    }

    @Test
    public void testRunAfterPendingActionsCompleteDelaysRemainingRunnablesForNewAction() {
        final var tracer = testTracer(0L, 1L, 2L, 3L, 4L);
        final var firstInvocations = new AtomicInteger();
        final var secondInvocations = new AtomicInteger();
        final var tracePointA = tracer.startTrace("A");
        final var tracePointB = new AtomicReference<LatencyTracer.TracePoint>();
        tracer.runAfterPendingActionsComplete(() -> {
            firstInvocations.incrementAndGet();
            tracePointB.set(tracer.startTrace("B"));
        });
        tracer.runAfterPendingActionsComplete(secondInvocations::incrementAndGet);

        tracer.finishTrace(tracePointA, null);
        assertEquals(firstInvocations.get(), 1);
        assertEquals(secondInvocations.get(), 0);

        tracer.finishTrace(tracePointB.get(), null);
        assertEquals(secondInvocations.get(), 1);
    }

    @Test
    public void testZeroMs() {
        final var tracer = testTracer(0L, 0L, 999_999L, 999_999L, 2_000_000L, 2_000_000L, 2_100_000L);
        tracer.finishTrace(tracer.startTrace("A"), null);
        tracer.finishTrace(tracer.startTrace("B"), null);
        tracer.finishTrace(tracer.startTrace("C"), null);
        final var snapshot = tracer.getSnapshot();
        assertLatencyDescription(snapshot.description(), "total: 2 ms, A: 999 us, B: 1 ms, C: 100 us", "succeed");
        assertEquals(snapshot.elapsedInMillis(), 2);
    }

    @Test
    public void testTraceFuture() throws Exception {
        final var tracer = new LatencyTracer(System::nanoTime);
        final var future = CompletableFuture.completedFuture(100);
        assertNotSame(tracer.trace("A", future), future);
        final var latency = tracer.getSnapshot().description();
        assertTrue(Pattern.compile("state: succeed, start timestamp: \\d+, end timestamp: \\d+, total: \\d+ ms, A: \\d+ (ms|us)")
                .matcher(latency).matches(), latency);

        final var future2 = new CompletableFuture<Integer>();
        CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS).execute(() -> future2.complete(1));
        final var tracedFuture = tracer.trace("B", future2);
        assertNotSame(tracedFuture, future2);
        assertEquals(tracedFuture.get(), 1);
        final var snapshot = tracer.getSnapshot();
        Matcher m = Pattern.compile("state: succeed, start timestamp: \\d+, end timestamp: \\d+, total: \\d+ ms, A: \\d+ (ms|us), B: (\\d+) ms")
                .matcher(snapshot.description());
        assertTrue(m.matches(), snapshot.description());
        assertEquals(Long.parseLong(m.group(2)), snapshot.elapsedInMillis(), snapshot.description());
        assertTrue(snapshot.elapsedInMillis() >= 500, snapshot.description());
    }

    @Test
    public void testTraceFailedFuture() throws Exception {
        final var tracer = testTracer(1L, 2L, 3L);

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
