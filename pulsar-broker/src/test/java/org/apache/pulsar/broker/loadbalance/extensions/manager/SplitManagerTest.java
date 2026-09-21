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
package org.apache.pulsar.broker.loadbalance.extensions.manager;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.VERSION_ID_INIT;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Sessions;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Unknown;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.CustomLog;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker")
public class SplitManagerTest {

    private static final int CONCURRENT_REQUEST_COUNT = 32;

    String bundle = "bundle-1";

    String dstBroker = "broker-1";

    @Test
    public void testEventPubFutureHasException() {
        var counter = new SplitCounter();
        SplitManager manager = new SplitManager(counter);
        var decision = new SplitDecision();
        CompletableFuture<Void> future =
                manager.waitAsync(FutureUtil.failedFuture(new Exception("test")),
                        bundle, decision, 10, TimeUnit.SECONDS);

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertEquals(ex.getCause().getMessage(), "test");
        }
        var counterExpected = new SplitCounter();
        counterExpected.update(SplitDecision.Label.Failure, Unknown);
        assertEquals(counter.toMetrics(null).toString(),
                counterExpected.toMetrics(null).toString());
    }

    @Test
    public void testTimeout() {
        var counter = new SplitCounter();
        SplitManager manager = new SplitManager(counter);
        var decision = new SplitDecision();
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        bundle, decision, 3, TimeUnit.SECONDS);
        CompletableFuture<Integer> inFlightRequestCountOnCompletion =
                captureInFlightRequestCountOnCompletion(future, manager);

        assertEquals(manager.getInFlightSplitRequestCount(), 1);

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof TimeoutException);
        }

        assertEquals(inFlightRequestCountOnCompletion.join(), 0);
        assertEquals(manager.getInFlightSplitRequestCount(), 0);
        var counterExpected = new SplitCounter();
        counterExpected.update(SplitDecision.Label.Failure, Unknown);
        assertEquals(counter.toMetrics(null).toString(),
                counterExpected.toMetrics(null).toString());

        CompletableFuture<Void> nextFuture = manager.waitAsync(CompletableFuture.completedFuture(null),
                bundle, decision, 5, TimeUnit.SECONDS);
        assertFalse(nextFuture.isDone());
        assertEquals(manager.getInFlightSplitRequestCount(), 1);
        manager.close();
    }

    @Test
    public void testSuccess() throws ExecutionException, InterruptedException {
        var counter = new SplitCounter();
        SplitManager manager = new SplitManager(counter);
        var counterExpected = new SplitCounter();
        var decision = new SplitDecision();
        decision.succeed(Sessions);
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        bundle, decision, 5, TimeUnit.SECONDS);
        CompletableFuture<Integer> inFlightRequestCountOnCompletion =
                captureInFlightRequestCountOnCompletion(future, manager);
        assertEquals(manager.getInFlightSplitRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Assigning, dstBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightSplitRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Splitting, dstBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightSplitRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Releasing, dstBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightSplitRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Free, dstBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightSplitRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Deleted, dstBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightSplitRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Owned, dstBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightSplitRequestCount(), 1);

        // Success with Init state.
        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Init, dstBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightSplitRequestCount(), 0);
        counterExpected.update(SplitDecision.Label.Success, Sessions);
        assertEquals(counter.toMetrics(null).toString(),
                counterExpected.toMetrics(null).toString());

        future.get();
        assertEquals(inFlightRequestCountOnCompletion.join(), 0);
    }

    @Test
    public void testFailedStage() {
        var counter = new SplitCounter();
        SplitManager manager = new SplitManager(counter);
        var decision = new SplitDecision();
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        bundle, decision, 5, TimeUnit.SECONDS);
        CompletableFuture<Integer> inFlightRequestCountOnCompletion =
                captureInFlightRequestCountOnCompletion(future, manager);
        assertEquals(manager.getInFlightSplitRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Owned, dstBroker, VERSION_ID_INIT),
                new IllegalStateException("Failed stage."));

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof IllegalStateException);
            assertEquals(ex.getCause().getMessage(), "Failed stage.");
        }

        assertEquals(manager.getInFlightSplitRequestCount(), 0);
        assertEquals(inFlightRequestCountOnCompletion.join(), 0);
        var counterExpected = new SplitCounter();
        counterExpected.update(SplitDecision.Label.Failure, Unknown);
        assertEquals(counter.toMetrics(null).toString(),
                counterExpected.toMetrics(null).toString());
    }

    @Test
    public void testClose() {
        SplitManager manager = new SplitManager(new SplitCounter());
        var decision = new SplitDecision();
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        bundle, decision, 5, TimeUnit.SECONDS);
        CompletableFuture<Integer> inFlightRequestCountOnCompletion =
                captureInFlightRequestCountOnCompletion(future, manager);
        assertEquals(manager.getInFlightSplitRequestCount(), 1);
        manager.close();
        assertEquals(manager.getInFlightSplitRequestCount(), 0);

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof IllegalStateException);
        }
        assertEquals(inFlightRequestCountOnCompletion.join(), 0);
    }

    @Test(timeOut = 10_000)
    public void testConcurrentWaitersShareOneInFlightRequest() {
        SplitManager manager = new SplitManager(new SplitCounter());
        var decision = new SplitDecision();
        decision.succeed(Sessions);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<CompletableFuture<CompletableFuture<Void>>> registrations = new ArrayList<>();
            for (int i = 0; i < CONCURRENT_REQUEST_COUNT; i++) {
                registrations.add(CompletableFuture.supplyAsync(() -> {
                    await(start);
                    return manager.waitAsync(CompletableFuture.completedFuture(null),
                            bundle, decision, 30, TimeUnit.SECONDS);
                }, executor));
            }

            start.countDown();
            List<CompletableFuture<Void>> waiters = registrations.stream()
                    .map(CompletableFuture::join)
                    .toList();
            assertEquals(manager.getInFlightSplitRequestCount(), 1);
            List<CompletableFuture<Integer>> countsOnCompletion = waiters.stream()
                    .map(future -> captureInFlightRequestCountOnCompletion(future, manager))
                    .toList();

            manager.handleEvent(bundle,
                    new ServiceUnitStateData(ServiceUnitState.Init, dstBroker, VERSION_ID_INIT), null);

            waiters.forEach(CompletableFuture::join);
            countsOnCompletion.forEach(count -> assertEquals(count.join().intValue(), 0));
            assertEquals(manager.getInFlightSplitRequestCount(), 0);

            String timeoutBundle = "concurrent-timeout";
            List<CompletableFuture<Void>> timeoutWaiters = new ArrayList<>();
            for (int i = 0; i < CONCURRENT_REQUEST_COUNT; i++) {
                timeoutWaiters.add(manager.waitAsync(CompletableFuture.completedFuture(null),
                        timeoutBundle, decision, 1, TimeUnit.SECONDS));
            }
            assertEquals(manager.getInFlightSplitRequestCount(), 1);
            List<CompletableFuture<Integer>> timeoutCountsOnCompletion = timeoutWaiters.stream()
                    .map(future -> captureInFlightRequestCountOnCompletion(future, manager))
                    .toList();
            List<CompletableFuture<Throwable>> timeoutFailures = timeoutWaiters.stream()
                    .map(future -> future.handle((__, ex) -> ex))
                    .toList();

            CompletableFuture.allOf(timeoutWaiters.stream()
                    .map(SplitManagerTest::ignoreFailure)
                    .toArray(CompletableFuture[]::new)).join();
            timeoutCountsOnCompletion.forEach(count -> assertEquals(count.join().intValue(), 0));
            timeoutFailures.forEach(failure ->
                    assertTrue(FutureUtil.unwrapCompletionException(failure.join()) instanceof TimeoutException));
            assertEquals(manager.getInFlightSplitRequestCount(), 0);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(timeOut = 10_000)
    public void testCloseRacingWithConcurrentCompletions() {
        SplitManager manager = new SplitManager(new SplitCounter());
        var decision = new SplitDecision();
        decision.succeed(Sessions);
        List<String> bundles = new ArrayList<>();
        List<CompletableFuture<Void>> waiters = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_REQUEST_COUNT; i++) {
            String requestBundle = "close-race-" + i;
            bundles.add(requestBundle);
            waiters.add(manager.waitAsync(CompletableFuture.completedFuture(null),
                    requestBundle, decision, 30, TimeUnit.SECONDS));
        }
        assertEquals(manager.getInFlightSplitRequestCount(), CONCURRENT_REQUEST_COUNT);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<CompletableFuture<Void>> terminalSignals = new ArrayList<>();
            terminalSignals.add(CompletableFuture.runAsync(() -> {
                await(start);
                manager.close();
            }, executor));
            for (String requestBundle : bundles) {
                terminalSignals.add(CompletableFuture.runAsync(() -> {
                    await(start);
                    manager.handleEvent(requestBundle,
                            new ServiceUnitStateData(ServiceUnitState.Init, dstBroker, VERSION_ID_INIT), null);
                }, executor));
            }

            start.countDown();
            CompletableFuture.allOf(terminalSignals.toArray(CompletableFuture[]::new)).join();
            CompletableFuture.allOf(waiters.stream()
                    .map(SplitManagerTest::ignoreFailure)
                    .toArray(CompletableFuture[]::new)).join();
            assertEquals(manager.getInFlightSplitRequestCount(), 0);
        } finally {
            executor.shutdownNow();
        }
    }

    private static CompletableFuture<Integer> captureInFlightRequestCountOnCompletion(
            CompletableFuture<Void> future, SplitManager manager) {
        return future.handle((__, ex) -> manager.getInFlightSplitRequestCount());
    }

    private static CompletableFuture<Void> ignoreFailure(CompletableFuture<Void> future) {
        return future.handle((__, ex) -> null);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

}
