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
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Admin;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
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
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker")
public class UnloadManagerTest {

    private static final int CONCURRENT_REQUEST_COUNT = 32;

    @Test
    public void testEventPubFutureHasException() {
        UnloadCounter counter = new UnloadCounter();
        UnloadManager manager = new UnloadManager(counter, "mockBrokerId");
        var unloadDecision =
                new UnloadDecision(new Unload("broker-1", "bundle-1"), Success, Admin);
        CompletableFuture<Void> future =
                manager.waitAsync(FutureUtil.failedFuture(new Exception("test")),
                        "bundle-1", unloadDecision, 10, TimeUnit.SECONDS);

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertEquals(ex.getCause().getMessage(), "test");
        }
        assertEquals(counter.getBreakdownCounters().get(Failure).get(Unknown).get(), 1);
    }

    @Test
    public void testTimeout() {
        UnloadCounter counter = new UnloadCounter();
        UnloadManager manager = new UnloadManager(counter, "mockBrokerId");
        var unloadDecision =
                new UnloadDecision(new Unload("broker-1", "bundle-1"), Success, Admin);
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        "bundle-1", unloadDecision, 3, TimeUnit.SECONDS);
        CompletableFuture<Integer> inFlightRequestCountOnCompletion =
                captureInFlightRequestCountOnCompletion(future, manager);

        assertEquals(manager.getInFlightUnloadRequestCount(), 1);

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof TimeoutException);
        }

        assertEquals(inFlightRequestCountOnCompletion.join(), 0);
        assertEquals(manager.getInFlightUnloadRequestCount(), 0);
        assertEquals(counter.getBreakdownCounters().get(Failure).get(Unknown).get(), 1);

        CompletableFuture<Void> nextFuture = manager.waitAsync(CompletableFuture.completedFuture(null),
                "bundle-1", unloadDecision, 5, TimeUnit.SECONDS);
        assertFalse(nextFuture.isDone());
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);
        manager.close();
    }

    @Test
    public void testSuccess() throws ExecutionException, InterruptedException {
        UnloadCounter counter = new UnloadCounter();
        UnloadManager manager = new UnloadManager(counter, "mockBrokerId");
        String dstBroker = "broker-2";
        String srcBroker = "broker-1";
        String bundle = "bundle-1";
        var unloadDecision =
                new UnloadDecision(new Unload(srcBroker, bundle), Success, Admin);
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        bundle, unloadDecision, 5, TimeUnit.SECONDS);
        CompletableFuture<Integer> inFlightRequestCountOnCompletion =
                captureInFlightRequestCountOnCompletion(future, manager);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Assigning, null, srcBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Deleted, null, srcBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Splitting, null, srcBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Releasing, null, srcBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Free, null, srcBroker, true, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);

        // Success with Init state.
        manager.handleEvent(bundle, null, null);
        assertEquals(manager.getInFlightUnloadRequestCount(), 0);
        future.get();
        assertEquals(inFlightRequestCountOnCompletion.join(), 0);
        assertEquals(counter.getBreakdownCounters().get(Success).get(Admin).get(), 1);

        // Success with Owned state.
        future = manager.waitAsync(CompletableFuture.completedFuture(null),
                bundle, unloadDecision, 5, TimeUnit.SECONDS);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);
        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Owned, dstBroker, null, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);
        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Owned, dstBroker, srcBroker, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightUnloadRequestCount(), 0);
        future.get();
        assertEquals(counter.getBreakdownCounters().get(Success).get(Admin).get(), 2);

        // Success with Free state.
        future = manager.waitAsync(CompletableFuture.completedFuture(null),
                bundle, unloadDecision, 5, TimeUnit.SECONDS);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);
        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Free, dstBroker, srcBroker, true, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);
        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Free, dstBroker, srcBroker, false, VERSION_ID_INIT), null);
        assertEquals(manager.getInFlightUnloadRequestCount(), 0);
        future.get();
        assertEquals(counter.getBreakdownCounters().get(Success).get(Admin).get(), 3);


    }

    @Test
    public void testFailedStage() {
        UnloadCounter counter = new UnloadCounter();
        UnloadManager manager = new UnloadManager(counter, "mockBrokerId");
        var unloadDecision =
                new UnloadDecision(new Unload("broker-1", "bundle-1"), Success, Admin);
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        "bundle-1", unloadDecision, 5, TimeUnit.SECONDS);
        CompletableFuture<Integer> inFlightRequestCountOnCompletion =
                captureInFlightRequestCountOnCompletion(future, manager);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);

        manager.handleEvent("bundle-1",
                new ServiceUnitStateData(ServiceUnitState.Owned, null, "broker-1", VERSION_ID_INIT),
                new IllegalStateException("Failed stage."));

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof IllegalStateException);
            assertEquals(ex.getCause().getMessage(), "Failed stage.");
        }

        assertEquals(manager.getInFlightUnloadRequestCount(), 0);
        assertEquals(inFlightRequestCountOnCompletion.join(), 0);
        assertEquals(counter.getBreakdownCounters().get(Failure).get(Unknown).get(), 1);
    }

    @Test
    public void testClose() {
        UnloadCounter counter = new UnloadCounter();
        UnloadManager manager = new UnloadManager(counter, "mockBrokerId");
        var unloadDecision =
                new UnloadDecision(new Unload("broker-1", "bundle-1"), Success, Admin);
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        "bundle-1", unloadDecision, 5, TimeUnit.SECONDS);
        CompletableFuture<Integer> inFlightRequestCountOnCompletion =
                captureInFlightRequestCountOnCompletion(future, manager);
        assertEquals(manager.getInFlightUnloadRequestCount(), 1);
        manager.close();
        assertEquals(manager.getInFlightUnloadRequestCount(), 0);

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof IllegalStateException);
        }
        assertEquals(inFlightRequestCountOnCompletion.join(), 0);
        assertEquals(counter.getBreakdownCounters().get(Failure).get(Unknown).get(), 1);
    }

    @Test(timeOut = 10_000)
    public void testConcurrentWaitersShareOneInFlightRequest() {
        UnloadManager manager = new UnloadManager(new UnloadCounter(), "mockBrokerId");
        var decision = newUnloadDecision("bundle-1");
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<CompletableFuture<CompletableFuture<Void>>> registrations = new ArrayList<>();
            for (int i = 0; i < CONCURRENT_REQUEST_COUNT; i++) {
                registrations.add(CompletableFuture.supplyAsync(() -> {
                    await(start);
                    return manager.waitAsync(CompletableFuture.completedFuture(null),
                            "bundle-1", decision, 30, TimeUnit.SECONDS);
                }, executor));
            }

            start.countDown();
            List<CompletableFuture<Void>> waiters = registrations.stream()
                    .map(CompletableFuture::join)
                    .toList();
            assertEquals(manager.getInFlightUnloadRequestCount(), 1);
            List<CompletableFuture<Integer>> countsOnCompletion = waiters.stream()
                    .map(future -> captureInFlightRequestCountOnCompletion(future, manager))
                    .toList();

            manager.handleEvent("bundle-1", null, null);

            waiters.forEach(CompletableFuture::join);
            countsOnCompletion.forEach(count -> assertEquals(count.join().intValue(), 0));
            assertEquals(manager.getInFlightUnloadRequestCount(), 0);

            String timeoutBundle = "concurrent-timeout";
            var timeoutDecision = newUnloadDecision(timeoutBundle);
            List<CompletableFuture<Void>> timeoutWaiters = new ArrayList<>();
            for (int i = 0; i < CONCURRENT_REQUEST_COUNT; i++) {
                timeoutWaiters.add(manager.waitAsync(CompletableFuture.completedFuture(null),
                        timeoutBundle, timeoutDecision, 1, TimeUnit.SECONDS));
            }
            assertEquals(manager.getInFlightUnloadRequestCount(), 1);
            List<CompletableFuture<Integer>> timeoutCountsOnCompletion = timeoutWaiters.stream()
                    .map(future -> captureInFlightRequestCountOnCompletion(future, manager))
                    .toList();
            List<CompletableFuture<Throwable>> timeoutFailures = timeoutWaiters.stream()
                    .map(future -> future.handle((__, ex) -> ex))
                    .toList();

            CompletableFuture.allOf(timeoutWaiters.stream()
                    .map(UnloadManagerTest::ignoreFailure)
                    .toArray(CompletableFuture[]::new)).join();
            timeoutCountsOnCompletion.forEach(count -> assertEquals(count.join().intValue(), 0));
            timeoutFailures.forEach(failure ->
                    assertTrue(FutureUtil.unwrapCompletionException(failure.join()) instanceof TimeoutException));
            assertEquals(manager.getInFlightUnloadRequestCount(), 0);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(timeOut = 10_000)
    public void testCloseRacingWithConcurrentCompletions() {
        UnloadManager manager = new UnloadManager(new UnloadCounter(), "mockBrokerId");
        List<String> bundles = new ArrayList<>();
        List<CompletableFuture<Void>> waiters = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_REQUEST_COUNT; i++) {
            String bundle = "close-race-" + i;
            bundles.add(bundle);
            waiters.add(manager.waitAsync(CompletableFuture.completedFuture(null),
                    bundle, newUnloadDecision(bundle), 30, TimeUnit.SECONDS));
        }
        assertEquals(manager.getInFlightUnloadRequestCount(), CONCURRENT_REQUEST_COUNT);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<CompletableFuture<Void>> terminalSignals = new ArrayList<>();
            terminalSignals.add(CompletableFuture.runAsync(() -> {
                await(start);
                manager.close();
            }, executor));
            for (String bundle : bundles) {
                terminalSignals.add(CompletableFuture.runAsync(() -> {
                    await(start);
                    manager.handleEvent(bundle, null, null);
                }, executor));
            }

            start.countDown();
            CompletableFuture.allOf(terminalSignals.toArray(CompletableFuture[]::new)).join();
            CompletableFuture.allOf(waiters.stream()
                    .map(UnloadManagerTest::ignoreFailure)
                    .toArray(CompletableFuture[]::new)).join();
            assertEquals(manager.getInFlightUnloadRequestCount(), 0);
        } finally {
            executor.shutdownNow();
        }
    }

    private static CompletableFuture<Integer> captureInFlightRequestCountOnCompletion(
            CompletableFuture<Void> future, UnloadManager manager) {
        return future.handle((__, ex) -> manager.getInFlightUnloadRequestCount());
    }

    private static UnloadDecision newUnloadDecision(String bundle) {
        return new UnloadDecision(new Unload("broker-1", bundle), Success, Admin);
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
