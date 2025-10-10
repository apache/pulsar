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
package org.apache.pulsar.common.semaphore;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.LimitType;
import org.apache.pulsar.common.semaphore.AsyncSemaphore.PermitAcquireAlreadyClosedException;
import org.apache.pulsar.common.semaphore.AsyncSemaphore.PermitAcquireCancelledException;
import org.apache.pulsar.common.semaphore.AsyncSemaphore.PermitAcquireQueueFullException;
import org.apache.pulsar.common.semaphore.AsyncSemaphore.PermitAcquireTimeoutException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class AsyncDualMemoryLimiterImplTest {

    private AsyncDualMemoryLimiterImpl limiter;
    private ScheduledExecutorService executor;

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (limiter != null) {
            limiter.close();
            limiter = null;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @Test
    public void testAcquireAndReleaseHeapMemory() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        CompletableFuture<AsyncDualMemoryLimiterPermit> future =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);
        AsyncDualMemoryLimiterPermit permit = future.get(1, TimeUnit.SECONDS);

        assertNotNull(permit);
        assertEquals(permit.getPermits(), 100);
        assertEquals(permit.getLimitType(), LimitType.HEAP_MEMORY);

        limiter.release(permit);
    }

    @Test
    public void testAcquireAndReleaseDirectMemory() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        CompletableFuture<AsyncDualMemoryLimiterPermit> future =
                limiter.acquire(100, LimitType.DIRECT_MEMORY, () -> false);
        AsyncDualMemoryLimiterPermit permit = future.get(1, TimeUnit.SECONDS);

        assertNotNull(permit);
        assertEquals(permit.getPermits(), 100);
        assertEquals(permit.getLimitType(), LimitType.DIRECT_MEMORY);

        limiter.release(permit);
    }

    @Test
    public void testAcquireMultiplePermitsHeap() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        List<AsyncDualMemoryLimiterPermit> permits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            CompletableFuture<AsyncDualMemoryLimiterPermit> future =
                    limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);
            permits.add(future.get(1, TimeUnit.SECONDS));
        }

        assertEquals(permits.size(), 5);
        for (AsyncDualMemoryLimiterPermit permit : permits) {
            assertEquals(permit.getPermits(), 100);
            assertEquals(permit.getLimitType(), LimitType.HEAP_MEMORY);
            limiter.release(permit);
        }
    }

    @Test
    public void testAcquireMultiplePermitsDirect() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        List<AsyncDualMemoryLimiterPermit> permits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            CompletableFuture<AsyncDualMemoryLimiterPermit> future =
                    limiter.acquire(100, LimitType.DIRECT_MEMORY, () -> false);
            permits.add(future.get(1, TimeUnit.SECONDS));
        }

        assertEquals(permits.size(), 5);
        for (AsyncDualMemoryLimiterPermit permit : permits) {
            assertEquals(permit.getPermits(), 100);
            assertEquals(permit.getLimitType(), LimitType.DIRECT_MEMORY);
            limiter.release(permit);
        }
    }

    @Test
    public void testIndependentHeapAndDirectLimits() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        // Acquire all heap memory
        AsyncDualMemoryLimiterPermit heapPermit =
                limiter.acquire(1000, LimitType.HEAP_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        // Should still be able to acquire direct memory
        AsyncDualMemoryLimiterPermit directPermit =
                limiter.acquire(500, LimitType.DIRECT_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        assertNotNull(heapPermit);
        assertNotNull(directPermit);
        assertEquals(heapPermit.getLimitType(), LimitType.HEAP_MEMORY);
        assertEquals(directPermit.getLimitType(), LimitType.DIRECT_MEMORY);

        limiter.release(heapPermit);
        limiter.release(directPermit);
    }

    @Test
    public void testQueueingWhenHeapMemoryNotAvailable() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        // Acquire all heap memory
        AsyncDualMemoryLimiterPermit permit1 =
                limiter.acquire(1000, LimitType.HEAP_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        // Try to acquire more - should be queued
        CompletableFuture<AsyncDualMemoryLimiterPermit> future =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);
        assertFalse(future.isDone());

        // Release the first permit
        limiter.release(permit1);

        // The queued request should now complete
        AsyncDualMemoryLimiterPermit permit2 = future.get(1, TimeUnit.SECONDS);
        assertNotNull(permit2);
        assertEquals(permit2.getPermits(), 100);

        limiter.release(permit2);
    }

    @Test
    public void testQueueingWhenDirectMemoryNotAvailable() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        // Acquire all direct memory
        AsyncDualMemoryLimiterPermit permit1 =
                limiter.acquire(1000, LimitType.DIRECT_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        // Try to acquire more - should be queued
        CompletableFuture<AsyncDualMemoryLimiterPermit> future =
                limiter.acquire(100, LimitType.DIRECT_MEMORY, () -> false);
        assertFalse(future.isDone());

        // Release the first permit
        limiter.release(permit1);

        // The queued request should now complete
        AsyncDualMemoryLimiterPermit permit2 = future.get(1, TimeUnit.SECONDS);
        assertNotNull(permit2);
        assertEquals(permit2.getPermits(), 100);

        limiter.release(permit2);
    }

    @Test
    public void testHeapQueueFullException() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 2, 5000, 1000, 10, 5000);

        // Acquire all heap memory
        limiter.acquire(1000, LimitType.HEAP_MEMORY, () -> false)
                .get(1, TimeUnit.SECONDS);

        // Fill the queue
        limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);
        limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);

        // This should fail with queue full exception
        CompletableFuture<AsyncDualMemoryLimiterPermit> future =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);

        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Expected exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireQueueFullException);
        }
    }

    @Test
    public void testDirectQueueFullException() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 2, 5000);

        // Acquire all direct memory
        limiter.acquire(1000, LimitType.DIRECT_MEMORY, () -> false)
                .get(1, TimeUnit.SECONDS);

        // Fill the queue
        limiter.acquire(100, LimitType.DIRECT_MEMORY, () -> false);
        limiter.acquire(100, LimitType.DIRECT_MEMORY, () -> false);

        // This should fail with queue full exception
        CompletableFuture<AsyncDualMemoryLimiterPermit> future =
                limiter.acquire(100, LimitType.DIRECT_MEMORY, () -> false);

        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Expected exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireQueueFullException);
        }
    }

    @Test
    public void testHeapTimeoutException() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 100, 1000, 10, 5000);

        // Acquire all heap memory
        limiter.acquire(1000, LimitType.HEAP_MEMORY, () -> false)
                .get(1, TimeUnit.SECONDS);

        // Try to acquire more - should timeout
        CompletableFuture<AsyncDualMemoryLimiterPermit> future =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);

        try {
            future.get(2, TimeUnit.SECONDS);
            fail("Expected timeout exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireTimeoutException);
        }
    }

    @Test
    public void testDirectTimeoutException() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 100);

        // Acquire all direct memory
        limiter.acquire(1000, LimitType.DIRECT_MEMORY, () -> false)
                .get(1, TimeUnit.SECONDS);

        // Try to acquire more - should timeout
        CompletableFuture<AsyncDualMemoryLimiterPermit> future =
                limiter.acquire(100, LimitType.DIRECT_MEMORY, () -> false);

        try {
            future.get(2, TimeUnit.SECONDS);
            fail("Expected timeout exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireTimeoutException);
        }
    }

    @Test
    public void testHeapCancellation() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 500, 1000, 10, 500);

        // Acquire all direct memory
        CompletableFuture<AsyncDualMemoryLimiterPermit> future1 =
                limiter.acquire(1000, LimitType.HEAP_MEMORY, () -> false);
        AsyncDualMemoryLimiterPermit permit = future1.get(1, TimeUnit.SECONDS);

        // Try to acquire more with cancellation
        AtomicBoolean cancelled = new AtomicBoolean(false);
        CompletableFuture<AsyncDualMemoryLimiterPermit> future2 =
                limiter.acquire(100, LimitType.HEAP_MEMORY, cancelled::get);

        assertFalse(future2.isDone());

        // Cancel the request
        cancelled.set(true);

        // Release first permit
        limiter.release(permit);

        try {
            future2.get(1, TimeUnit.SECONDS);
            fail("Expected cancellation exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireCancelledException);
        }
    }

    @Test
    public void testDirectCancellation() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 500, 1000, 10, 500);

        // Acquire all direct memory
        CompletableFuture<AsyncDualMemoryLimiterPermit> future1 =
                limiter.acquire(1000, LimitType.DIRECT_MEMORY, () -> false);
        AsyncDualMemoryLimiterPermit permit = future1.get(1, TimeUnit.SECONDS);

        // Try to acquire more with cancellation
        AtomicBoolean cancelled = new AtomicBoolean(false);
        CompletableFuture<AsyncDualMemoryLimiterPermit> future2 =
                limiter.acquire(100, LimitType.DIRECT_MEMORY, cancelled::get);

        assertFalse(future2.isDone());

        // Cancel the request
        cancelled.set(true);

        // Release first permit
        limiter.release(permit);

        try {
            future2.get(1, TimeUnit.SECONDS);
            fail("Expected cancellation exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireCancelledException);
        }
    }

    @Test
    public void testUpdateHeapPermitsIncrease() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        AsyncDualMemoryLimiterPermit permit =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        assertEquals(permit.getPermits(), 100);

        AsyncDualMemoryLimiterPermit updatedPermit =
                limiter.update(permit, 200, () -> false).get(1, TimeUnit.SECONDS);

        assertEquals(updatedPermit.getPermits(), 200);
        assertEquals(updatedPermit.getLimitType(), LimitType.HEAP_MEMORY);

        limiter.release(updatedPermit);
    }

    @Test
    public void testUpdateHeapPermitsDecrease() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        AsyncDualMemoryLimiterPermit permit =
                limiter.acquire(200, LimitType.HEAP_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        assertEquals(permit.getPermits(), 200);

        AsyncDualMemoryLimiterPermit updatedPermit =
                limiter.update(permit, 100, () -> false).get(1, TimeUnit.SECONDS);

        assertEquals(updatedPermit.getPermits(), 100);
        assertEquals(updatedPermit.getLimitType(), LimitType.HEAP_MEMORY);

        limiter.release(updatedPermit);
    }

    @Test
    public void testUpdateDirectPermitsIncrease() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        AsyncDualMemoryLimiterPermit permit =
                limiter.acquire(100, LimitType.DIRECT_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        assertEquals(permit.getPermits(), 100);

        AsyncDualMemoryLimiterPermit updatedPermit =
                limiter.update(permit, 200, () -> false).get(1, TimeUnit.SECONDS);

        assertEquals(updatedPermit.getPermits(), 200);
        assertEquals(updatedPermit.getLimitType(), LimitType.DIRECT_MEMORY);

        limiter.release(updatedPermit);
    }

    @Test
    public void testUpdateDirectPermitsDecrease() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        AsyncDualMemoryLimiterPermit permit =
                limiter.acquire(200, LimitType.DIRECT_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        assertEquals(permit.getPermits(), 200);

        AsyncDualMemoryLimiterPermit updatedPermit =
                limiter.update(permit, 100, () -> false).get(1, TimeUnit.SECONDS);

        assertEquals(updatedPermit.getPermits(), 100);
        assertEquals(updatedPermit.getLimitType(), LimitType.DIRECT_MEMORY);

        limiter.release(updatedPermit);
    }

    @Test
    public void testUpdateWithInvalidPermitType() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        // Create a mock permit with invalid type
        AsyncDualMemoryLimiterPermit invalidPermit = new AsyncDualMemoryLimiterPermit() {
            @Override
            public long getPermits() {
                return 100;
            }

            @Override
            public LimitType getLimitType() {
                return LimitType.HEAP_MEMORY;
            }
        };

        try {
            limiter.update(invalidPermit, 200, () -> false);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid permit type"));
        }
    }

    @Test
    public void testReleaseWithInvalidPermitType() {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        // Create a mock permit with invalid type
        AsyncDualMemoryLimiterPermit invalidPermit = new AsyncDualMemoryLimiterPermit() {
            @Override
            public long getPermits() {
                return 100;
            }

            @Override
            public LimitType getLimitType() {
                return LimitType.HEAP_MEMORY;
            }
        };

        try {
            limiter.release(invalidPermit);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid permit type"));
        }
    }

    @Test
    public void testConcurrentAcquireAndReleaseHeap() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 100, 5000, 1000, 100, 5000);

        int numThreads = 10;
        int numOperations = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < numOperations; j++) {
                        AsyncDualMemoryLimiterPermit permit =
                                limiter.acquire(10, LimitType.HEAP_MEMORY, () -> false)
                                        .get(5, TimeUnit.SECONDS);
                        Thread.sleep(1);
                        limiter.release(permit);
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
    }

    @Test
    public void testConcurrentAcquireAndReleaseDirect() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 100, 5000, 1000, 100, 5000);

        int numThreads = 10;
        int numOperations = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < numOperations; j++) {
                        AsyncDualMemoryLimiterPermit permit =
                                limiter.acquire(10, LimitType.DIRECT_MEMORY, () -> false)
                                        .get(5, TimeUnit.SECONDS);
                        Thread.sleep(1);
                        limiter.release(permit);
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
    }

    @Test
    public void testConcurrentMixedMemoryTypes() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 100, 5000, 1000, 100, 5000);

        int numThreads = 20; // 10 for heap, 10 for direct
        int numOperations = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final LimitType limitType = i < 10
                    ? LimitType.HEAP_MEMORY
                    : LimitType.DIRECT_MEMORY;

            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < numOperations; j++) {
                        AsyncDualMemoryLimiterPermit permit =
                                limiter.acquire(10, limitType, () -> false).get(5, TimeUnit.SECONDS);
                        Thread.sleep(1);
                        limiter.release(permit);
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
    }

    @Test
    public void testCloseWithOwnExecutor() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        AsyncDualMemoryLimiterPermit permit =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        limiter.release(permit);
        limiter.close();

        // After close, acquire should fail
        try {
            limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false)
                    .get(1, TimeUnit.SECONDS);
            fail("Expected exception after close");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireAlreadyClosedException);
        }
    }

    @Test
    public void testCloseWithProvidedExecutor() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000, executor);

        AsyncDualMemoryLimiterPermit permit =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        limiter.release(permit);

        assertFalse(executor.isShutdown());

        limiter.close();

        // Executor should not be shut down when provided externally
        assertFalse(executor.isShutdown());

        // After close, acquire should fail
        try {
            limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false)
                    .get(1, TimeUnit.SECONDS);
            fail("Expected exception after close");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireAlreadyClosedException);
        }
    }

    @Test
    public void testPermitGettersHeap() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        AsyncDualMemoryLimiterPermit permit =
                limiter.acquire(250, LimitType.HEAP_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        assertEquals(permit.getPermits(), 250);
        assertEquals(permit.getLimitType(), LimitType.HEAP_MEMORY);

        limiter.release(permit);
    }

    @Test
    public void testPermitGettersDirect() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        AsyncDualMemoryLimiterPermit permit =
                limiter.acquire(350, LimitType.DIRECT_MEMORY, () -> false)
                        .get(1, TimeUnit.SECONDS);

        assertEquals(permit.getPermits(), 350);
        assertEquals(permit.getLimitType(), LimitType.DIRECT_MEMORY);

        limiter.release(permit);
    }

    @Test
    public void testMultipleReleasesProcessQueueHeap() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        // Acquire permits that fill the available memory
        List<AsyncDualMemoryLimiterPermit> permits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            permits.add(limiter.acquire(200, LimitType.HEAP_MEMORY, () -> false)
                    .get(1, TimeUnit.SECONDS));
        }

        // Queue up some requests
        List<CompletableFuture<AsyncDualMemoryLimiterPermit>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            futures.add(limiter.acquire(150, LimitType.HEAP_MEMORY, () -> false));
        }

        // All should be pending
        for (CompletableFuture<AsyncDualMemoryLimiterPermit> future : futures) {
            assertFalse(future.isDone());
        }

        // Release permits one by one
        for (AsyncDualMemoryLimiterPermit permit : permits) {
            limiter.release(permit);
        }

        // All queued requests should complete
        for (CompletableFuture<AsyncDualMemoryLimiterPermit> future : futures) {
            AsyncDualMemoryLimiterPermit permit = future.get(2, TimeUnit.SECONDS);
            assertNotNull(permit);
            limiter.release(permit);
        }
    }

    @Test
    public void testMultipleReleasesProcessQueueDirect() throws Exception {
        limiter = new AsyncDualMemoryLimiterImpl(1000, 10, 5000, 1000, 10, 5000);

        // Acquire permits that fill the available memory
        List<AsyncDualMemoryLimiterPermit> permits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            permits.add(limiter.acquire(200, LimitType.DIRECT_MEMORY, () -> false)
                    .get(1, TimeUnit.SECONDS));
        }

        // Queue up some requests
        List<CompletableFuture<AsyncDualMemoryLimiterPermit>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            futures.add(limiter.acquire(150, LimitType.DIRECT_MEMORY, () -> false));
        }

        // All should be pending
        for (CompletableFuture<AsyncDualMemoryLimiterPermit> future : futures) {
            assertFalse(future.isDone());
        }

        // Release permits one by one
        for (AsyncDualMemoryLimiterPermit permit : permits) {
            limiter.release(permit);
        }

        // All queued requests should complete
        for (CompletableFuture<AsyncDualMemoryLimiterPermit> future : futures) {
            AsyncDualMemoryLimiterPermit permit = future.get(2, TimeUnit.SECONDS);
            assertNotNull(permit);
            limiter.release(permit);
        }
    }
}