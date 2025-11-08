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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.common.semaphore.AsyncSemaphore.AsyncSemaphorePermit;
import org.apache.pulsar.common.semaphore.AsyncSemaphore.PermitAcquireAlreadyClosedException;
import org.apache.pulsar.common.semaphore.AsyncSemaphore.PermitAcquireCancelledException;
import org.apache.pulsar.common.semaphore.AsyncSemaphore.PermitAcquireQueueFullException;
import org.apache.pulsar.common.semaphore.AsyncSemaphore.PermitAcquireTimeoutException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class AsyncSemaphoreImplTest {

    private AsyncSemaphoreImpl semaphore;

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (semaphore != null) {
            semaphore.close();
            semaphore = null;
        }
    }

    @Test
    public void testAcquireAndReleaseSinglePermit() throws Exception {
        semaphore = new AsyncSemaphoreImpl(1, 10, 5000);

        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(1, () -> false);
        AsyncSemaphorePermit permit = future.get(1, TimeUnit.SECONDS);

        assertNotNull(permit);
        assertEquals(permit.getPermits(), 1);

        semaphore.release(permit);
    }

    @Test
    public void testAcquireMultiplePermits() throws Exception {
        semaphore = new AsyncSemaphoreImpl(10, 10, 5000);

        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(5, () -> false);
        AsyncSemaphorePermit permit = future.get(1, TimeUnit.SECONDS);

        assertNotNull(permit);
        assertEquals(permit.getPermits(), 5);

        semaphore.release(permit);
    }

    @Test
    public void testQueueingWhenNoPermitsAvailable() throws Exception {
        semaphore = new AsyncSemaphoreImpl(5, 10, 5000);

        // Acquire all permits
        CompletableFuture<AsyncSemaphorePermit> future1 = semaphore.acquire(5, () -> false);
        AsyncSemaphorePermit permit1 = future1.get(1, TimeUnit.SECONDS);

        // Try to acquire more - should be queued
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.acquire(3, () -> false);
        assertFalse(future2.isDone());

        // Release permits
        semaphore.release(permit1);

        // Now the queued request should complete
        AsyncSemaphorePermit permit2 = future2.get(1, TimeUnit.SECONDS);
        assertNotNull(permit2);
        assertEquals(permit2.getPermits(), 3);

        semaphore.release(permit2);
    }

    @Test
    public void testQueueFullException() throws Exception {
        semaphore = new AsyncSemaphoreImpl(1, 2, 5000);

        // Acquire the only permit
        CompletableFuture<AsyncSemaphorePermit> future1 = semaphore.acquire(1, () -> false);
        future1.get(1, TimeUnit.SECONDS);

        // Fill the queue
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.acquire(1, () -> false);
        CompletableFuture<AsyncSemaphorePermit> future3 = semaphore.acquire(1, () -> false);

        // This should fail with queue full
        CompletableFuture<AsyncSemaphorePermit> future4 = semaphore.acquire(1, () -> false);

        assertTrue(future4.isCompletedExceptionally());
        try {
            future4.get();
            fail("Should have thrown exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireQueueFullException);
        }
    }

    @Test
    public void testTimeoutException() throws Exception {
        semaphore = new AsyncSemaphoreImpl(1, 10, 100); // 100ms timeout

        // Acquire the only permit
        CompletableFuture<AsyncSemaphorePermit> future1 = semaphore.acquire(1, () -> false);
        AsyncSemaphorePermit permit1 = future1.get(1, TimeUnit.SECONDS);

        // Try to acquire another permit - should timeout
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.acquire(1, () -> false);

        Thread.sleep(200); // Wait for timeout

        assertTrue(future2.isCompletedExceptionally());
        try {
            future2.get();
            fail("Should have thrown exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireTimeoutException);
        }

        semaphore.release(permit1);
    }

    @Test
    public void testCancellation() throws Exception {
        semaphore = new AsyncSemaphoreImpl(1, 10, 5000);

        // Acquire the only permit
        CompletableFuture<AsyncSemaphorePermit> future1 = semaphore.acquire(1, () -> false);
        AsyncSemaphorePermit permit1 = future1.get(1, TimeUnit.SECONDS);

        // Try to acquire with cancellation flag
        AtomicBoolean cancelled = new AtomicBoolean(false);
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.acquire(1, cancelled::get);

        assertFalse(future2.isDone());

        // Cancel the request
        cancelled.set(true);

        // Release the first permit
        semaphore.release(permit1);

        // Give time for the queue to process
        Thread.sleep(100);

        assertTrue(future2.isCompletedExceptionally());
        try {
            future2.get();
            fail("Should have thrown exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireCancelledException);
        }
    }

    @Test
    public void testInvalidPermits() {
        semaphore = new AsyncSemaphoreImpl(10, 10, 5000);

        assertThatThrownBy(() ->
                semaphore.acquire(-1, () -> false)
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid negative permits value: -1");
    }

    @Test
    public void testInvalidPermitsExceedingMaxPermits() {
        semaphore = new AsyncSemaphoreImpl(10, 10, 5000);

        assertThatThrownBy(() ->
            semaphore.acquire(11, () -> false)
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Requested permits=11 is larger than maxPermits=10");
    }

    @Test
    public void testClose() throws Exception {
        semaphore = new AsyncSemaphoreImpl(5, 10, 5000);

        // Acquire some permits
        CompletableFuture<AsyncSemaphorePermit> future1 = semaphore.acquire(3, () -> false);
        future1.get(1, TimeUnit.SECONDS);

        // Queue another request
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.acquire(5, () -> false);
        assertFalse(future2.isDone());

        // Close the semaphore
        semaphore.close();

        // Queued request should fail
        assertTrue(future2.isCompletedExceptionally());
        try {
            future2.get();
            fail("Should have thrown exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireAlreadyClosedException);
        }

        // New acquisitions should fail
        CompletableFuture<AsyncSemaphorePermit> future3 = semaphore.acquire(1, () -> false);
        assertTrue(future3.isCompletedExceptionally());
        try {
            future3.get();
            fail("Should have thrown exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PermitAcquireAlreadyClosedException);
        }
    }

    @Test
    public void testUpdatePermitsIncrease() throws Exception {
        semaphore = new AsyncSemaphoreImpl(10, 10, 5000);

        // Acquire initial permits
        CompletableFuture<AsyncSemaphorePermit> future1 = semaphore.acquire(5, () -> false);
        AsyncSemaphorePermit permit1 = future1.get(1, TimeUnit.SECONDS);
        assertEquals(permit1.getPermits(), 5);

        // Update to more permits
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.update(permit1, 8, () -> false);
        AsyncSemaphorePermit permit2 = future2.get(1, TimeUnit.SECONDS);

        assertNotNull(permit2);
        assertEquals(permit2.getPermits(), 8);

        CompletableFuture<AsyncSemaphorePermit> future3 = semaphore.acquire(2, () -> false);
        AsyncSemaphorePermit permit3 = future3.get(1, TimeUnit.SECONDS);
        assertNotNull(permit3);
        assertEquals(permit3.getPermits(), 2);

        CompletableFuture<AsyncSemaphorePermit> future4 = semaphore.acquire(1, () -> false);
        Thread.sleep(1000);
        // no more permits available, this won't complete
        assertThat(future4).isNotDone();

        // release permits
        semaphore.release(permit2);

        // now future4 should complete
        assertThat(future4).succeedsWithin(1, TimeUnit.SECONDS)
                .satisfies(p -> assertEquals(p.getPermits(), 1));
    }

    @Test
    public void testUpdatePermitsDecrease() throws Exception {
        semaphore = new AsyncSemaphoreImpl(10, 10, 5000);

        // Acquire initial permits
        CompletableFuture<AsyncSemaphorePermit> future1 = semaphore.acquire(8, () -> false);
        AsyncSemaphorePermit permit1 = future1.get(1, TimeUnit.SECONDS);
        assertEquals(permit1.getPermits(), 8);

        // Update to fewer permits (should be immediate)
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.update(permit1, 5, () -> false);
        AsyncSemaphorePermit permit2 = future2.get(100, TimeUnit.MILLISECONDS);

        assertNotNull(permit2);
        assertEquals(permit2.getPermits(), 5);
        assertTrue(future2.isDone());

        semaphore.release(permit2);
    }

    @Test
    public void testUpdateWithInvalidPermits() throws Exception {
        semaphore = new AsyncSemaphoreImpl(10, 10, 5000);

        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(5, () -> false);
        AsyncSemaphorePermit permit = future.get(1, TimeUnit.SECONDS);

        assertThatThrownBy(() ->
                semaphore.update(permit, -1, () -> false)
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid negative permits value: -1");

        semaphore.release(permit);
    }

    @Test
    public void testUpdateWithInvalidPermitsExceedingMaxPermits() throws Exception {
        semaphore = new AsyncSemaphoreImpl(10, 10, 5000);

        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(5, () -> false);
        AsyncSemaphorePermit permit = future.get(1, TimeUnit.SECONDS);

        assertThatThrownBy(() ->
                semaphore.update(permit, 11, () -> false)
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Requested permits=11 is larger than maxPermits=10");

        semaphore.release(permit);
    }

    @Test
    public void testUpdateWithZeroPermitsShouldReleasePermits() throws Exception {
        semaphore = new AsyncSemaphoreImpl(10, 10, 5000);

        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(5, () -> false);
        AsyncSemaphorePermit permit = future.get(1, TimeUnit.SECONDS);

        AsyncSemaphorePermit updatedPermit = semaphore.update(permit, 0, () -> false).get(1, TimeUnit.SECONDS);

        AsyncSemaphorePermit permit2 = semaphore.acquire(10, () -> false).get(1, TimeUnit.SECONDS);

        semaphore.release(updatedPermit);
        semaphore.release(permit2);
    }

    @Test
    public void testConcurrentAcquireAndRelease() throws Exception {
        semaphore = new AsyncSemaphoreImpl(10, 100, 5000);

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Start multiple threads acquiring and releasing permits
        for (int i = 0; i < 20; i++) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < 10; j++) {
                        CompletableFuture<AsyncSemaphorePermit> permitFuture =
                                semaphore.acquire(1, () -> false);
                        AsyncSemaphorePermit permit = permitFuture.get(5, TimeUnit.SECONDS);
                        Thread.sleep(10); // Hold permit briefly
                        semaphore.release(permit);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future);
        }

        // Wait for all threads to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(30, TimeUnit.SECONDS);
    }

    @Test
    public void testTimeoutProcessesNextRequest() throws Exception {
        semaphore = new AsyncSemaphoreImpl(5, 10, 250);

        // Acquire all permits
        CompletableFuture<AsyncSemaphorePermit> future1 = semaphore.acquire(5, () -> false);
        AsyncSemaphorePermit permit1 = future1.get(1, TimeUnit.SECONDS);

        // Request that will timeout (needs more permits than available)
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.acquire(5, () -> false);

        // make requests 250ms apart
        Thread.sleep(250);

        // Request that can be satisfied
        CompletableFuture<AsyncSemaphorePermit> future3 = semaphore.acquire(3, () -> false);

        // Release permits
        semaphore.release(permit1);

        // Expect the second request to timeout
        Throwable throwable = future2.handle((permit, t) -> t).join();
        assertThat(throwable).isInstanceOf(PermitAcquireTimeoutException.class);

        // The third request should have succeeded now
        AsyncSemaphorePermit permit3 = future3.join();
        assertNotNull(permit3);
        assertEquals(permit3.getPermits(), 3);

        semaphore.release(permit3);

    }

    @Test
    public void testMultipleReleasesProcessQueue() throws Exception {
        semaphore = new AsyncSemaphoreImpl(10, 10, 5000);

        // Acquire all permits in small chunks
        List<AsyncSemaphorePermit> permits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(2, () -> false);
            permits.add(future.get(1, TimeUnit.SECONDS));
        }

        // Queue multiple requests
        CompletableFuture<AsyncSemaphorePermit> queued1 = semaphore.acquire(3, () -> false);
        CompletableFuture<AsyncSemaphorePermit> queued2 = semaphore.acquire(4, () -> false);
        CompletableFuture<AsyncSemaphorePermit> queued3 = semaphore.acquire(2, () -> false);

        assertFalse(queued1.isDone());
        assertFalse(queued2.isDone());
        assertFalse(queued3.isDone());

        // Release permits one by one
        semaphore.release(permits.get(0)); // 2 available
        semaphore.release(permits.get(1)); // 4 available

        // First queued request should complete
        Thread.sleep(100);
        assertTrue(queued1.isDone());
        assertFalse(queued2.isDone());

        semaphore.release(permits.get(2)); // 3 available (4 - 3 from queued1 + 2)

        Thread.sleep(100);
        assertFalse(queued2.isDone()); // Still needs 4

        semaphore.release(permits.get(3)); // 5 available

        // Second queued request should complete
        Thread.sleep(100);
        assertTrue(queued2.isDone());

        semaphore.release(permits.get(4)); // 3 available (5 - 4 from queued2 + 2)

        // Third queued request should complete
        Thread.sleep(100);
        assertTrue(queued3.isDone());
    }

    @Test
    public void testUnboundedSemaphoreAcquireSinglePermit() throws Exception {
        semaphore = new AsyncSemaphoreImpl(0, 10, 5000);

        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(1, () -> false);
        AsyncSemaphorePermit permit = future.get(100, TimeUnit.MILLISECONDS);

        assertNotNull(permit);
        assertEquals(permit.getPermits(), 1);
        assertTrue(future.isDone());
    }

    @Test
    public void testUnboundedSemaphoreAcquireMultiplePermits() throws Exception {
        semaphore = new AsyncSemaphoreImpl(0, 10, 5000);

        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(100, () -> false);
        AsyncSemaphorePermit permit = future.get(100, TimeUnit.MILLISECONDS);

        assertNotNull(permit);
        assertEquals(permit.getPermits(), 100);
        assertTrue(future.isDone());
    }

    @Test
    public void testUnboundedSemaphoreNoQueueing() throws Exception {
        semaphore = new AsyncSemaphoreImpl(0, 10, 5000);

        List<CompletableFuture<AsyncSemaphorePermit>> futures = new ArrayList<>();

        // Acquire many permits concurrently - all should complete immediately
        for (int i = 0; i < 100; i++) {
            futures.add(semaphore.acquire(10, () -> false));
        }

        // All futures should complete immediately without queueing
        for (CompletableFuture<AsyncSemaphorePermit> future : futures) {
            AsyncSemaphorePermit permit = future.get(100, TimeUnit.MILLISECONDS);
            assertNotNull(permit);
            assertEquals(permit.getPermits(), 10);
            assertTrue(future.isDone());
        }
    }

    @Test
    public void testUnboundedSemaphoreReleaseIsNoop() throws Exception {
        semaphore = new AsyncSemaphoreImpl(0, 10, 5000);

        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(50, () -> false);
        AsyncSemaphorePermit permit = future.get(100, TimeUnit.MILLISECONDS);

        // Release should not throw exception but is a no-op
        semaphore.release(permit);

        // Should still be able to acquire more permits
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.acquire(100, () -> false);
        AsyncSemaphorePermit permit2 = future2.get(100, TimeUnit.MILLISECONDS);
        assertNotNull(permit2);
    }

    @Test
    public void testUnboundedSemaphoreAvailablePermits() {
        semaphore = new AsyncSemaphoreImpl(0, 10, 5000);

        assertEquals(semaphore.getAvailablePermits(), Long.MAX_VALUE);
    }

    @Test
    public void testUnboundedSemaphoreAcquiredPermits() throws Exception {
        semaphore = new AsyncSemaphoreImpl(0, 10, 5000);

        // Acquire some permits
        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(100, () -> false);
        future.get(100, TimeUnit.MILLISECONDS);

        // Acquired permits should always be 0 for unbounded semaphore
        assertEquals(semaphore.getAcquiredPermits(), 0);
    }

    @Test
    public void testUnboundedSemaphoreQueueSize() throws Exception {
        semaphore = new AsyncSemaphoreImpl(0, 10, 5000);

        // Acquire multiple permits
        for (int i = 0; i < 10; i++) {
            semaphore.acquire(1, () -> false).get(100, TimeUnit.MILLISECONDS);
        }

        // Queue size should always be 0 since requests complete immediately
        assertEquals(semaphore.getQueueSize(), 0);
    }

    @Test
    public void testUnboundedSemaphoreUpdate() throws Exception {
        semaphore = new AsyncSemaphoreImpl(0, 10, 5000);

        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(10, () -> false);
        AsyncSemaphorePermit permit = future.get(100, TimeUnit.MILLISECONDS);

        // Update should complete immediately
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.update(permit, 50, () -> false);
        AsyncSemaphorePermit permit2 = future2.get(100, TimeUnit.MILLISECONDS);

        assertNotNull(permit2);
        assertEquals(permit2.getPermits(), 50);
        assertTrue(future2.isDone());
    }

    @Test
    public void testUnboundedSemaphoreUpdateToZero() throws Exception {
        semaphore = new AsyncSemaphoreImpl(0, 10, 5000);

        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(100, () -> false);
        AsyncSemaphorePermit permit = future.get(100, TimeUnit.MILLISECONDS);

        // Update to zero should complete immediately
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.update(permit, 0, () -> false);
        AsyncSemaphorePermit permit2 = future2.get(100, TimeUnit.MILLISECONDS);

        assertNotNull(permit2);
        assertEquals(permit2.getPermits(), 0);
        assertTrue(future2.isDone());
    }

    @Test
    public void testUnboundedSemaphoreNoTimeout() throws Exception {
        semaphore = new AsyncSemaphoreImpl(0, 10, 100); // Short timeout

        // Even with short timeout, requests should complete immediately
        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(1000, () -> false);
        AsyncSemaphorePermit permit = future.get(50, TimeUnit.MILLISECONDS);

        assertNotNull(permit);
        assertEquals(permit.getPermits(), 1000);

        Thread.sleep(150); // Wait longer than timeout

        // Permit should still be valid
        assertEquals(permit.getPermits(), 1000);
    }

    @Test
    public void testUnboundedSemaphoreCancellationIgnored() throws Exception {
        semaphore = new AsyncSemaphoreImpl(0, 10, 5000);

        AtomicBoolean cancelled = new AtomicBoolean(true);

        // Even though cancelled is true, unbounded semaphore completes immediately
        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(10, cancelled::get);
        AsyncSemaphorePermit permit = future.get(100, TimeUnit.MILLISECONDS);

        assertNotNull(permit);
        assertEquals(permit.getPermits(), 10);
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testPermitsArentAcquiredInUpdateWhenCancelled() throws Exception {
        semaphore = new AsyncSemaphoreImpl(10, 10, 5000);

        // setup
        AtomicBoolean cancelled = new AtomicBoolean(false);
        // acquire 5 permits
        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(5, cancelled::get);
        assertThat(future).succeedsWithin(Duration.ofSeconds(1))
                .satisfies(p -> assertThat(p.getPermits()).isEqualTo(5));
        AsyncSemaphorePermit permit = future.join();
        assertThat(semaphore.getAcquiredPermits()).isEqualTo(5);

        // when permits are update when the request is already cancelled
        cancelled.set(true);
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.update(permit, 10, cancelled::get);
        assertThat(future2).failsWithin(Duration.ofSeconds(1))
                .withThrowableThat().havingRootCause()
                .isInstanceOf(AsyncSemaphore.PermitAcquireCancelledException.class);
        assertThat(semaphore.getAcquiredPermits()).isEqualTo(5);

        // then no permits should be acquired so that
        // when a new acquisition with update is made, it should succeed
        cancelled.set(false);
        CompletableFuture<AsyncSemaphorePermit> future3 = semaphore.update(permit, 10, cancelled::get);
        assertThat(future3).succeedsWithin(Duration.ofSeconds(1))
                .satisfies(p -> assertThat(p.getPermits()).isEqualTo(10));
        assertThat(semaphore.getAcquiredPermits()).isEqualTo(10);

        // when original permit is released, it shouldn't reduce acquired permits
        semaphore.release(permit);
        assertThat(semaphore.getAcquiredPermits()).isEqualTo(10);

        // when updated permit is released, it should reduce acquired permits
        semaphore.release(future3.join());
        assertThat(semaphore.getAcquiredPermits()).isEqualTo(0);
    }

    @Test
    public void testSupportCompletableFutureCancel() throws Exception {
        int timeoutMillis = 200;
        semaphore = new AsyncSemaphoreImpl(10, 10, timeoutMillis);

        // setup
        AtomicBoolean cancelled = new AtomicBoolean(false);
        // acquire 5 permits
        CompletableFuture<AsyncSemaphorePermit> future = semaphore.acquire(5, cancelled::get);
        assertThat(future).succeedsWithin(Duration.ofSeconds(1))
                .satisfies(p -> assertThat(p.getPermits()).isEqualTo(5));
        assertThat(semaphore.getAcquiredPermits()).isEqualTo(5);

        // attempt to acquire 5 permits
        CompletableFuture<AsyncSemaphorePermit> future2 = semaphore.acquire(10, cancelled::get);

        // attempt to acquire 5 permits
        CompletableFuture<AsyncSemaphorePermit> future3 = semaphore.acquire(5, cancelled::get);
        // cancel future2
        future2.cancel(true);
        assertThat(future2).isCancelled();
        // now future3 should succeed
        assertThat(future3).succeedsWithin(Duration.ofSeconds(1))
                .satisfies(p -> assertThat(p.getPermits()).isEqualTo(5));

        assertThat(semaphore.getAcquiredPermits()).isEqualTo(10);
   }
}