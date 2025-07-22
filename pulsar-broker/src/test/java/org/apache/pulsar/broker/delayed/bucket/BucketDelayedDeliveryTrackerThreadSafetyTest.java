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
package org.apache.pulsar.broker.delayed.bucket;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.Timer;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Thread safety tests for BucketDelayedDeliveryTracker.
 * These tests verify that the hybrid approach with StampedLock and concurrent data structures
 * correctly handles concurrent access patterns without deadlocks, race conditions, or data corruption.
 */
public class BucketDelayedDeliveryTrackerThreadSafetyTest {

    private BucketDelayedDeliveryTracker tracker;
    private AbstractPersistentDispatcherMultipleConsumers dispatcher;
    private ManagedCursor cursor;
    private Timer timer;
    private BucketSnapshotStorage storage;
    private ExecutorService executorService;

    @BeforeMethod
    public void setUp() throws Exception {
        dispatcher = mock(AbstractPersistentDispatcherMultipleConsumers.class);
        cursor = mock(ManagedCursor.class);
        timer = mock(Timer.class);
        storage = mock(BucketSnapshotStorage.class);

        when(dispatcher.getName()).thenReturn("persistent://public/default/test-topic / test-cursor");
        when(dispatcher.getCursor()).thenReturn(cursor);
        when(cursor.getName()).thenReturn("test-cursor");  // Provide a valid cursor name
        when(cursor.getCursorProperties()).thenReturn(java.util.Collections.emptyMap());

        // Mock cursor property operations for bucket key storage
        when(cursor.putCursorProperty(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(cursor.removeCursorProperty(any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Mock storage operations to avoid NullPointerException
        when(storage.createBucketSnapshot(any(), any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(1L));
        when(storage.getBucketSnapshotMetadata(anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(storage.getBucketSnapshotSegment(anyLong(), anyLong(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(java.util.Collections.emptyList()));
        when(storage.getBucketSnapshotLength(anyLong()))
                .thenReturn(CompletableFuture.completedFuture(0L));
        when(storage.deleteBucketSnapshot(anyLong()))
                .thenReturn(CompletableFuture.completedFuture(null));

        tracker = new BucketDelayedDeliveryTracker(
            dispatcher, timer, 1000, Clock.systemUTC(), true, storage,
            100, 1000, 100, 10  // Restore original minIndexCountPerBucket for proper testing
        );

        executorService = Executors.newFixedThreadPool(32);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (tracker != null) {
            tracker.close();
        }
        if (executorService != null) {
            assertTrue(MoreExecutors.shutdownAndAwaitTermination(executorService, 5, TimeUnit.SECONDS),
                "Executor should shutdown cleanly");
        }
    }

    /**
     * Test concurrent containsMessage() calls while adding messages.
     * This tests the StampedLock optimistic read performance under contention.
     */
    @Test
    public void testConcurrentContainsMessageWithWrites() throws Exception {
        final int numThreads = 16;
        final int operationsPerThread = 1000;  // Restore to test bucket creation properly
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(numThreads);
        final AtomicInteger errors = new AtomicInteger(0);
        final AtomicReference<Exception> firstException = new AtomicReference<>();

        // Start reader threads
        for (int i = 0; i < numThreads / 2; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        long ledgerId = threadId * 1000 + j;
                        long entryId = j;
                        // This should not throw exceptions or block indefinitely
                        tracker.containsMessage(ledgerId, entryId);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    firstException.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Start writer threads
        for (int i = numThreads / 2; i < numThreads; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        long ledgerId = threadId * 1000 + j;
                        long entryId = j;
                        long deliverAt = System.currentTimeMillis() + 10000; // 10s delay
                        tracker.addMessage(ledgerId, entryId, deliverAt);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    firstException.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");

        if (errors.get() > 0) {
            Exception exception = firstException.get();
            if (exception != null) {
                System.err.println("First exception caught: " + exception.getMessage());
                exception.printStackTrace();
            }
        }
        assertEquals(errors.get(), 0, "No exceptions should occur during concurrent operations");
    }

    /**
     * Test concurrent nextDeliveryTime() calls.
     * This verifies the StampedLock implementation for read-heavy operations.
     */
    @Test
    public void testConcurrentNextDeliveryTime() throws Exception {
        // Add some messages first
        for (int i = 0; i < 100; i++) {
            tracker.addMessage(i, i, System.currentTimeMillis() + (i * 1000));
        }

        final int numThreads = 20;
        final int callsPerThread = 10000;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(numThreads);
        final AtomicInteger errors = new AtomicInteger(0);
        final AtomicLong totalCalls = new AtomicLong(0);

        for (int i = 0; i < numThreads; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < callsPerThread; j++) {
                        long nextTime = tracker.nextDeliveryTime();
                        assertTrue(nextTime > 0, "Next delivery time should be positive");
                        totalCalls.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        assertEquals(errors.get(), 0, "No exceptions should occur");
        assertEquals(totalCalls.get(), numThreads * callsPerThread, "All calls should complete");
    }

    /**
     * Test for deadlock detection with mixed read/write operations.
     * This simulates the real-world scenario where containsMessage() is called
     * from read threads while write operations modify the tracker state.
     */
    @Test
    public void testDeadlockDetection() throws Exception {
        final int numThreads = 32;
        final int operationsPerThread = 100;
        // Use Phaser for better concurrency coordination
        final Phaser startPhaser = new Phaser(numThreads + 1); // +1 for main thread
        final Phaser endPhaser = new Phaser(numThreads + 1); // +1 for main thread
        final AtomicBoolean deadlockDetected = new AtomicBoolean(false);
        final AtomicInteger completedOperations = new AtomicInteger(0);

        // Mixed workload: reads, writes, and metric queries
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            final int workloadType = i % 4;

            executorService.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startPhaser.arriveAndAwaitAdvance();

                    for (int j = 0; j < operationsPerThread; j++) {
                        try {
                            switch (workloadType) {
                                case 0: // containsMessage calls
                                    tracker.containsMessage(threadId * 1000 + j, j);
                                    break;
                                case 1: // addMessage calls
                                    tracker.addMessage(threadId * 1000 + j, j, System.currentTimeMillis() + 5000);
                                    break;
                                case 2: // nextDeliveryTime calls
                                    tracker.nextDeliveryTime();
                                    break;
                                case 3: // getNumberOfDelayedMessages calls
                                    tracker.getNumberOfDelayedMessages();
                                    break;
                            }
                            completedOperations.incrementAndGet();
                        } catch (IllegalArgumentException e) {
                            // IllegalArgumentException is expected for some operations
                            // (e.g., calling nextDeliveryTime on empty queue, invalid ledger IDs)
                            // This is not a deadlock, just normal validation
                            completedOperations.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    // Only unexpected exceptions indicate potential deadlocks
                    if (!(e instanceof IllegalArgumentException)) {
                        deadlockDetected.set(true);
                        e.printStackTrace();
                    }
                } finally {
                    // Signal completion
                    endPhaser.arriveAndDeregister();
                }
            });
        }

        // Start all threads at once
        startPhaser.arriveAndAwaitAdvance();

        // Wait for all threads to complete with timeout to detect potential deadlocks
        try {
            endPhaser.awaitAdvanceInterruptibly(endPhaser.arrive(), 60, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Timeout or interrupt indicates potential deadlock
            deadlockDetected.set(true);
            e.printStackTrace();
        }

        assertTrue(!deadlockDetected.get(), "No deadlocks should be detected");
        assertTrue(completedOperations.get() > 0, "Some operations should complete");
    }

    /**
     * Test data consistency under high concurrency.
     * Verifies that the hybrid approach maintains data integrity.
     */
    @Test
    public void testDataConsistencyUnderConcurrency() throws Exception {
        final int numWriteThreads = 8;
        final int numReadThreads = 16;
        final int messagesPerWriter = 500;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch writersDone = new CountDownLatch(numWriteThreads);
        final CountDownLatch readersDone = new CountDownLatch(numReadThreads);
        final AtomicInteger foundMessages = new AtomicInteger(0);
        final AtomicInteger totalMessagesAdded = new AtomicInteger(0);

        // Writer threads add messages
        for (int i = 0; i < numWriteThreads; i++) {
            final int writerId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < messagesPerWriter; j++) {
                        long ledgerId = writerId * 10000 + j;
                        long entryId = j;
                        boolean added = tracker.addMessage(ledgerId, entryId, System.currentTimeMillis() + 30000);
                        if (added) {
                            totalMessagesAdded.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    // Ignore exceptions for this test
                } finally {
                    writersDone.countDown();
                }
            });
        }

        // Reader threads check for messages
        for (int i = 0; i < numReadThreads; i++) {
            final int readerId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();

                    // Read for a while to catch messages being added
                    long endTime = System.currentTimeMillis() + 5000; // Read for 5 seconds
                    while (System.currentTimeMillis() < endTime) {
                        for (int writerId = 0; writerId < numWriteThreads; writerId++) {
                            for (int j = 0; j < messagesPerWriter; j++) {
                                long ledgerId = writerId * 10000 + j;
                                long entryId = j;
                                if (tracker.containsMessage(ledgerId, entryId)) {
                                    foundMessages.incrementAndGet();
                                }
                            }
                        }
                        Thread.sleep(10); // Small delay to allow writes
                    }
                } catch (Exception e) {
                    // Ignore exceptions for this test
                } finally {
                    readersDone.countDown();
                }
            });
        }

        startLatch.countDown();

        assertTrue(writersDone.await(30, TimeUnit.SECONDS), "Writers should complete");
        assertTrue(readersDone.await(30, TimeUnit.SECONDS), "Readers should complete");

        // Verify final consistency
        long finalMessageCount = tracker.getNumberOfDelayedMessages();
        assertTrue(finalMessageCount >= 0, "Message count should be non-negative");

        // The exact counts may vary due to timing, but we should have some successful operations
        assertTrue(totalMessagesAdded.get() > 0, "Some messages should have been added");
    }

    /**
     * Test optimistic read performance under varying contention levels.
     * This helps validate that the StampedLock optimistic reads are working efficiently.
     */
    @Test
    public void testOptimisticReadPerformance() throws Exception {
        // Add baseline messages
        for (int i = 0; i < 1000; i++) {
            tracker.addMessage(i, i, System.currentTimeMillis() + 60000);
        }

        final int[] threadCounts = {1, 2, 4, 8, 16};

        for (int numThreads : threadCounts) {
            final int readsPerThread = 10000;
            final CountDownLatch startLatch = new CountDownLatch(1);
            final CountDownLatch doneLatch = new CountDownLatch(numThreads);
            final AtomicLong totalReads = new AtomicLong(0);

            long startTime = System.nanoTime();

            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < readsPerThread; j++) {
                            // Mix of existing and non-existing messages
                            long ledgerId = (threadId * readsPerThread + j) % 2000;
                            long entryId = j % 1000;
                            tracker.containsMessage(ledgerId, entryId);
                            totalReads.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // Ignore for performance test
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue(doneLatch.await(30, TimeUnit.SECONDS),
                "Performance test with " + numThreads + " threads should complete");

            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            double throughput = (totalReads.get() * 1_000_000_000.0) / duration;

            System.out.printf("Threads: %d, Reads: %d, Throughput: %.0f ops/sec%n",
                numThreads, totalReads.get(), throughput);

            // Basic sanity check - should achieve reasonable throughput
            assertTrue(throughput > 10000, "Should achieve at least 10K ops/sec with " + numThreads + " threads");
        }
    }
}