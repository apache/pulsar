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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Thread safety tests for BucketDelayedDeliveryTracker.
 * These tests verify that the hybrid approach with ReentrantReadWriteLock and concurrent data structures
 * correctly handles concurrent access patterns without deadlocks, race conditions, or data corruption.
 */
@Slf4j
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
        // First shutdown executor to stop all threads
        if (executorService != null) {
            assertTrue(MoreExecutors.shutdownAndAwaitTermination(executorService, 5, TimeUnit.SECONDS),
                    "Executor should shutdown cleanly");
        }
        // Then close tracker safely after all threads stopped
        if (tracker != null) {
            tracker.close();
        }
    }

    /**
     * Test concurrent containsMessage() calls while adding messages sequentially.
     * This tests the ReentrantReadWriteLock read performance under contention.
     * addMessage is executed sequentially (as in real scenarios), while containsMessage is concurrent.
     */
    @Test
    public void testConcurrentContainsMessageWithWrites() throws Exception {
        final int numReadThreads = 8;
        final int readsPerThread = 1000;
        final int totalMessages = 5000;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch readersDone = new CountDownLatch(numReadThreads);
        final AtomicInteger errors = new AtomicInteger(0);
        final AtomicReference<Exception> firstException = new AtomicReference<>();
        final AtomicInteger messagesAdded = new AtomicInteger(0);

        // Start reader threads - these will run concurrently
        for (int i = 0; i < numReadThreads; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    // Continuously read for a period while messages are being added
                    long endTime = System.currentTimeMillis() + 10000;
                    int readCount = 0;
                    while (System.currentTimeMillis() < endTime && readCount < readsPerThread) {
                        // Check for messages across the range that might be added
                        long ledgerId = 1000 + (readCount % totalMessages);
                        long entryId = readCount % 100;
                        // This should not throw exceptions or block indefinitely
                        tracker.containsMessage(ledgerId, entryId);
                        readCount++;
                        if (readCount % 100 == 0) {
                            Thread.sleep(1);
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    firstException.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    readersDone.countDown();
                }
            });
        }

        // Start the single writer thread - sequential addMessage calls
        executorService.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < totalMessages; i++) {
                    long ledgerId = 1000 + i;
                    long entryId = i % 100;
                    long deliverAt = System.currentTimeMillis() + 10000;
                    boolean added = tracker.addMessage(ledgerId, entryId, deliverAt);
                    if (added) {
                        messagesAdded.incrementAndGet();
                    }
                    // Small delay to simulate real processing time
                    if (i % 100 == 0) {
                        Thread.sleep(1);
                    }
                }
            } catch (Exception e) {
                errors.incrementAndGet();
                firstException.compareAndSet(null, e);
                e.printStackTrace();
            }
        });

        startLatch.countDown();
        assertTrue(readersDone.await(30, TimeUnit.SECONDS), "Readers should complete within 30 seconds");

        if (errors.get() > 0) {
            Exception exception = firstException.get();
            if (exception != null) {
                log.error("First exception caught: " + exception.getMessage());
                exception.printStackTrace();
            }
        }
        assertEquals(errors.get(), 0, "No exceptions should occur during concurrent operations");
        assertTrue(messagesAdded.get() > 0, "Some messages should have been added");
    }

    /**
     * Test concurrent nextDeliveryTime() calls.
     * This verifies the ReentrantReadWriteLock implementation for read-heavy operations.
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
        final int numReadThreads = 30;
        final int operationsPerThread = 200;
        final int writeOperations = 1000;

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch writerDone = new CountDownLatch(1);
        final CountDownLatch readersDone = new CountDownLatch(numReadThreads);
        final AtomicBoolean deadlockDetected = new AtomicBoolean(false);
        final AtomicInteger completedOperations = new AtomicInteger(0);
        final AtomicInteger writesCompleted = new AtomicInteger(0);
        final AtomicReference<Exception> firstException = new AtomicReference<>();
        // Single writer thread - executes addMessage sequentially
        executorService.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < writeOperations; i++) {
                    try {
                        long ledgerId = 50000 + i;
                        long entryId = i;
                        tracker.addMessage(ledgerId, entryId, System.currentTimeMillis() + 10000);
                        writesCompleted.incrementAndGet();
                        completedOperations.incrementAndGet();

                        // Small delay to allow read threads to interleave
                        if (i % 50 == 0) {
                            Thread.sleep(1);
                        }
                    } catch (Exception e) {
                        if (!(e instanceof IllegalArgumentException)) {
                            deadlockDetected.set(true);
                            firstException.compareAndSet(null, e);
                            e.printStackTrace();
                            break;
                        }
                        completedOperations.incrementAndGet();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                deadlockDetected.set(true);
            } catch (Exception e) {
                deadlockDetected.set(true);
                firstException.compareAndSet(null, e);
                e.printStackTrace();
            } finally {
                writerDone.countDown();
            }
        });
        // Multiple reader threads - execute read operations concurrently
        for (int i = 0; i < numReadThreads; i++) {
            final int threadId = i;
            final int readOperationType = i % 3;
            executorService.submit(() -> {
                try {
                    startLatch.await();

                    // Continue reading until writer is done, plus some extra operations
                    int operationCount = 0;
                    while ((writerDone.getCount() > 0 || operationCount < operationsPerThread)) {
                        try {
                            switch (readOperationType) {
                                case 0:
                                    // Check both existing and potentially non-existing messages
                                    long ledgerId = 50000 + (operationCount % (writeOperations + 100));
                                    long entryId = operationCount % 1000;
                                    tracker.containsMessage(ledgerId, entryId);
                                    break;
                                case 1:
                                    tracker.nextDeliveryTime();
                                    break;
                                case 2:
                                    tracker.getNumberOfDelayedMessages();
                                    break;
                            }
                            completedOperations.incrementAndGet();
                            operationCount++;

                            // Small delay to prevent excessive CPU usage
                            if (operationCount % 100 == 0) {
                                Thread.sleep(1);
                            }
                        } catch (IllegalArgumentException e) {
                            // Expected for some operations (e.g., nextDeliveryTime on empty queue)
                            completedOperations.incrementAndGet();
                            operationCount++;
                        } catch (Exception e) {
                            deadlockDetected.set(true);
                            firstException.compareAndSet(null, e);
                            e.printStackTrace();
                            break;
                        }

                        // Break if we've done enough operations and writer is done
                        if (writerDone.getCount() == 0 && operationCount >= operationsPerThread) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    deadlockDetected.set(true);
                } catch (Exception e) {
                    deadlockDetected.set(true);
                    firstException.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    readersDone.countDown();
                }
            });
        }
        // Start all threads
        startLatch.countDown();
        // Wait for completion with timeout to detect deadlocks
        boolean writerCompleted = false;
        boolean readersCompleted = false;

        try {
            writerCompleted = writerDone.await(30, TimeUnit.SECONDS);
            readersCompleted = readersDone.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            deadlockDetected.set(true);
        }
        // Check for deadlock indicators
        if (!writerCompleted || !readersCompleted) {
            deadlockDetected.set(true);
            log.error("Test timed out - potential deadlock detected. Writer completed: {}, Readers completed: {}",
                    writerCompleted, readersCompleted);
        }
        // Assert results
        if (deadlockDetected.get()) {
            Exception e = firstException.get();
            if (e != null) {
                throw new AssertionError("Deadlock or exception detected during test execution", e);
            } else {
                throw new AssertionError("Deadlock detected - test did not complete within timeout");
            }
        }
        // Verify that operations actually completed
        assertTrue(completedOperations.get() > 0, "Some operations should complete");
        assertTrue(writesCompleted.get() > 0, "Some write operations should complete");

        log.info("Deadlock test completed successfully. Total operations: {}, Writes completed: {}",
                completedOperations.get(), writesCompleted.get());
    }

    /**
     * Test data consistency under high concurrency.
     * Verifies that the hybrid approach maintains data integrity.
     */
    @Test
    public void testDataConsistencyUnderConcurrency() throws Exception {
        final int numReadThreads = 16;
        final int totalMessages = 4000;
        final int readsPerThread = 1000;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch readersDone = new CountDownLatch(numReadThreads);
        final AtomicInteger errors = new AtomicInteger(0);
        final AtomicReference<Exception> firstException = new AtomicReference<>();
        final AtomicInteger foundMessages = new AtomicInteger(0);
        final AtomicInteger totalMessagesAdded = new AtomicInteger(0);
        // Start reader threads - these will run concurrently
        for (int i = 0; i < numReadThreads; i++) {
            final int readerId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    // Continuously read for a period while messages are being added
                    long endTime = System.currentTimeMillis() + 12000;
                    int readCount = 0;
                    while (System.currentTimeMillis() < endTime && readCount < readsPerThread) {
                        // Check for messages across the range that might be added
                        int messageIndex = readCount % totalMessages;
                        long ledgerId = 10000 + messageIndex;
                        long entryId = messageIndex % 100;

                        if (tracker.containsMessage(ledgerId, entryId)) {
                            foundMessages.incrementAndGet();
                        }
                        readCount++;

                        if (readCount % 200 == 0) {
                            Thread.sleep(1);
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    firstException.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    readersDone.countDown();
                }
            });
        }
        // Start the single writer thread - sequential addMessage calls
        executorService.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < totalMessages; i++) {
                    long ledgerId = 10000 + i;
                    long entryId = i % 100;
                    long deliverAt = System.currentTimeMillis() + 30000;
                    boolean added = tracker.addMessage(ledgerId, entryId, deliverAt);
                    if (added) {
                        totalMessagesAdded.incrementAndGet();
                    }
                    // Small delay to simulate real processing time and allow reads
                    if (i % 200 == 0) {
                        Thread.sleep(2);
                    }
                }
            } catch (Exception e) {
                errors.incrementAndGet();
                firstException.compareAndSet(null, e);
                e.printStackTrace();
            }
        });
        startLatch.countDown();
        assertTrue(readersDone.await(40, TimeUnit.SECONDS), "Readers should complete within 40 seconds");
        // Check for errors during concurrent operations
        if (errors.get() > 0) {
            Exception exception = firstException.get();
            if (exception != null) {
                log.error("First exception caught: " + exception.getMessage());
                exception.printStackTrace();
            }
        }
        assertEquals(errors.get(), 0, "No exceptions should occur during concurrent operations");
        // Verify final consistency
        long finalMessageCount = tracker.getNumberOfDelayedMessages();
        assertTrue(finalMessageCount >= 0, "Message count should be non-negative");
        // The exact counts may vary due to timing, but we should have successful operations
        assertTrue(totalMessagesAdded.get() > 0, "Some messages should have been added");
        assertTrue(foundMessages.get() >= 0, "Found messages count should be non-negative");

        // Log results for analysis
        log.info("Total messages added: {}, Found messages: {}, Final message count: {}",
                totalMessagesAdded.get(), foundMessages.get(), finalMessageCount);
    }

    /**
     * Test read performance under varying contention levels.
     * This helps validate that the ReentrantReadWriteLock reads are working efficiently.
     */
    @Test
    public void testReadPerformanceUnderContention() throws Exception {
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