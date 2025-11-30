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
import java.util.NavigableSet;
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
import org.apache.bookkeeper.mledger.Position;
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
    private Timer timer;
    private BucketSnapshotStorage storage;
    private ExecutorService executorService;

    @BeforeMethod
    public void setUp() throws Exception {
        dispatcher = mock(AbstractPersistentDispatcherMultipleConsumers.class);
        final ManagedCursor cursor = mock(ManagedCursor.class);
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

    /**
     * Test concurrent getScheduledMessages() calls with read operations.
     * getScheduledMessages() uses write lock while read operations use read lock.
     * Messages are added beforehand to avoid concurrent addMessage calls.
     */
    @Test
    public void testConcurrentGetScheduledMessagesWithReads() throws Exception {
        // Add messages that will be ready for delivery after a short delay
        final long baseTime = System.currentTimeMillis();
        final int totalMessages = 500;

        // Add messages with delivery time slightly in the future, then wait for them to become ready
        for (int i = 0; i < totalMessages; i++) {
            tracker.addMessage(i, i, baseTime + 1000);
        }
        assertEquals(tracker.getNumberOfDelayedMessages(), totalMessages, "All messages should be added");
        // Wait for messages to become ready for delivery
        Thread.sleep(3000);
        final int numReadThreads = 12;
        final int numScheduleThreads = 4;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch readersDone = new CountDownLatch(numReadThreads);
        final CountDownLatch schedulersDone = new CountDownLatch(numScheduleThreads);
        final AtomicInteger errors = new AtomicInteger(0);
        final AtomicReference<Exception> firstException = new AtomicReference<>();
        final AtomicInteger totalMessagesRetrieved = new AtomicInteger(0);
        // Start read threads (containsMessage and nextDeliveryTime)
        for (int i = 0; i < numReadThreads; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 1000; j++) {
                        if (threadId % 2 == 0) {
                            tracker.containsMessage(j % totalMessages, j % totalMessages);
                        } else {
                            try {
                                tracker.nextDeliveryTime();
                            } catch (IllegalArgumentException e) {
                                // Expected when no messages available
                            }
                        }
                        if (j % 100 == 0) {
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
        // Start getScheduledMessages threads - continue until all messages are retrieved
        for (int i = 0; i < numScheduleThreads; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    int consecutiveEmptyReturns = 0;
                    final int maxConsecutiveEmpty = 5;

                    while (totalMessagesRetrieved.get() < totalMessages
                            && consecutiveEmptyReturns < maxConsecutiveEmpty) {
                        NavigableSet<Position> messages = tracker.getScheduledMessages(50);
                        int retrieved = messages.size();
                        totalMessagesRetrieved.addAndGet(retrieved);

                        if (retrieved == 0) {
                            consecutiveEmptyReturns++;
                            Thread.sleep(10);
                        } else {
                            consecutiveEmptyReturns = 0;
                            Thread.sleep(5);
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    firstException.compareAndSet(null, e);
                    e.printStackTrace();
                } finally {
                    schedulersDone.countDown();
                }
            });
        }
        startLatch.countDown();
        assertTrue(readersDone.await(30, TimeUnit.SECONDS), "Readers should complete within 30 seconds");
        assertTrue(schedulersDone.await(30, TimeUnit.SECONDS), "Schedulers should complete within 30 seconds");
        if (errors.get() > 0) {
            Exception exception = firstException.get();
            if (exception != null) {
                throw new AssertionError("Concurrent getScheduledMessages test failed", exception);
            }
        }
        assertEquals(errors.get(), 0, "No exceptions should occur during concurrent operations");

        // Verify that most or all messages were retrieved
        assertEquals(totalMessagesRetrieved.get(), 500, "All messages should be retrieved");

        log.info("Total messages retrieved: {} out of {}", totalMessagesRetrieved.get(), totalMessages);
    }

    /**
     * Test concurrent clear() operations with read operations.
     * This verifies that clear() properly coordinates with ongoing read operations.
     * Messages are added beforehand, then clear() is tested with concurrent reads.
     */
    @Test
    public void testConcurrentClearWithReads() throws Exception {
        final int initialMessages = 1000;
        final int numReadThreads = 10;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch readersDone = new CountDownLatch(numReadThreads);
        final AtomicInteger errors = new AtomicInteger(0);
        final AtomicReference<Exception> firstException = new AtomicReference<>();
        final AtomicBoolean clearCompleted = new AtomicBoolean(false);
        // Add initial messages (single thread)
        for (int i = 0; i < initialMessages; i++) {
            tracker.addMessage(i, i, System.currentTimeMillis() + 60000);
        }
        // Start read threads that will run during clear operation
        for (int i = 0; i < numReadThreads; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    while (!clearCompleted.get()) {
                        switch (threadId % 3) {
                            case 0:
                                tracker.containsMessage(threadId, threadId);
                                break;
                            case 1:
                                try {
                                    tracker.nextDeliveryTime();
                                } catch (IllegalArgumentException e) {
                                    // Expected when no messages available
                                }
                                break;
                            case 2:
                                tracker.getNumberOfDelayedMessages();
                                break;
                        }
                        Thread.sleep(1);
                    }
                    // Continue reading for a bit after clear
                    for (int j = 0; j < 100; j++) {
                        tracker.containsMessage(j, j);
                        Thread.sleep(1);
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
        // Start clear operation after a short delay
        executorService.submit(() -> {
            try {
                startLatch.await();
                Thread.sleep(100);
                tracker.clear().get(30, TimeUnit.SECONDS);
                clearCompleted.set(true);
            } catch (Exception e) {
                errors.incrementAndGet();
                firstException.compareAndSet(null, e);
                e.printStackTrace();
                clearCompleted.set(true);
            }
        });
        startLatch.countDown();
        assertTrue(readersDone.await(60, TimeUnit.SECONDS), "Readers should complete within 60 seconds");
        if (errors.get() > 0) {
            Exception exception = firstException.get();
            if (exception != null) {
                throw new AssertionError("Concurrent clear test failed", exception);
            }
        }
        assertEquals(errors.get(), 0, "No exceptions should occur during concurrent clear operations");
        assertEquals(tracker.getNumberOfDelayedMessages(), 0, "All messages should be cleared");
    }

    /**
     * Test concurrent close() operations.
     * This verifies that close() properly handles concurrent access and shuts down cleanly.
     * Messages are added beforehand to test close() behavior with existing data.
     */
    @Test
    public void testConcurrentClose() throws Exception {
        final int numReadThreads = 8;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch readersDone = new CountDownLatch(numReadThreads);
        final AtomicInteger errors = new AtomicInteger(0);
        final AtomicReference<Exception> firstException = new AtomicReference<>();
        final AtomicBoolean closeInitiated = new AtomicBoolean(false);
        // Add some messages first (single thread)
        for (int i = 0; i < 100; i++) {
            tracker.addMessage(i, i, System.currentTimeMillis() + 60000);
        }
        // Start read threads that will be interrupted by close
        for (int i = 0; i < numReadThreads; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    while (!closeInitiated.get()) {
                        try {
                            switch (threadId % 4) {
                                case 0:
                                    tracker.containsMessage(threadId, threadId);
                                    break;
                                case 1:
                                    tracker.nextDeliveryTime();
                                    break;
                                case 2:
                                    tracker.getNumberOfDelayedMessages();
                                    break;
                                case 3:
                                    tracker.getScheduledMessages(10);
                                    break;
                            }
                        } catch (IllegalArgumentException e) {
                            // Expected for some operations when tracker is being closed
                        }
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    // Some exceptions may be expected during close
                    if (!closeInitiated.get()) {
                        errors.incrementAndGet();
                        firstException.compareAndSet(null, e);
                        e.printStackTrace();
                    }
                } finally {
                    readersDone.countDown();
                }
            });
        }
        // Start close operation after a short delay
        executorService.submit(() -> {
            try {
                startLatch.await();
                Thread.sleep(100);
                closeInitiated.set(true);
                tracker.close();
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
                log.warn("Exception during concurrent close test (may be expected): " + exception.getMessage());
            }
        }
        // Create a new tracker for the next test since this one is closed
        tracker = new BucketDelayedDeliveryTracker(
                dispatcher, timer, 1000, Clock.systemUTC(), true, storage,
                100, 1000, 100, 10
        );
    }

    /**
     * Test mixed read operations with sequential addMessage and concurrent getScheduledMessages.
     * This tests the ReentrantReadWriteLock behavior when read and write operations are mixed.
     * addMessage is executed in single thread, while reads and getScheduledMessages are concurrent.
     * Ensures all deliverable messages are retrieved before test completion.
     */
    @Test
    public void testMixedReadWriteOperationsDeadlockDetection() throws Exception {
        final int numReadThreads = 16;
        final int numScheduleThreads = 4;
        final int totalMessages = 2000;
        final int readsPerThread = 500;

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch readersDone = new CountDownLatch(numReadThreads);
        final CountDownLatch schedulersDone = new CountDownLatch(numScheduleThreads);
        final CountDownLatch writerDone = new CountDownLatch(1);
        final AtomicBoolean deadlockDetected = new AtomicBoolean(false);
        final AtomicInteger completedOperations = new AtomicInteger(0);
        final AtomicReference<Exception> firstException = new AtomicReference<>();
        final AtomicInteger messagesAdded = new AtomicInteger(0);
        final AtomicInteger deliverableMessagesCount = new AtomicInteger(0);
        final AtomicInteger totalMessagesRetrieved = new AtomicInteger(0);
        // Single writer thread for addMessage (sequential execution)
        executorService.submit(() -> {
            try {
                startLatch.await();
                final long baseTime = System.currentTimeMillis();

                for (int i = 0; i < totalMessages; i++) {
                    try {
                        long ledgerId = 10000 + i;
                        long entryId = i % 1000;

                        // Create mix of messages: some ready for delivery, some delayed
                        long deliverAt;
                        if (i % 3 == 0) {
                            // Messages that will be ready for delivery after a short delay
                            deliverAt = baseTime + 500;
                            deliverableMessagesCount.incrementAndGet();
                        } else {
                            // Messages for future delivery (much later)
                            deliverAt = baseTime + 30000;
                        }

                        boolean added = tracker.addMessage(ledgerId, entryId, deliverAt);
                        if (added) {
                            messagesAdded.incrementAndGet();
                        }
                        completedOperations.incrementAndGet();

                        if (i % 200 == 0) {
                            Thread.sleep(1);
                        }
                    } catch (Exception e) {
                        deadlockDetected.set(true);
                        firstException.compareAndSet(null, e);
                        e.printStackTrace();
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
                writerDone.countDown();
            }
        });
        // Start read threads (using read locks)
        for (int i = 0; i < numReadThreads; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    // Continue reading until writer is done, plus some extra operations
                    int operationCount = 0;
                    while ((writerDone.getCount() > 0 || operationCount < readsPerThread)) {
                        try {
                            switch (threadId % 3) {
                                case 0:
                                    long ledgerId = 10000 + (operationCount % (totalMessages + 100));
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
                            if (operationCount % 100 == 0) {
                                Thread.sleep(1);
                            }
                        } catch (IllegalArgumentException e) {
                            // Expected for some operations
                            completedOperations.incrementAndGet();
                            operationCount++;
                        } catch (Exception e) {
                            deadlockDetected.set(true);
                            firstException.compareAndSet(null, e);
                            e.printStackTrace();
                            break;
                        }
                        if (writerDone.getCount() == 0 && operationCount >= readsPerThread) {
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
        // Start getScheduledMessages threads (using write locks)
        for (int i = 0; i < numScheduleThreads; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();

                    // Wait for writer to finish and messages to become deliverable
                    writerDone.await();
                    Thread.sleep(1000); // Wait 1 second for messages to become ready for delivery

                    int consecutiveEmptyReturns = 0;
                    final int maxConsecutiveEmpty = 5;

                    // Continue until we've retrieved all deliverable messages or hit max empty returns
                    while (totalMessagesRetrieved.get() < deliverableMessagesCount.get()
                            && consecutiveEmptyReturns < maxConsecutiveEmpty) {
                        try {
                            NavigableSet<Position> messages = tracker.getScheduledMessages(50);
                            int retrieved = messages.size();
                            totalMessagesRetrieved.addAndGet(retrieved);
                            completedOperations.incrementAndGet();

                            if (retrieved == 0) {
                                consecutiveEmptyReturns++;
                                Thread.sleep(5); // Short wait for more messages
                            } else {
                                consecutiveEmptyReturns = 0;
                                Thread.sleep(2); // Short processing delay
                            }

                        } catch (Exception e) {
                            deadlockDetected.set(true);
                            firstException.compareAndSet(null, e);
                            e.printStackTrace();
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
                    schedulersDone.countDown();
                }
            });
        }
        // Start all threads
        startLatch.countDown();

        // Wait for completion with reasonable timeout to detect deadlocks
        boolean writerCompleted = writerDone.await(10, TimeUnit.SECONDS);
        boolean readersCompleted = readersDone.await(15, TimeUnit.SECONDS);
        boolean schedulersCompleted = schedulersDone.await(20, TimeUnit.SECONDS);
        if (!writerCompleted || !readersCompleted || !schedulersCompleted) {
            deadlockDetected.set(true);
            log.error("Test timed out - potential deadlock detected. Writer: {}, Readers: {}, Schedulers: {}",
                    writerCompleted, readersCompleted, schedulersCompleted);
        }
        if (deadlockDetected.get()) {
            Exception e = firstException.get();
            if (e != null) {
                throw new AssertionError("Deadlock or exception detected during mixed operations test", e);
            } else {
                throw new AssertionError("Deadlock detected - test did not complete within timeout");
            }
        }

        // Verify operations completed successfully
        assertTrue(completedOperations.get() > 0, "Some operations should complete");
        assertTrue(messagesAdded.get() > 0, "Some messages should have been added");
        assertTrue(deliverableMessagesCount.get() > 0, "Some messages should be deliverable");

        // Verify that all deliverable messages were retrieved
        assertEquals(totalMessagesRetrieved.get(), deliverableMessagesCount.get(),
                "All deliverable messages should be retrieved");
        log.info("Mixed operations test completed successfully. Total operations: {}, Messages added: {}, "
                        + "Deliverable messages: {}, Retrieved messages: {}",
                completedOperations.get(), messagesAdded.get(),
                deliverableMessagesCount.get(), totalMessagesRetrieved.get());
    }
}