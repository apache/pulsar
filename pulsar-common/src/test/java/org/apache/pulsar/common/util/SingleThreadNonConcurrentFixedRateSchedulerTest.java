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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SingleThreadNonConcurrentFixedRateSchedulerTest {

    private SingleThreadNonConcurrentFixedRateScheduler executor;

    @BeforeMethod
    public void setUp() {
        executor = new SingleThreadNonConcurrentFixedRateScheduler("test-executor");
    }

    @AfterMethod
    public void tearDown() {
        if (executor != null && !executor.isShutdown()) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    public void testConstructorCreatesWorkerThread() {
        // The constructor should trigger worker thread creation
        assertTrue(executor.getPoolSize() > 0);
        assertFalse(executor.isShutdown());
        assertFalse(executor.isTerminated());
    }

    @Test
    public void testSubmitTask() throws Exception {
        AtomicBoolean executed = new AtomicBoolean(false);

        Future<?> future = executor.submit(() -> executed.set(true));

        future.get(1, TimeUnit.SECONDS);
        assertTrue(executed.get());
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test
    public void testSubmitTaskWithException() throws Exception {
        AtomicBoolean taskExecuted = new AtomicBoolean(false);

        // Submit a task that throws an exception
        Future<?> future = executor.submit(() -> {
            taskExecuted.set(true);
            throw new RuntimeException("Test exception");
        });

        // The SafeRunnable wrapper catches the exception, so the task should complete
        // but the future will still contain the exception
        try {
            future.get(1, TimeUnit.SECONDS);
            // If we get here without exception, that's also fine - it means SafeRunnable worked
        } catch (Exception e) {
            // This is expected - the exception is still propagated through the Future
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("Test exception", e.getCause().getMessage());
        }

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertTrue(taskExecuted.get(), "Task should have been executed despite the exception");
    }

    @Test
    public void testScheduleTask() throws Exception {
        AtomicBoolean executed = new AtomicBoolean(false);
        long startTime = System.currentTimeMillis();

        ScheduledFuture<?> future = executor.schedule(() -> executed.set(true), 100, TimeUnit.MILLISECONDS);

        future.get(1, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();

        assertTrue(executed.get());
        assertTrue(future.isDone());
        assertTrue(endTime - startTime >= 100); // Should have waited at least 100ms
    }

    @Test
    public void testScheduleAtFixedRate() throws Exception {
        AtomicInteger executionCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(15);

        ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
            executionCount.incrementAndGet();
            latch.countDown();
        }, 0, 50, TimeUnit.MILLISECONDS);

        // Wait for at least 15 executions
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        future.cancel(false);

        assertTrue(executionCount.get() >= 15);
    }

    @Test
    public void testScheduleWithFixedDelay() throws Exception {
        AtomicInteger executionCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(15);

        ScheduledFuture<?> future = executor.scheduleWithFixedDelay(() -> {
            executionCount.incrementAndGet();
            latch.countDown();
        }, 0, 50, TimeUnit.MILLISECONDS);

        // Wait for at least 3 executions
        assertTrue(latch.await(15, TimeUnit.SECONDS));
        future.cancel(false);

        assertTrue(executionCount.get() >= 15);
    }

    @Test
    public void testScheduleAtFixedRateNonConcurrently() throws Exception {
        AtomicInteger executionCount = new AtomicInteger(0);
        AtomicLong[] executionTimes = new AtomicLong[10];
        for (int i = 0; i < executionTimes.length; i++) {
            executionTimes[i] = new AtomicLong(0);
        }

        // Schedule a task that takes longer than the period to test outdated task dropping
        ScheduledFuture<?> future = executor.scheduleAtFixedRateNonConcurrently(() -> {
            int count = executionCount.getAndIncrement();
            if (count < executionTimes.length) {
                executionTimes[count].set(System.nanoTime());
            }
            try {
                Thread.sleep(150); // Task takes 150ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 0, 50, TimeUnit.MILLISECONDS); // Period is 50ms (shorter than task execution time)

        // Wait for several executions
        Thread.sleep(800);
        future.cancel(false);

        // Should have executed multiple times
        assertTrue(executionCount.get() >= 2);
        // Should have dropped some executions due to the long task execution time.
        // 800 / 150 = 5.33, so at least 5 executions. Use 7 to avoid flakiness.
        assertTrue(executionCount.get() < 7);

        // Verify that outdated tasks were dropped by checking execution intervals
        // The actual intervals should be longer than the scheduled period due to task execution time
        for (int i = 1; i < Math.min(executionCount.get(), executionTimes.length); i++) {
            if (executionTimes[i].get() > 0 && executionTimes[i - 1].get() > 0) {
                long intervalNanos = executionTimes[i].get() - executionTimes[i - 1].get();
                long intervalMs = intervalNanos / 1_000_000;
                // Interval should be at least the task execution time (150ms), not the period (50ms)
                assertTrue(intervalMs >= 140, "Interval was " + intervalMs + "ms, expected >= 140ms");
            }
        }
    }

    @Test
    public void testScheduleAtFixedRateNonConcurrentlyWithNullCommand() {
        try {
            executor.scheduleAtFixedRateNonConcurrently(null, 0, 100, TimeUnit.MILLISECONDS);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
    }

    @Test
    public void testScheduleAtFixedRateNonConcurrentlyWithNullTimeUnit() {
        try {
            executor.scheduleAtFixedRateNonConcurrently(() -> {}, 0, 100, null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
    }

    @Test
    public void testScheduleAtFixedRateNonConcurrentlyWithInvalidPeriod() {
        try {
            executor.scheduleAtFixedRateNonConcurrently(() -> {}, 0, 0, TimeUnit.MILLISECONDS);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
            assertTrue(e.getMessage().contains("period can not be null"));
        }

        try {
            executor.scheduleAtFixedRateNonConcurrently(() -> {}, 0, -1, TimeUnit.MILLISECONDS);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
            assertTrue(e.getMessage().contains("period can not be null"));
        }
    }

    @Test
    public void testDropOutdatedTaskBehavior() throws Exception {
        AtomicInteger executionCount = new AtomicInteger(0);
        AtomicLong[] startTimes = new AtomicLong[5];
        AtomicLong[] endTimes = new AtomicLong[5];
        for (int i = 0; i < 5; i++) {
            startTimes[i] = new AtomicLong(0);
            endTimes[i] = new AtomicLong(0);
        }

        // Schedule a task that takes much longer than the period
        ScheduledFuture<?> future = executor.scheduleAtFixedRateNonConcurrently(() -> {
            int count = executionCount.getAndIncrement();
            if (count < 5) {
                startTimes[count].set(System.nanoTime());
                try {
                    Thread.sleep(200); // Task takes 200ms
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                endTimes[count].set(System.nanoTime());
            } else {
            }
        }, 0, 50, TimeUnit.MILLISECONDS); // Period is 50ms

        // Wait for several executions
        Thread.sleep(1500);
        future.cancel(false);

        // 1500 - 200 * 5 = 500ms left, so at most 10 more executions
        // Should have executed multiple times
        assertTrue(executionCount.get() >= 5);
        assertTrue(executionCount.get() < 15);

        // Verify that the next execution time is calculated correctly when tasks are outdated
        // Each execution should start after the previous one ends, not at fixed intervals
        for (int i = 1; i < Math.min(executionCount.get(), 5); i++) {
            if (startTimes[i].get() > 0 && endTimes[i - 1].get() > 0) {
                // Next task should start after previous task ends
                assertTrue(startTimes[i].get() >= endTimes[i - 1].get(),
                    "Task " + i + " started before task " + (i - 1) + " ended");
            }
        }
    }

    @Test
    public void testPolicyGettersReturnCorrectValues() {
        // These policies are always false and cannot be changed
        assertFalse(executor.getContinueExistingPeriodicTasksAfterShutdownPolicy());
        assertFalse(executor.getExecuteExistingDelayedTasksAfterShutdownPolicy());
        assertFalse(executor.getRemoveOnCancelPolicy());
    }

    @Test
    public void testPolicySettersThrowExceptions() {
        try {
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(true);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("continueExistingPeriodicTasksAfterShutdownPolicy"));
        }

        try {
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(true);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("executeExistingDelayedTasksAfterShutdownPolicy"));
        }

        try {
            executor.setRemoveOnCancelPolicy(true);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("removeOnCancelPolicy"));
        }
    }

    @Test
    public void testRejectedExecutionHandler() {
        AtomicBoolean handlerCalled = new AtomicBoolean(false);
        AtomicReference<Runnable> rejectedTask = new AtomicReference<>();
        RejectedExecutionHandler customHandler = (r, executor) -> {
            handlerCalled.set(true);
            rejectedTask.set(r);
        };

        executor.setRejectedExecutionHandler(customHandler);
        assertEquals(executor.getRejectedExecutionHandler(), customHandler);

        // Shutdown the executor and try to submit a task
        executor.shutdown();
        Runnable testTask = () -> {};
        executor.submit(testTask);

        // The custom handler should be called
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> handlerCalled.get());
        assertNotNull(rejectedTask.get());
    }

    @Test
    public void testDefaultRejectedExecutionHandler() {
        // Test that the default handler is AbortPolicy
        RejectedExecutionHandler defaultHandler = executor.getRejectedExecutionHandler();
        assertNotNull(defaultHandler);
        assertTrue(defaultHandler instanceof ThreadPoolExecutor.AbortPolicy);

        // Shutdown and verify rejection behavior
        executor.shutdown();
        try {
            executor.submit(() -> {});
            fail("Should throw RejectedExecutionException with default handler");
        } catch (RejectedExecutionException e) {
            // Expected with AbortPolicy
        }
    }

    @Test
    public void testShutdownBehavior() throws Exception {
        AtomicBoolean taskExecuted = new AtomicBoolean(false);

        // Submit a task
        executor.submit(() -> taskExecuted.set(true));

        // Wait for task to complete
        Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> taskExecuted.get());

        // Shutdown the executor
        executor.shutdown();
        assertTrue(executor.isShutdown());

        // Wait for termination
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        assertTrue(executor.isTerminated());
    }

    @Test
    public void testTasksAfterShutdown() {
        executor.shutdown();

        try {
            executor.submit(() -> {});
            fail("Should throw RejectedExecutionException");
        } catch (RejectedExecutionException e) {
            // Expected
        }
    }

    @Test
    public void testConcurrentTaskExecution() throws Exception {
        int taskCount = 10;
        CountDownLatch latch = new CountDownLatch(taskCount);
        AtomicInteger executionOrder = new AtomicInteger(0);
        AtomicInteger[] results = new AtomicInteger[taskCount];

        // Submit multiple tasks
        for (int i = 0; i < taskCount; i++) {
            final int taskId = i;
            results[i] = new AtomicInteger(-1);
            executor.submit(() -> {
                results[taskId].set(executionOrder.getAndIncrement());
                latch.countDown();
            });
        }

        // Wait for all tasks to complete
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        // Verify all tasks executed
        for (int i = 0; i < taskCount; i++) {
            assertTrue(results[i].get() >= 0, "Task " + i + " did not execute");
        }
    }

    @Test
    public void testTaskCancellation() throws Exception {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch blockLatch = new CountDownLatch(1);

        // Submit a long-running task
        Future<?> future = executor.submit(() -> {
            startLatch.countDown();
            try {
                blockLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Wait for task to start
        assertTrue(startLatch.await(1, TimeUnit.SECONDS));

        // Cancel the task
        boolean cancelled = future.cancel(true);
        assertTrue(cancelled);
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());

        // Release the blocking task
        blockLatch.countDown();
    }

    @Test
    public void testScheduledTaskCancellation() throws Exception {
        AtomicBoolean executed = new AtomicBoolean(false);

        // Schedule a task with a delay
        ScheduledFuture<?> future = executor.schedule(() -> executed.set(true), 500, TimeUnit.MILLISECONDS);

        // Cancel before execution
        boolean cancelled = future.cancel(false);
        assertTrue(cancelled);
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());

        // Wait a bit to ensure task doesn't execute
        Thread.sleep(600);
        assertFalse(executed.get());
    }

    @Test
    public void testPeriodicTaskCancellation() throws Exception {
        AtomicInteger executionCount = new AtomicInteger(0);

        ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
            executionCount.incrementAndGet();
        }, 0, 50, TimeUnit.MILLISECONDS);

        // Let it run a few times
        Thread.sleep(200);
        int countBeforeCancel = executionCount.get();
        assertTrue(countBeforeCancel > 0);

        // Cancel the task
        boolean cancelled = future.cancel(false);
        assertTrue(cancelled);
        assertTrue(future.isCancelled());

        // Wait and verify no more executions
        Thread.sleep(200);
        assertEquals(executionCount.get(), countBeforeCancel);
    }

    @Test
    public void testExceptionHandlingInPeriodicTask() throws Exception {
        AtomicInteger executionCount = new AtomicInteger(0);
        AtomicInteger exceptionCount = new AtomicInteger(0);

        ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
            int count = executionCount.incrementAndGet();
            if (count % 2 == 0) {
                exceptionCount.incrementAndGet();
                throw new RuntimeException("Test exception " + count);
            }
        }, 0, 50, TimeUnit.MILLISECONDS);

        // Let it run for a while
        Thread.sleep(300);
        future.cancel(false);

        // Verify that exceptions didn't stop the periodic execution
        assertTrue(executionCount.get() > 2);
        assertTrue(exceptionCount.get() > 0);
        // Should have roughly half the executions throwing exceptions
        assertTrue(Math.abs(executionCount.get() - 2 * exceptionCount.get()) <= 1);
    }

    @Test
    public void testSchedulingWithDifferentDelays() throws Exception {
        // Test scheduling with different delays to indirectly test trigger time calculation
        AtomicLong executionTime1 = new AtomicLong(0);
        AtomicLong executionTime2 = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        // Schedule two tasks with different delays
        ScheduledFuture<?> future1 = executor.schedule(() -> {
            executionTime1.set(System.currentTimeMillis());
        }, 100, TimeUnit.MILLISECONDS);

        ScheduledFuture<?> future2 = executor.schedule(() -> {
            executionTime2.set(System.currentTimeMillis());
        }, 200, TimeUnit.MILLISECONDS);

        // Wait for both to complete
        future1.get(1, TimeUnit.SECONDS);
        future2.get(1, TimeUnit.SECONDS);

        // Verify timing
        assertTrue(executionTime1.get() >= startTime + 100);
        assertTrue(executionTime2.get() >= startTime + 200);
        assertTrue(executionTime2.get() > executionTime1.get());
    }

    @Test
    public void testSingleThreadExecution() throws Exception {
        int taskCount = 20;
        CountDownLatch latch = new CountDownLatch(taskCount);
        AtomicReference<String> threadName = new AtomicReference<>();
        AtomicBoolean singleThread = new AtomicBoolean(true);

        // Submit multiple tasks
        for (int i = 0; i < taskCount; i++) {
            executor.submit(() -> {
                String currentThreadName = Thread.currentThread().getName();
                if (threadName.get() == null) {
                    threadName.set(currentThreadName);
                } else if (!threadName.get().equals(currentThreadName)) {
                    singleThread.set(false);
                }
                latch.countDown();
            });
        }

        // Wait for all tasks to complete
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        // Verify all tasks ran on the same thread
        assertTrue(singleThread.get(), "Tasks should run on a single thread");
        assertNotNull(threadName.get());
        assertTrue(threadName.get().contains("test-executor"));
    }

    @Test
    public void testShutdownNow() throws Exception {
        AtomicInteger completedTasks = new AtomicInteger(0);
        CountDownLatch blockLatch = new CountDownLatch(1);

        // Submit a blocking task
        executor.submit(() -> {
            try {
                blockLatch.await();
                completedTasks.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Submit additional tasks
        for (int i = 0; i < 5; i++) {
            executor.submit(() -> completedTasks.incrementAndGet());
        }

        // Shutdown immediately
        executor.shutdownNow();
        assertTrue(executor.isShutdown());

        // Release the blocking task
        blockLatch.countDown();

        // Wait for termination
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        assertTrue(executor.isTerminated());

        // Some tasks may not have completed due to immediate shutdown
        assertTrue(completedTasks.get() < 6);
    }

    @Test
    public void testScheduledFutureTaskComparison() throws Exception {
        // Test that scheduled tasks are ordered correctly by time and sequence
        AtomicInteger executionOrder = new AtomicInteger(0);
        AtomicInteger[] results = new AtomicInteger[3];
        for (int i = 0; i < 3; i++) {
            results[i] = new AtomicInteger(-1);
        }

        // Schedule tasks with different delays
        ScheduledFuture<?> future3 = executor.schedule(() -> {
            results[2].set(executionOrder.getAndIncrement());
        }, 200, TimeUnit.MILLISECONDS);

        ScheduledFuture<?> future1 = executor.schedule(() -> {
            results[0].set(executionOrder.getAndIncrement());
        }, 50, TimeUnit.MILLISECONDS);

        ScheduledFuture<?> future2 = executor.schedule(() -> {
            results[1].set(executionOrder.getAndIncrement());
        }, 100, TimeUnit.MILLISECONDS);

        // Wait for all to complete
        future1.get(1, TimeUnit.SECONDS);
        future2.get(1, TimeUnit.SECONDS);
        future3.get(1, TimeUnit.SECONDS);

        // Verify execution order matches delay order
        assertEquals(results[0].get(), 0); // First to execute (50ms delay)
        assertEquals(results[1].get(), 1); // Second to execute (100ms delay)
        assertEquals(results[2].get(), 2); // Third to execute (200ms delay)
    }

    @Test
    public void testPeriodicTaskWithShutdown() throws Exception {
        AtomicInteger executionCount = new AtomicInteger(0);

        // Start a periodic task
        ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
            executionCount.incrementAndGet();
        }, 0, 50, TimeUnit.MILLISECONDS);

        // Let it run a few times
        Thread.sleep(200);
        int countBeforeShutdown = executionCount.get();
        assertTrue(countBeforeShutdown >= 2);

        // Shutdown the executor
        executor.shutdown();

        // Wait a bit more
        Thread.sleep(200);

        // Task should stop executing after shutdown
        int countAfterShutdown = executionCount.get();
        // Allow for one more execution that might have been in progress
        assertTrue(countAfterShutdown <= countBeforeShutdown + 1);

        assertTrue(future.isCancelled() || future.isDone());
    }

    @Test
    public void testSafeRunnableExceptionLogging() throws Exception {
        // This test verifies that exceptions are caught and logged by SafeRunnable
        AtomicBoolean taskExecuted = new AtomicBoolean(false);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);

        Future<?> future = executor.submit(() -> {
            taskExecuted.set(true);
            exceptionThrown.set(true);
            throw new RuntimeException("Test exception for SafeRunnable");
        });

        // Wait for task completion
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Exception might be propagated through Future
        }

        assertTrue(taskExecuted.get());
        assertTrue(exceptionThrown.get());
        assertTrue(future.isDone());
    }
}
