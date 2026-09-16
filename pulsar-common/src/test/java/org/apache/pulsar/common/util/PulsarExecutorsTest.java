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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.common.util.PulsarExecutors.AutoShutdownExecutorService;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarExecutorsTest {
    @DataProvider
    public Object[][] autoShutdownModes() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "autoShutdownModes", timeOut = 15000)
    public void unregistersOnlyAfterAcceptedTasksFinish(boolean autoShutdownOnGc) throws Exception {
        Queue<Runnable> passes = new ArrayDeque<>();
        ExecutorQueueTrimmer group = new ExecutorQueueTrimmer(1000, 1250, passes::add);
        var executor = PulsarExecutors.newSingleThreadExecutor(
                Executors.defaultThreadFactory(), autoShutdownOnGc, group);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        try {
            Future<?> running = executor.submit(() -> {
                started.countDown();
                finish.await();
                return null;
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < 2048; i++) {
                executor.execute(() -> { });
            }
            executor.shutdown();
            assertEquals(group.registrations().size(), 1);
            assertEquals(group.retainedCapacity(), 2048L);
            finish.countDown();
            running.get(5, TimeUnit.SECONDS);
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(group.registrations().isEmpty());
            assertEquals(group.retainedCapacity(), 0L);
            passes.remove().run();
            assertTrue(passes.isEmpty());
        } finally {
            finish.countDown();
            executor.shutdownNow();
        }
    }

    @Test(dataProvider = "autoShutdownModes", timeOut = 15000)
    public void standardTaskMethodsPreserveResultsAndFailures(boolean autoShutdownOnGc) throws Exception {
        var executor = PulsarExecutors.newSingleThreadExecutor(Executors.defaultThreadFactory(), autoShutdownOnGc);
        try {
            var results = executor.<Integer>invokeAll(List.of(() -> 7, () -> 11));
            assertEquals(results.get(0).get().intValue(), 7);
            assertEquals(results.get(1).get().intValue(), 11);
            int successful = executor.invokeAny(List.of(() -> {
                throw new IllegalStateException("expected task failure");
            }, () -> 13));
            assertEquals(successful, 13);
            assertEquals(executor.submit(() -> { }, 17).get().intValue(), 17);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(timeOut = 15000)
    public void cleanupDrainsAcceptedTasksWithoutInterruptingWorker() throws Exception {
        AutoShutdownExecutorService executor =
                (AutoShutdownExecutorService) PulsarExecutors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        try {
            Future<?> running = executor.submit(() -> {
                started.countDown();
                finish.await();
                return null;
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            Future<Integer> queued = executor.submit(() -> 42);
            executor.runCleanup();
            assertTrue(executor.isShutdown());
            assertFalse(running.isDone());
            assertThrows(RejectedExecutionException.class, () -> executor.submit(() -> 0));
            finish.countDown();
            running.get(5, TimeUnit.SECONDS);
            assertEquals(queued.get(5, TimeUnit.SECONDS).intValue(), 42);
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            finish.countDown();
            executor.shutdownNow();
        }
    }

    @Test(dataProvider = "autoShutdownModes", timeOut = 15000)
    public void preservesLazyWorkerCreationAndOrder(boolean autoShutdownOnGc) throws Exception {
        AtomicReference<Thread> worker = new AtomicReference<>();
        ThreadFactory factory = task -> {
            Thread thread = Executors.defaultThreadFactory().newThread(task);
            worker.set(thread);
            return thread;
        };
        var executor = PulsarExecutors.newSingleThreadExecutor(factory, autoShutdownOnGc);
        try {
            assertNull(worker.get());
            assertSame(executor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS), worker.get());
            AtomicInteger sequence = new AtomicInteger();
            List<Future<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                futures.add(executor.submit(sequence::getAndIncrement));
            }
            for (int i = 0; i < futures.size(); i++) {
                assertEquals(futures.get(i).get(5, TimeUnit.SECONDS).intValue(), i);
            }
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(dataProvider = "autoShutdownModes", timeOut = 15000)
    public void shutdownNowReturnsPendingTasksAndInterruptsWorker(boolean autoShutdownOnGc) throws Exception {
        var executor = PulsarExecutors.newSingleThreadExecutor(Executors.defaultThreadFactory(), autoShutdownOnGc);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch blocked = new CountDownLatch(1);
        AtomicInteger queuedExecutions = new AtomicInteger();
        Runnable queued = queuedExecutions::incrementAndGet;
        try {
            Future<Boolean> running = executor.submit(() -> {
                started.countDown();
                try {
                    blocked.await();
                    return false;
                } catch (InterruptedException expected) {
                    return true;
                }
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            executor.execute(queued);
            List<Runnable> pending = executor.shutdownNow();
            assertEquals(pending.size(), 1);
            assertSame(pending.get(0), queued);
            assertTrue(running.get(5, TimeUnit.SECONDS));
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            assertEquals(queuedExecutions.get(), 0);
        } finally {
            blocked.countDown();
            executor.shutdownNow();
        }
    }
}
