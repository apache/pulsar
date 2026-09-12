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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class ListenerTaskSchedulerTest {
    @Test
    public void drainsArrivalQueuedBehindAnAlreadyScheduledDrain() {
        QueuedExecutor executor = new QueuedExecutor();
        Queue<Integer> arrivals = new ArrayDeque<>();
        List<Integer> received = new ArrayList<>();
        AtomicInteger runs = new AtomicInteger();
        ListenerTaskScheduler scheduler = new ListenerTaskScheduler(executor, () -> {
            runs.incrementAndGet();
            Integer arrival;
            while ((arrival = arrivals.poll()) != null) {
                received.add(arrival);
            }
        });
        executor.execute(() -> arrivals.add(1));
        scheduler.trigger();
        executor.execute(() -> arrivals.add(2));
        scheduler.trigger();
        assertEquals(executor.tasks.size(), 3);
        executor.runNext();
        executor.runNext();
        assertEquals(received, List.of(1));
        executor.runAll();
        assertEquals(received, List.of(1, 2));
        assertEquals(runs.get(), 2);
    }

    @Test
    public void coalescesABurstAndReturnsToIdle() {
        QueuedExecutor executor = new QueuedExecutor();
        AtomicInteger runs = new AtomicInteger();
        ListenerTaskScheduler scheduler = new ListenerTaskScheduler(executor, runs::incrementAndGet);
        for (int i = 0; i < 1000; i++) {
            scheduler.trigger();
        }
        assertEquals(executor.tasks.size(), 1);
        executor.runAll();
        assertEquals(runs.get(), 2);
        scheduler.trigger();
        executor.runAll();
        assertEquals(runs.get(), 3);
    }

    @Test
    public void triggerDuringDrainSchedulesAnotherTurn() {
        QueuedExecutor executor = new QueuedExecutor();
        AtomicInteger runs = new AtomicInteger();
        AtomicReference<ListenerTaskScheduler> reference = new AtomicReference<>();
        ListenerTaskScheduler scheduler = new ListenerTaskScheduler(executor, () -> {
            if (runs.incrementAndGet() == 1) {
                reference.get().trigger();
            }
        });
        reference.set(scheduler);
        scheduler.trigger();
        executor.runAll();
        assertEquals(runs.get(), 2);
    }

    @Test
    public void drainFailurePreservesLaterTriggers() {
        QueuedExecutor executor = new QueuedExecutor();
        AtomicInteger runs = new AtomicInteger();
        ListenerTaskScheduler scheduler = new ListenerTaskScheduler(executor, () -> {
            if (runs.incrementAndGet() == 1) {
                throw new IllegalStateException("drain failed");
            }
        });
        scheduler.trigger();
        scheduler.trigger();
        expectThrows(IllegalStateException.class, executor::runNext);
        executor.runAll();
        assertEquals(runs.get(), 2);
    }

    @Test
    public void rejectedSubmissionAllowsLaterRetry() {
        QueuedExecutor executor = new QueuedExecutor();
        AtomicInteger submissions = new AtomicInteger();
        AtomicInteger runs = new AtomicInteger();
        ListenerTaskScheduler scheduler = new ListenerTaskScheduler(task -> {
            if (submissions.incrementAndGet() == 1) {
                throw new RejectedExecutionException("first submission rejected");
            }
            executor.execute(task);
        }, runs::incrementAndGet);
        expectThrows(RejectedExecutionException.class, scheduler::trigger);
        scheduler.trigger();
        executor.runAll();
        assertEquals(runs.get(), 1);
    }

    @Test
    public void rejectedFollowUpAllowsLaterRetry() {
        QueuedExecutor executor = new QueuedExecutor();
        AtomicInteger submissions = new AtomicInteger();
        AtomicInteger runs = new AtomicInteger();
        ListenerTaskScheduler scheduler = new ListenerTaskScheduler(task -> {
            if (submissions.incrementAndGet() == 2) {
                throw new RejectedExecutionException("follow-up submission rejected");
            }
            executor.execute(task);
        }, runs::incrementAndGet);
        scheduler.trigger();
        scheduler.trigger();
        expectThrows(RejectedExecutionException.class, executor::runNext);
        assertEquals(runs.get(), 1);
        assertTrue(executor.tasks.isEmpty());
        scheduler.trigger();
        executor.runAll();
        assertEquals(runs.get(), 2);
    }

    @Test
    public void drainFailureReturnsToIdle() {
        QueuedExecutor executor = new QueuedExecutor();
        AtomicInteger runs = new AtomicInteger();
        ListenerTaskScheduler scheduler = new ListenerTaskScheduler(executor, () -> {
            if (runs.incrementAndGet() == 1) {
                throw new IllegalStateException("drain failed");
            }
        });
        scheduler.trigger();
        expectThrows(IllegalStateException.class, executor::runNext);
        assertTrue(executor.tasks.isEmpty());
        scheduler.trigger();
        executor.runAll();
        assertEquals(runs.get(), 2);
    }

    @Test(invocationCount = 5, timeOut = 30000)
    public void concurrentNotificationsDoNotLoseArrivals() throws Exception {
        int producerCount = 4;
        int perProducer = 5000;
        int total = producerCount * perProducer;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ExecutorService producers = Executors.newFixedThreadPool(producerCount);
        Queue<Integer> arrivals = new ArrayDeque<>();
        BitSet received = new BitSet(total);
        AtomicInteger duplicates = new AtomicInteger();
        CountDownLatch complete = new CountDownLatch(total);
        CountDownLatch start = new CountDownLatch(1);
        ListenerTaskScheduler scheduler = new ListenerTaskScheduler(executor, () -> {
            Integer id;
            while ((id = arrivals.poll()) != null) {
                if (received.get(id)) {
                    duplicates.incrementAndGet();
                }
                received.set(id);
                complete.countDown();
            }
        });
        try {
            List<Future<?>> jobs = new ArrayList<>();
            for (int producer = 0; producer < producerCount; producer++) {
                int first = producer * perProducer;
                jobs.add(producers.submit(() -> {
                    start.await();
                    for (int i = 0; i < perProducer; i++) {
                        int id = first + i;
                        executor.execute(() -> arrivals.add(id));
                        scheduler.trigger();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> job : jobs) {
                job.get(10, TimeUnit.SECONDS);
            }
            assertTrue(complete.await(10, TimeUnit.SECONDS), "An arrival was left without a drain");
            executor.submit(() -> {
                assertEquals(received.cardinality(), total);
                assertEquals(duplicates.get(), 0);
                assertTrue(arrivals.isEmpty());
            }).get(10, TimeUnit.SECONDS);
        } finally {
            start.countDown();
            producers.shutdownNow();
            executor.shutdownNow();
            assertTrue(producers.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        void runNext() {
            tasks.remove().run();
        }

        void runAll() {
            while (!tasks.isEmpty()) {
                runNext();
            }
        }
    }
}
