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
import static org.testng.Assert.assertTrue;
import java.lang.ref.Reference;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.testng.annotations.Test;

public class ExecutorQueueTrimmerTest {
    @Test
    public void retriesAfterQueuesDrainWithoutFurtherGrowth() {
        Queue<Runnable> passes = new ArrayDeque<>();
        ExecutorQueueTrimmer group = new ExecutorQueueTrimmer(1000, 1250, passes::add);
        GrowableArrayBlockingQueue<Integer> queue = group.newQueue();
        fill(queue, 2048);
        assertEquals(passes.size(), 1);
        passes.remove().run();
        assertEquals(queue.capacity(), 2048);
        assertEquals(passes.size(), 1);
        queue.clear();
        passes.remove().run();
        assertEquals(queue.capacity(), 64);
        assertEquals(group.retainedCapacity(), 64L);
        assertTrue(passes.isEmpty());
    }

    @Test
    public void trimsLargestSavingsFirstAndStopsAtLowWatermark() {
        Queue<Runnable> passes = new ArrayDeque<>();
        ExecutorQueueTrimmer group = new ExecutorQueueTrimmer(1000, 1250, passes::add);
        GrowableArrayBlockingQueue<Integer> small = group.newQueue();
        GrowableArrayBlockingQueue<Integer> large = group.newQueue();
        fill(small, 512);
        fill(large, 512);
        assertTrue(passes.isEmpty()); // Combined storage is between 4% and 5%; no activation yet.
        fill(large, 1);
        assertEquals(passes.size(), 1);
        small.clear();
        large.clear();
        passes.remove().run();
        assertEquals(large.capacity(), 64);
        assertEquals(small.capacity(), 512);
        assertEquals(group.retainedCapacity(), 576L);
        assertTrue(passes.isEmpty());
    }

    @Test
    public void remainsActiveBetweenWatermarks() {
        Queue<Runnable> passes = new ArrayDeque<>();
        ExecutorQueueTrimmer group = new ExecutorQueueTrimmer(1000, 1250, passes::add);
        GrowableArrayBlockingQueue<Integer> busy = group.newQueue();
        GrowableArrayBlockingQueue<Integer> idle = group.newQueue();
        fill(busy, 1024);
        fill(idle, 256);
        idle.clear();
        passes.remove().run();
        assertEquals(group.retainedCapacity(), 1088L);
        assertEquals(passes.size(), 1); // Below 5%, but still above the 4% target.
        busy.clear();
        passes.remove().run();
        assertEquals(busy.capacity(), 64);
        assertTrue(passes.isEmpty());
    }

    @Test
    public void unregistersWithoutRetainingPeakStorageOrDoubleCounting() {
        Queue<Runnable> passes = new ArrayDeque<>();
        ExecutorQueueTrimmer group = new ExecutorQueueTrimmer(1000, 1250, passes::add);
        GrowableArrayBlockingQueue<Integer> queue = group.newQueue();
        fill(queue, 2048);
        queue.clear();
        group.unregister(queue);
        assertEquals(queue.capacity(), 64);
        assertEquals(group.retainedCapacity(), 0L);
        assertTrue(group.registrations().isEmpty());
        group.unregister(queue);
        assertEquals(group.retainedCapacity(), 0L);
        passes.remove().run();
        assertTrue(passes.isEmpty());
    }

    @Test
    public void referenceQueueCleanupRemovesCollectedAccounting() {
        Queue<Runnable> passes = new ArrayDeque<>();
        ExecutorQueueTrimmer group = new ExecutorQueueTrimmer(1000, 1250, passes::add);
        GrowableArrayBlockingQueue<Integer> queue = group.newQueue();
        fill(queue, 2048);
        // Exercise the ReferenceQueue path deterministically, without relying on GC timing.
        Reference<?> registration = group.registrations().get(0);
        registration.clear();
        assertTrue(registration.enqueue());
        passes.remove().run();
        assertEquals(group.retainedCapacity(), 0L);
        assertTrue(group.registrations().isEmpty());
        assertTrue(passes.isEmpty());
        assertEquals(queue.capacity(), 2048);
    }

    @Test
    public void boundsSuccessfulTrimsPerPass() {
        Queue<Runnable> passes = new ArrayDeque<>();
        ExecutorQueueTrimmer group = new ExecutorQueueTrimmer(10_000, 12_500, passes::add);
        List<GrowableArrayBlockingQueue<Integer>> queues = new ArrayList<>();
        for (int i = 0; i < 75; i++) {
            GrowableArrayBlockingQueue<Integer> queue = group.newQueue();
            fill(queue, 1024);
            queue.clear();
            queues.add(queue);
        }
        assertEquals(passes.size(), 1);
        passes.remove().run();
        assertEquals(queues.stream().filter(queue -> queue.capacity() == 64).count(), 64L);
        assertEquals(passes.size(), 1);
        passes.remove().run();
        assertTrue(group.retainedCapacity() <= 10_000);
        assertTrue(passes.isEmpty());
    }

    @Test
    public void ordinaryQueuesDoNotJoinTheGroup() {
        Queue<Runnable> passes = new ArrayDeque<>();
        ExecutorQueueTrimmer group = new ExecutorQueueTrimmer(1000, 1250, passes::add);
        GrowableArrayBlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>();
        fill(queue, 2048);
        queue.clear();
        assertTrue(group.registrations().isEmpty());
        assertTrue(passes.isEmpty());
        group.unregister(queue);
        assertEquals(queue.capacity(), 2048);
    }

    @Test
    public void stopsWhenOnlyMinimumCapacityRemains() {
        Queue<Runnable> passes = new ArrayDeque<>();
        ExecutorQueueTrimmer group = new ExecutorQueueTrimmer(64, 128, passes::add);
        List<GrowableArrayBlockingQueue<Integer>> queues =
                List.of(group.newQueue(), group.newQueue(), group.newQueue());
        assertTrue(passes.isEmpty());
        fill(queues.get(0), 65);
        assertEquals(passes.size(), 1);
        queues.get(0).clear();
        passes.remove().run();
        assertEquals(group.retainedCapacity(), 192L);
        assertTrue(passes.isEmpty()); // No useful work remains, even though the floor exceeds the budget.
        fill(queues.get(1), 65);
        assertEquals(passes.size(), 1);
    }

    @Test(timeOut = 30000)
    public void concurrentResizesAndPassesKeepAccountingBalanced() throws Exception {
        Queue<Runnable> passes = new ConcurrentLinkedQueue<>();
        ExecutorQueueTrimmer group = new ExecutorQueueTrimmer(500, 1000, passes::add);
        List<GrowableArrayBlockingQueue<Integer>> queues = List.of(group.newQueue(), group.newQueue());
        var workers = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean stopped = new AtomicBoolean();
        try {
            Future<?> maintenance = workers.submit(() -> {
                start.await();
                while (!stopped.get()) {
                    Runnable pass = passes.poll();
                    if (pass != null) {
                        pass.run();
                    } else {
                        Thread.yield();
                    }
                }
                return null;
            });
            List<Future<?>> producers = new ArrayList<>();
            for (GrowableArrayBlockingQueue<Integer> queue : queues) {
                producers.add(workers.submit(() -> {
                    start.await();
                    for (int round = 0; round < 100; round++) {
                        fill(queue, 2048);
                        queue.clear();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> producer : producers) {
                producer.get(20, TimeUnit.SECONDS);
            }
            stopped.set(true);
            maintenance.get(5, TimeUnit.SECONDS);
            assertEquals(group.retainedCapacity(), queues.stream().mapToLong(queue -> queue.capacity()).sum());
            queues.forEach(group::unregister);
            assertEquals(group.retainedCapacity(), 0L);
        } finally {
            start.countDown();
            stopped.set(true);
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static void fill(GrowableArrayBlockingQueue<Integer> queue, int count) {
        for (int i = 0; i < count; i++) {
            queue.add(i);
        }
    }
}
