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
package org.apache.pulsar.common.util.collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;

public class GrowableArrayBlockingQueueTrimTest {
    @Test
    public void trimsWrappedContentsAndCanGrowAgain() {
        GrowableArrayBlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(1024);
        for (int i = 0; i < 1000; i++) {
            queue.add(i);
        }
        for (int i = 0; i < 950; i++) {
            assertEquals(queue.poll().intValue(), i);
        }
        for (int i = 1000; i < 1100; i++) {
            queue.add(i);
        }
        // 150 retained elements straddle the old array boundary. Round 2 * 150 up to 512.
        assertEquals(queue.trim(), 512);
        assertEquals(queue.capacity(), 512);
        assertEquals(queue.size(), 150);
        for (int i = 1100; i < 2000; i++) {
            queue.add(i);
        }
        for (int i = 950; i < 2000; i++) {
            assertEquals(queue.poll().intValue(), i);
        }
        assertTrue(queue.isEmpty());
        queue.trim();
        assertEquals(queue.capacity(), 64);
        assertEquals(queue.trim(), 0);
    }

    @Test
    public void preservesHeadroomAndSmallQueues() {
        GrowableArrayBlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(256);
        for (int i = 0; i < 65; i++) {
            queue.add(i);
        }
        assertEquals(queue.trim(), 0);
        queue.poll();
        assertEquals(queue.trim(), 128);
        assertEquals(queue.capacity(), 128);
        assertEquals(queue.trim(), 0);
        queue.clear();
        assertEquals(queue.trim(), 64);
        GrowableArrayBlockingQueue<Integer> small = new GrowableArrayBlockingQueue<>(4);
        assertEquals(small.trim(), 0);
        assertEquals(small.capacity(), 4); // Trimming must never grow an existing smaller queue.
    }

    @Test(timeOut = 15000)
    public void skipsBusyLocksRatherThanBlocking() throws Exception {
        GrowableArrayBlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(1024);
        queue.add(1);
        CountDownLatch holdingLocks = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> task = worker.submit(() -> queue.forEach(value -> {
                holdingLocks.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }));
            assertTrue(holdingLocks.await(5, TimeUnit.SECONDS));
            assertEquals(queue.trim(), 0);
            release.countDown();
            task.get(5, TimeUnit.SECONDS);
            assertEquals(queue.trim(), 960);
            assertEquals(queue.poll().intValue(), 1);
        } finally {
            release.countDown();
            worker.shutdownNow();
        }
    }

    @Test(timeOut = 30000)
    public void concurrentProducersConsumerAndTrimmingPreserveEveryElementInOrder() throws Exception {
        GrowableArrayBlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(16384);
        ExecutorService workers = Executors.newFixedThreadPool(4);
        AtomicBoolean stopTrimming = new AtomicBoolean();
        CountDownLatch start = new CountDownLatch(1);
        int messages = 20000;
        try {
            Future<?> trimmer = workers.submit(() -> {
                start.await();
                while (!stopTrimming.get()) {
                    queue.trim();
                    Thread.yield();
                }
                return null;
            });
            List<Future<?>> producers = new ArrayList<>();
            for (int producer = 0; producer < 2; producer++) {
                int id = producer;
                producers.add(workers.submit(() -> {
                    start.await();
                    for (int i = 0; i < messages; i++) {
                        queue.put(i * 2 + id);
                    }
                    return null;
                }));
            }
            Future<?> consumer = workers.submit(() -> {
                start.await();
                int[] next = new int[2];
                for (int i = 0; i < messages * 2; i++) {
                    Integer value = queue.poll(5, TimeUnit.SECONDS);
                    assertTrue(value != null, "Missing queued element");
                    int producer = value % 2;
                    assertEquals(value / 2, next[producer]++);
                }
                assertEquals(next, new int[]{messages, messages});
                return null;
            });
            start.countDown();
            for (Future<?> producer : producers) {
                producer.get(20, TimeUnit.SECONDS);
            }
            consumer.get(20, TimeUnit.SECONDS);
            stopTrimming.set(true);
            trimmer.get(5, TimeUnit.SECONDS);
            assertTrue(queue.isEmpty());
            queue.trim();
            assertEquals(queue.capacity(), 64);
        } finally {
            start.countDown();
            stopTrimming.set(true);
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
