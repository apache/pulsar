/**
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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.Cleanup;
import org.apache.pulsar.common.util.collections.GrowablePriorityLongPairQueue.LongPair;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class GrowablePriorityLongPairQueueTest {

    @Test
    public void testConstructor() {
        try {
            new GrowablePriorityLongPairQueue(0);
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

    }

    @Test
    public void simpleInsertions() {
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue(16);

        assertTrue(queue.isEmpty());
        queue.add(1, 1);
        assertFalse(queue.isEmpty());

        queue.add(2, 2);
        queue.add(3, 3);

        assertEquals(queue.size(), 3);

        assertEquals(queue.size(), 3);

        assertTrue(queue.remove(1, 1));
        assertEquals(queue.size(), 2);

        assertEquals(queue.size(), 2);

        queue.add(1, 1);
        assertEquals(queue.size(), 3);
        queue.add(1, 1);
        assertEquals(queue.size(), 4);
    }

    @Test
    public void testRemove() {
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue();

        assertTrue(queue.isEmpty());
        queue.add(1, 1);
        assertFalse(queue.isEmpty());

        assertFalse(queue.remove(1, 0));
        assertFalse(queue.isEmpty());
        assertTrue(queue.remove(1, 1));
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testExpandQueue() {
        int n = 16;
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue(n / 2);
        assertEquals(queue.capacity(), n / 2);
        assertEquals(queue.size(), 0);

        for (int i = 0; i < n; i++) {
            queue.add(i, 1);
        }

        assertEquals(queue.capacity(), n);
        assertEquals(queue.size(), n);
    }

    @Test
    public void testExpandRemoval() {
        int n = 16;
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue(n / 2);
        assertEquals(queue.capacity(), n / 2);
        assertEquals(queue.size(), 0);

        int insertItems = 1000 * n;
        for (int i = 0; i < insertItems; i++) {
            queue.add(i, -1);
        }

        int newSize = (int) Math.pow(2, 32 - Integer.numberOfLeadingZeros(insertItems - 1));
        assertEquals(queue.capacity(), newSize);
        assertEquals(queue.size(), insertItems);

        Set<LongPair> pairs = new HashSet<>();
        queue.forEach((first, second) -> {
            pairs.add(new LongPair(first, second));
        });

        pairs.forEach(pair -> queue.remove(pair.first, -1));
        assertEquals(queue.capacity(), newSize);
        assertEquals(queue.size(), 0);
    }

    @Test
    public void testExpandWithDeletes() {
        int n = 16;
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue(n / 2);
        assertEquals(queue.capacity(), n / 2);
        assertEquals(queue.size(), 0);

        for (int i = 0; i < n / 2; i++) {
            queue.add(i, i);
        }

        for (int i = 0; i < n / 2; i++) {
            assertTrue(queue.remove(i, i));
        }

        assertEquals(queue.capacity(), n / 2);
        assertEquals(queue.size(), 0);

        for (int i = n; i < (n); i++) {
            queue.add(i, i);
        }

        assertEquals(queue.capacity(), n / 2);
        assertEquals(queue.size(), 0);
    }

    @Test
    public void concurrentInsertions() throws Throwable {
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int N = 100_000;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                Random random = new Random();

                for (int j = 0; j < N; j++) {
                    long key = random.nextLong();
                    // Ensure keys are unique
                    key -= key % (threadIdx + 1);
                    key = Math.abs(key);
                    queue.add(key, key);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(queue.size(), N * nThreads);
    }

    @Test
    public void concurrentInsertionsAndReads() throws Throwable {
        GrowablePriorityLongPairQueue map = new GrowablePriorityLongPairQueue();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int N = 100_000;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                Random random = new Random();

                for (int j = 0; j < N; j++) {
                    long key = random.nextLong();
                    // Ensure keys are unique
                    key -= key % (threadIdx + 1);
                    key = Math.abs(key);
                    map.add(key, key);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(map.size(), N * nThreads);
    }

    @Test
    public void testIteration() {
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue();

        assertEquals(queue.items(), Collections.emptyList());

        queue.add(0l, 0l);

        assertEquals(new LongPair(0l, 0l), queue.items().iterator().next());

        queue.remove(0l, 0l);

        assertEquals(queue.items(), Collections.emptyList());

        queue.add(0l, 0l);
        queue.add(1l, 1l);
        queue.add(2l, 2l);

        List<LongPair> values = new ArrayList<>(queue.items());
        values.sort(null);
        assertEquals(values, Lists.newArrayList(new LongPair(0, 0), new LongPair(1, 1), new LongPair(2, 2)));

        queue.clear();
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testRemoval() {
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue();

        queue.add(0, 0);
        queue.add(1, 1);
        queue.add(3, 3);
        queue.add(6, 6);
        queue.add(7, 7);

        List<LongPair> values = new ArrayList<>(queue.items());
        values.sort(null);
        assertEquals(values, Lists.newArrayList(new LongPair(0, 0), new LongPair(1, 1), new LongPair(3, 3),
                new LongPair(6, 6), new LongPair(7, 7)));

        List<LongPair> removeList = new ArrayList<>();
        queue.forEach((first, second) -> {
            System.out.println(first + "," + second);
            if (first < 5) {
                removeList.add(new LongPair(first, second));
            }
        });
        removeList.forEach((pair) -> queue.remove(pair.first, pair.second));
        assertEquals(queue.size(), values.size() - 3);
        values = new ArrayList<>(queue.items());
        values.sort(null);
        assertEquals(values, Lists.newArrayList(new LongPair(6, 6), new LongPair(7, 7)));
    }

    @Test
    public void testIfRemoval() {
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue();

        queue.add(0, 0);
        queue.add(1, 1);
        queue.add(3, 3);
        queue.add(6, 6);
        queue.add(7, 7);

        List<LongPair> values = new ArrayList<>(queue.items());
        values.sort(null);
        assertEquals(values, Lists.newArrayList(new LongPair(0, 0), new LongPair(1, 1), new LongPair(3, 3),
                new LongPair(6, 6), new LongPair(7, 7)));

        int removeItems = queue.removeIf((first, second) -> first < 5);

        assertEquals(3, removeItems);
        assertEquals(queue.size(), values.size() - 3);
        values = new ArrayList<>(queue.items());
        values.sort(null);
        assertEquals(values, Lists.newArrayList(new LongPair(6, 6), new LongPair(7, 7)));
    }

    @Test
    public void testItems() {
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue();

        int n = 100;
        int limit = 10;
        for (int i = 0; i < n; i++) {
            queue.add(i, i);
        }

        Set<LongPair> items = queue.items();
        Set<LongPair> limitItems = queue.items(limit);
        assertEquals(items.size(), n);
        assertEquals(limitItems.size(), limit);

        int totalRemovedItems = queue.removeIf((first, second) -> limitItems.contains((new LongPair(first, second))));
        assertEquals(limitItems.size(), totalRemovedItems);
        assertEquals(queue.size(), n - limit);
    }

    @Test
    public void testEqualsObjects() {

        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue();

        long t1 = 1;
        long t2 = 2;
        long t1_b = 1;
        assertEquals(t1, t1_b);
        assertNotEquals(t2, t1);
        assertNotEquals(t2, t1_b);
        queue.add(t1, t1);
        assertTrue(queue.remove(t1_b, t1_b));
    }

    @Test
    public void testInsertAndRemove() throws Exception {
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue(8);
        queue.add(10, 10);
        queue.add(10, 4);
        queue.add(10, 5);
        queue.add(8, 10);
        queue.add(3, 15);
        queue.add(23, 15);
        queue.add(1, 155);
        queue.add(1, 155);
        queue.add(3, 15);
        queue.add(33, 1);
        assertEquals(queue.remove(), new LongPair(1, 155));
        assertEquals(queue.remove(), new LongPair(1, 155));
        assertEquals(queue.remove(), new LongPair(3, 15));
        assertEquals(queue.remove(), new LongPair(3, 15));
        assertEquals(queue.remove(), new LongPair(8, 10));
        assertEquals(queue.remove(), new LongPair(10, 4));
        assertEquals(queue.remove(), new LongPair(10, 5));
        assertEquals(queue.remove(), new LongPair(10, 10));
        assertEquals(queue.remove(), new LongPair(23, 15));
        assertEquals(queue.remove(), new LongPair(33, 1));
    }

    @Test
    public void testSetWithDuplicateInsert() {
        GrowablePriorityLongPairQueue queue = new GrowablePriorityLongPairQueue(1);

        assertTrue(queue.isEmpty());
        queue.add(20, 20);
        queue.add(12, 12);
        queue.add(14, 14);
        queue.add(6, 6);
        queue.add(1, 1);
        queue.add(7, 7);
        queue.add(2, 2);
        queue.add(3, 3);
        assertTrue(queue.exists(7, 7));
        assertFalse(queue.exists(7, 1));

    }

}
