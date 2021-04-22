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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.Cleanup;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet.LongPair;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class ConcurrentSortedLongPairSetTest {

    @Test
    public void simpleInsertions() {
        LongPairSet set = new ConcurrentSortedLongPairSet(16);

        assertTrue(set.isEmpty());
        assertTrue(set.add(1, 1));
        assertFalse(set.isEmpty());

        assertTrue(set.add(2, 2));
        assertTrue(set.add(3, 3));

        assertEquals(set.size(), 3);

        assertTrue(set.contains(1, 1));
        assertEquals(set.size(), 3);

        assertTrue(set.remove(1, 1));
        assertEquals(set.size(), 2);
        assertFalse(set.contains(1, 1));
        assertFalse(set.contains(5, 5));
        assertEquals(set.size(), 2);

        assertTrue(set.add(1, 1));
        assertEquals(set.size(), 3);
        assertFalse(set.add(1, 1));
        assertEquals(set.size(), 3);
    }

    @Test
    public void testRemove() {
        LongPairSet set = new ConcurrentSortedLongPairSet(16);

        assertTrue(set.isEmpty());
        assertTrue(set.add(1, 1));
        assertFalse(set.isEmpty());

        assertFalse(set.remove(1, 0));
        assertFalse(set.isEmpty());
        assertTrue(set.remove(1, 1));
        assertTrue(set.isEmpty());
    }

    @Test
    public void concurrentInsertions() throws Throwable {
        LongPairSet set = new ConcurrentSortedLongPairSet(16);
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 8;
        final int N = 1000;

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
                    set.add(key, key);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(set.size(), N * nThreads);
    }

    @Test
    public void testIteration() {
        LongPairSet set = new ConcurrentSortedLongPairSet(16);

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                set.add(i, j);
            }
        }

        for (int i = 0; i < 10; i++) {
            final int firstKey = i;
            Set<LongPair> longSetResult = set.items(10);
            assertEquals(longSetResult.size(), 10);
            longSetResult.forEach(longPair -> {
                assertEquals(firstKey, longPair.first);
            });
            set.removeIf((item1, item2) -> item1 == firstKey);
        }

    }

    @Test
    public void testRemoval() {
        LongPairSet set = new ConcurrentSortedLongPairSet(16);

        set.add(0, 0);
        set.add(1, 1);
        set.add(3, 3);
        set.add(6, 6);
        set.add(7, 7);

        List<LongPair> values = new ArrayList<>(set.items());
        values.sort(null);
        assertEquals(values, Lists.newArrayList(new LongPair(0, 0), new LongPair(1, 1), new LongPair(3, 3),
                new LongPair(6, 6), new LongPair(7, 7)));

        set.forEach((first, second) -> {
            if (first < 5) {
                set.remove(first, second);
            }
        });
        assertEquals(set.size(), values.size() - 3);
        values = new ArrayList<>(set.items());
        values.sort(null);
        assertEquals(values, Lists.newArrayList(new LongPair(6, 6), new LongPair(7, 7)));
    }

    @Test
    public void testIfRemoval() {
        LongPairSet set = new ConcurrentSortedLongPairSet(16, 1, 1);

        set.add(0, 0);
        set.add(1, 1);
        set.add(3, 3);
        set.add(6, 6);
        set.add(7, 7);

        List<LongPair> values = new ArrayList<>(set.items());
        values.sort(null);
        assertEquals(values, Lists.newArrayList(new LongPair(0, 0), new LongPair(1, 1), new LongPair(3, 3),
                new LongPair(6, 6), new LongPair(7, 7)));

        int removeItems = set.removeIf((first, second) -> first < 5);

        assertEquals(3, removeItems);
        assertEquals(set.size(), values.size() - 3);
        values = new ArrayList<>(set.items());
        values.sort(null);
        assertEquals(values, Lists.newArrayList(new LongPair(6, 6), new LongPair(7, 7)));
    }

    @Test
    public void testItems() {
        LongPairSet set = new ConcurrentSortedLongPairSet(16);

        int n = 100;
        int limit = 10;
        for (int i = 0; i < n; i++) {
            set.add(i, i);
        }

        Set<LongPair> items = set.items();
        Set<LongPair> limitItems = set.items(limit);
        assertEquals(items.size(), n);
        assertEquals(limitItems.size(), limit);

        int totalRemovedItems = set.removeIf((first, second) -> limitItems.contains((new LongPair(first, second))));
        assertEquals(limitItems.size(), totalRemovedItems);
        assertEquals(set.size(), n - limit);
    }

    @Test
    public void testEqualsObjects() {

        LongPairSet set = new ConcurrentSortedLongPairSet(16);

        long t1 = 1;
        long t2 = 2;
        long t1_b = 1;
        assertEquals(t1, t1_b);
        assertNotEquals(t2, t1);
        assertNotEquals(t2, t1_b);

        set.add(t1, t1);
        assertTrue(set.contains(t1, t1));
        assertTrue(set.contains(t1_b, t1_b));
        assertFalse(set.contains(t2, t2));

        assertTrue(set.remove(t1_b, t1_b));
        assertFalse(set.contains(t1, t1));
        assertFalse(set.contains(t1_b, t1_b));
    }

    @Test
    public void testToString() {

        LongPairSet set = new ConcurrentSortedLongPairSet(16);

        set.add(0, 0);
        set.add(1, 1);
        set.add(3, 3);
        final String toString = "{[0:0], [1:1], [3:3]}";
        System.out.println(set.toString());
        assertEquals(set.toString(), toString);
    }

}
