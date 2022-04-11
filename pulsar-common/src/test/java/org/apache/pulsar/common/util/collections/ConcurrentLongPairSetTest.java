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
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet.LongPair;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class ConcurrentLongPairSetTest {

    @Test
    public void testConstructor() {
        try {
            ConcurrentLongPairSet.newBuilder()
                    .expectedItems(0)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentLongPairSet.newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(0)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentLongPairSet.newBuilder()
                    .expectedItems(4)
                    .concurrencyLevel(8)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void testReduceUnnecessaryExpansions() {
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .build();
        assertTrue(set.add(1, 1));
        assertTrue(set.add(2, 2));
        assertTrue(set.add(3, 3));
        assertTrue(set.add(4, 4));

        assertTrue(set.remove(1, 1));
        assertTrue(set.remove(2, 2));
        assertTrue(set.remove(3, 3));
        assertTrue(set.remove(4, 4));

        assertEquals(0, set.getUsedBucketCount());
    }

    @Test
    public void simpleInsertions() {
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder()
                .expectedItems(16)
                .build();

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
    public void testClear() {
        ConcurrentLongPairSet map = ConcurrentLongPairSet.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertTrue(map.capacity() == 4);

        assertTrue(map.add(1, 1));
        assertTrue(map.add(2, 2));
        assertTrue(map.add(3, 3));

        assertTrue(map.capacity() == 8);
        map.clear();
        assertTrue(map.capacity() == 4);
    }

    @Test
    public void testExpandAndShrink() {
        ConcurrentLongPairSet map = ConcurrentLongPairSet.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertTrue(map.capacity() == 4);

        assertTrue(map.add(1, 1));
        assertTrue(map.add(2, 2));
        assertTrue(map.add(3, 3));

        // expand hashmap
        assertTrue(map.capacity() == 8);

        assertTrue(map.remove(1, 1));
        // not shrink
        assertTrue(map.capacity() == 8);
        assertTrue(map.remove(2, 2));
        // shrink hashmap
        assertTrue(map.capacity() == 4);

        // expand hashmap
        assertTrue(map.add(4, 4));
        assertTrue(map.add(5, 5));
        assertTrue(map.capacity() == 8);

        //verify that the map does not keep shrinking at every remove() operation
        assertTrue(map.add(6, 6));
        assertTrue(map.remove(6, 6));
        assertTrue(map.capacity() == 8);
    }


    @Test
    public void testRemove() {
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder().build();

        assertTrue(set.isEmpty());
        assertTrue(set.add(1, 1));
        assertFalse(set.isEmpty());

        assertFalse(set.remove(1, 0));
        assertFalse(set.isEmpty());
        assertTrue(set.remove(1, 1));
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRehashing() {
        int n = 16;
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(set.capacity(), n);
        assertEquals(set.size(), 0);

        for (int i = 0; i < n; i++) {
            set.add(i, 1);
        }

        assertEquals(set.capacity(), 2 * n);
        assertEquals(set.size(), n);
    }

    @Test
    public void testRehashingRemoval() {
        int n = 16;
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(set.capacity(), n);
        assertEquals(set.size(), 0);

        int insertItems = 1000 * n;
        for (int i = 0; i < insertItems; i++) {
            set.add(i, -1);
        }

        int newSize = (int) Math.pow(2, 32 - Integer.numberOfLeadingZeros(insertItems - 1));
        assertEquals(set.capacity(), newSize * 2);
        assertEquals(set.size(), insertItems);

        Set<LongPair> pairs = new HashSet<>();
        set.forEach((first, second) -> {
            pairs.add(new LongPair(first, second));
        });

        pairs.forEach(pair -> set.remove(pair.first, -1));
        assertEquals(set.capacity(), newSize * 2);
        assertEquals(set.size(), 0);
    }

    @Test
    public void testRehashingWithDeletes() {
        int n = 16;
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(set.capacity(), n);
        assertEquals(set.size(), 0);

        for (int i = 0; i < n / 2; i++) {
            set.add(i, i);
        }

        for (int i = 0; i < n / 2; i++) {
            assertTrue(set.remove(i, i));
        }

        assertEquals(set.capacity(), n);
        assertEquals(set.size(), 0);

        for (int i = n; i < (2 * n); i++) {
            set.add(i, i);
        }

        assertEquals(set.capacity(), 2 * n);
        assertEquals(set.size(), n);
    }

    @Test
    public void concurrentInsertions() throws Throwable {
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder().build();
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
    public void concurrentInsertionsAndReads() throws Throwable {
        ConcurrentLongPairSet map = ConcurrentLongPairSet.newBuilder().build();
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
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder().build();

        assertEquals(set.items(), Collections.emptyList());

        set.add(0l, 0l);

        assertEquals(new LongPair(0l, 0l), set.items().iterator().next());

        set.remove(0l, 0l);

        assertEquals(set.items(), Collections.emptyList());

        set.add(0l, 0l);
        set.add(1l, 1l);
        set.add(2l, 2l);

        List<LongPair> values = new ArrayList<>(set.items());
        values.sort(null);
        assertEquals(values, Lists.newArrayList(new LongPair(0, 0), new LongPair(1, 1), new LongPair(2, 2)));

        set.clear();
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRemoval() {
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder().build();

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
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder().build();

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
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder().build();

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
    public void testHashConflictWithDeletion() {
        final int Buckets = 16;
        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder()
                .expectedItems(Buckets)
                .concurrencyLevel(1)
                .build();

        // Pick 2 keys that fall into the same bucket
        long key1 = 1;
        long key2 = 538515;

        int bucket1 = ConcurrentLongPairSet.signSafeMod(ConcurrentLongPairSet.hash(key1, key1), Buckets);
        int bucket2 = ConcurrentLongPairSet.signSafeMod(ConcurrentLongPairSet.hash(key2, key2), Buckets);

        assertEquals(bucket1, bucket2);

        assertTrue(set.add(key1, key1));
        assertTrue(set.add(key2, key2));
        assertEquals(set.size(), 2);

        assertTrue(set.remove(key1, key1));
        assertEquals(set.size(), 1);

        assertTrue(set.add(key1, key1));
        assertEquals(set.size(), 2);

        assertTrue(set.remove(key1, key1));
        assertEquals(set.size(), 1);

        assertFalse(set.add(key2, key2));
        assertTrue(set.contains(key2, key2));

        assertEquals(set.size(), 1);
        assertTrue(set.remove(key2, key2));
        assertTrue(set.isEmpty());
    }

    @Test
    public void testEqualsObjects() {

        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder().build();

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

        ConcurrentLongPairSet set = ConcurrentLongPairSet.newBuilder().build();

        set.add(0, 0);
        set.add(1, 1);
        set.add(3, 3);
        final String toString = "{[3:3], [0:0], [1:1]}";
        assertEquals(set.toString(), toString);
    }

}
