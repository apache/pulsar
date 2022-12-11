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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap.LongPair;
import org.testng.annotations.Test;

/**
 * Test the concurrent long-long pair hashmap class.
 */
public class ConcurrentLongLongPairHashMapTest {

    @Test
    public void testConstructor() {
        try {
             ConcurrentLongLongPairHashMap.newBuilder()
                    .expectedItems(0)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentLongLongPairHashMap.newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(0)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentLongLongPairHashMap.newBuilder()
                    .expectedItems(4)
                    .concurrencyLevel(8)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void simpleInsertions() {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .expectedItems(16)
                .build();
        assertTrue(map.isEmpty());
        assertTrue(map.put(1, 1, 11, 11));
        assertFalse(map.isEmpty());

        assertTrue(map.put(2, 2, 22, 22));
        assertTrue(map.put(3, 3, 33, 33));

        assertEquals(map.size(), 3);

        assertEquals(map.get(1, 1), new LongPair(11, 11));
        assertEquals(map.size(), 3);

        assertTrue(map.remove(1, 1));
        assertEquals(map.size(), 2);
        assertNull(map.get(1, 1));
        assertNull(map.get(5, 5));
        assertEquals(map.size(), 2);

        assertTrue(map.put(1, 1, 11, 11));
        assertEquals(map.size(), 3);
        assertTrue(map.put(1, 1, 111, 111));
        assertEquals(map.size(), 3);
    }

    @Test
    public void testRemove() {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap
                .newBuilder()
                .build();

        assertTrue(map.isEmpty());
        assertTrue(map.put(1, 1, 11, 11));
        assertFalse(map.isEmpty());

        assertFalse(map.remove(0, 0));
        assertFalse(map.remove(1, 1, 111, 111));

        assertFalse(map.isEmpty());
        assertTrue(map.remove(1, 1, 11, 11));
        assertTrue(map.isEmpty());
    }

    @Test
    public void testClear() {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertEquals(map.capacity(), 4);

        assertTrue(map.put(1, 1, 11, 11));
        assertTrue(map.put(2, 2, 22, 22));
        assertTrue(map.put(3, 3, 33, 33));

        assertEquals(map.capacity(), 8);
        map.clear();
        assertEquals(map.capacity(), 4);
    }

    @Test
    public void testExpandAndShrink() {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertTrue(map.put(1, 1, 11, 11));
        assertTrue(map.put(2, 2, 22, 22));
        assertTrue(map.put(3, 3, 33, 33));

        // expand hashmap
        assertEquals(map.capacity(), 8);

        assertTrue(map.remove(1, 1, 11, 11));
        // not shrink
        assertEquals(map.capacity(), 8);
        assertTrue(map.remove(2, 2, 22, 22));
        // shrink hashmap
        assertEquals(map.capacity(), 4);

        // expand hashmap
        assertTrue(map.put(4, 4, 44, 44));
        assertTrue(map.put(5, 5, 55, 55));
        assertEquals(map.capacity(), 8);

        //verify that the map does not keep shrinking at every remove() operation
        assertTrue(map.put(6, 6, 66, 66));
        assertTrue(map.remove(6, 6, 66, 66));
        assertEquals(map.capacity(), 8);
    }

    @Test
    public void testExpandShrinkAndClear() {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        final long initCapacity = map.capacity();
        assertTrue(map.put(1, 1, 11, 11));
        assertTrue(map.put(2, 2, 22, 22));
        assertTrue(map.put(3, 3, 33, 33));

        // expand hashmap
        assertTrue(map.capacity() == 8);

        assertTrue(map.remove(1, 1, 11, 11));
        // not shrink
        assertTrue(map.capacity() == 8);
        assertTrue(map.remove(2, 2, 22, 22));
        // shrink hashmap
        assertTrue(map.capacity() == 4);

        assertTrue(map.remove(3, 3, 33, 33));
        // Will not shrink the hashmap again because shrink capacity is less than initCapacity
        // current capacity is equal than the initial capacity
        assertTrue(map.capacity() == initCapacity);
        map.clear();
        // after clear, because current capacity is equal than the initial capacity, so not shrinkToInitCapacity
        assertTrue(map.capacity() == initCapacity);
    }

    @Test
    public void testNegativeUsedBucketCount() {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        map.put(0, 0, 0, 0);
        assertEquals(map.getUsedBucketCount(), 1);
        map.put(0, 0, 1, 1);
        assertEquals(map.getUsedBucketCount(), 1);
        map.remove(0, 0);
        assertEquals(map.getUsedBucketCount(), 0);
        map.remove(0, 0);
        assertEquals(map.getUsedBucketCount(), 0);
    }

    @Test
    public void testRehashing() {
        int n = 16;
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(map.capacity(), n);
        assertEquals(map.size(), 0);

        for (int i = 0; i < n; i++) {
            map.put(i, i, i, i);
        }

        assertEquals(map.capacity(), 2 * n);
        assertEquals(map.size(), n);
    }

    @Test
    public void testRehashingWithDeletes() {
        int n = 16;
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(map.capacity(), n);
        assertEquals(map.size(), 0);

        for (int i = 0; i < n / 2; i++) {
            map.put(i, i, i, i);
        }

        for (int i = 0; i < n / 2; i++) {
            map.remove(i, i);
        }

        for (int i = n; i < (2 * n); i++) {
            map.put(i, i, i, i);
        }

        assertEquals(map.capacity(), 2 * n);
        assertEquals(map.size(), n);
    }

    @Test
    public void concurrentInsertions() throws Throwable {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .build();
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int n = 100_000;
        long value = 55;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                Random random = new Random();

                for (int j = 0; j < n; j++) {
                    long key1 = Math.abs(random.nextLong());
                    // Ensure keys are uniques
                    key1 -= key1 % (threadIdx + 1);

                    long key2 = Math.abs(random.nextLong());
                    // Ensure keys are uniques
                    key2 -= key2 % (threadIdx + 1);

                    map.put(key1, key2, value, value);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(map.size(), n * nThreads);

        executor.shutdown();
    }

    @Test
    public void concurrentInsertionsAndReads() throws Throwable {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .build();
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int n = 100_000;
        final long value = 55;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                Random random = new Random();

                for (int j = 0; j < n; j++) {
                    long key1 = Math.abs(random.nextLong());
                    // Ensure keys are uniques
                    key1 -= key1 % (threadIdx + 1);

                    long key2 = Math.abs(random.nextLong());
                    // Ensure keys are uniques
                    key2 -= key2 % (threadIdx + 1);

                    map.put(key1, key2, value, value);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(map.size(), n * nThreads);

        executor.shutdown();
    }

    @Test
    public void testIteration() {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .build();

        assertEquals(map.keys(), Collections.emptyList());
        assertEquals(map.values(), Collections.emptyList());

        map.put(0, 0, 0, 0);

        assertEquals(map.keys(), Lists.newArrayList(new LongPair(0, 0)));
        assertEquals(map.values(), Lists.newArrayList(new LongPair(0, 0)));

        map.remove(0, 0);

        assertEquals(map.keys(), Collections.emptyList());
        assertEquals(map.values(), Collections.emptyList());

        map.put(0, 0, 0, 0);
        map.put(1, 1, 11, 11);
        map.put(2, 2, 22, 22);

        List<LongPair> keys = map.keys();
        Collections.sort(keys);
        assertEquals(keys, Lists.newArrayList(new LongPair(0, 0), new LongPair(1, 1), new LongPair(2, 2)));

        List<LongPair> values = map.values();
        Collections.sort(values);
        assertEquals(values, Lists.newArrayList(new LongPair(0, 0), new LongPair(11, 11), new LongPair(22, 22)));

        map.put(1, 1, 111, 111);

        keys = map.keys();
        Collections.sort(keys);
        assertEquals(keys, Lists.newArrayList(new LongPair(0, 0), new LongPair(1, 1), new LongPair(2, 2)));

        values = map.values();
        Collections.sort(values);
        assertEquals(values, Lists.newArrayList(new LongPair(0, 0), new LongPair(22, 22), new LongPair(111, 111)));

        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testPutIfAbsent() {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .build();

        assertTrue(map.putIfAbsent(1, 1, 11, 11));
        assertEquals(map.get(1, 1), new LongPair(11, 11));

        assertFalse(map.putIfAbsent(1, 1, 111, 111));
        assertEquals(map.get(1, 1), new LongPair(11, 11));
    }

    @Test
    public void testIvalidKeys() {
        ConcurrentLongLongPairHashMap map = ConcurrentLongLongPairHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();


        try {
            map.put(-5, 3, 4, 4);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.get(-1, 0);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.containsKey(-1, 0);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.putIfAbsent(-1, 1, 1, 1);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void testAsMap() {
        ConcurrentLongLongPairHashMap lmap = ConcurrentLongLongPairHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        lmap.put(1, 1, 11, 11);
        lmap.put(2, 2, 22, 22);
        lmap.put(3, 3, 33, 33);

        Map<LongPair, LongPair> map = new HashMap<>();
        map.put(new LongPair(1, 1), new LongPair(11, 11));
        map.put(new LongPair(2, 2), new LongPair(22, 22));
        map.put(new LongPair(3, 3), new LongPair(33, 33));

        assertEquals(map, lmap.asMap());
    }
}
