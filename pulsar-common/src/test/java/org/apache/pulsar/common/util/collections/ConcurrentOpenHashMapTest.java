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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import lombok.Cleanup;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class ConcurrentOpenHashMapTest {

    @Test
    public void testConstructor() {
        try {
            ConcurrentOpenHashMap.<String, String>newBuilder()
                    .expectedItems(0)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentOpenHashMap.<String, String>newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(0)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentOpenHashMap.<String, String>newBuilder()
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
        ConcurrentOpenHashMap<String, String> map =
                ConcurrentOpenHashMap.<String, String>newBuilder()
                .expectedItems(16)
                .build();

        assertTrue(map.isEmpty());
        assertNull(map.put("1", "one"));
        assertFalse(map.isEmpty());

        assertNull(map.put("2", "two"));
        assertNull(map.put("3", "three"));

        assertEquals(map.size(), 3);

        assertEquals(map.get("1"), "one");
        assertEquals(map.size(), 3);

        assertEquals(map.remove("1"), "one");
        assertEquals(map.size(), 2);
        assertNull(map.get("1"));
        assertNull(map.get("5"));
        assertEquals(map.size(), 2);

        assertNull(map.put("1", "one"));
        assertEquals(map.size(), 3);
        assertEquals(map.put("1", "uno"), "one");
        assertEquals(map.size(), 3);
    }

    @Test
    public void testReduceUnnecessaryExpansions() {
        ConcurrentOpenHashMap<String, String> map = ConcurrentOpenHashMap.<String, String>newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .build();
        assertNull(map.put("1", "1"));
        assertNull(map.put("2", "2"));
        assertNull(map.put("3", "3"));
        assertNull(map.put("4", "4"));

        assertEquals(map.remove("1"), "1");
        assertEquals(map.remove("2"), "2");
        assertEquals(map.remove("3"), "3");
        assertEquals(map.remove("4"), "4");

        assertEquals(0, map.getUsedBucketCount());
    }

    @Test
    public void testClear() {
        ConcurrentOpenHashMap<String, String> map = ConcurrentOpenHashMap.<String, String>newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertTrue(map.capacity() == 4);

        assertNull(map.put("k1", "v1"));
        assertNull(map.put("k2", "v2"));
        assertNull(map.put("k3", "v3"));

        assertTrue(map.capacity() == 8);
        map.clear();
        assertTrue(map.capacity() == 4);
    }

    @Test
    public void testExpandAndShrink() {
        ConcurrentOpenHashMap<String, String> map = ConcurrentOpenHashMap.<String, String>newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertTrue(map.capacity() == 4);

        assertNull(map.put("k1", "v1"));
        assertNull(map.put("k2", "v2"));
        assertNull(map.put("k3", "v3"));

        // expand hashmap
        assertTrue(map.capacity() == 8);

        assertTrue(map.remove("k1", "v1"));
        // not shrink
        assertTrue(map.capacity() == 8);
        assertTrue(map.remove("k2", "v2"));
        // shrink hashmap
        assertTrue(map.capacity() == 4);

        // expand hashmap
        assertNull(map.put("k4", "v4"));
        assertNull(map.put("k5", "v5"));
        assertTrue(map.capacity() == 8);

        //verify that the map does not keep shrinking at every remove() operation
        assertNull(map.put("k6", "v6"));
        assertTrue(map.remove("k6", "v6"));
        assertTrue(map.capacity() == 8);
    }

    @Test
    public void testRemove() {
        ConcurrentOpenHashMap<String, String> map =
                ConcurrentOpenHashMap.<String, String>newBuilder().build();

        assertTrue(map.isEmpty());
        assertNull(map.put("1", "one"));
        assertFalse(map.isEmpty());

        assertFalse(map.remove("0", "zero"));
        assertFalse(map.remove("1", "uno"));

        assertFalse(map.isEmpty());
        assertTrue(map.remove("1", "one"));
        assertTrue(map.isEmpty());
    }

    @Test
    public void testRehashing() {
        int n = 16;
        ConcurrentOpenHashMap<String, Integer> map = ConcurrentOpenHashMap.<String, Integer>newBuilder()
                        .expectedItems(n / 2)
                        .concurrencyLevel(1)
                        .build();
        assertEquals(map.capacity(), n);
        assertEquals(map.size(), 0);

        for (int i = 0; i < n; i++) {
            map.put(Integer.toString(i), i);
        }

        assertEquals(map.capacity(), 2 * n);
        assertEquals(map.size(), n);
    }

    @Test
    public void testRehashingWithDeletes() {
        int n = 16;
        ConcurrentOpenHashMap<Integer, Integer> map =
                ConcurrentOpenHashMap.<Integer, Integer>newBuilder()
                        .expectedItems(n / 2)
                        .concurrencyLevel(1)
                        .build();
        assertEquals(map.capacity(), n);
        assertEquals(map.size(), 0);

        for (int i = 0; i < n / 2; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < n / 2; i++) {
            map.remove(i);
        }

        for (int i = n; i < (2 * n); i++) {
            map.put(i, i);
        }

        assertEquals(map.capacity(), 2 * n);
        assertEquals(map.size(), n);
    }

    @Test
    public void concurrentInsertions() throws Throwable {
        ConcurrentOpenHashMap<Long, String> map = ConcurrentOpenHashMap.<Long, String>newBuilder()
                        .expectedItems(16)
                        .concurrencyLevel(1)
                        .build();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int N = 100_000;
        String value = "value";

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                Random random = new Random();

                for (int j = 0; j < N; j++) {
                    long key = random.nextLong();
                    // Ensure keys are uniques
                    key -= key % (threadIdx + 1);

                    map.put(key, value);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(map.size(), N * nThreads);
    }

    @Test
    public void concurrentInsertionsAndReads() throws Throwable {
        ConcurrentOpenHashMap<Long, String> map =
                ConcurrentOpenHashMap.<Long, String>newBuilder().build();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int N = 100_000;
        String value = "value";

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                Random random = new Random();

                for (int j = 0; j < N; j++) {
                    long key = random.nextLong();
                    // Ensure keys are uniques
                    key -= key % (threadIdx + 1);

                    map.put(key, value);
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
        ConcurrentOpenHashMap<Long, String> map =
                ConcurrentOpenHashMap.<Long, String>newBuilder().build();

        assertEquals(map.keys(), Collections.emptyList());
        assertEquals(map.values(), Collections.emptyList());

        map.put(0l, "zero");

        assertEquals(map.keys(), Lists.newArrayList(0l));
        assertEquals(map.values(), Lists.newArrayList("zero"));

        map.remove(0l);

        assertEquals(map.keys(), Collections.emptyList());
        assertEquals(map.values(), Collections.emptyList());

        map.put(0l, "zero");
        map.put(1l, "one");
        map.put(2l, "two");

        List<Long> keys = map.keys();
        keys.sort(null);
        assertEquals(keys, Lists.newArrayList(0l, 1l, 2l));

        List<String> values = map.values();
        values.sort(null);
        assertEquals(values, Lists.newArrayList("one", "two", "zero"));

        map.put(1l, "uno");

        keys = map.keys();
        keys.sort(null);
        assertEquals(keys, Lists.newArrayList(0l, 1l, 2l));

        values = map.values();
        values.sort(null);
        assertEquals(values, Lists.newArrayList("two", "uno", "zero"));

        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testHashConflictWithDeletion() {
        final int Buckets = 16;
        ConcurrentOpenHashMap<Long, String> map = ConcurrentOpenHashMap.<Long, String>newBuilder()
                .expectedItems(Buckets)
                .concurrencyLevel(1)
                .build();

        // Pick 2 keys that fall into the same bucket
        long key1 = 1;
        long key2 = 27;

        int bucket1 = ConcurrentOpenHashMap.signSafeMod(ConcurrentOpenHashMap.hash(key1), Buckets);
        int bucket2 = ConcurrentOpenHashMap.signSafeMod(ConcurrentOpenHashMap.hash(key2), Buckets);
        assertEquals(bucket1, bucket2);

        assertNull(map.put(key1, "value-1"));
        assertNull(map.put(key2, "value-2"));
        assertEquals(map.size(), 2);

        assertEquals(map.remove(key1), "value-1");
        assertEquals(map.size(), 1);

        assertNull(map.put(key1, "value-1-overwrite"));
        assertEquals(map.size(), 2);

        assertEquals(map.remove(key1), "value-1-overwrite");
        assertEquals(map.size(), 1);

        assertEquals(map.put(key2, "value-2-overwrite"), "value-2");
        assertEquals(map.get(key2), "value-2-overwrite");

        assertEquals(map.size(), 1);
        assertEquals(map.remove(key2), "value-2-overwrite");
        assertTrue(map.isEmpty());
    }

    @Test
    public void testPutIfAbsent() {
        ConcurrentOpenHashMap<Long, String> map =
                ConcurrentOpenHashMap.<Long, String>newBuilder().build();
        assertNull(map.putIfAbsent(1l, "one"));
        assertEquals(map.get(1l), "one");

        assertEquals(map.putIfAbsent(1l, "uno"), "one");
        assertEquals(map.get(1l), "one");
    }

    @Test
    public void testComputeIfAbsent() {
        ConcurrentOpenHashMap<Integer, Integer> map = ConcurrentOpenHashMap.<Integer, Integer>newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        AtomicInteger counter = new AtomicInteger();
        Function<Integer, Integer> provider = key -> counter.getAndIncrement();

        assertEquals(map.computeIfAbsent(0, provider).intValue(), 0);
        assertEquals(map.get(0).intValue(), 0);

        assertEquals(map.computeIfAbsent(1, provider).intValue(), 1);
        assertEquals(map.get(1).intValue(), 1);

        assertEquals(map.computeIfAbsent(1, provider).intValue(), 1);
        assertEquals(map.get(1).intValue(), 1);

        assertEquals(map.computeIfAbsent(2, provider).intValue(), 2);
        assertEquals(map.get(2).intValue(), 2);
    }

    @Test
    public void testEqualsKeys() {
        class T {
            int value;

            T(int value) {
                this.value = value;
            }

            @Override
            public int hashCode() {
                return Integer.hashCode(value);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj instanceof T) {
                    return value == ((T) obj).value;
                }

                return false;
            }
        }

        ConcurrentOpenHashMap<T, String> map =
                ConcurrentOpenHashMap.<T, String>newBuilder().build();

        T t1 = new T(1);
        T t1_b = new T(1);
        T t2 = new T(2);

        assertEquals(t1, t1_b);
        assertNotEquals(t2, t1);
        assertNotEquals(t2, t1_b);

        assertNull(map.put(t1, "t1"));
        assertEquals(map.get(t1), "t1");
        assertEquals(map.get(t1_b), "t1");
        assertNull(map.get(t2));

        assertEquals(map.remove(t1_b), "t1");
        assertNull(map.get(t1));
        assertNull(map.get(t1_b));
    }

    @Test
    public void testNullValue() {
        ConcurrentOpenHashMap<String, String> map =
                ConcurrentOpenHashMap.<String, String>newBuilder()
                        .expectedItems(16)
                        .concurrencyLevel(1)
                        .build();
        String key = "a";
        assertThrows(NullPointerException.class, () -> map.put(key, null));

        //put a null value.
        assertNull(map.computeIfAbsent(key, k -> null));
        assertEquals(1, map.size());
        assertEquals(1, map.keys().size());
        assertEquals(1, map.values().size());
        assertNull(map.get(key));
        assertFalse(map.containsKey(key));

        //test remove null value
        map.removeNullValue(key);
        assertTrue(map.isEmpty());
        assertEquals(0, map.keys().size());
        assertEquals(0, map.values().size());
        assertNull(map.get(key));
        assertFalse(map.containsKey(key));


        //test not remove non-null value
        map.put(key, "V");
        assertEquals(1, map.size());
        map.removeNullValue(key);
        assertEquals(1, map.size());

    }

    static final int Iterations = 1;
    static final int ReadIterations = 1000;
    static final int N = 1_000_000;

    public void benchConcurrentOpenHashMap() throws Exception {
        ConcurrentOpenHashMap<Long, String> map = ConcurrentOpenHashMap.<Long, String>newBuilder()
                .expectedItems(N)
                .concurrencyLevel(1)
                .build();

        for (long i = 0; i < Iterations; i++) {
            for (int j = 0; j < N; j++) {
                map.put(i, "value");
            }

            for (long h = 0; h < ReadIterations; h++) {
                for (int j = 0; j < N; j++) {
                    map.get(i);
                }
            }

            for (long j = 0; j < N; j++) {
                map.remove(i);
            }
        }
    }

    public void benchConcurrentHashMap() throws Exception {
        ConcurrentHashMap<Long, String> map = new ConcurrentHashMap<Long, String>(N, 0.66f, 1);

        for (long i = 0; i < Iterations; i++) {
            for (int j = 0; j < N; j++) {
                map.put(i, "value");
            }

            for (long h = 0; h < ReadIterations; h++) {
                for (int j = 0; j < N; j++) {
                    map.get(i);
                }
            }

            for (int j = 0; j < N; j++) {
                map.remove(i);
            }
        }
    }

    void benchHashMap() {
        HashMap<Long, String> map = new HashMap<>(N, 0.66f);

        for (long i = 0; i < Iterations; i++) {
            for (int j = 0; j < N; j++) {
                map.put(i, "value");
            }

            for (long h = 0; h < ReadIterations; h++) {
                for (int j = 0; j < N; j++) {
                    map.get(i);
                }
            }

            for (int j = 0; j < N; j++) {
                map.remove(i);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ConcurrentOpenHashMapTest t = new ConcurrentOpenHashMapTest();

        long start = System.nanoTime();
        t.benchHashMap();
        long end = System.nanoTime();

        System.out.println("HM:   " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms");

        start = System.nanoTime();
        t.benchConcurrentHashMap();
        end = System.nanoTime();

        System.out.println("CHM:  " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms");

        start = System.nanoTime();
        t.benchConcurrentOpenHashMap();
        end = System.nanoTime();

        System.out.println("CLHM: " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms");

    }
}
