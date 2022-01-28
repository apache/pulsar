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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.Cleanup;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class ConcurrentOpenHashSetTest {

    @Test
    public void testConstructor() {
        try {
            new ConcurrentOpenHashSet<String>(0);
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            new ConcurrentOpenHashSet<String>(16, 0);
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            new ConcurrentOpenHashSet<String>(4, 8);
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void simpleInsertions() {
        ConcurrentOpenHashSet<String> set = new ConcurrentOpenHashSet<>(16);

        assertTrue(set.isEmpty());
        assertTrue(set.add("1"));
        assertFalse(set.isEmpty());

        assertTrue(set.add("2"));
        assertTrue(set.add("3"));

        assertEquals(set.size(), 3);

        assertTrue(set.contains("1"));
        assertEquals(set.size(), 3);

        assertTrue(set.remove("1"));
        assertEquals(set.size(), 2);
        assertFalse(set.contains("1"));
        assertFalse(set.contains("5"));
        assertEquals(set.size(), 2);

        assertTrue(set.add("1"));
        assertEquals(set.size(), 3);
        assertFalse(set.add("1"));
        assertEquals(set.size(), 3);
    }

    @Test
    public void testRemove() {
        ConcurrentOpenHashSet<String> set = new ConcurrentOpenHashSet<>();

        assertTrue(set.isEmpty());
        assertTrue(set.add("1"));
        assertFalse(set.isEmpty());

        assertFalse(set.remove("0"));
        assertFalse(set.isEmpty());
        assertTrue(set.remove("1"));
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRehashing() {
        int n = 16;
        ConcurrentOpenHashSet<Integer> set = new ConcurrentOpenHashSet<>(n / 2, 1);
        assertEquals(set.capacity(), n);
        assertEquals(set.size(), 0);

        for (int i = 0; i < n; i++) {
            set.add(i);
        }

        assertEquals(set.capacity(), 2 * n);
        assertEquals(set.size(), n);
    }

    @Test
    public void testRehashingWithDeletes() {
        int n = 16;
        ConcurrentOpenHashSet<Integer> set = new ConcurrentOpenHashSet<>(n / 2, 1);
        assertEquals(set.capacity(), n);
        assertEquals(set.size(), 0);

        for (int i = 0; i < n / 2; i++) {
            set.add(i);
        }

        for (int i = 0; i < n / 2; i++) {
            set.remove(i);
        }

        for (int i = n; i < (2 * n); i++) {
            set.add(i);
        }

        assertEquals(set.capacity(), 2 * n);
        assertEquals(set.size(), n);
    }

    @Test
    public void concurrentInsertions() throws Throwable {
        ConcurrentOpenHashSet<Long> set = new ConcurrentOpenHashSet<>();
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

                    set.add(key);
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
        ConcurrentOpenHashSet<Long> map = new ConcurrentOpenHashSet<>();
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

                    map.add(key);
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
        ConcurrentOpenHashSet<Long> set = new ConcurrentOpenHashSet<>();

        assertEquals(set.values(), Collections.emptyList());

        set.add(0l);

        assertEquals(set.values(), Lists.newArrayList(0l));

        set.remove(0l);

        assertEquals(set.values(), Collections.emptyList());

        set.add(0l);
        set.add(1l);
        set.add(2l);

        List<Long> values = set.values();
        values.sort(null);
        assertEquals(values, Lists.newArrayList(0l, 1l, 2l));

        set.clear();
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRemoval() {
        ConcurrentOpenHashSet<Integer> set = new ConcurrentOpenHashSet<>();

        set.add(0);
        set.add(1);
        set.add(3);
        set.add(6);
        set.add(7);

        List<Integer> values = set.values();
        values.sort(null);
        assertEquals(values, Lists.newArrayList(0, 1, 3, 6, 7));

        int numOfItemsDeleted = set.removeIf(i -> i < 5);
        assertEquals(numOfItemsDeleted, 3);
        assertEquals(set.size(), values.size() - numOfItemsDeleted);
        values = set.values();
        values.sort(null);
        assertEquals(values, Lists.newArrayList(6, 7));
    }

    @Test
    public void testHashConflictWithDeletion() {
        final int Buckets = 16;
        ConcurrentOpenHashSet<Long> set = new ConcurrentOpenHashSet<>(Buckets, 1);

        // Pick 2 keys that fall into the same bucket
        long key1 = 1;
        long key2 = 27;

        int bucket1 = ConcurrentOpenHashSet.signSafeMod(ConcurrentOpenHashSet.hash(key1), Buckets);
        int bucket2 = ConcurrentOpenHashSet.signSafeMod(ConcurrentOpenHashSet.hash(key2), Buckets);
        assertEquals(bucket1, bucket2);

        assertTrue(set.add(key1));
        assertTrue(set.add(key2));
        assertEquals(set.size(), 2);

        assertTrue(set.remove(key1));
        assertEquals(set.size(), 1);

        assertTrue(set.add(key1));
        assertEquals(set.size(), 2);

        assertTrue(set.remove(key1));
        assertEquals(set.size(), 1);

        assertFalse(set.add(key2));
        assertTrue(set.contains(key2));

        assertEquals(set.size(), 1);
        assertTrue(set.remove(key2));
        assertTrue(set.isEmpty());
    }

    @Test
    public void testEqualsObjects() {
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

        ConcurrentOpenHashSet<T> set = new ConcurrentOpenHashSet<>();

        T t1 = new T(1);
        T t1_b = new T(1);
        T t2 = new T(2);

        assertEquals(t1, t1_b);
        assertNotEquals(t2, t1);
        assertNotEquals(t2, t1_b);

        set.add(t1);
        assertTrue(set.contains(t1));
        assertTrue(set.contains(t1_b));
        assertFalse(set.contains(t2));

        assertTrue(set.remove(t1_b));
        assertFalse(set.contains(t1));
        assertFalse(set.contains(t1_b));
    }

}
