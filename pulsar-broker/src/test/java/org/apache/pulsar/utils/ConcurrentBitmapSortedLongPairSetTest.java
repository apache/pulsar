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
package org.apache.pulsar.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.Cleanup;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet;
import org.testng.annotations.Test;

@Test(groups = "utils")
public class ConcurrentBitmapSortedLongPairSetTest {

    @Test
    public void testAdd() {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        int items = 10;
        for (int i = 0; i < items; i++) {
            set.add(1, i);
        }
        assertEquals(set.size(), items);

        for (int i = 0; i < items; i++) {
            set.add(2, i);
        }
        assertEquals(set.size(), items * 2);

        for (int i = 0; i < items; i++) {
            set.add(2, i);
        }
        assertEquals(set.size(), items * 2);
    }

    @Test
    public void testRemove() {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        int items = 10;
        for (int i = 0; i < items; i++) {
            set.add(1, i);
        }

        for (int i = 0; i < items / 2; i++) {
            set.remove(1, i);
        }
        assertEquals(set.size(), items / 2);

        for (int i = 0; i < items / 2; i++) {
            set.remove(2, i);
        }
        assertEquals(set.size(), items / 2);

        for (int i = 0; i < items / 2; i++) {
            set.remove(1, i + 10000);
        }
        assertEquals(set.size(), items / 2);

        for (int i = 0; i < items / 2; i++) {
            set.remove(1, i + items / 2);
        }
        assertEquals(set.size(), 0);
        assertTrue(set.isEmpty());
    }

    @Test
    public void testContains() {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        assertFalse(set.contains(1, 1));

        int items = 10;
        for (int i = 0; i < items; i++) {
            set.add(1, i);
        }

        for (int i = 0; i < items; i++) {
            assertTrue(set.contains(1, i));
        }

        assertFalse(set.contains(1, 10000));
    }

    @Test
    public void testRemoveUpTo() {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        set.removeUpTo(0, 1000);
        set.removeUpTo(10, 10000);
        assertTrue(set.isEmpty());

        set.add(1, 0);

        int items = 10;
        for (int i = 0; i < items; i++) {
            set.add(1, i);
        }

        set.removeUpTo(1, 5);
        assertFalse(set.isEmpty());
        assertEquals(set.size(), 5);

        for (int i = 5; i < items; i++) {
            assertTrue(set.contains(1, i));
        }

        set.removeUpTo(2, 0);
        assertTrue(set.isEmpty());
    }

    @Test
    public void testItems() {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        Set<ConcurrentLongPairSet.LongPair> items = set.items(10, ConcurrentLongPairSet.LongPair::new);
        assertEquals(items.size(), 0);
        for (int i = 0; i < 100; i++) {
            set.add(1, i);
            set.add(2, i);
            set.add(5, i);
        }
        for (int i = 0; i < 100; i++) {
            set.add(1, i + 1000);
            set.add(2, i + 1000);
            set.add(5, i + 1000);
        }

        for (int i = 0; i < 100; i++) {
            set.add(1, i + 500);
            set.add(2, i + 500);
            set.add(5, i + 500);
        }
        assertEquals(set.size(), 900);
        assertFalse(set.isEmpty());
        items = set.items(10, ConcurrentLongPairSet.LongPair::new);
        assertEquals(items.size(), 10);
        ConcurrentLongPairSet.LongPair last = null;
        for (ConcurrentLongPairSet.LongPair item : items) {
            if (last != null) {
                assertTrue(item.compareTo(last) > 0);
            }
            last = item;
        }

        items = set.items(900, ConcurrentLongPairSet.LongPair::new);
        assertEquals(items.size(), 900);
        last = null;
        for (ConcurrentLongPairSet.LongPair item : items) {
            if (last != null) {
                assertTrue(item.compareTo(last) > 0);
            }
            last = item;
        }

        items = set.items(1000, ConcurrentLongPairSet.LongPair::new);
        assertEquals(items.size(), 900);
    }

    @Test
    public void concurrentInsertions() throws Throwable {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 8;
        final int N = 1000;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;
            futures.add(executor.submit(() -> {

                int start = N * (threadIdx + 1);
                for (int j = 0; j < N; j++) {
                    int key = start + j;
                    // Ensure keys are unique
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
    public void testValueLargerThanIntegerMAX_VALUE() {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        long baseValue = Integer.MAX_VALUE;
        List<Long> addedValues = new ArrayList<>();
        int items = 10;
        for (int i = 0; i < items; i++) {
            long value = baseValue + i;
            set.add(1, value);
            addedValues.add(value);
        }
        assertEquals(set.size(), items);
        Set<Long> values = set.items(items, (item1, item2) -> {
            assertEquals(item1, 1);
            return item2;
        });
        assertThat(values).containsExactlyInAnyOrderElementsOf(addedValues);
    }
}
