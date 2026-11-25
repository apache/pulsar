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
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet.LongPair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "utils")
public class ConcurrentBitmapSortedLongPairSetTest {

    @DataProvider
    public Object[][] longValues() {
        return new Object[][] {
                {Long.MIN_VALUE}, {-1L}, {0L}, {1L}, {(long) Integer.MAX_VALUE}, {Integer.MAX_VALUE + 1L},
                {(1L << 32) - 1}, {1L << 32}, {(1L << 32) + 1}, {1L << 48}, {Long.MAX_VALUE}
        };
    }

    @Test(dataProvider = "longValues")
    public void testAddAndRemoveLongValues(long value) {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        set.add(1, value);
        set.add(1, value);
        set.add(2, value);
        assertThat(set.size()).isEqualTo(2);
        assertThat(set.contains(1, value)).isTrue();
        assertThat(set.first(LongPair::new)).contains(new LongPair(1, value));

        set.remove(1, value);
        assertThat(set.contains(1, value)).isFalse();
        assertThat(set.contains(2, value)).isTrue();
        assertThat(set.size()).isEqualTo(1);
        set.remove(2, value);
        assertThat(set.isEmpty()).isTrue();
        assertThat(set.first(LongPair::new)).isEmpty();

        set.add(1, value);
        set.clear();
        assertThat(set.size()).isZero();
        assertThat(set.isEmpty()).isTrue();
    }

    @Test
    public void testOrderedLongValues() {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        long[] values = {Long.MIN_VALUE, -1, 0, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L, (1L << 32) - 1,
                1L << 32, (1L << 32) + 1, 1L << 48, Long.MAX_VALUE - 1, Long.MAX_VALUE};
        List<LongPair> expected = new ArrayList<>();
        for (long ledgerId = 1; ledgerId <= 2; ledgerId++) {
            for (long value : values) {
                expected.add(new LongPair(ledgerId, value));
            }
            for (int i = values.length - 1; i >= 0; i--) {
                set.add(ledgerId, values[i]);
            }
        }

        List<LongPair> actual = new ArrayList<>();
        set.processItems(LongPair::new, item -> {
            actual.add(item);
            return true;
        });
        assertThat(actual).containsExactlyElementsOf(expected);
        assertThat(set.items(7, LongPair::new)).containsExactlyElementsOf(expected.subList(0, 7));
        assertThat(set.size()).isEqualTo(expected.size());

        actual.clear();
        set.processItems(LongPair::new, item -> {
            actual.add(item);
            return actual.size() < 7;
        });
        assertThat(actual).containsExactlyElementsOf(expected.subList(0, 7));

        assertThat(set.contains(1, 2L << 32)).isFalse();
        set.remove(1, 1L << 32);
        assertThat(set.contains(1, 1L << 32)).isFalse();
        assertThat(set.contains(1, 0)).isTrue();
        assertThat(set.contains(1, 1L << 48)).isTrue();
        assertThat(set.size()).isEqualTo(expected.size() - 1);
    }

    @Test(dataProvider = "longValues")
    public void testRemoveUpToLongBoundary(long value) {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        set.add(Long.MIN_VALUE, Long.MAX_VALUE);
        set.add(0, value);
        set.add(Long.MAX_VALUE, Long.MIN_VALUE);

        assertThat(set.removeUpTo(0, value)).isTrue();
        assertThat(set.items(10, LongPair::new)).containsExactly(
                new LongPair(0, value), new LongPair(Long.MAX_VALUE, Long.MIN_VALUE));
        assertThat(set.removeUpTo(0, value)).isFalse();
        assertThat(set.removeUpToInclusive(0, value)).isTrue();
        assertThat(set.removeUpToInclusive(0, value)).isFalse();
        assertThat(set.items(10, LongPair::new)).containsExactly(new LongPair(Long.MAX_VALUE, Long.MIN_VALUE));
        assertThat(set.removeUpToInclusive(Long.MAX_VALUE, Long.MIN_VALUE)).isTrue();
        assertThat(set.isEmpty()).isTrue();
    }

    @Test
    public void testRemoveUpToAcrossLongValueRanges() {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        long boundary = 1L << 32;
        set.add(1, Long.MAX_VALUE);
        set.add(2, 0);
        set.add(2, boundary - 1);
        set.add(2, boundary);
        set.add(2, boundary + 5);
        set.add(2, (1L << 48) + 100);
        set.add(2, Long.MAX_VALUE);
        set.add(3, 0);

        assertThat(set.removeUpTo(2, boundary)).isTrue();
        assertThat(set.items(10, LongPair::new)).containsExactly(new LongPair(2, boundary),
                new LongPair(2, boundary + 5), new LongPair(2, (1L << 48) + 100),
                new LongPair(2, Long.MAX_VALUE), new LongPair(3, 0));
        assertThat(set.removeUpTo(2, boundary)).isFalse();

        assertThat(set.removeUpTo(2, boundary + 11)).isTrue();
        assertThat(set.removeUpTo(2, boundary + 11)).isFalse();
        assertThat(set.removeUpTo(2, 1L << 48)).isFalse();
        assertThat(set.removeUpTo(2, Long.MAX_VALUE)).isTrue();
        assertThat(set.removeUpTo(2, Long.MAX_VALUE)).isFalse();
        assertThat(set.items(10, LongPair::new)).containsExactly(
                new LongPair(2, Long.MAX_VALUE), new LongPair(3, 0));

        assertThat(set.removeUpTo(3, 0)).isTrue();
        assertThat(set.items(10, LongPair::new)).containsExactly(new LongPair(3, 0));
        set.remove(3, 0);
        assertThat(set.isEmpty()).isTrue();
    }

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
    public void testRemoveUpToReportsASparsePrefix() {
        ConcurrentBitmapSortedLongPairSet set = new ConcurrentBitmapSortedLongPairSet();
        set.add(1, 5);
        set.add(1, 100);

        // removes 1:5 although 1:0 to 1:4 and 1:6 to 1:10 were never there
        assertTrue(set.removeUpTo(1, 11));
        assertFalse(set.contains(1, 5));
        assertTrue(set.contains(1, 100));
        assertFalse(set.removeUpTo(1, 11));
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
        final int num = 1000;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;
            futures.add(executor.submit(() -> {

                int start = num * (threadIdx + 1);
                for (int j = 0; j < num; j++) {
                    int key = start + j;
                    // Ensure keys are unique
                    set.add(key, key);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(set.size(), num * nThreads);
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
        List<Long> values = new ArrayList<>();
        set.processItems((item1, item2) -> {
            assertEquals(item1, 1);
            return item2;
        }, (value) -> {
            values.add(value);
            return true;
        });
        assertThat(values).containsExactlyElementsOf(addedValues);
    }
}
