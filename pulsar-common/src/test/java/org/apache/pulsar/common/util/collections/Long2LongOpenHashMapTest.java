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
import static org.testng.Assert.assertTrue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.testng.annotations.Test;

public class Long2LongOpenHashMapTest {

    @Test
    public void testEmpty() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap();
        assertTrue(map.isEmpty());
        assertEquals(map.size(), 0);
        assertEquals(map.get(0), 0L);
        assertFalse(map.containsKey(0));
    }

    @Test
    public void testPutGet() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap();
        assertEquals(map.put(1, 10), 0L);
        assertEquals(map.put(2, Long.MAX_VALUE), 0L);
        assertFalse(map.isEmpty());
        assertEquals(map.size(), 2);
        assertTrue(map.containsKey(1));
        assertEquals(map.get(1), 10L);
        assertEquals(map.get(2), Long.MAX_VALUE);
        assertEquals(map.get(3), 0L);
    }

    @Test
    public void testPutReplace() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap();
        map.put(1, 10);
        assertEquals(map.put(1, 100), 10L);
        assertEquals(map.get(1), 100L);
        assertEquals(map.size(), 1);
    }

    @Test
    public void testRemove() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap();
        map.put(1, 10);
        map.put(2, 20);
        assertEquals(map.remove(1), 10L);
        assertFalse(map.containsKey(1));
        assertEquals(map.get(1), 0L);
        assertEquals(map.remove(99), 0L);
        assertEquals(map.size(), 1);
    }

    @Test
    public void testGetOrDefault() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap();
        map.put(1, 10);
        assertEquals(map.getOrDefault(1, -1), 10L);
        assertEquals(map.getOrDefault(2, -1), -1L);
    }

    @Test
    public void testZeroValueCanBeDistinguishedFromMissingKey() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap();
        map.put(1, 0);

        assertTrue(map.containsKey(1));
        assertEquals(map.get(1), 0L);
        assertEquals(map.getOrDefault(1, -1), 0L);
        assertFalse(map.containsKey(2));
        assertEquals(map.get(2), 0L);
        assertEquals(map.getOrDefault(2, -1), -1L);
    }

    @Test
    public void testComputeIfAbsent() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap();
        assertEquals(map.computeIfAbsent(1, k -> 10), 10L);
        assertEquals(map.computeIfAbsent(1, k -> 99), 10L);
    }

    @Test
    public void testClear() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap();
        map.put(1, 10);
        map.put(2, 20);
        map.clear();
        assertTrue(map.isEmpty());
        assertEquals(map.size(), 0);
        assertEquals(map.get(1), 0L);
    }

    @Test
    public void testForEach() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap();
        map.put(1, 10);
        map.put(2, 20);
        Map<Long, Long> values = new HashMap<>();

        map.forEach(values::put);

        assertEquals(values, Map.of(1L, 10L, 2L, 20L));
    }

    @Test
    public void testRemoveIf() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i * 10L);
        }

        int removed = map.removeIf((key, value) -> key % 2 == 0);

        assertEquals(removed, 50);
        assertEquals(map.size(), 50);
        for (int i = 0; i < 100; i++) {
            assertEquals(map.containsKey(i), i % 2 != 0);
        }
    }

    @Test
    public void testRehash() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap(4);
        for (int i = 0; i < 100; i++) {
            map.put(i, i * 10L);
        }
        assertEquals(map.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), i * 10L);
        }
    }

    @Test
    public void testRandomOperationsAgainstHashMap() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap(4);
        Map<Long, Long> expected = new HashMap<>();
        Random random = new Random(0);

        for (int i = 0; i < 10_000; i++) {
            long key = random.nextInt(512) - 256L;
            switch (random.nextInt(5)) {
                case 0 -> {
                    long value = random.nextLong();
                    Long previous = expected.put(key, value);
                    assertEquals(map.put(key, value), previous == null ? 0L : previous.longValue());
                }
                case 1 -> {
                    Long previous = expected.remove(key);
                    assertEquals(map.remove(key), previous == null ? 0L : previous.longValue());
                }
                case 2 -> assertEquals(map.get(key), expected.getOrDefault(key, 0L).longValue());
                case 3 -> assertEquals(map.containsKey(key), expected.containsKey(key));
                case 4 -> {
                    long value = key * 37 + i;
                    Long previous = expected.get(key);
                    assertEquals(map.computeIfAbsent(key, ignored -> value),
                            previous == null ? value : previous.longValue());
                    expected.putIfAbsent(key, value);
                }
                default -> throw new IllegalStateException("Unexpected random operation");
            }

            if (i % 257 == 0) {
                int removed = map.removeIf((entryKey, value) -> (entryKey & 3) == 0);
                int expectedRemoved = 0;
                Iterator<Map.Entry<Long, Long>> iterator = expected.entrySet().iterator();
                while (iterator.hasNext()) {
                    if ((iterator.next().getKey() & 3) == 0) {
                        iterator.remove();
                        expectedRemoved++;
                    }
                }
                assertEquals(removed, expectedRemoved);
            }

            assertEquals(map.size(), expected.size());
            for (Map.Entry<Long, Long> entry : expected.entrySet()) {
                assertTrue(map.containsKey(entry.getKey()));
                assertEquals(map.get(entry.getKey()), entry.getValue().longValue());
            }
        }
    }
}
