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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.testng.annotations.Test;

public class Long2ObjectOpenHashMapTest {

    @Test
    public void testEmpty() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        assertTrue(map.isEmpty());
        assertEquals(map.size(), 0);
        assertNull(map.get(0));
        assertNull(map.get(1));
        assertNull(map.remove(1));
    }

    @Test
    public void testPutGet() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        assertNull(map.put(1, "one"));
        assertNull(map.put(2, "two"));
        assertNull(map.put(3, "three"));
        assertEquals(map.size(), 3);
        assertFalse(map.isEmpty());
        assertEquals(map.get(1), "one");
        assertEquals(map.get(2), "two");
        assertEquals(map.get(3), "three");
        assertNull(map.get(4));
    }

    @Test
    public void testPutReplace() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        assertNull(map.put(1, "one"));
        assertEquals(map.put(1, "ONE"), "one");
        assertEquals(map.size(), 1);
        assertEquals(map.get(1), "ONE");
    }

    @Test
    public void testRemove() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");
        assertEquals(map.remove(2), "two");
        assertEquals(map.size(), 2);
        assertNull(map.get(2));
        assertEquals(map.get(1), "one");
        assertEquals(map.get(3), "three");
    }

    @Test
    public void testComputeIfAbsent() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        assertEquals(map.computeIfAbsent(1, k -> "one"), "one");
        assertEquals(map.size(), 1);
        assertEquals(map.computeIfAbsent(1, k -> "other"), "one");
        assertEquals(map.size(), 1);
    }

    @Test
    public void testClear() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        map.clear();
        assertTrue(map.isEmpty());
        assertEquals(map.size(), 0);
        assertNull(map.get(1));
    }

    @Test
    public void testForEach() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");
        Map<Long, String> collected = new HashMap<>();
        map.forEach(collected::put);
        assertEquals(collected.size(), 3);
        assertEquals(collected.get(1L), "one");
        assertEquals(collected.get(2L), "two");
        assertEquals(collected.get(3L), "three");
    }

    @Test
    public void testValues() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");
        List<String> values = new ArrayList<>(map.values());
        assertEquals(values.size(), 3);
        assertTrue(values.contains("one"));
        assertTrue(values.contains("two"));
        assertTrue(values.contains("three"));
    }

    @Test
    public void testRehash() {
        Long2ObjectOpenHashMap<Integer> map = new Long2ObjectOpenHashMap<>(4);
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        assertEquals(map.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), Integer.valueOf(i));
        }
    }

    @Test
    public void testZeroKey() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        map.put(0, "zero");
        assertEquals(map.get(0), "zero");
        assertEquals(map.size(), 1);
        assertEquals(map.remove(0), "zero");
        assertTrue(map.isEmpty());
    }

    @Test
    public void testNegativeKeys() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        map.put(-1, "neg1");
        map.put(Long.MIN_VALUE, "min");
        assertEquals(map.get(-1), "neg1");
        assertEquals(map.get(Long.MIN_VALUE), "min");
    }

    @Test
    public void testRemoveAndReinsert() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();
        for (int i = 0; i < 50; i++) {
            map.put(i, "v" + i);
        }
        for (int i = 0; i < 25; i++) {
            map.remove(i);
        }
        assertEquals(map.size(), 25);
        for (int i = 25; i < 50; i++) {
            assertEquals(map.get(i), "v" + i);
        }
        // Reinsert
        for (int i = 0; i < 25; i++) {
            map.put(i, "new" + i);
        }
        assertEquals(map.size(), 50);
        for (int i = 0; i < 25; i++) {
            assertEquals(map.get(i), "new" + i);
        }
    }

    @Test
    public void testRemovePreservesProbeChainWithCollisions() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>(4);
        List<Long> keys = collidingLongKeys(16, 8);

        for (int i = 0; i < keys.size(); i++) {
            assertNull(map.put(keys.get(i), "v" + i));
        }

        assertEquals(map.remove(keys.get(0)), "v0");
        assertEquals(map.remove(keys.get(4)), "v4");
        assertEquals(map.remove(keys.get(7)), "v7");

        for (int i = 1; i < keys.size() - 1; i++) {
            long key = keys.get(i);
            if (i != 4) {
                assertEquals(map.get(key), "v" + i);
            }
        }
        assertNull(map.get(keys.get(0)));
        assertNull(map.get(keys.get(4)));
        assertNull(map.get(keys.get(7)));

        assertNull(map.put(keys.get(4), "new-v4"));
        assertEquals(map.get(keys.get(4)), "new-v4");
    }

    @Test
    public void testComputeIfAbsentDoesNotInsertNullValue() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>();

        assertNull(map.computeIfAbsent(Long.MAX_VALUE, __ -> null));
        assertNull(map.get(Long.MAX_VALUE));
        assertEquals(map.size(), 0);

        assertEquals(map.computeIfAbsent(Long.MAX_VALUE, __ -> "value"), "value");
        assertEquals(map.computeIfAbsent(Long.MAX_VALUE, __ -> null), "value");
        assertEquals(map.size(), 1);
    }

    @Test
    public void testRandomizedOperationsAgainstHashMap() {
        Long2ObjectOpenHashMap<String> map = new Long2ObjectOpenHashMap<>(4);
        Map<Long, String> expected = new HashMap<>();
        Set<Long> seenKeys = new HashSet<>();
        Random random = new Random(0x5eed1234L);

        for (int i = 0; i < 20_000; i++) {
            long key = randomLongWithEdgeCases(random, 256);
            seenKeys.add(key);

            int operation = random.nextInt(100);
            if (operation < 35) {
                String value = randomValue(random);
                assertEquals(map.put(key, value), expected.put(key, value));
            } else if (operation < 55) {
                assertEquals(map.remove(key), expected.remove(key));
            } else if (operation < 75) {
                String value = randomValue(random);
                assertEquals(map.computeIfAbsent(key, __ -> value),
                        expected.computeIfAbsent(key, __ -> value));
            } else if (operation < 95) {
                assertEquals(map.get(key), expected.get(key));
            } else {
                map.clear();
                expected.clear();
            }

            assertLong2ObjectMapMatches(expected, seenKeys, map);
        }
    }

    private static void assertLong2ObjectMapMatches(Map<Long, String> expected, Set<Long> seenKeys,
                                                    Long2ObjectOpenHashMap<String> actual) {
        assertEquals(actual.isEmpty(), expected.isEmpty());
        assertEquals(actual.size(), expected.size());
        for (long key : seenKeys) {
            assertEquals(actual.get(key), expected.get(key));
        }

        Map<Long, String> entries = new HashMap<>();
        actual.forEach(entries::put);
        assertEquals(entries, expected);

        List<String> actualValues = new ArrayList<>(actual.values());
        List<String> expectedValues = new ArrayList<>(expected.values());
        Collections.sort(actualValues);
        Collections.sort(expectedValues);
        assertEquals(actualValues, expectedValues);
    }

    private static String randomValue(Random random) {
        return "v" + random.nextInt(512);
    }

    private static long randomLongWithEdgeCases(Random random, int bound) {
        return switch (random.nextInt(64)) {
            case 0 -> 0L;
            case 1 -> Long.MIN_VALUE;
            case 2 -> Long.MAX_VALUE;
            default -> random.nextInt(bound) - bound / 2L;
        };
    }

    private static List<Long> collidingLongKeys(int capacity, int count) {
        int mask = capacity - 1;
        int bucket = Long2ObjectOpenHashMap.hash(0) & mask;
        List<Long> keys = new ArrayList<>();
        for (long candidate = 0; keys.size() < count; candidate++) {
            if ((Long2ObjectOpenHashMap.hash(candidate) & mask) == bucket) {
                keys.add(candidate);
            }
        }
        return keys;
    }
}
