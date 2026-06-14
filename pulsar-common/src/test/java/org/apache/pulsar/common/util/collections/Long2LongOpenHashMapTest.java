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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.testng.Reporter;
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
    public void testEdgeKeysAndValuesRoundTrip() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap(4);
        Map<Long, Long> expected = new HashMap<>();
        long[][] entries = {
                {0L, 0L},
                {Long.MIN_VALUE, Long.MIN_VALUE},
                {Long.MAX_VALUE, Long.MAX_VALUE},
                {-1L, 1L},
                {1L, -1L},
                {Long.MIN_VALUE + 1, Long.MAX_VALUE - 1},
                {Long.MAX_VALUE - 1, Long.MIN_VALUE + 1}
        };

        for (long[] entry : entries) {
            expected.put(entry[0], entry[1]);
            assertEquals(map.put(entry[0], entry[1]), 0L);
        }

        assertLong2LongMapMatches(expected, expected.keySet(), map, "edge values");
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
    public void testRemovePreservesProbeChainWithCollisions() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap(4);
        List<Long> keys = collidingLongKeys(16, 12);

        for (int i = 0; i < keys.size(); i++) {
            assertEquals(map.put(keys.get(i), valueForIndex(i)), 0L);
        }

        assertEquals(map.remove(keys.get(0)), valueForIndex(0));
        assertEquals(map.remove(keys.get(5)), valueForIndex(5));
        assertEquals(map.remove(keys.get(11)), valueForIndex(11));

        for (int i = 1; i < keys.size() - 1; i++) {
            long key = keys.get(i);
            if (i != 5) {
                assertEquals(map.get(key), valueForIndex(i));
                assertTrue(map.containsKey(key));
            }
        }
        assertFalse(map.containsKey(keys.get(0)));
        assertFalse(map.containsKey(keys.get(5)));
        assertFalse(map.containsKey(keys.get(11)));

        assertEquals(map.put(keys.get(5), Long.MIN_VALUE), 0L);
        assertEquals(map.getOrDefault(keys.get(5), -1L), Long.MIN_VALUE);
    }

    @Test
    public void testRandomizedOperationsAgainstHashMap() {
        Long2LongOpenHashMap map = new Long2LongOpenHashMap(4);
        Map<Long, Long> expected = new HashMap<>();
        Set<Long> seenKeys = new HashSet<>();
        long seed = randomSeed("testRandomizedOperationsAgainstHashMap");
        Random random = new Random(seed);

        for (int i = 0; i < 20_000; i++) {
            long key = randomLongWithEdgeCases(random, 512);
            seenKeys.add(key);
            String context = "seed=" + seed + " iteration=" + i + " key=" + key;

            switch (random.nextInt(100)) {
                case 0 -> {
                    long value = randomValue(random);
                    Long previous = expected.put(key, value);
                    assertEquals(map.put(key, value), previous == null ? 0L : previous.longValue(), context);
                }
                case 1 -> {
                    Long previous = expected.remove(key);
                    assertEquals(map.remove(key), previous == null ? 0L : previous.longValue(), context);
                }
                case 2 -> {
                    long value = randomValue(random);
                    Long previous = expected.get(key);
                    assertEquals(map.computeIfAbsent(key, ignored -> value),
                            previous == null ? value : previous.longValue(), context);
                    expected.putIfAbsent(key, value);
                }
                case 3 -> assertEquals(map.get(key), expected.getOrDefault(key, 0L).longValue(), context);
                case 4 -> {
                    long defaultValue = randomValue(random);
                    assertEquals(map.getOrDefault(key, defaultValue),
                            expected.getOrDefault(key, defaultValue).longValue(), context);
                }
                case 5 -> assertEquals(map.containsKey(key), expected.containsKey(key), context);
                case 6 -> runRemoveIfScenario(map, expected, random, context);
                case 7 -> {
                    map.clear();
                    expected.clear();
                }
                default -> {
                    long otherKey = randomLongWithEdgeCases(random, 512);
                    seenKeys.add(otherKey);
                    long value = randomValue(random);
                    Long previous = expected.put(otherKey, value);
                    assertEquals(map.put(otherKey, value), previous == null ? 0L : previous.longValue(),
                            context + " otherKey=" + otherKey);
                }
            }

            if (i % 257 == 0) {
                runRemoveIfScenario(map, expected, random, context + " periodicRemoveIf");
            }

            assertLong2LongMapMatches(expected, seenKeys, map, context);
        }
    }

    private static void runRemoveIfScenario(Long2LongOpenHashMap map, Map<Long, Long> expected, Random random,
                                            String context) {
        int selector = random.nextInt(4);
        int removed = map.removeIf((entryKey, value) -> removeIfMatches(selector, entryKey, value));

        int expectedRemoved = 0;
        Iterator<Map.Entry<Long, Long>> iterator = expected.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Long> entry = iterator.next();
            if (removeIfMatches(selector, entry.getKey(), entry.getValue())) {
                iterator.remove();
                expectedRemoved++;
            }
        }
        assertEquals(removed, expectedRemoved, context + " removeIfSelector=" + selector);
    }

    private static boolean removeIfMatches(int selector, long key, long value) {
        return switch (selector) {
            case 0 -> (key & 3L) == 0;
            case 1 -> (value & 7L) == 0;
            case 2 -> key < 0 && value <= 0;
            case 3 -> key == Long.MIN_VALUE || value == Long.MAX_VALUE;
            default -> throw new IllegalArgumentException("Unknown selector: " + selector);
        };
    }

    private static void assertLong2LongMapMatches(Map<Long, Long> expected, Iterable<Long> seenKeys,
                                                  Long2LongOpenHashMap actual, String context) {
        long missingValue = 0x5A5A_5A5A_5A5A_5A5AL;
        assertEquals(actual.isEmpty(), expected.isEmpty(), context);
        assertEquals(actual.size(), expected.size(), context);

        for (long key : seenKeys) {
            Long expectedValue = expected.get(key);
            assertEquals(actual.containsKey(key), expectedValue != null, context + " checkedKey=" + key);
            assertEquals(actual.get(key), expectedValue == null ? 0L : expectedValue.longValue(),
                    context + " checkedKey=" + key);
            assertEquals(actual.getOrDefault(key, missingValue),
                    expectedValue == null ? missingValue : expectedValue.longValue(), context + " checkedKey=" + key);
        }

        Map<Long, Long> actualEntries = new HashMap<>();
        actual.forEach(actualEntries::put);
        assertEquals(actualEntries, expected, context);
    }

    private static long randomSeed(String testName) {
        String configuredSeed = System.getProperty("pulsar.collections.randomSeed");
        long seed = configuredSeed != null ? Long.parseLong(configuredSeed) : ThreadLocalRandom.current().nextLong();
        String message = Long2LongOpenHashMapTest.class.getSimpleName() + "." + testName + " seed=" + seed;
        Reporter.log(message, true);
        System.err.println(message);
        return seed;
    }

    private static long randomValue(Random random) {
        return switch (random.nextInt(32)) {
            case 0 -> 0L;
            case 1 -> Long.MIN_VALUE;
            case 2 -> Long.MAX_VALUE;
            default -> random.nextInt(1_024) - 512L;
        };
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

    private static long valueForIndex(int index) {
        return index % 3 == 0 ? 0L : index * 101L - 17L;
    }
}
