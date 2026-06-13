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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.testng.annotations.Test;

public class Long2IntOpenHashMapTest {

    @Test
    public void testEmpty() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        assertTrue(map.isEmpty());
        assertEquals(map.get(0), 0);
        assertEquals(map.get(1), 0);
    }

    @Test
    public void testPutGet() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        assertEquals(map.put(1, 10), 0);
        assertEquals(map.put(2, 20), 0);
        assertFalse(map.isEmpty());
        assertEquals(map.get(1), 10);
        assertEquals(map.get(2), 20);
        assertEquals(map.get(3), 0); // default
    }

    @Test
    public void testPutReplace() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        map.put(1, 10);
        assertEquals(map.put(1, 100), 10);
        assertEquals(map.get(1), 100);
    }

    @Test
    public void testRemove() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        map.put(1, 10);
        map.put(2, 20);
        assertEquals(map.remove(1), 10);
        assertEquals(map.get(1), 0); // default after removal
        assertEquals(map.remove(99), 0); // not present
    }

    @Test
    public void testGetOrDefault() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        map.put(1, 10);
        assertEquals(map.getOrDefault(1, -1), 10);
        assertEquals(map.getOrDefault(2, -1), -1);
    }

    @Test
    public void testComputeIfAbsent() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        assertEquals(map.computeIfAbsent(1, k -> 10), 10);
        assertEquals(map.computeIfAbsent(1, k -> 99), 10);
    }

    @Test
    public void testClear() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        map.put(1, 10);
        map.put(2, 20);
        map.clear();
        assertTrue(map.isEmpty());
        assertEquals(map.get(1), 0);
    }

    @Test
    public void testRehash() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap(4);
        for (int i = 0; i < 100; i++) {
            map.put(i, i * 10);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), i * 10);
        }
    }

    @Test
    public void testRemovePreservesProbeChainWithCollisions() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap(4);
        List<Long> keys = collidingLongKeys(16, 8);

        for (int i = 0; i < keys.size(); i++) {
            assertEquals(map.put(keys.get(i), i + 100), 0);
        }

        assertEquals(map.remove(keys.get(0)), 100);
        assertEquals(map.remove(keys.get(4)), 104);
        assertEquals(map.remove(keys.get(7)), 107);

        for (int i = 1; i < keys.size() - 1; i++) {
            long key = keys.get(i);
            if (i != 4) {
                assertEquals(map.get(key), i + 100);
            }
        }
        assertEquals(map.get(keys.get(0)), 0);
        assertEquals(map.get(keys.get(4)), 0);
        assertEquals(map.get(keys.get(7)), 0);

        assertEquals(map.put(keys.get(4), 1_004), 0);
        assertEquals(map.get(keys.get(4)), 1_004);
    }

    @Test
    public void testGetOrDefaultDistinguishesExplicitZeroValue() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        map.put(Long.MAX_VALUE, 0);

        assertEquals(map.get(Long.MAX_VALUE), 0);
        assertEquals(map.getOrDefault(Long.MAX_VALUE, -1), 0);
        assertEquals(map.getOrDefault(Long.MIN_VALUE, -1), -1);
    }

    @Test
    public void testRandomizedOperationsAgainstHashMap() {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap(4);
        Map<Long, Integer> expected = new HashMap<>();
        Set<Long> seenKeys = new HashSet<>();
        Random random = new Random(0x5eed1234L);

        for (int i = 0; i < 20_000; i++) {
            long key = randomLongWithEdgeCases(random, 256);
            seenKeys.add(key);

            int operation = random.nextInt(100);
            if (operation < 35) {
                int value = randomValue(random);
                Integer previous = expected.put(key, value);
                assertEquals(map.put(key, value), previous == null ? 0 : previous.intValue());
            } else if (operation < 55) {
                Integer previous = expected.remove(key);
                assertEquals(map.remove(key), previous == null ? 0 : previous.intValue());
            } else if (operation < 75) {
                int value = randomValue(random);
                int expectedValue = expected.computeIfAbsent(key, __ -> value);
                assertEquals(map.computeIfAbsent(key, __ -> value), expectedValue);
            } else if (operation < 95) {
                int defaultValue = 10_000 + random.nextInt(1_000);
                assertEquals(map.getOrDefault(key, defaultValue),
                        expected.getOrDefault(key, defaultValue).intValue());
            } else {
                map.clear();
                expected.clear();
            }

            assertLong2IntMapMatches(expected, seenKeys, map);
        }
    }

    private static void assertLong2IntMapMatches(Map<Long, Integer> expected, Set<Long> seenKeys,
                                                 Long2IntOpenHashMap actual) {
        int missingValue = Integer.MIN_VALUE;
        assertEquals(actual.isEmpty(), expected.isEmpty());
        for (long key : seenKeys) {
            assertEquals(actual.getOrDefault(key, missingValue),
                    expected.getOrDefault(key, missingValue).intValue());
            assertEquals(actual.get(key), expected.getOrDefault(key, 0).intValue());
        }
    }

    private static int randomValue(Random random) {
        return switch (random.nextInt(32)) {
            case 0 -> 0;
            case 1 -> Integer.MIN_VALUE + 1;
            case 2 -> Integer.MAX_VALUE;
            default -> random.nextInt(512) - 256;
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
}
