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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.testng.annotations.Test;

public class Int2ObjectOpenHashMapTest {

    @Test
    public void testEmpty() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        assertTrue(map.isEmpty());
        assertNull(map.get(1));
    }

    @Test
    public void testPutGet() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        assertNull(map.put(1, "one"));
        assertNull(map.put(2, "two"));
        assertEquals(map.get(1), "one");
        assertEquals(map.get(2), "two");
        assertNull(map.get(3));
        assertEquals(map.size(), 2);
    }

    @Test
    public void testRemove() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        assertEquals(map.remove(1), "one");
        assertNull(map.get(1));
        assertEquals(map.size(), 1);
    }

    @Test
    public void testRemoveConditional() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        String val = "one";
        map.put(1, val);
        assertFalse(map.remove(1, "other")); // different ref
        assertTrue(map.remove(1, val));
        assertTrue(map.isEmpty());
    }

    @Test
    public void testRemoveConditionalUsesReferenceEquality() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        String value = new String("one");
        String equalValue = new String("one");
        map.put(1, value);

        assertFalse(map.remove(1, equalValue));
        assertEquals(map.get(1), value);
        assertTrue(map.remove(1, value));
        assertTrue(map.isEmpty());
    }

    @Test
    public void testClear() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        map.clear();
        assertTrue(map.isEmpty());
        assertNull(map.get(1));
    }

    @Test
    public void testRehash() {
        Int2ObjectOpenHashMap<Integer> map = new Int2ObjectOpenHashMap<>(4);
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        assertEquals(map.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), Integer.valueOf(i));
        }
    }

    @Test
    public void testRemovePreservesProbeChainWithCollisions() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>(4);
        List<Integer> keys = collidingIntKeys(16, 8);

        for (int i = 0; i < keys.size(); i++) {
            assertNull(map.put(keys.get(i), "v" + i));
        }

        assertEquals(map.remove(keys.get(0)), "v0");
        assertEquals(map.remove(keys.get(4)), "v4");
        assertEquals(map.remove(keys.get(7)), "v7");

        for (int i = 1; i < keys.size() - 1; i++) {
            int key = keys.get(i);
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
    public void testRandomizedOperationsAgainstHashMap() {
        Int2ObjectOpenHashMap<String> map = new Int2ObjectOpenHashMap<>(4);
        Map<Integer, String> expected = new HashMap<>();
        Set<Integer> seenKeys = new HashSet<>();
        Random random = new Random(0x5eed1234L);

        for (int i = 0; i < 20_000; i++) {
            int key = randomIntWithEdgeCases(random, 256);
            seenKeys.add(key);

            int operation = random.nextInt(100);
            if (operation < 35) {
                String value = randomValue(random);
                assertEquals(map.put(key, value), expected.put(key, value));
            } else if (operation < 55) {
                assertEquals(map.remove(key), expected.remove(key));
            } else if (operation < 75) {
                String current = expected.get(key);
                String candidate = current != null && random.nextBoolean() ? current : randomValue(random);
                boolean expectedRemoved = current != null && current == candidate;
                if (expectedRemoved) {
                    expected.remove(key);
                }
                assertEquals(map.remove(key, candidate), expectedRemoved);
            } else if (operation < 95) {
                assertEquals(map.get(key), expected.get(key));
            } else {
                map.clear();
                expected.clear();
            }

            assertInt2ObjectMapMatches(expected, seenKeys, map);
        }
    }

    private static void assertInt2ObjectMapMatches(Map<Integer, String> expected, Set<Integer> seenKeys,
                                                   Int2ObjectOpenHashMap<String> actual) {
        assertEquals(actual.isEmpty(), expected.isEmpty());
        assertEquals(actual.size(), expected.size());
        for (int key : seenKeys) {
            assertEquals(actual.get(key), expected.get(key));
        }
    }

    private static String randomValue(Random random) {
        return "v" + random.nextInt(512);
    }

    private static int randomIntWithEdgeCases(Random random, int bound) {
        return switch (random.nextInt(64)) {
            case 0 -> 0;
            case 1 -> Integer.MIN_VALUE;
            case 2 -> Integer.MAX_VALUE;
            default -> random.nextInt(bound) - bound / 2;
        };
    }

    private static List<Integer> collidingIntKeys(int capacity, int count) {
        int mask = capacity - 1;
        int bucket = hash(0) & mask;
        List<Integer> keys = new ArrayList<>();
        for (int candidate = 0; keys.size() < count; candidate++) {
            if ((hash(candidate) & mask) == bucket) {
                keys.add(candidate);
            }
        }
        return keys;
    }

    private static int hash(int key) {
        int h = key * 0x9E3779B9;
        return h ^ (h >>> 16);
    }
}
