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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.testng.annotations.Test;

public class HashSetTest {

    @Test
    public void testLongOpenHashSetBasic() {
        LongOpenHashSet set = new LongOpenHashSet();
        assertTrue(set.isEmpty());
        assertTrue(set.add(1));
        assertTrue(set.add(2));
        assertFalse(set.add(1)); // duplicate
        assertEquals(set.size(), 2);
        assertTrue(set.contains(1));
        assertTrue(set.contains(2));
        assertFalse(set.contains(3));
    }

    @Test
    public void testLongOpenHashSetIterable() {
        LongOpenHashSet set = new LongOpenHashSet();
        set.add(3);
        set.add(1);
        set.add(2);
        List<Long> values = new ArrayList<>();
        for (long v : set) {
            values.add(v);
        }
        Collections.sort(values);
        assertEquals(values, List.of(1L, 2L, 3L));
    }

    @Test
    public void testLongOpenHashSetForEach() {
        LongOpenHashSet set = new LongOpenHashSet();
        set.add(10);
        set.add(20);
        List<Long> values = new ArrayList<>();
        set.forEach((long v) -> values.add(v));
        Collections.sort(values);
        assertEquals(values, List.of(10L, 20L));
    }

    @Test
    public void testLongOpenHashSetRehash() {
        LongOpenHashSet set = new LongOpenHashSet(4);
        for (int i = 0; i < 100; i++) {
            set.add(i);
        }
        assertEquals(set.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertTrue(set.contains(i));
        }
    }

    @Test
    public void testLongOpenHashSetHandlesCollisionCluster() {
        LongOpenHashSet set = new LongOpenHashSet(4);
        List<Long> values = collidingLongValues(16, 20);

        for (long value : values) {
            assertTrue(set.add(value));
            assertFalse(set.add(value));
        }

        assertEquals(set.size(), values.size());
        for (long value : values) {
            assertTrue(set.contains(value));
        }
        assertFalse(set.contains(Long.MIN_VALUE + 1));
    }

    @Test
    public void testLongOpenHashSetRandomizedAgainstHashSet() {
        LongOpenHashSet set = new LongOpenHashSet(4);
        Set<Long> expected = new HashSet<>();
        Set<Long> seenValues = new HashSet<>();
        Random random = new Random(0x5eed1234L);

        for (int i = 0; i < 10_000; i++) {
            long value = randomLongWithEdgeCases(random, 512);
            seenValues.add(value);

            assertEquals(set.add(value), expected.add(value));
            assertLongOpenHashSetMatches(expected, seenValues, set);
        }
    }

    @Test
    public void testIntOpenHashSetBasic() {
        IntOpenHashSet set = new IntOpenHashSet();
        assertTrue(set.isEmpty());
        assertTrue(set.add(1));
        assertTrue(set.add(2));
        assertFalse(set.add(1)); // duplicate
        assertEquals(set.size(), 2);
        assertTrue(set.contains(1));
        assertTrue(set.contains(2));
        assertFalse(set.contains(3));
    }

    @Test
    public void testIntOpenHashSetRehash() {
        IntOpenHashSet set = new IntOpenHashSet(4);
        for (int i = 0; i < 100; i++) {
            set.add(i);
        }
        assertEquals(set.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertTrue(set.contains(i));
        }
    }

    @Test
    public void testIntOpenHashSetHandlesCollisionCluster() {
        IntOpenHashSet set = new IntOpenHashSet(4);
        List<Integer> values = collidingIntValues(16, 20);

        for (int value : values) {
            assertTrue(set.add(value));
            assertFalse(set.add(value));
        }

        assertEquals(set.size(), values.size());
        for (int value : values) {
            assertTrue(set.contains(value));
        }
        assertFalse(set.contains(Integer.MIN_VALUE + 1));
    }

    @Test
    public void testIntOpenHashSetRandomizedAgainstHashSet() {
        IntOpenHashSet set = new IntOpenHashSet(4);
        Set<Integer> expected = new HashSet<>();
        Set<Integer> seenValues = new HashSet<>();
        Random random = new Random(0x5eed1234L);

        for (int i = 0; i < 10_000; i++) {
            int value = randomIntWithEdgeCases(random, 512);
            seenValues.add(value);

            assertEquals(set.add(value), expected.add(value));
            assertIntOpenHashSetMatches(expected, seenValues, set);
        }
    }

    private static void assertLongOpenHashSetMatches(Set<Long> expected, Set<Long> seenValues, LongOpenHashSet actual) {
        assertEquals(actual.isEmpty(), expected.isEmpty());
        assertEquals(actual.size(), expected.size());
        for (long value : seenValues) {
            assertEquals(actual.contains(value), expected.contains(value));
        }

        Set<Long> iterated = new HashSet<>();
        for (long value : actual) {
            iterated.add(value);
        }
        assertEquals(iterated, expected);

        Set<Long> forEachValues = new HashSet<>();
        actual.forEach((long value) -> forEachValues.add(value));
        assertEquals(forEachValues, expected);
    }

    private static void assertIntOpenHashSetMatches(Set<Integer> expected, Set<Integer> seenValues,
                                                    IntOpenHashSet actual) {
        assertEquals(actual.isEmpty(), expected.isEmpty());
        assertEquals(actual.size(), expected.size());
        for (int value : seenValues) {
            assertEquals(actual.contains(value), expected.contains(value));
        }
    }

    private static long randomLongWithEdgeCases(Random random, int bound) {
        return switch (random.nextInt(64)) {
            case 0 -> 0L;
            case 1 -> Long.MIN_VALUE;
            case 2 -> Long.MAX_VALUE;
            default -> random.nextInt(bound) - bound / 2L;
        };
    }

    private static int randomIntWithEdgeCases(Random random, int bound) {
        return switch (random.nextInt(64)) {
            case 0 -> 0;
            case 1 -> Integer.MIN_VALUE;
            case 2 -> Integer.MAX_VALUE;
            default -> random.nextInt(bound) - bound / 2;
        };
    }

    private static List<Long> collidingLongValues(int capacity, int count) {
        int mask = capacity - 1;
        int bucket = Long2ObjectOpenHashMap.hash(0) & mask;
        List<Long> values = new ArrayList<>();
        for (long candidate = 0; values.size() < count; candidate++) {
            if ((Long2ObjectOpenHashMap.hash(candidate) & mask) == bucket) {
                values.add(candidate);
            }
        }
        return values;
    }

    private static List<Integer> collidingIntValues(int capacity, int count) {
        int mask = capacity - 1;
        int bucket = hash(0) & mask;
        List<Integer> values = new ArrayList<>();
        for (int candidate = 0; values.size() < count; candidate++) {
            if ((hash(candidate) & mask) == bucket) {
                values.add(candidate);
            }
        }
        return values;
    }

    private static int hash(int value) {
        int h = value * 0x9E3779B9;
        return h ^ (h >>> 16);
    }
}
