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
package org.apache.pulsar.common.scalable;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

public class HashRangeTest {

    @Test
    public void testFullRange() {
        HashRange full = HashRange.full();
        assertEquals(full.start(), 0x0000);
        assertEquals(full.end(), 0xFFFF);
        assertEquals(full.size(), 65536);
    }

    @Test
    public void testContainsHash() {
        HashRange range = HashRange.of(0x1000, 0x2000);
        assertTrue(range.contains(0x1000));
        assertTrue(range.contains(0x1500));
        assertTrue(range.contains(0x2000));
        assertFalse(range.contains(0x0FFF));
        assertFalse(range.contains(0x2001));
    }

    @Test
    public void testContainsRange() {
        HashRange outer = HashRange.of(0x1000, 0x3000);
        HashRange inner = HashRange.of(0x1500, 0x2500);
        assertTrue(outer.contains(inner));
        assertFalse(inner.contains(outer));
    }

    @Test
    public void testSplit() {
        HashRange full = HashRange.full();
        HashRange[] halves = full.split();
        assertEquals(halves[0], HashRange.of(0x0000, 0x7FFF));
        assertEquals(halves[1], HashRange.of(0x8000, 0xFFFF));
        assertEquals(halves[0].size() + halves[1].size(), full.size());
    }

    @Test
    public void testSplitSmallRange() {
        HashRange range = HashRange.of(100, 101);
        HashRange[] parts = range.split();
        assertEquals(parts[0], HashRange.of(100, 100));
        assertEquals(parts[1], HashRange.of(101, 101));
    }

    @Test
    public void testCannotSplitSingleValue() {
        HashRange range = HashRange.of(42, 42);
        assertThrows(IllegalStateException.class, range::split);
    }

    @Test
    public void testMergeAdjacent() {
        HashRange left = HashRange.of(0x0000, 0x7FFF);
        HashRange right = HashRange.of(0x8000, 0xFFFF);
        assertEquals(left.merge(right), HashRange.full());
        assertEquals(right.merge(left), HashRange.full());
    }

    @Test
    public void testMergeNonAdjacent() {
        HashRange a = HashRange.of(0x0000, 0x3FFF);
        HashRange b = HashRange.of(0x8000, 0xFFFF);
        assertThrows(IllegalArgumentException.class, () -> a.merge(b));
    }

    @Test
    public void testIsAdjacentTo() {
        HashRange a = HashRange.of(0x0000, 0x7FFF);
        HashRange b = HashRange.of(0x8000, 0xFFFF);
        assertTrue(a.isAdjacentTo(b));
        assertTrue(b.isAdjacentTo(a));
        assertFalse(a.isAdjacentTo(HashRange.of(0x9000, 0xFFFF)));
    }

    @Test
    public void testHexRoundTrip() {
        HashRange range = HashRange.of(0x0A0B, 0xF0F0);
        String hex = range.toHexString();
        assertEquals(hex, "0a0b-f0f0");
        assertEquals(HashRange.fromHexString(hex), range);
    }

    @Test
    public void testComparable() {
        HashRange a = HashRange.of(0x1000, 0x2000);
        HashRange b = HashRange.of(0x3000, 0x4000);
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
        assertEquals(a.compareTo(a), 0);
    }

    @Test
    public void testInvalidRange() {
        assertThrows(IllegalArgumentException.class, () -> HashRange.of(-1, 100));
        assertThrows(IllegalArgumentException.class, () -> HashRange.of(0, 0x10000));
        assertThrows(IllegalArgumentException.class, () -> HashRange.of(100, 50));
    }

    @Test
    public void testToString() {
        HashRange range = HashRange.of(0x0000, 0xFFFF);
        assertEquals(range.toString(), "[0000, ffff]");
    }
}
