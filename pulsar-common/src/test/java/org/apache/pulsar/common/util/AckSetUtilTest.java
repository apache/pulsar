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
package org.apache.pulsar.common.util;

import java.util.BitSet;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AckSetUtilTest {

    // ---- cardinality ----

    @Test
    public void testCardinalityEmptyArray() {
        Assert.assertEquals(AckSetUtil.cardinality(new long[0]), 0);
    }

    @Test
    public void testCardinalityAllZero() {
        Assert.assertEquals(AckSetUtil.cardinality(new long[]{0L, 0L, 0L}), 0);
    }

    @Test
    public void testCardinalityAllOnes() {
        // -1L == all 64 bits set
        Assert.assertEquals(AckSetUtil.cardinality(new long[]{-1L}), 64);
    }

    @Test
    public void testCardinalityMixed() {
        // 0b1011 has 3 bits set; Long.MIN_VALUE (0x8000...0) has 1 bit set → total 4
        Assert.assertEquals(AckSetUtil.cardinality(new long[]{0b1011L, 0L, Long.MIN_VALUE}), 4);
    }

    @Test
    public void testCardinalityLongMaxValue() {
        // Long.MAX_VALUE has 63 bits set
        Assert.assertEquals(AckSetUtil.cardinality(new long[]{Long.MAX_VALUE}), 63);
    }

    @Test
    public void testCardinalityMultipleWords() {
        // 0b1111 = 4 bits; 0b1010 = 2 bits → total 6
        Assert.assertEquals(AckSetUtil.cardinality(new long[]{0b1111L, 0b1010L}), 6);
    }

    // ---- cardinalityOfIntersection ----

    @Test
    public void testAndCardinalityBothEmpty() {
        Assert.assertEquals(AckSetUtil.cardinalityOfIntersection(new long[0], new long[0]), 0);
    }

    @Test
    public void testAndCardinalityFirstEmpty() {
        Assert.assertEquals(AckSetUtil.cardinalityOfIntersection(new long[0], new long[]{Long.MAX_VALUE}), 0);
    }

    @Test
    public void testAndCardinalitySecondEmpty() {
        Assert.assertEquals(AckSetUtil.cardinalityOfIntersection(new long[]{Long.MAX_VALUE}, new long[0]), 0);
    }

    @Test
    public void testAndCardinalityNoOverlap() {
        // 0b1010 & 0b0101 == 0
        Assert.assertEquals(AckSetUtil.cardinalityOfIntersection(new long[]{0b1010L}, new long[]{0b0101L}), 0);
    }

    @Test
    public void testAndCardinalityFullOverlap() {
        // -1L & -1L == -1L → 64 bits
        Assert.assertEquals(AckSetUtil.cardinalityOfIntersection(new long[]{-1L}, new long[]{-1L}), 64);
    }

    @Test
    public void testAndCardinalityPartialOverlap() {
        // 0b1111 & 0b1010 == 0b1010 → 2 bits
        Assert.assertEquals(AckSetUtil.cardinalityOfIntersection(new long[]{0b1111L}, new long[]{0b1010L}), 2);
    }

    @Test
    public void testAndCardinalityFirstLonger() {
        // Extra words in first array beyond second's length contribute 0 to AND result
        // Long.MAX_VALUE & (nothing) == 0; 0b1010 & 0b1110 == 0b1010 → 2 bits
        Assert.assertEquals(AckSetUtil.cardinalityOfIntersection(
                new long[]{Long.MAX_VALUE, 0b1010L, Long.MAX_VALUE},
                new long[]{0L, 0b1110L}), 2);
    }

    @Test
    public void testAndCardinalitySecondLonger() {
        // Extra words in second array beyond first's length contribute 0
        Assert.assertEquals(AckSetUtil.cardinalityOfIntersection(
                new long[]{0b1110L},
                new long[]{0b1010L, Long.MAX_VALUE, Long.MAX_VALUE}), 2);
    }

    @Test
    public void testAndCardinalityMultipleWords() {
        // word 0: 0b1111 & 0b1010 == 0b1010 → 2 bits
        // word 1: -1L   & 0b0011 == 0b0011 → 2 bits
        // total: 4
        Assert.assertEquals(AckSetUtil.cardinalityOfIntersection(
                new long[]{0b1111L, -1L},
                new long[]{0b1010L, 0b0011L}), 4);
    }

    // ---- intersect ----

    @Test
    public void testIntersectBothEmpty() {
        Assert.assertEquals(AckSetUtil.intersect(new long[0], new long[0]), new long[0]);
    }

    @Test
    public void testIntersectFirstEmpty() {
        Assert.assertEquals(AckSetUtil.intersect(new long[0], new long[]{-1L}), new long[0]);
    }

    @Test
    public void testIntersectSecondEmpty() {
        Assert.assertEquals(AckSetUtil.intersect(new long[]{-1L}, new long[0]), new long[0]);
    }

    @Test
    public void testIntersectNoOverlap() {
        // 0b1010 & 0b0101 == 0 → the all-zero word is trimmed, leaving an empty array
        Assert.assertEquals(AckSetUtil.intersect(new long[]{0b1010L}, new long[]{0b0101L}), new long[0]);
    }

    @Test
    public void testIntersectFullOverlap() {
        Assert.assertEquals(AckSetUtil.intersect(new long[]{-1L}, new long[]{-1L}), new long[]{-1L});
    }

    @Test
    public void testIntersectPartialOverlap() {
        // 0b1111 & 0b1010 == 0b1010
        Assert.assertEquals(AckSetUtil.intersect(new long[]{0b1111L}, new long[]{0b1010L}),
                new long[]{0b1010L});
    }

    @Test
    public void testIntersectFirstLonger() {
        // Result length = min(3, 2) = 2; extra word in first is dropped
        Assert.assertEquals(
                AckSetUtil.intersect(new long[]{0b1111L, 0b1010L, -1L}, new long[]{0b1100L, 0b1110L}),
                new long[]{0b1100L, 0b1010L});
    }

    @Test
    public void testIntersectSecondLonger() {
        // Result length = min(2, 3) = 2; extra word in second is dropped
        Assert.assertEquals(
                AckSetUtil.intersect(new long[]{0b1111L, 0b1010L}, new long[]{0b1100L, 0b1110L, -1L}),
                new long[]{0b1100L, 0b1010L});
    }

    @Test
    public void testIntersectMultipleWords() {
        Assert.assertEquals(
                AckSetUtil.intersect(new long[]{0b1111L, -1L}, new long[]{0b1010L, 0b0011L}),
                new long[]{0b1010L, 0b0011L});
    }

    @Test
    public void testIntersectTrimsTrailingZeroWords() {
        // High words AND to zero → result must be trimmed to the canonical form of BitSet#toLongArray
        Assert.assertEquals(
                AckSetUtil.intersect(new long[]{-1L, 0b01L}, new long[]{-1L, 0b10L}),
                new long[]{-1L});
        // Same result as BitSet-based AND followed by toLongArray()
        BitSet bitSet = BitSet.valueOf(new long[]{-1L, 0b01L});
        bitSet.and(BitSet.valueOf(new long[]{-1L, 0b10L}));
        Assert.assertEquals(AckSetUtil.intersect(new long[]{-1L, 0b01L}, new long[]{-1L, 0b10L}),
                bitSet.toLongArray());
    }

    @Test
    public void testIntersectTrimsAllZeroResultToEmpty() {
        Assert.assertEquals(
                AckSetUtil.intersect(new long[]{0b1010L, 0b01L}, new long[]{0b0101L, 0b10L}),
                new long[0]);
    }
}
