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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import lombok.Cleanup;
import org.testng.annotations.Test;

public class SegmentedLongArrayTest {

    /** Small segment size so tests can exercise many-segment behavior without 16 MiB allocations. */
    private static final int TEST_SEGMENT_SIZE = 1024;

    @Test
    public void testArray() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(4);
        assertEquals(a.getCapacity(), 4);
        assertEquals(a.bytesCapacity(), 4 * 8);
        assertEquals(a.getInitialCapacity(), 4);

        a.writeLong(0, 0);
        a.writeLong(1, 1);
        a.writeLong(2, 2);
        a.writeLong(3, Long.MAX_VALUE);

        assertThrows(IndexOutOfBoundsException.class, () -> a.writeLong(4, Long.MIN_VALUE));

        a.increaseCapacity();
        a.writeLong(4, Long.MIN_VALUE);

        assertEquals(a.getCapacity(), 8);
        assertEquals(a.bytesCapacity(), 8 * 8);
        assertEquals(a.getInitialCapacity(), 4);

        assertEquals(a.readLong(0), 0);
        assertEquals(a.readLong(1), 1);
        assertEquals(a.readLong(2), 2);
        assertEquals(a.readLong(3), Long.MAX_VALUE);
        assertEquals(a.readLong(4), Long.MIN_VALUE);

        a.shrink(5);
        assertEquals(a.getCapacity(), 5);
        assertEquals(a.bytesCapacity(), 5 * 8);
        assertEquals(a.getInitialCapacity(), 4);
    }

    @Test
    public void testLargeArray() {
        long initialCap = TEST_SEGMENT_SIZE + TEST_SEGMENT_SIZE / 2;

        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(initialCap, TEST_SEGMENT_SIZE);
        assertEquals(a.getCapacity(), initialCap);
        assertEquals(a.bytesCapacity(), initialCap * 8);
        assertEquals(a.getInitialCapacity(), initialCap);

        long baseOffset = initialCap - 100;

        a.writeLong(baseOffset, 0);
        a.writeLong(baseOffset + 1, 1);
        a.writeLong(baseOffset + 2, 2);
        a.writeLong(baseOffset + 3, Long.MAX_VALUE);
        a.writeLong(baseOffset + 4, Long.MIN_VALUE);

        a.increaseCapacity();

        long expectedCap = initialCap + TEST_SEGMENT_SIZE;
        assertEquals(a.getCapacity(), expectedCap);
        assertEquals(a.bytesCapacity(), expectedCap * 8);
        assertEquals(a.getInitialCapacity(), initialCap);

        assertEquals(a.readLong(baseOffset), 0);
        assertEquals(a.readLong(baseOffset + 1), 1);
        assertEquals(a.readLong(baseOffset + 2), 2);
        assertEquals(a.readLong(baseOffset + 3), Long.MAX_VALUE);
        assertEquals(a.readLong(baseOffset + 4), Long.MIN_VALUE);

        a.shrink(initialCap);
        assertEquals(a.getCapacity(), initialCap);
        assertEquals(a.bytesCapacity(), initialCap * 8);
        assertEquals(a.getInitialCapacity(), initialCap);
    }

    @Test
    public void testIncreaseCapacityGrowthPattern() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(4);

        a.increaseCapacity();
        assertEquals(a.getCapacity(), 8);
        a.increaseCapacity();
        assertEquals(a.getCapacity(), 16);
        a.increaseCapacity();
        assertEquals(a.getCapacity(), 32);
    }

    @Test
    public void testIncreaseCapacityReachesSegmentBoundary() {
        long start = TEST_SEGMENT_SIZE - 100;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(start, TEST_SEGMENT_SIZE);
        assertEquals(a.getCapacity(), start);

        a.increaseCapacity();
        assertEquals(a.getCapacity(), TEST_SEGMENT_SIZE);

        a.increaseCapacity();
        assertEquals(a.getCapacity(), TEST_SEGMENT_SIZE * 2L);
    }

    @Test
    public void testMultiSegmentIncreaseCapacity() {
        long initialCap = TEST_SEGMENT_SIZE * 3;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(initialCap, TEST_SEGMENT_SIZE);

        for (int i = 0; i < 20; i++) {
            a.increaseCapacity();
        }

        long expectedCap = TEST_SEGMENT_SIZE * 23L;
        assertEquals(a.getCapacity(), expectedCap);

        for (int i = 0; i < 23; i++) {
            long offset = (long) i * TEST_SEGMENT_SIZE + 42;
            a.writeLong(offset, i);
            assertEquals(a.readLong(offset), i);
        }
    }

    @Test
    public void testShrinkDropsWholeSegments() {
        long segSize = TEST_SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(segSize, TEST_SEGMENT_SIZE);
        for (int i = 0; i < 4; i++) {
            a.increaseCapacity();
        }
        assertEquals(a.getCapacity(), segSize * 5);

        for (int i = 0; i < 5; i++) {
            a.writeLong((long) i * segSize, 100L + i);
        }

        a.shrink(segSize * 3);
        assertEquals(a.getCapacity(), segSize * 3);

        for (int i = 0; i < 3; i++) {
            assertEquals(a.readLong((long) i * segSize), 100L + i);
        }
    }

    @Test
    public void testShrinkToInitialCapacity() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(4);
        a.increaseCapacity();
        a.increaseCapacity();

        a.shrink(4);
        assertEquals(a.getCapacity(), 4);
        assertEquals(a.getInitialCapacity(), 4);
    }

    @Test
    public void testShrinkBelowInitialFails() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(100);
        a.shrink(50);
        assertEquals(a.getCapacity(), 100);
    }

    @Test
    public void testSegmentBoundaryReadWrite() {
        long segSize = TEST_SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(segSize * 2, TEST_SEGMENT_SIZE);

        a.writeLong(segSize - 1, 111L);
        a.writeLong(segSize, 222L);

        assertEquals(a.readLong(segSize - 1), 111L);
        assertEquals(a.readLong(segSize), 222L);
    }

    @Test
    public void testRoundTripAllValues() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(1000);

        long[] testValues = {0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE,
                255L, 256L, 65535L, 65536L,
                Integer.MAX_VALUE, Integer.MIN_VALUE};

        for (int i = 0; i < testValues.length; i++) {
            a.writeLong(i, testValues[i]);
        }
        for (int i = 0; i < testValues.length; i++) {
            assertEquals(a.readLong(i), testValues[i]);
        }
    }

    @Test
    public void testCloseReleasesMemory() {
        SegmentedLongArray a = new SegmentedLongArray(100);
        a.close();
        assertThrows(NullPointerException.class, () -> a.readLong(0));
    }

    @Test
    public void testZeroCapacityRejected() {
        assertThrows(IllegalArgumentException.class, () -> {
            @Cleanup
            SegmentedLongArray ignored = new SegmentedLongArray(0);
        });
    }

    @Test
    public void testNegativeCapacityRejected() {
        assertThrows(IllegalArgumentException.class, () -> {
            @Cleanup
            SegmentedLongArray ignored = new SegmentedLongArray(-1);
        });
    }

    @Test
    public void testGrowAfterShrink() {
        long segSize = TEST_SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(segSize, TEST_SEGMENT_SIZE);
        a.increaseCapacity();
        a.increaseCapacity();
        a.shrink(segSize);
        assertEquals(a.getCapacity(), segSize);

        a.writeLong(0, 42L);

        a.increaseCapacity();
        a.writeLong(segSize, 99L);

        assertEquals(a.readLong(0), 42L);
        assertEquals(a.readLong(segSize), 99L);
    }

    @Test
    public void testShrinkNoOpWhenEqual() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(100);
        a.increaseCapacity(); // 200
        a.shrink(200); // newCapacity == capacity, no-op
        assertEquals(a.getCapacity(), 200);
    }

    @Test
    public void testShrinkNoOpWhenExceeds() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(100);
        a.increaseCapacity(); // 200
        a.shrink(300); // newCapacity > capacity, no-op
        assertEquals(a.getCapacity(), 200);
    }

    @Test
    public void testNegativeOffset() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(10);
        assertThrows(IndexOutOfBoundsException.class, () -> a.readLong(-1));
    }

    @Test
    public void testSmallInitialCapacityDoesNotAllocateFullSegment() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(48, TEST_SEGMENT_SIZE);
        assertEquals(a.getCapacity(), 48);
        assertEquals(a.bytesCapacity(), 48 * 8);
        assertTrue(a.bytesCapacity() < TEST_SEGMENT_SIZE * 8);
    }

    @Test
    public void testPartialLastSegmentMapping() {
        long seg = TEST_SEGMENT_SIZE;
        long cap = seg + 1000;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(cap, TEST_SEGMENT_SIZE);
        assertEquals(a.getCapacity(), cap);
        assertEquals(a.bytesCapacity(), cap * 8);
        a.writeLong(0, 1L);
        a.writeLong(seg - 1, 2L);
        a.writeLong(seg, 3L);
        a.writeLong(cap - 1, 4L);
        assertEquals(a.readLong(0), 1L);
        assertEquals(a.readLong(seg - 1), 2L);
        assertEquals(a.readLong(seg), 3L);
        assertEquals(a.readLong(cap - 1), 4L);
    }

    @Test
    public void testIncreaseCapacityPromotesPartialLastToFull() {
        long seg = TEST_SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(seg * 2 + seg / 2, TEST_SEGMENT_SIZE);
        long partialBase = seg * 2;
        a.writeLong(partialBase, 99L);
        a.writeLong(partialBase + seg / 2 - 1, 100L);
        long capacityBefore = a.getCapacity();
        a.increaseCapacity();
        assertTrue(a.getCapacity() > capacityBefore);
        assertEquals(a.readLong(partialBase), 99L);
        assertEquals(a.readLong(partialBase + seg / 2 - 1), 100L);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
    }

    @Test
    public void testBytesCapacityTracksPhysicalThroughGrowAndShrink() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(1000);
        assertEquals(a.getCapacity(), 1000L);
        assertEquals(a.bytesCapacity(), 8000L);
        for (int i = 0; i < 5; i++) {
            a.increaseCapacity();
            assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        }
        a.shrink(2000);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
    }

    @Test
    public void testOffsetMappingAndCapacityAcrossOperations() {
        long seg = TEST_SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(50, TEST_SEGMENT_SIZE);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);

        for (int i = 0; i < 5; i++) {
            a.increaseCapacity();
            assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        }
        a.ensureCapacity(seg * 3 + 1000);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        a.writeLong(0, 1L);
        a.writeLong(seg - 1, 2L);
        a.writeLong(seg, 3L);
        a.writeLong(a.getCapacity() - 1, 4L);
        assertEquals(a.readLong(0), 1L);
        assertEquals(a.readLong(seg - 1), 2L);
        assertEquals(a.readLong(seg), 3L);
        assertEquals(a.readLong(a.getCapacity() - 1), 4L);

        a.shrink(seg * 2 + 500);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        a.shrink(seg);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        a.ensureCapacity(seg * 2 + 100);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        a.writeLong(seg, 7L);
        assertEquals(a.readLong(seg), 7L);
    }

    @Test
    public void testAllocatedBytesDeltaAtEveryMutationSite() {
        long seg = TEST_SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(100, TEST_SEGMENT_SIZE);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);

        for (int i = 0; i < 4; i++) {
            a.increaseCapacity();
            assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        }
        a.ensureCapacity(seg * 3 + 1000);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        a.shrink(seg + 500);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        a.shrink(seg / 2);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        a.ensureCapacity(seg * 2 + 100);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
    }

    @Test
    public void testShrinkTrimsOverallocatedContainer() {
        long seg = TEST_SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(50, TEST_SEGMENT_SIZE);
        a.ensureCapacity(seg * 30);
        assertTrue(a.getCapacity() >= seg * 30);
        a.shrink(seg / 2);
        assertEquals(a.bytesCapacity(), a.getCapacity() * 8);
        a.writeLong(0, 1L);
        a.writeLong(a.getCapacity() - 1, 2L);
        assertEquals(a.readLong(0), 1L);
        assertEquals(a.readLong(a.getCapacity() - 1), 2L);
    }

    @Test
    public void testCustomSegmentSizeRejectsNonPowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> {
            @Cleanup
            SegmentedLongArray ignored = new SegmentedLongArray(100, 1000);
        });
    }

    @Test
    public void testCustomSegmentSizeRejectsZero() {
        assertThrows(IllegalArgumentException.class, () -> {
            @Cleanup
            SegmentedLongArray ignored = new SegmentedLongArray(100, 0);
        });
    }

    @Test
    public void testCustomSegmentSizeRejectsNegative() {
        assertThrows(IllegalArgumentException.class, () -> {
            @Cleanup
            SegmentedLongArray ignored = new SegmentedLongArray(100, -1024);
        });
    }
}
