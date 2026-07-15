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
import lombok.Cleanup;
import org.testng.annotations.Test;

public class SegmentedLongArrayTest {

    @Test
    public void testArray() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(4);
        assertEquals(a.getCapacity(), 4);
        assertEquals(a.bytesCapacity(), 4 * Long.BYTES);
        assertEquals(a.getInitialCapacity(), 4);

        a.writeLong(0, 0);
        a.writeLong(1, 1);
        a.writeLong(2, 2);
        a.writeLong(3, Long.MAX_VALUE);

        a.increaseCapacity();
        a.writeLong(4, Long.MIN_VALUE);

        assertEquals(a.getCapacity(), 8);
        assertEquals(a.bytesCapacity(), 8 * Long.BYTES);

        assertEquals(a.readLong(0), 0);
        assertEquals(a.readLong(1), 1);
        assertEquals(a.readLong(2), 2);
        assertEquals(a.readLong(3), Long.MAX_VALUE);
        assertEquals(a.readLong(4), Long.MIN_VALUE);

        a.shrink(5);
        assertEquals(a.getCapacity(), 5);
        assertEquals(a.getInitialCapacity(), 4);
    }

    @Test
    public void testLargeArray() {
        long initialCap = 3 * 1024 * 1024;

        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(initialCap);
        assertEquals(a.getCapacity(), initialCap);
        assertEquals(a.bytesCapacity(), initialCap * Long.BYTES);
        assertEquals(a.getInitialCapacity(), initialCap);

        long baseOffset = initialCap - 100;

        a.writeLong(baseOffset, 0);
        a.writeLong(baseOffset + 1, 1);
        a.writeLong(baseOffset + 2, 2);
        a.writeLong(baseOffset + 3, Long.MAX_VALUE);
        a.writeLong(baseOffset + 4, Long.MIN_VALUE);

        a.increaseCapacity();

        assertEquals(a.getCapacity(), 5 * 1024 * 1024);
        assertEquals(a.bytesCapacity(), 5 * 1024 * 1024 * Long.BYTES);
        assertEquals(a.getInitialCapacity(), initialCap);

        assertEquals(a.readLong(baseOffset), 0);
        assertEquals(a.readLong(baseOffset + 1), 1);
        assertEquals(a.readLong(baseOffset + 2), 2);
        assertEquals(a.readLong(baseOffset + 3), Long.MAX_VALUE);
        assertEquals(a.readLong(baseOffset + 4), Long.MIN_VALUE);

        a.shrink(initialCap);
        assertEquals(a.getCapacity(), initialCap);
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
        long start = SegmentedLongArray.SEGMENT_SIZE - 100;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(start);
        assertEquals(a.getCapacity(), start);

        a.increaseCapacity();
        assertEquals(a.getCapacity(), SegmentedLongArray.SEGMENT_SIZE);

        a.increaseCapacity();
        assertEquals(a.getCapacity(), SegmentedLongArray.SEGMENT_SIZE * 2);
    }

    @Test
    public void testMultiSegmentIncreaseCapacity() {
        long initialCap = SegmentedLongArray.SEGMENT_SIZE * 3;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(initialCap);

        for (int i = 0; i < 20; i++) {
            a.increaseCapacity();
        }

        long expectedCap = SegmentedLongArray.SEGMENT_SIZE * 23L;
        assertEquals(a.getCapacity(), expectedCap);

        for (int i = 0; i < 23; i++) {
            long offset = (long) i * SegmentedLongArray.SEGMENT_SIZE + 42;
            a.writeLong(offset, i);
            assertEquals(a.readLong(offset), i);
        }
    }

    @Test
    public void testShrinkDropsWholeSegments() {
        long segSize = SegmentedLongArray.SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(segSize);
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
    public void testShrinkNoOpWhenEqual() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(100);
        a.increaseCapacity();
        a.shrink(200);
        assertEquals(a.getCapacity(), 200);
    }

    @Test
    public void testShrinkNoOpWhenExceeds() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(100);
        a.increaseCapacity();
        a.shrink(300);
        assertEquals(a.getCapacity(), 200);
    }

    @Test
    public void testSegmentBoundaryReadWrite() {
        long segSize = SegmentedLongArray.SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(segSize * 2);

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
        long segSize = SegmentedLongArray.SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(segSize);
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
    public void testNegativeOffset() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(10);
        assertThrows(IndexOutOfBoundsException.class, () -> a.readLong(-1));
    }

    @Test
    public void testWriteAcrossSegmentBoundaryAfterGrowth() {
        long segSize = SegmentedLongArray.SEGMENT_SIZE;
        long initialCap = segSize * 3 / 2;

        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(initialCap);
        a.increaseCapacity();

        long testOffset = initialCap;
        a.writeLong(testOffset, 12345L);
        assertEquals(a.readLong(testOffset), 12345L);

        long boundaryOffset = segSize * 2 - 1;
        a.writeLong(boundaryOffset, 67890L);
        assertEquals(a.readLong(boundaryOffset), 67890L);

        a.writeLong(segSize * 2, 11111L);
        assertEquals(a.readLong(segSize * 2), 11111L);
    }

    @Test
    public void testNonSegmentMultipleInitialCapacity() {
        long segSize = SegmentedLongArray.SEGMENT_SIZE;
        long[] testCaps = {1, 100, segSize - 1, segSize + 1, segSize * 2 + 50};

        for (long cap : testCaps) {
            @Cleanup
            SegmentedLongArray a = new SegmentedLongArray(cap);
            long limit = Math.min(cap, 1000);
            for (long i = 0; i < limit; i++) {
                a.writeLong(i, i * 7);
            }
            for (long i = 0; i < limit; i++) {
                assertEquals(a.readLong(i), i * 7, "Failed at capacity " + cap + " offset " + i);
            }
        }
    }
}
