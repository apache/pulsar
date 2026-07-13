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
import static org.testng.Assert.fail;
import lombok.Cleanup;
import org.testng.annotations.Test;

public class SegmentedLongArrayTest {

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

        try {
            a.writeLong(4, Long.MIN_VALUE);
            fail("should have failed");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

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
        long initialCap = 3 * 1024 * 1024;

        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(initialCap);
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

        assertEquals(a.getCapacity(), 5 * 1024 * 1024);
        assertEquals(a.bytesCapacity(), 5 * 1024 * 1024 * 8);
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

        // Small capacity growth: doubles while <= 256
        a.increaseCapacity();
        assertEquals(a.getCapacity(), 8);   // 4 + 4
        a.increaseCapacity();
        assertEquals(a.getCapacity(), 16);  // 8 + 8
        a.increaseCapacity();
        assertEquals(a.getCapacity(), 32);  // 16 + 16
    }

    @Test
    public void testIncreaseCapacityReachesSegmentBoundary() {
        // Start just below SEGMENT_SIZE and grow to exactly SEGMENT_SIZE
        long start = SegmentedLongArray.SEGMENT_SIZE - 100;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(start);
        assertEquals(a.getCapacity(), start);

        a.increaseCapacity();
        // Should cap at SEGMENT_SIZE
        assertEquals(a.getCapacity(), SegmentedLongArray.SEGMENT_SIZE);

        // Next increase should add a new segment
        a.increaseCapacity();
        assertEquals(a.getCapacity(), SegmentedLongArray.SEGMENT_SIZE * 2);
    }

    @Test
    public void testMultiSegmentIncreaseCapacity() {
        // Start at 3 segments
        long initialCap = SegmentedLongArray.SEGMENT_SIZE * 3;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(initialCap);

        // Increase by adding segments
        for (int i = 0; i < 20; i++) {
            a.increaseCapacity();
        }

        // Should have 23 segments (3 + 20)
        long expectedCap = SegmentedLongArray.SEGMENT_SIZE * 23L;
        assertEquals(a.getCapacity(), expectedCap);

        // Verify data integrity across all segments
        for (int i = 0; i < 23; i++) {
            long offset = (long) i * SegmentedLongArray.SEGMENT_SIZE + 42;
            a.writeLong(offset, i);
            assertEquals(a.readLong(offset), i);
        }
    }

    @Test
    public void testShrinkDropsWholeSegments() {
        long segSize = SegmentedLongArray.SEGMENT_SIZE;
        // Start with 1 segment, grow to 5
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(segSize);
        for (int i = 0; i < 4; i++) {
            a.increaseCapacity();
        }
        assertEquals(a.getCapacity(), segSize * 5);

        // Write data in each segment
        for (int i = 0; i < 5; i++) {
            a.writeLong((long) i * segSize, 100L + i);
        }

        // Shrink by 2 segments (3 remaining)
        a.shrink(segSize * 3);
        assertEquals(a.getCapacity(), segSize * 3);

        // Verify remaining data
        for (int i = 0; i < 3; i++) {
            assertEquals(a.readLong((long) i * segSize), 100L + i);
        }
    }

    @Test
    public void testShrinkToInitialCapacity() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(4);
        a.increaseCapacity(); // capacity = 8
        a.increaseCapacity(); // capacity = 12

        a.shrink(4);
        assertEquals(a.getCapacity(), 4);
        assertEquals(a.getInitialCapacity(), 4);
    }

    @Test
    public void testShrinkBelowInitialFails() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(100);
        a.shrink(50); // Should be no-op (below initial)
        assertEquals(a.getCapacity(), 100);
    }

    @Test
    public void testSegmentBoundaryReadWrite() {
        // Test read/write at exact segment boundaries
        long segSize = SegmentedLongArray.SEGMENT_SIZE;
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(segSize * 2);

        // Last element of first segment
        a.writeLong(segSize - 1, 111L);
        // First element of second segment
        a.writeLong(segSize, 222L);

        assertEquals(a.readLong(segSize - 1), 111L);
        assertEquals(a.readLong(segSize), 222L);
    }

    @Test
    public void testRoundTripAllValues() {
        @Cleanup
        SegmentedLongArray a = new SegmentedLongArray(1000);

        // Write and read back various long values
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
        // After close, array should be unusable
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
        a.increaseCapacity(); // 2 segments
        a.increaseCapacity(); // 3 segments
        a.shrink(segSize);    // back to 1 segment
        assertEquals(a.getCapacity(), segSize);

        // Write in remaining segment
        a.writeLong(0, 42L);

        // Grow again — should reuse slots in segments array
        a.increaseCapacity(); // 2 segments
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
}
