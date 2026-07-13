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

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Arrays;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;

/**
 * A segmented array of {@code long} values backed by the Java heap.
 *
 * <p>The array is split into fixed-size {@code long[]} segments of
 * {@value #SEGMENT_SIZE} elements (16MB). Segment lookup uses
 * bit-shift and bit-mask operations instead of division, enabling efficient
 * indexed access with minimal overhead.
 *
 * <p>Segmentation allows the array to grow beyond
 * {@code Integer.MAX_VALUE} elements while keeping individual backing arrays
 * within the JVM's maximum array size.
 */
@NotThreadSafe
public class SegmentedLongArray implements AutoCloseable {

    /**
     * Each segment holds at most 2M longs -> 16 MB. Must be a power of two
     * so that segment lookup uses bit-shift / bit-mask instead of division.
     */
    static final int SEGMENT_SIZE = 2 * 1024 * 1024;
    private static final int SEGMENT_SHIFT = Integer.numberOfTrailingZeros(SEGMENT_SIZE);
    private static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

    static {
        assert Integer.bitCount(SEGMENT_SIZE) == 1 : "SEGMENT_SIZE must be a power of 2";
    }

    private long[][] segments;
    private int segmentCount;

    @Getter
    private final long initialCapacity;

    /**
     * Current capacity measured in number of longs (not bytes).
     * Use {@link #bytesCapacity()} for the byte equivalent.
     */
    @Getter
    private long capacity;

    public SegmentedLongArray(long initialCapacity) {
        checkArgument(initialCapacity > 0, "initialCapacity must be positive");
        this.initialCapacity = initialCapacity;
        this.capacity = initialCapacity;
        allocateSegments(initialCapacity);
    }

    private void allocateSegments(long longCapacity) {
        segmentCount = Math.max(1, (int) ((longCapacity + SEGMENT_SIZE - 1) / SEGMENT_SIZE));
        segments = new long[segmentCount][];
        for (int i = 0; i < segmentCount; i++) {
            int size = (int) Math.min(SEGMENT_SIZE, longCapacity - (long) i * SEGMENT_SIZE);
            segments[i] = new long[size];
        }
    }

    public void writeLong(long offset, long value) {
        long[] segment = segments[(int) (offset >>> SEGMENT_SHIFT)];
        segment[(int) (offset & SEGMENT_MASK)] = value;
    }

    public long readLong(long offset) {
        long[] segment = segments[(int) (offset >>> SEGMENT_SHIFT)];
        return segment[(int) (offset & SEGMENT_MASK)];
    }

    public void increaseCapacity() {
        if (capacity < SEGMENT_SIZE) {
            // Resize the first segment by allocating a larger backing array
            long grown = capacity + (capacity <= 256 ? capacity : capacity / 2);
            grown = Math.min(grown, SEGMENT_SIZE);
            long[] oldSeg = segments[0];
            long[] newSeg = new long[(int) grown];
            System.arraycopy(oldSeg, 0, newSeg, 0, (int) capacity);
            segments[0] = newSeg;
            capacity = grown;
        } else {
            // Add a new full-size segment
            if (segmentCount == segments.length) {
                segments = Arrays.copyOf(segments,
                        Math.max(segmentCount + 1, segments.length + segments.length / 2));
            }
            segments[segmentCount] = new long[SEGMENT_SIZE];
            segmentCount++;
            capacity += SEGMENT_SIZE;
        }
    }

    public void shrink(long newCapacity) {
        if (newCapacity >= capacity || newCapacity < initialCapacity) {
            return;
        }

        long sizeToReduce = capacity - newCapacity;

        // Drop whole segments from the end
        while (sizeToReduce >= SEGMENT_SIZE && segmentCount > 1) {
            segmentCount--;
            segments[segmentCount] = null;
            capacity -= SEGMENT_SIZE;
            sizeToReduce -= SEGMENT_SIZE;
        }

        // Shrink the first segment if needed
        if (segmentCount == 1 && sizeToReduce > 0) {
            long newSize = capacity - sizeToReduce;
            long[] oldSeg = segments[0];
            long[] newSeg = new long[(int) newSize];
            System.arraycopy(oldSeg, 0, newSeg, 0, (int) newSize);
            segments[0] = newSeg;
            capacity = newSize;
        }
    }

    @Override
    public void close() {
        segments = null;
    }

    /**
     * The amount of memory used to back the array of longs.
     */
    public long bytesCapacity() {
        return capacity * Long.BYTES;
    }
}
