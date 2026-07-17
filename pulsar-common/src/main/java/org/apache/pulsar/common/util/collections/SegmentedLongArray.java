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
 * A growable array of {@code long} values backed by heap-allocated segments.
 *
 * <p>This class provides a logical contiguous {@code long[]} whose size may exceed
 * {@link Integer#MAX_VALUE}. Internally, the storage is split into fixed-size
 * segments, allowing capacities larger than a single Java array while keeping
 * element access in constant time.
 *
 * <p>Segment layout invariant:
 * <ul>
 *   <li>Every segment except the last has length {@link #SEGMENT_SIZE}.</li>
 *   <li>The last segment may be partially filled.</li>
 *   <li>{@code capacity} always equals the total number of allocated elements
 *       across all segments.</li>
 * </ul>
 *
 * <p>The segment invariant guarantees that the bit-based address mapping
 * ({@code offset >>> SEGMENT_SHIFT}, {@code offset & SEGMENT_MASK}) remains
 * valid for every logical offset.
 *
 * <p>Growing and shrinking preserve existing contents while maintaining the
 * segment layout invariant.
 *
 * <p>This class is not thread-safe.
 */
@NotThreadSafe
public class SegmentedLongArray implements AutoCloseable {

    /**
     * Number of {@code long} values in a full segment.
     *
     * <p>Must be a power of two so segment lookup can use bit operations
     * instead of division and modulo.
     */
    static final int SEGMENT_SIZE = 2 * 1024 * 1024;

    private static final int SEGMENT_SHIFT = Integer.numberOfTrailingZeros(SEGMENT_SIZE);
    private static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

    static {
        assert Integer.bitCount(SEGMENT_SIZE) == 1 : "SEGMENT_SIZE must be a power of 2";
    }

    private long[][] segments;
    private int segmentCount;

    /** Minimum capacity to which this array may be shrunk. */
    @Getter
    private final long initialCapacity;

    /** Logical capacity, measured in {@code long} elements. */
    @Getter
    private long capacity;

    /** Total bytes allocated by all live backing segments. */
    private long allocatedBytes;

    /**
     * Creates a segmented array with the specified initial capacity.
     *
     * @param initialCapacity initial capacity in {@code long} elements
     * @throws IllegalArgumentException if {@code initialCapacity <= 0}
     */
    public SegmentedLongArray(long initialCapacity) {
        checkArgument(initialCapacity > 0, "initialCapacity must be positive");
        this.initialCapacity = initialCapacity;
        this.capacity = initialCapacity;
        allocateSegments(initialCapacity);
    }

    /**
     * Allocates the initial segment layout.
     */
    private void allocateSegments(long longCapacity) {
        segmentCount = Math.max(1, (int) ((longCapacity + SEGMENT_SIZE - 1) / SEGMENT_SIZE));
        segments = new long[segmentCount][];

        long remaining = longCapacity;
        long bytes = 0;

        for (int i = 0; i < segmentCount; i++) {
            int size = (int) Math.min(SEGMENT_SIZE, remaining);
            segments[i] = new long[size];
            bytes += (long) size * Long.BYTES;
            remaining -= size;
        }

        allocatedBytes = bytes;
    }

    public void writeLong(long offset, long value) {
        long[] segment = segments[(int) (offset >>> SEGMENT_SHIFT)];
        segment[(int) (offset & SEGMENT_MASK)] = value;
    }

    public long readLong(long offset) {
        long[] segment = segments[(int) (offset >>> SEGMENT_SHIFT)];
        return segment[(int) (offset & SEGMENT_MASK)];
    }

    /**
     * Ensures that the backing storage can hold at least {@code required}
     * elements.
     *
     * @param required minimum required capacity in {@code long} elements
     */
    public void ensureCapacity(long required) {
        if (required <= capacity) {
            return;
        }

        long geometric;
        if (capacity < SEGMENT_SIZE) {
            geometric = Math.min(
                    capacity + (capacity <= 256 ? capacity : capacity / 2),
                    SEGMENT_SIZE);
        } else {
            geometric = capacity + SEGMENT_SIZE;
        }

        growTo(Math.max(required, geometric));
    }

    public void increaseCapacity() {
        ensureCapacity(capacity + 1);
    }

    /**
     * Expands the backing storage to exactly {@code newCapacity}.
     */
    private void growTo(long newCapacity) {
        if (newCapacity <= capacity) {
            return;
        }

        int newSegmentCount = (int) ((newCapacity + SEGMENT_SIZE - 1) / SEGMENT_SIZE);

        if (segments.length < newSegmentCount) {
            segments = Arrays.copyOf(segments, newSegmentCount);
        }

        // If the current last segment becomes an interior segment,
        // it must be expanded to preserve the bit-based address mapping.
        if (newSegmentCount > segmentCount && segmentCount >= 1) {
            int oldLastIdx = segmentCount - 1;
            if (segments[oldLastIdx].length < SEGMENT_SIZE) {
                resizeLastSegment(oldLastIdx, SEGMENT_SIZE);
            }
        }

        for (int i = segmentCount; i < newSegmentCount - 1; i++) {
            segments[i] = new long[SEGMENT_SIZE];
            allocatedBytes += (long) SEGMENT_SIZE * Long.BYTES;
        }

        int newLastIdx = newSegmentCount - 1;
        int newLastSize = (int) (newCapacity - (long) newLastIdx * SEGMENT_SIZE);

        if (newLastIdx >= segmentCount) {
            segments[newLastIdx] = new long[newLastSize];
            allocatedBytes += (long) newLastSize * Long.BYTES;
        } else {
            resizeLastSegment(newLastIdx, newLastSize);
        }

        segmentCount = newSegmentCount;
        capacity = newCapacity;
    }

    private void resizeLastSegment(int idx, int newSize) {
        long[] old = segments[idx];
        if (old.length == newSize) {
            return;
        }

        allocatedBytes += (long) (newSize - old.length) * Long.BYTES;
        segments[idx] = Arrays.copyOf(old, newSize);
    }

    /**
     * Shrinks the backing storage to {@code newCapacity}.
     *
     * @param newCapacity target capacity in {@code long} elements
     */
    public void shrink(long newCapacity) {
        if (newCapacity >= capacity || newCapacity < initialCapacity) {
            return;
        }

        int newSegmentCount = (int) ((newCapacity + SEGMENT_SIZE - 1) / SEGMENT_SIZE);
        int newLastIdx = newSegmentCount - 1;
        int newLastSize = (int) (newCapacity - (long) newLastIdx * SEGMENT_SIZE);

        for (int i = newSegmentCount; i < segmentCount; i++) {
            allocatedBytes -= (long) segments[i].length * Long.BYTES;
            segments[i] = null;
        }

        resizeLastSegment(newLastIdx, newLastSize);

        segmentCount = newSegmentCount;
        capacity = newCapacity;

        if (segments.length > Math.max(segmentCount * 2L, 16)) {
            segments = Arrays.copyOf(segments, segmentCount);
        }
    }

    @Override
    public void close() {
        segments = null;
        segmentCount = 0;
        capacity = 0;
        allocatedBytes = 0;
    }

    /**
     * Returns the physical heap memory reserved by the backing arrays.
     *
     * @return allocated bytes occupied by all backing segments
     */
    public long bytesCapacity() {
        return allocatedBytes;
    }
}
