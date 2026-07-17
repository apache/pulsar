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
import com.google.common.annotations.VisibleForTesting;
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
 *   <li>Every segment except the last has length {@code segmentSize}.</li>
 *   <li>The last segment may be partially filled.</li>
 *   <li>{@code capacity} always equals the total number of allocated elements
 *       across all segments.</li>
 * </ul>
 *
 * <p>The segment invariant guarantees that the bit based address mapping
 * ({@code offset >>> segmentShift}, {@code offset & segmentMask}) remains
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
     * Default number of {@code long} values in a full segment. Production behavior
     * is fixed by this constant; tests may override it via the
     * {@link #SegmentedLongArray(long, int) package-private constructor}.
     */
    static final int DEFAULT_SEGMENT_SIZE = 2 * 1024 * 1024;

    static {
        assert Integer.bitCount(DEFAULT_SEGMENT_SIZE) == 1
                : "DEFAULT_SEGMENT_SIZE must be a power of 2";
    }

    private final int segmentSize;
    private final int segmentShift;
    private final int segmentMask;

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
     * Creates a segmented array with the specified initial capacity and the default
     * segment size ({@link #DEFAULT_SEGMENT_SIZE}).
     *
     * @param initialCapacity initial capacity in {@code long} elements
     * @throws IllegalArgumentException if {@code initialCapacity <= 0}
     */
    public SegmentedLongArray(long initialCapacity) {
        this(initialCapacity, DEFAULT_SEGMENT_SIZE);
    }

    /**
     * Creates a segmented array with a custom segment size.
     *
     * <p>Intended for unit tests that exercise multi-segment grow/shrink behavior
     * without allocating production-sized (16 MiB) backing arrays. Production
     * callers should use {@link #SegmentedLongArray(long)}.
     *
     * @param initialCapacity initial capacity in {@code long} elements
     * @param segmentSize     number of {@code long} values per full segment; must be
     *                        a positive power of two
     * @throws IllegalArgumentException if {@code initialCapacity <= 0} or
     *                                  {@code segmentSize} is not a positive power of two
     */
    @VisibleForTesting
    SegmentedLongArray(long initialCapacity, int segmentSize) {
        checkArgument(initialCapacity > 0, "initialCapacity must be positive");
        checkArgument(segmentSize > 0 && Integer.bitCount(segmentSize) == 1,
                "segmentSize must be a positive power of two");
        this.segmentSize = segmentSize;
        this.segmentShift = Integer.numberOfTrailingZeros(segmentSize);
        this.segmentMask = segmentSize - 1;
        this.initialCapacity = initialCapacity;
        this.capacity = initialCapacity;
        allocateSegments(initialCapacity);
    }

    /**
     * Allocates the initial segment layout.
     */
    private void allocateSegments(long longCapacity) {
        segmentCount = Math.max(1, (int) ((longCapacity + segmentSize - 1) / segmentSize));
        segments = new long[segmentCount][];

        long remaining = longCapacity;
        long bytes = 0;

        for (int i = 0; i < segmentCount; i++) {
            int size = (int) Math.min(segmentSize, remaining);
            segments[i] = new long[size];
            bytes += (long) size * Long.BYTES;
            remaining -= size;
        }

        allocatedBytes = bytes;
    }

    public void writeLong(long offset, long value) {
        long[] segment = segments[(int) (offset >>> segmentShift)];
        segment[(int) (offset & segmentMask)] = value;
    }

    public long readLong(long offset) {
        long[] segment = segments[(int) (offset >>> segmentShift)];
        return segment[(int) (offset & segmentMask)];
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
        if (capacity < segmentSize) {
            geometric = Math.min(
                    capacity + (capacity <= 256 ? capacity : capacity / 2),
                    segmentSize);
        } else {
            geometric = capacity + segmentSize;
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

        int newSegmentCount = (int) ((newCapacity + segmentSize - 1) / segmentSize);

        if (segments.length < newSegmentCount) {
            segments = Arrays.copyOf(segments, newSegmentCount);
        }

        // If the current last segment becomes an interior segment,
        // it must be expanded to preserve the bit-based address mapping.
        if (newSegmentCount > segmentCount && segmentCount >= 1) {
            int oldLastIdx = segmentCount - 1;
            if (segments[oldLastIdx].length < segmentSize) {
                resizeLastSegment(oldLastIdx, segmentSize);
            }
        }

        for (int i = segmentCount; i < newSegmentCount - 1; i++) {
            segments[i] = new long[segmentSize];
            allocatedBytes += (long) segmentSize * Long.BYTES;
        }

        int newLastIdx = newSegmentCount - 1;
        int newLastSize = (int) (newCapacity - (long) newLastIdx * segmentSize);

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

        int newSegmentCount = (int) ((newCapacity + segmentSize - 1) / segmentSize);
        int newLastIdx = newSegmentCount - 1;
        int newLastSize = (int) (newCapacity - (long) newLastIdx * segmentSize);

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
