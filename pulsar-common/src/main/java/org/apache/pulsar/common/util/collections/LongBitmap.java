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

import java.util.function.LongConsumer;

/**
 * Thread-safe bitmap abstraction for tracking long values.
 *
 * <p>The current implementation supports values in the unsigned 32-bit range
 * {@code [0, 2^32 - 1]}. Methods that modify the bitmap reject values outside this
 * range with {@link IllegalArgumentException}. Query methods return {@code false}
 * or {@code -1} for out-of-range values where applicable.
 *
 * <p>Supports point and range operations, bulk union, atomic draining, iteration,
 * and serialization. All operations are thread-safe.
 *
 * <p>This abstraction is used for high-throughput broker metadata tracking,
 * including delayed-delivery tracking, consumer-name allocation, and
 * draining-hash tracking.
 *
 * <p>Example:
 * <pre>{@code
 * LongBitmap bitmap = LongBitmaps.create();
 * bitmap.add(12345L);
 * if (bitmap.contains(12345L)) { ... }
 *
 * byte[] bytes = bitmap.serialize();
 * LongBitmap restored = LongBitmaps.deserialize(Unpooled.wrappedBuffer(bytes));
 * }</pre>
 */
public interface LongBitmap {

    /**
     * Adds a value.
     *
     * @param value value to add, must be in {@code [0, 2^32 - 1]}
     * @throws IllegalArgumentException if value is outside the supported range
     */
    void add(long value);

    /**
     * Adds a value if it is not already present.
     *
     * <p>This operation is atomic. Unlike {@code if (!contains(value)) add(value)},
     * the check and add are performed as a single operation.
     *
     * @param value value to add, must be in {@code [0, 2^32 - 1]}
     * @return {@code true} if the value was added, {@code false} if it already existed
     * @throws IllegalArgumentException if value is outside the supported range
     */
    boolean checkedAdd(long value);

    /**
     * Adds all values in the half-open range {@code [from, to)}.
     *
     * <p>No-op if {@code to <= from}.
     *
     * @param from inclusive lower bound
     * @param to exclusive upper bound
     * @throws IllegalArgumentException if the range exceeds the supported value range
     */
    void add(long from, long to);

    /**
     * Removes a value. No-op if absent.
     *
     * @param value value to remove
     * @throws IllegalArgumentException if value is outside the supported range
     */
    void remove(long value);

    /**
     * Removes all values in the half-open range {@code [from, to)}.
     *
     * <p>No-op if {@code to <= from}.
     *
     * @param from inclusive lower bound
     * @param to exclusive upper bound
     * @throws IllegalArgumentException if the range exceeds the supported value range
     */
    void remove(long from, long to);

    /**
     * Returns whether the bitmap contains the given value.
     *
     * @param value value to check
     * @return {@code true} if present, otherwise {@code false}
     */
    boolean contains(long value);

    /**
     * Returns whether all values in {@code [from, to)} are present.
     *
     * @param from inclusive lower bound
     * @param to exclusive upper bound
     * @return {@code true} if all values in the range are present
     */
    boolean contains(long from, long to);

    /** Returns the number of values currently stored. */
    long cardinality();

    /** Returns {@code true} if no values are stored. */
    boolean isEmpty();

    /**
     * Returns the smallest absent value greater than or equal to {@code from}.
     *
     * @param from inclusive lower bound
     * @return next absent value, or {@code -1} if none exists
     */
    long nextAbsentValue(long from);

    /**
     * Adds all values from {@code other} into this bitmap.
     *
     * @param other bitmap to merge
     */
    void or(LongBitmap other);

    /**
     * Iterates values in ascending order.
     *
     * <p>The iteration observes a stable view of the bitmap. Implementations may
     * choose the mechanism used to provide this guarantee.
     *
     * @param action callback invoked for each value
     */
    void forEachLong(LongConsumer action);

    /**
     * Atomically removes up to {@code limit} values and invokes {@code action}
     * for each removed value.
     *
     * <p>Selection and removal are performed atomically. The callback is invoked
     * after removal has completed.
     *
     * @param limit maximum number of values to drain
     * @param action callback invoked for each removed value
     * @return number of values drained
     */
    long drainTo(long limit, LongConsumer action);

    /**
     * Returns an upper bound of the serialized size.
     */
    long serializedSize();

    /**
     * Serializes the bitmap into a newly allocated byte array.
     */
    byte[] serialize();
}
