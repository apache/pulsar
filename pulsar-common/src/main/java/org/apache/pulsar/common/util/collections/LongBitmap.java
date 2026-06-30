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
 * Thread-safe bitmap over the unsigned 32-bit range {@code [0, 2^32 - 1]}.
 *
 * <p>Provides point/range add/remove/contains, bulk union ({@link #or}), atomic
 * drain ({@link #drainTo}), iteration ({@link #forEachLong}), and serialization
 * ({@link #serialize}/{@link #serializedSize}). All methods are thread-safe.
 *
 * <p>Used by Pulsar's delayed-delivery tracker, consumer-name-index allocator,
 * and draining-hash tracker.
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

    /** Adds a value. */
    void add(long value);

    /** Adds all values in {@code [from, to)}. No-op if {@code to <= from}. */
    void add(long from, long to);

    /** Removes a value. No-op if absent. */
    void remove(long value);

    /** Removes all values in {@code [from, to)}. No-op if {@code to <= from}. */
    void remove(long from, long to);

    /** Returns true if {@code value} is present. Out-of-range values return false. */
    boolean contains(long value);

    /**
     * Returns true iff every value in {@code [from, to)} is present.
     * Returns false if the range is empty, reversed, or out of bounds.
     */
    boolean contains(long from, long to);

    /** Number of values currently set. */
    long cardinality();

    /** True if no values are set. O(1). */
    boolean isEmpty();

    /**
     * Smallest absent value {@code >= from}, or {@code -1} if every value in
     * {@code [from, MAX_UINT32]} is present. {@code from} outside {@code [0, MAX_UINT32]}
     * returns {@code -1}.
     */
    long nextAbsentValue(long from);

    /**
     * In-place union: adds every value in {@code other} to this bitmap. Container-level
     * bulk operation (O(containers), not O(values)). Locks are acquired in
     * identityHashCode order so concurrent {@code A.or(B)} and {@code B.or(A)} don't
     * deadlock. {@code a.or(a)} is a no-op.
     */
    void or(LongBitmap other);

    /**
     * Iterates values in ascending order. The action runs without holding any lock,
     * so a slow action does not block concurrent operations and may safely mutate
     * this bitmap (the iteration sees a point-in-time snapshot, not a live view).
     */
    void forEachLong(LongConsumer action);

    /**
     * Atomically removes up to {@code limit} values in ascending order and invokes
     * {@code action} for each. The action runs without holding any lock, so a slow
     * action does not block concurrent operations. Returns the number of values
     * drained (may be less than {@code limit} if the bitmap is smaller).
     *
     * <p>Select and remove happen in a single critical section, so a value added
     * concurrently after {@code drainTo} begins is never lost.
     */
    long drainTo(long limit, LongConsumer action);

    /**
     * Upper bound on {@link #serialize()} length, without running {@code runOptimize()}.
     * Safe for buffer allocation: {@code runOptimize} only shrinks, so
     * {@code serializedSize() >= serialize().length} always holds.
     */
    long serializedSize();

    /**
     * Optimizes the layout and returns the serialized form as a freshly-allocated
     * byte array. Clones the bitmap under a brief read lock, then optimizes and
     * serializes with no lock held, so concurrent mutations don't affect the output.
     */
    byte[] serialize();
}
