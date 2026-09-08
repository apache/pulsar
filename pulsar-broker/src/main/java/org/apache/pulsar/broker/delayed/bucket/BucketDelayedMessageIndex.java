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
package org.apache.pulsar.broker.delayed.bucket;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pulsar.common.util.collections.LongBitmap;
import org.apache.pulsar.common.util.collections.LongBitmaps;

/**
 * Runtime truth for delayed messages that have been accepted but not yet delivered.
 * Co-locates the bitmap and its cardinality so the counter is an invariant of the bitmap
 * rather than a discipline callers must maintain. {@link ImmutableBucket#delayedIndexBitMap}
 * is a frozen snapshot for BookKeeper writes/merge and is intentionally not consulted here.
 */
@ThreadSafe
final class BucketDelayedMessageIndex {

    private final Long2ObjectMap<LongBitmap> inflightIndex = new Long2ObjectOpenHashMap<>();
    private final AtomicLong size = new AtomicLong(0);

    /** Idempotent: re-tracking a position already in the index is a no-op. */
    void track(long ledgerId, long entryId) {
        if (inflightIndex.computeIfAbsent(ledgerId, k -> LongBitmaps.create()).checkedAdd(entryId)) {
            size.incrementAndGet();
        }
    }

    /** @return true if the bit was present and removed; false if it was already absent. */
    boolean untrack(long ledgerId, long entryId) {
        LongBitmap bitSet = inflightIndex.get(ledgerId);
        if (bitSet == null || !bitSet.contains(entryId)) {
            return false;
        }
        bitSet.remove(entryId);
        if (bitSet.isEmpty()) {
            inflightIndex.remove(ledgerId);
        }
        size.decrementAndGet();
        return true;
    }

    boolean contains(long ledgerId, long entryId) {
        LongBitmap bitSet = inflightIndex.get(ledgerId);
        return bitSet != null && bitSet.contains(entryId);
    }

    long size() {
        return size.get();
    }

    void clear() {
        inflightIndex.clear();
        size.set(0);
    }

    /**
     * Bulk-load after recovery. Built on {@link #track} so overlapping bits merge, not double-count.
     */
    void restore(Map<Long, LongBitmap> snapshot) {
        snapshot.forEach((ledgerId, bitmap) ->
                bitmap.forEachLong(entryId -> track(ledgerId, entryId)));
    }
}
