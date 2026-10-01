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

import static org.assertj.core.api.Assertions.assertThat;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.pulsar.common.util.collections.LongBitmap;
import org.apache.pulsar.common.util.collections.LongBitmaps;
import org.testng.annotations.Test;

public class BucketDelayedMessageIndexTest {

    @Test
    public void trackThenContains() {
        BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();

        index.track(7L, 100L);

        assertThat(index.contains(7L, 100L)).isTrue();
        assertThat(index.contains(7L, 101L)).isFalse();
        assertThat(index.contains(8L, 100L)).isFalse();
        assertThat(index.size()).isEqualTo(1L);
    }

    @Test
    public void untrackReturnsTrueFirstTimeAndFalseAfter() {
        BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();
        index.track(1L, 1L);

        assertThat(index.untrack(1L, 1L)).isTrue();
        assertThat(index.size()).isZero();

        assertThat(index.untrack(1L, 1L)).isFalse();
        assertThat(index.size()).isZero();
    }

    @Test
    public void untrackOnAbsentBitIsSafe() {
        BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();

        assertThat(index.untrack(99L, 99L)).isFalse();
        assertThat(index.size()).isZero();
        assertThat(index.contains(99L, 99L)).isFalse();
    }

    @Test
    public void trackIsIdempotent() {
        BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();

        index.track(3L, 5L);
        index.track(3L, 5L);
        index.track(3L, 5L);

        assertThat(index.size()).isEqualTo(1L);
        assertThat(index.contains(3L, 5L)).isTrue();
    }

    @Test
    public void trackAcrossManyLedgersKeepsCounterCorrect() {
        BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();

        for (long ledger = 1; ledger <= 5; ledger++) {
            for (long entry = 1; entry <= 10; entry++) {
                index.track(ledger, entry);
            }
        }

        assertThat(index.size()).isEqualTo(50L);

        // Drain half.
        for (long ledger = 1; ledger <= 5; ledger++) {
            for (long entry = 1; entry <= 5; entry++) {
                assertThat(index.untrack(ledger, entry)).isTrue();
            }
        }
        assertThat(index.size()).isEqualTo(25L);
    }

    @Test
    public void clearResetsBitmapAndCounter() {
        BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();
        index.track(1L, 1L);
        index.track(2L, 2L);
        assertThat(index.size()).isEqualTo(2L);

        index.clear();

        assertThat(index.size()).isZero();
        assertThat(index.contains(1L, 1L)).isFalse();
        assertThat(index.contains(2L, 2L)).isFalse();

        // Index remains usable after clear.
        index.track(3L, 3L);
        assertThat(index.size()).isEqualTo(1L);
        assertThat(index.contains(3L, 3L)).isTrue();
    }

    @Test
    public void restoreLoadsBitsFromSnapshot() {
        BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();

        Long2ObjectMap<LongBitmap> snapshot = new Long2ObjectOpenHashMap<>();
        LongBitmap ledger1 = LongBitmaps.create();
        ledger1.add(10L);
        ledger1.add(11L);
        snapshot.put(1L, ledger1);
        LongBitmap ledger2 = LongBitmaps.create();
        ledger2.add(20L);
        snapshot.put(2L, ledger2);

        index.restore(snapshot);

        assertThat(index.size()).isEqualTo(3L);
        assertThat(index.contains(1L, 10L)).isTrue();
        assertThat(index.contains(1L, 11L)).isTrue();
        assertThat(index.contains(2L, 20L)).isTrue();
    }

    @Test
    public void restoreIsIdempotentOnOverlappingSnapshots() {
        BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();

        Long2ObjectMap<LongBitmap> firstBucket = new Long2ObjectOpenHashMap<>();
        LongBitmap bits = LongBitmaps.create();
        bits.add(5L);
        bits.add(6L);
        firstBucket.put(7L, bits);

        Long2ObjectMap<LongBitmap> secondBucket = new Long2ObjectOpenHashMap<>();
        LongBitmap overlapping = LongBitmaps.create();
        overlapping.add(5L);  // overlap with firstBucket
        overlapping.add(8L);
        secondBucket.put(7L, overlapping);

        index.restore(firstBucket);
        index.restore(secondBucket);

        assertThat(index.size()).isEqualTo(3L);  // 5, 6, 8 — not 4
        assertThat(index.contains(7L, 5L)).isTrue();
        assertThat(index.contains(7L, 6L)).isTrue();
        assertThat(index.contains(7L, 8L)).isTrue();
    }

    @Test
    public void restoreAfterTrackMergesCorrectly() {
        BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();
        index.track(1L, 1L);
        assertThat(index.size()).isEqualTo(1L);

        Long2ObjectMap<LongBitmap> snapshot = new Long2ObjectOpenHashMap<>();
        LongBitmap bits = LongBitmaps.create();
        bits.add(1L);  // overlap with the existing tracked bit
        bits.add(2L);
        snapshot.put(1L, bits);

        index.restore(snapshot);

        assertThat(index.size()).isEqualTo(2L);
        assertThat(index.contains(1L, 1L)).isTrue();
        assertThat(index.contains(1L, 2L)).isTrue();
    }
}
