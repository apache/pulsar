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
package org.apache.pulsar.client.impl.v5;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.ScalableTopicHashing;
import org.apache.pulsar.common.util.Murmur3_32Hash;

/**
 * Routes messages to the correct segment based on key hashing.
 *
 * <p>Segment routing uses the <b>high 16 bits</b> of the key's {@code Murmur3_32} hash as a 16-bit
 * hash space (0x0000-0xFFFF), and finds the active segment whose hash range contains it. The
 * <b>low 16 bits</b> of the same hash are reserved for PIP-486 entry-bucketing
 * ({@link #entryBucketHash}), so the two are independent.
 */
final class SegmentRouter {

    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    /**
     * Route a message key to the segment that owns its hash range.
     *
     * <p>If every active segment is a legacy segment (synthetic layout for a not-yet-migrated
     * regular topic), the routing switches to {@code signSafeMod(murmurHash3_32(key), N)} over
     * {@code segment_id} so V5 producers route the same way v4 partitioned-topic producers do
     * — preserving per-key destinations while clients gradually upgrade.
     *
     * @param key the message key
     * @param activeSegments the currently active segments (sorted by hash range)
     * @return the segment ID to route to
     * @throws IllegalStateException if no segment covers the hash
     */
    long route(String key, List<ActiveSegment> activeSegments) {
        if (activeSegments.isEmpty()) {
            throw new IllegalStateException("No active segments");
        }
        if (allLegacy(activeSegments)) {
            return routeModN(key, activeSegments);
        }
        int hash = hash(key);
        for (var segment : activeSegments) {
            if (segment.hashRange().contains(hash)) {
                return segment.segmentId();
            }
        }
        throw new IllegalStateException("No segment covers hash " + hash + " for key: " + key);
    }

    /**
     * Route a message without a key using round-robin across active segments.
     */
    long routeRoundRobin(List<ActiveSegment> activeSegments) {
        if (activeSegments.isEmpty()) {
            throw new IllegalStateException("No active segments");
        }
        int idx = Math.abs(roundRobinCounter.getAndIncrement() % activeSegments.size());
        return activeSegments.get(idx).segmentId();
    }

    /** True iff every active segment is a legacy segment — signals a synthetic-layout topic. */
    private static boolean allLegacy(List<ActiveSegment> activeSegments) {
        for (var s : activeSegments) {
            if (!s.isLegacy()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Mod-N routing over {@code segment_id}, matching v4 partitioned-topic routing
     * ({@code signSafeMod(murmurHash3_32(key), N)}).
     */
    private static long routeModN(String key, List<ActiveSegment> activeSegments) {
        int hash32 = Murmur3_32Hash.getInstance().makeHash(key.getBytes(StandardCharsets.UTF_8));
        int n = activeSegments.size();
        int partition = signSafeMod(hash32, n);
        for (var segment : activeSegments) {
            if (segment.segmentId() == partition) {
                return segment.segmentId();
            }
        }
        throw new IllegalStateException(
                "Synthetic layout missing segment_id=" + partition + " (N=" + n + ")");
    }

    private static int signSafeMod(int dividend, int divisor) {
        int mod = dividend % divisor;
        return mod < 0 ? mod + divisor : mod;
    }

    /**
     * The raw 32-bit {@code Murmur3_32} hash of a key. Its two 16-bit halves drive the two independent
     * rings — high half → segment routing ({@link #segmentHash}), low half → entry-bucketing
     * ({@link #entryBucketHash}, PIP-486). Compute this <b>once</b> per key and split it, rather than
     * hashing the key twice.
     *
     * <p>The <i>raw</i> (unmasked) hash is required so the high half is full-range: {@code makeHash}
     * clears bit 31, which would confine the high half to {@code [0, 0x7FFF]}.
     */
    static int murmur(byte[] keyBytes) {
        return ScalableTopicHashing.murmur(keyBytes);
    }

    /** The 16-bit segment-routing hash (high 16 bits) from a precomputed {@link #murmur(byte[])}. */
    static int segmentHash(int murmur) {
        return ScalableTopicHashing.segmentHash(murmur);
    }

    /** The 16-bit entry-bucket hash (low 16 bits) from a precomputed {@link #murmur(byte[])} (PIP-486).
     *  Independent of the segment-routing hash, so a segment's keys spread evenly across its buckets. */
    static int entryBucketHash(int murmur) {
        return ScalableTopicHashing.entryBucketHash(murmur);
    }

    /** Convenience: the segment-routing hash for a key. Equivalent to {@code segmentHash(murmur(key))}. */
    static int hash(String key) {
        return hash(key.getBytes(StandardCharsets.UTF_8));
    }

    /** Convenience: the segment-routing hash for key bytes. Equivalent to {@code segmentHash(murmur(b))}. */
    static int hash(byte[] keyBytes) {
        return segmentHash(murmur(keyBytes));
    }

    /**
     * Represents an active segment with its hash range and ID.
     *
     * <p>{@code legacyTopicName} is non-null for <i>legacy segments</i> — entries in a
     * synthetic layout that wrap an existing, externally managed {@code persistent://...}
     * topic (e.g. one partition of a not-yet-migrated regular topic). The per-segment v4
     * producer/consumer attaches to {@link #attachTopicName()}, which returns
     * {@code legacyTopicName} for legacy segments and the computed
     * {@code segmentTopicName} otherwise.
     */
    record ActiveSegment(long segmentId, HashRange hashRange, String segmentTopicName,
                         String legacyTopicName, List<Integer> entryBucketSplits,
                         List<HashRange> ownedBucketRanges) {

        ActiveSegment {
            // PIP-486: entry-bucket split points (empty = single bucket over the whole ring). Used by
            // the producer to bucket its batches.
            entryBucketSplits = entryBucketSplits != null ? List.copyOf(entryBucketSplits) : List.of();
            // PIP-486: the entry-bucket hash ranges this consumer owns within the segment (from the
            // controller assignment). Empty = the whole segment (subscribe Shared); non-empty = subscribe
            // Key_Shared STICKY declaring exactly these ranges. Unused on the producer side.
            ownedBucketRanges = ownedBucketRanges != null ? List.copyOf(ownedBucketRanges) : List.of();
        }

        /** Number of entry-buckets this segment is divided into ({@code entryBucketSplits.size()+1}). */
        int bucketCount() {
            return entryBucketSplits.size() + 1;
        }

        boolean isLegacy() {
            return legacyTopicName != null && !legacyTopicName.isEmpty();
        }

        /** The topic URI the per-segment v4 producer/consumer should attach to. */
        String attachTopicName() {
            return isLegacy() ? legacyTopicName : segmentTopicName;
        }
    }
}
