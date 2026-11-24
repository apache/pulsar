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
package org.apache.pulsar.common.scalable;

import java.util.List;

/**
 * Describes a single segment in a scalable topic's DAG.
 *
 * <p>Each segment covers an inclusive hash range and has a unique monotonically increasing ID.
 * Segments are linked by parent/child edges that form a DAG representing the split/merge history.
 * Active segments are the leaves (no children); sealed segments are internal nodes.
 *
 * <p>Two timestamps are recorded:
 * <ul>
 *   <li>{@code createdAtEpoch}/{@code sealedAtEpoch} — DAG generation numbers, used for
 *       layout-versioning. They are not wall-clock values.</li>
 *   <li>{@code createdAtMs}/{@code sealedAtMs} — wall-clock millis since the unix epoch.
 *       Used for retention-based segment GC and for timestamp-based seek.</li>
 * </ul>
 *
 * <p>A segment may be a <i>legacy segment</i> — one that is not managed by the
 * scalable-topic controller and has no {@code segment://...} URI of its own; instead it
 * wraps an existing, externally managed {@code persistent://...} topic. Legacy segments
 * appear in the synthetic-layout response returned for a regular (partitioned or
 * non-partitioned) topic that has not yet been migrated to a scalable topic.
 * {@code legacyTopicName} is non-null exactly for legacy segments.
 *
 * @param segmentId          monotonically increasing, unique within the topic
 * @param hashRange          inclusive hash range [start, end]
 * @param state              ACTIVE or SEALED
 * @param parentIds          parent segment IDs in the DAG (empty for initial/root segments)
 * @param childIds           child segment IDs in the DAG (empty for active leaf segments)
 * @param createdAtEpoch     DAG epoch when this segment was created
 * @param sealedAtEpoch      DAG epoch when sealed (-1 if still active)
 * @param createdAtMs        wall-clock millis at creation time
 * @param sealedAtMs         wall-clock millis at seal time (-1 if still active)
 * @param legacyTopicName    for legacy segments: the externally managed
 *                           {@code persistent://...} topic this segment wraps.
 *                           {@code null} for regular controller-managed segments.
 * @param entryBucketSplits  PIP-486 entry-bucket split points: the ascending, inclusive start hashes
 *                           of buckets 1..N-1 within the 16-bit entry-bucket ring (bucket 0 implicitly
 *                           starts at 0x0000). The segment has {@code entryBucketSplits.size() + 1}
 *                           buckets; an empty list means a single bucket over the whole ring. Splits
 *                           may be uneven. Immutable for the segment's life.
 */
public record SegmentInfo(
        long segmentId,
        HashRange hashRange,
        SegmentState state,
        List<Long> parentIds,
        List<Long> childIds,
        long createdAtEpoch,
        long sealedAtEpoch,
        long createdAtMs,
        long sealedAtMs,
        String legacyTopicName,
        List<Integer> entryBucketSplits
) {
    public SegmentInfo {
        parentIds = parentIds != null ? List.copyOf(parentIds) : List.of();
        childIds = childIds != null ? List.copyOf(childIds) : List.of();
        entryBucketSplits = entryBucketSplits != null ? List.copyOf(entryBucketSplits) : List.of();
    }

    /** Number of entry-buckets this segment is divided into ({@code entryBucketSplits.size() + 1}). */
    public int bucketCount() {
        return entryBucketSplits.size() + 1;
    }

    /** Create a new active segment with no parents (single entry-bucket). */
    public static SegmentInfo active(long segmentId, HashRange hashRange,
                                     long createdAtEpoch, long createdAtMs) {
        return new SegmentInfo(segmentId, hashRange, SegmentState.ACTIVE,
                List.of(), List.of(), createdAtEpoch, -1, createdAtMs, -1, null, List.of());
    }

    /** Create a new active segment with the given parent IDs (single entry-bucket). */
    public static SegmentInfo active(long segmentId, HashRange hashRange,
                                     List<Long> parentIds, long createdAtEpoch, long createdAtMs) {
        return new SegmentInfo(segmentId, hashRange, SegmentState.ACTIVE,
                parentIds, List.of(), createdAtEpoch, -1, createdAtMs, -1, null, List.of());
    }

    /**
     * Create a new active legacy segment that wraps the given externally managed
     * {@code persistent://...} topic instead of having its own {@code segment://...} URI.
     * Used by the synthetic-layout response for not-yet-migrated regular topics.
     */
    public static SegmentInfo activeLegacy(long segmentId, HashRange hashRange,
                                           String legacyTopicName,
                                           long createdAtEpoch, long createdAtMs) {
        return new SegmentInfo(segmentId, hashRange, SegmentState.ACTIVE,
                List.of(), List.of(), createdAtEpoch, -1, createdAtMs, -1, legacyTopicName, List.of());
    }

    /** Return a sealed copy of this segment with the given child IDs. */
    public SegmentInfo sealed(long sealedAtEpoch, long sealedAtMs, List<Long> childIds) {
        return new SegmentInfo(segmentId, hashRange, SegmentState.SEALED,
                parentIds, childIds, createdAtEpoch, sealedAtEpoch, createdAtMs, sealedAtMs,
                legacyTopicName, entryBucketSplits);
    }

    /** Return a copy with different parent IDs. */
    public SegmentInfo withParentIds(List<Long> parentIds) {
        return new SegmentInfo(segmentId, hashRange, state,
                parentIds, childIds, createdAtEpoch, sealedAtEpoch, createdAtMs, sealedAtMs,
                legacyTopicName, entryBucketSplits);
    }

    /** Return a copy with different child IDs. */
    public SegmentInfo withChildIds(List<Long> childIds) {
        return new SegmentInfo(segmentId, hashRange, state,
                parentIds, childIds, createdAtEpoch, sealedAtEpoch, createdAtMs, sealedAtMs,
                legacyTopicName, entryBucketSplits);
    }

    /** Return a copy with different entry-bucket split points (PIP-486). */
    public SegmentInfo withEntryBucketSplits(List<Integer> entryBucketSplits) {
        return new SegmentInfo(segmentId, hashRange, state,
                parentIds, childIds, createdAtEpoch, sealedAtEpoch, createdAtMs, sealedAtMs,
                legacyTopicName, entryBucketSplits);
    }

    /**
     * True if this is a legacy segment — one that wraps an existing, externally managed
     * {@code persistent://...} topic rather than owning a controller-managed
     * {@code segment://...} URI. An empty {@code legacyTopicName} does not count as legacy.
     */
    public boolean isLegacy() {
        return legacyTopicName != null && !legacyTopicName.isEmpty();
    }

    public boolean isActive() {
        return state == SegmentState.ACTIVE;
    }

    public boolean isSealed() {
        return state == SegmentState.SEALED;
    }

    public boolean isRoot() {
        return parentIds.isEmpty();
    }

    public boolean isLeaf() {
        return childIds.isEmpty();
    }
}
