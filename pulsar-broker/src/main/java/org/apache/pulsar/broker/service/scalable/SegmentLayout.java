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
package org.apache.pulsar.broker.service.scalable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentInfo;

/**
 * In-memory versioned view of a scalable topic's segment DAG.
 *
 * <p>This is an immutable snapshot. Mutation methods (split, merge, prune) return new instances
 * with the updated state.
 */
public class SegmentLayout {
    @Getter
    private final long epoch;
    @Getter
    private final long nextSegmentId;
    @Getter
    private final Map<Long, SegmentInfo> allSegments;
    @Getter
    private final Map<Long, SegmentInfo> activeSegments;

    public SegmentLayout(long epoch, long nextSegmentId, Map<Long, SegmentInfo> allSegments) {
        this.epoch = epoch;
        this.nextSegmentId = nextSegmentId;
        this.allSegments = Collections.unmodifiableMap(new LinkedHashMap<>(allSegments));
        this.activeSegments = allSegments.values().stream()
                .filter(SegmentInfo::isActive)
                .collect(Collectors.toUnmodifiableMap(SegmentInfo::segmentId, s -> s));
    }

    public static SegmentLayout fromMetadata(ScalableTopicMetadata metadata) {
        return new SegmentLayout(metadata.getEpoch(), metadata.getNextSegmentId(), metadata.getSegments());
    }

    /**
     * Find the active segment whose hash range contains the given hash value.
     */
    public SegmentInfo findActiveSegment(int hash) {
        for (SegmentInfo segment : activeSegments.values()) {
            if (segment.hashRange().contains(hash)) {
                return segment;
            }
        }
        throw new IllegalStateException("No active segment covers hash: " + hash);
    }

    /**
     * Get the full lineage chain for a segment (ancestors + descendants).
     */
    public List<SegmentInfo> getLineage(long segmentId) {
        List<SegmentInfo> lineage = new ArrayList<>();
        collectAncestors(segmentId, lineage);
        SegmentInfo self = allSegments.get(segmentId);
        if (self != null) {
            lineage.add(self);
        }
        collectDescendants(segmentId, lineage);
        return lineage;
    }

    /**
     * Get direct children of a segment.
     */
    public List<SegmentInfo> getChildren(long segmentId) {
        SegmentInfo segment = allSegments.get(segmentId);
        if (segment == null) {
            return List.of();
        }
        return segment.childIds().stream()
                .map(allSegments::get)
                .filter(s -> s != null)
                .collect(Collectors.toList());
    }

    /**
     * Get direct parents of a segment.
     */
    public List<SegmentInfo> getParents(long segmentId) {
        SegmentInfo segment = allSegments.get(segmentId);
        if (segment == null) {
            return List.of();
        }
        return segment.parentIds().stream()
                .map(allSegments::get)
                .filter(s -> s != null)
                .collect(Collectors.toList());
    }

    /**
     * Produce a new layout by splitting a segment at its midpoint.
     *
     * @param segmentId the active segment to split
     * @return a new SegmentLayout with the split applied
     */
    public SegmentLayout splitSegment(long segmentId) {
        SegmentInfo segment = allSegments.get(segmentId);
        if (segment == null) {
            throw new IllegalArgumentException("Segment not found: " + segmentId);
        }
        if (!segment.isActive()) {
            throw new IllegalArgumentException("Cannot split non-active segment: " + segmentId);
        }

        HashRange[] splitRanges = segment.hashRange().split();
        long newEpoch = epoch + 1;
        long childId1 = nextSegmentId;
        long childId2 = nextSegmentId + 1;

        SegmentInfo sealedParent = segment.sealed(newEpoch, List.of(childId1, childId2));
        SegmentInfo child1 = SegmentInfo.active(childId1, splitRanges[0], List.of(segmentId), newEpoch);
        SegmentInfo child2 = SegmentInfo.active(childId2, splitRanges[1], List.of(segmentId), newEpoch);

        Map<Long, SegmentInfo> newSegments = new LinkedHashMap<>(allSegments);
        newSegments.put(segmentId, sealedParent);
        newSegments.put(childId1, child1);
        newSegments.put(childId2, child2);

        return new SegmentLayout(newEpoch, nextSegmentId + 2, newSegments);
    }

    /**
     * Produce a new layout by merging two adjacent active segments.
     *
     * @param segmentId1 the first segment (must be active and adjacent to segmentId2)
     * @param segmentId2 the second segment (must be active and adjacent to segmentId1)
     * @return a new SegmentLayout with the merge applied
     */
    public SegmentLayout mergeSegments(long segmentId1, long segmentId2) {
        SegmentInfo seg1 = allSegments.get(segmentId1);
        SegmentInfo seg2 = allSegments.get(segmentId2);
        if (seg1 == null || seg2 == null) {
            throw new IllegalArgumentException("Segment not found");
        }
        if (!seg1.isActive() || !seg2.isActive()) {
            throw new IllegalArgumentException("Both segments must be active");
        }
        if (!seg1.hashRange().isAdjacentTo(seg2.hashRange())) {
            throw new IllegalArgumentException("Segments are not adjacent: "
                    + seg1.hashRange() + " and " + seg2.hashRange());
        }

        long newEpoch = epoch + 1;
        long mergedId = nextSegmentId;
        HashRange mergedRange = seg1.hashRange().merge(seg2.hashRange());

        SegmentInfo sealed1 = seg1.sealed(newEpoch, List.of(mergedId));
        SegmentInfo sealed2 = seg2.sealed(newEpoch, List.of(mergedId));
        SegmentInfo merged = SegmentInfo.active(mergedId, mergedRange,
                List.of(segmentId1, segmentId2), newEpoch);

        Map<Long, SegmentInfo> newSegments = new LinkedHashMap<>(allSegments);
        newSegments.put(segmentId1, sealed1);
        newSegments.put(segmentId2, sealed2);
        newSegments.put(mergedId, merged);

        return new SegmentLayout(newEpoch, nextSegmentId + 1, newSegments);
    }

    /**
     * Prune an expired segment from the DAG. The segment must be sealed and have no
     * children that are still in the DAG (i.e., children have already been pruned or
     * the segment is a leaf that was sealed).
     *
     * @param segmentId the segment to prune
     * @return a new SegmentLayout with the segment removed
     */
    public SegmentLayout pruneSegment(long segmentId) {
        SegmentInfo segment = allSegments.get(segmentId);
        if (segment == null) {
            throw new IllegalArgumentException("Segment not found: " + segmentId);
        }
        if (segment.isActive()) {
            throw new IllegalArgumentException("Cannot prune an active segment: " + segmentId);
        }

        Map<Long, SegmentInfo> newSegments = new LinkedHashMap<>(allSegments);
        newSegments.remove(segmentId);

        // Remove this segment from its children's parent lists
        for (long childId : segment.childIds()) {
            SegmentInfo child = newSegments.get(childId);
            if (child != null) {
                List<Long> newParentIds = child.parentIds().stream()
                        .filter(id -> id != segmentId)
                        .collect(Collectors.toList());
                newSegments.put(childId, child.withParentIds(newParentIds));
            }
        }

        // Remove this segment from its parents' child lists
        for (long parentId : segment.parentIds()) {
            SegmentInfo parent = newSegments.get(parentId);
            if (parent != null) {
                List<Long> newChildIds = parent.childIds().stream()
                        .filter(id -> id != segmentId)
                        .collect(Collectors.toList());
                newSegments.put(parentId, parent.withChildIds(newChildIds));
            }
        }

        return new SegmentLayout(epoch + 1, nextSegmentId, newSegments);
    }

    /**
     * Convert back to metadata for persistence.
     */
    public ScalableTopicMetadata toMetadata(Map<String, String> properties) {
        return ScalableTopicMetadata.builder()
                .epoch(epoch)
                .nextSegmentId(nextSegmentId)
                .segments(new LinkedHashMap<>(allSegments))
                .properties(properties)
                .build();
    }

    private void collectAncestors(long segmentId, List<SegmentInfo> result) {
        SegmentInfo segment = allSegments.get(segmentId);
        if (segment == null) {
            return;
        }
        for (long parentId : segment.parentIds()) {
            collectAncestors(parentId, result);
            SegmentInfo parent = allSegments.get(parentId);
            if (parent != null) {
                result.add(parent);
            }
        }
    }

    private void collectDescendants(long segmentId, List<SegmentInfo> result) {
        SegmentInfo segment = allSegments.get(segmentId);
        if (segment == null) {
            return;
        }
        for (long childId : segment.childIds()) {
            SegmentInfo child = allSegments.get(childId);
            if (child != null) {
                result.add(child);
            }
            collectDescendants(childId, result);
        }
    }
}
