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
 * @param segmentId      monotonically increasing, unique within the topic
 * @param hashRange      inclusive hash range [start, end]
 * @param state          ACTIVE or SEALED
 * @param parentIds      parent segment IDs in the DAG (empty for initial/root segments)
 * @param childIds       child segment IDs in the DAG (empty for active leaf segments)
 * @param createdAtEpoch epoch when this segment was created
 * @param sealedAtEpoch  epoch when sealed (-1 if still active)
 */
public record SegmentInfo(
        long segmentId,
        HashRange hashRange,
        SegmentState state,
        List<Long> parentIds,
        List<Long> childIds,
        long createdAtEpoch,
        long sealedAtEpoch
) {
    public SegmentInfo {
        parentIds = parentIds != null ? List.copyOf(parentIds) : List.of();
        childIds = childIds != null ? List.copyOf(childIds) : List.of();
    }

    /** Create a new active segment with no parents. */
    public static SegmentInfo active(long segmentId, HashRange hashRange, long createdAtEpoch) {
        return new SegmentInfo(segmentId, hashRange, SegmentState.ACTIVE,
                List.of(), List.of(), createdAtEpoch, -1);
    }

    /** Create a new active segment with the given parent IDs. */
    public static SegmentInfo active(long segmentId, HashRange hashRange,
                                     List<Long> parentIds, long createdAtEpoch) {
        return new SegmentInfo(segmentId, hashRange, SegmentState.ACTIVE,
                parentIds, List.of(), createdAtEpoch, -1);
    }

    /** Return a sealed copy of this segment with the given child IDs. */
    public SegmentInfo sealed(long sealedAtEpoch, List<Long> childIds) {
        return new SegmentInfo(segmentId, hashRange, SegmentState.SEALED,
                parentIds, childIds, createdAtEpoch, sealedAtEpoch);
    }

    /** Return a copy with different parent IDs. */
    public SegmentInfo withParentIds(List<Long> parentIds) {
        return new SegmentInfo(segmentId, hashRange, state,
                parentIds, childIds, createdAtEpoch, sealedAtEpoch);
    }

    /** Return a copy with different child IDs. */
    public SegmentInfo withChildIds(List<Long> childIds) {
        return new SegmentInfo(segmentId, hashRange, state,
                parentIds, childIds, createdAtEpoch, sealedAtEpoch);
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
