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
package org.apache.pulsar.broker.service.persistent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.function.ToLongFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.util.StringInterner;

/**
 * Store the last N snapshots that were scanned by a particular subscription.
 */
@Slf4j
public class ReplicatedSubscriptionSnapshotCache {
    private final String subscription;
    private final ToLongFunction<Range<Position>> distanceFunction;
    private final List<SnapshotEntry> snapshots;
    private final int maxSnapshotToCache;
    private SnapshotEntry lastEntry;

    public ReplicatedSubscriptionSnapshotCache(String subscription, int maxSnapshotToCache,
                                               ToLongFunction<Range<Position>> distanceFunction) {
        this.subscription = subscription;
        this.distanceFunction = distanceFunction;
        this.snapshots = new ArrayList<>();
        this.maxSnapshotToCache = maxSnapshotToCache;
    }

    /**
     * Memory footprint estimate for one SnapshotEntry with shared String cluster instances.
     *
     * Assumptions:
     * - 64-bit JVM with compressed OOPs
     * - Cluster name strings are shared/reused across entries
     * - 2 ClusterEntry objects per SnapshotEntry
     * - Each entry has its own Position objects
     * - 1 of the ClusterEntry objects is for the local cluster and shares the local cluster position
     * - Default ArrayList capacity accommodating 2 elements
     * - 8-byte memory alignment
     *
     * Breakdown:
     *
     * 1. SnapshotEntry object header: ~16 bytes
     *
     * 2. Position position: ~32 bytes
     *    - Object header: 16 bytes
     *    - 2 long fields: 16 bytes
     *
     * 3. List<ClusterEntry> clusters with 2 entries: ~32 bytes
     *    - ImmutableCollections.List12 object: 16 bytes
     *    - 2 slots (references): 16 bytes
     *
     * 4. 2x ClusterEntry (without String objects): ~64 bytes
     *    Each ClusterEntry: ~32 bytes
     *    - Object header: 16 bytes
     *    - String cluster reference: 8 bytes (shared, not counted)
     *    - Position reference: 8 bytes
     *
     * 5. Additional position instance: ~32 bytes
     *     - Object header: 16 bytes
     *     - 2 long fields: 16 bytes
     *
     * 6. MutableLong distanceToPrevious: ~24 bytes
     *    - Object header: 16 bytes
     *    - long value: 8 bytes
     *
     * Total per SnapshotEntry: ~200 bytes
     *
     * Note: Actual memory consumption may vary based on JVM implementation,
     * garbage collection behavior, and runtime optimizations.
     */
    record SnapshotEntry(Position position, List<ClusterEntry> clusters, MutableLong distanceToPrevious) {}

    public record ClusterEntry(String cluster, Position position) {}

    public record SnapshotResult(Position position, List<ClusterEntry> clusters) {}

    public synchronized void addNewSnapshot(ReplicatedSubscriptionsSnapshot snapshot) {
        MarkersMessageIdData msgId = snapshot.getLocalMessageId();
        Position position = PositionFactory.create(msgId.getLedgerId(), msgId.getEntryId());


        if (lastEntry != null && position.compareTo(lastEntry.position) <= 0) {
            // clear the entries in the cache if the new snapshot is older than the last one
            // this means that the subscription has been resetted
            snapshots.clear();
            lastEntry = null;
        }

        List<ClusterEntry> clusterEntryList = snapshot.getClustersList().stream()
                .map(cmid -> {
                    Position clusterPosition =
                            PositionFactory.create(cmid.getMessageId().getLedgerId(), cmid.getMessageId().getEntryId());
                    if (clusterPosition.equals(position)) {
                        // reduce memory usage by sharing the same instance for the local cluster
                        clusterPosition = position;
                    }
                    return new ClusterEntry(StringInterner.intern(cmid.getCluster()), clusterPosition);
                })
                .toList();
        // optimize heap memory consumption of the cache
        if (clusterEntryList.size() == 2) {
            clusterEntryList = List.of(clusterEntryList.get(0), clusterEntryList.get(1));
        } else if (clusterEntryList.size() == 3) {
            clusterEntryList = List.of(clusterEntryList.get(0), clusterEntryList.get(1), clusterEntryList.get(2));
        }
        SnapshotEntry entry = new SnapshotEntry(position, clusterEntryList, new MutableLong(
                lastEntry == null ? 0L : distanceFunction.applyAsLong(Range.open(lastEntry.position, position))));

        if (log.isDebugEnabled()) {
            log.debug("[{}] Added new replicated-subscription snapshot at {} -- {}", subscription, position,
                    snapshot.getSnapshotId());
        }

        snapshots.add(entry);
        lastEntry = entry;

        // Prune the cache
        if (snapshots.size() > maxSnapshotToCache) {
            removeSingleEntryWithMinimumTotalDistanceToPreviousAndNext();
        }
    }

    private void removeSingleEntryWithMinimumTotalDistanceToPreviousAndNext() {
        int minIndex = -1;
        long minDistance = Long.MAX_VALUE;
        // skip the first and last entry
        for (int i = 1; i < snapshots.size() - 1; i++) {
            long distance = snapshots.get(i).distanceToPrevious.longValue()
                    + snapshots.get(i + 1).distanceToPrevious.longValue();
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }
        if (minIndex >= 0) {
            snapshots.remove(minIndex);
            SnapshotEntry updateEntry = snapshots.get(minIndex);
            SnapshotEntry previousEntry = snapshots.get(minIndex - 1);
            updateEntry.distanceToPrevious
                    .setValue(distanceFunction.applyAsLong(Range.open(previousEntry.position, updateEntry.position)));
        }
    }

    /**
     * Signal that the mark-delete position on the subscription has been advanced. If there is a snapshot that
     * correspond to this position, it will returned, other it will return null.
     */
    public synchronized SnapshotResult advancedMarkDeletePosition(Position pos) {
        SnapshotEntry snapshot = null;

        while (!snapshots.isEmpty()) {
            Position first = snapshots.get(0).position();
            if (first.compareTo(pos) > 0) {
                // Snapshot is associated which an higher position, so it cannot be used now
                break;
            } else {
                // This snapshot is potentially good. Continue the search for to see if there is a higher snapshot we
                // can use
                snapshot = snapshots.remove(0);
            }
        }

        if (snapshots.isEmpty()) {
            lastEntry = null;
        }

        if (log.isDebugEnabled()) {
            if (snapshot != null) {
                log.debug("[{}] Advanced mark-delete position to {} -- found snapshot at {}", subscription, pos,
                        snapshot.position());
            } else {
                log.debug("[{}] Advanced mark-delete position to {} -- snapshot not found", subscription, pos);
            }
        }

        return snapshot != null ? new SnapshotResult(snapshot.position(), snapshot.clusters()) : null;
    }

    @VisibleForTesting
    List<SnapshotEntry> getSnapshots() {
        return snapshots;
    }
}
