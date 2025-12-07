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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.ToLongFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
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
    private final int maxSnapshotToCache;
    private SnapshotEntry head;
    private SnapshotEntry tail;
    private int numberOfSnapshots = 0;
    private SnapshotEntry lastSortedEntry;
    private final SortedSet<SnapshotEntry> sortedSnapshots;

    public ReplicatedSubscriptionSnapshotCache(String subscription, int maxSnapshotToCache,
                                               ToLongFunction<Range<Position>> distanceFunction) {
        this.subscription = subscription;
        this.distanceFunction = distanceFunction;
        if (maxSnapshotToCache < 3) {
            throw new IllegalArgumentException("maxSnapshotToCache must be >= 3");
        }
        this.maxSnapshotToCache = maxSnapshotToCache;
        this.sortedSnapshots = new TreeSet<>();
    }

    /**
     * Memory footprint estimate for one SnapshotEntry with shared String cluster instances.
     *
     * Assumptions:
     * - 64-bit JVM with compressed OOPs enabled (default for heap sizes < 32GB)
     * - Cluster name strings are shared/interned across entries
     * - 2 ClusterEntry objects per SnapshotEntry (typical case)
     * - Each entry has its own Position objects
     * - 1 of the ClusterEntry objects is for the local cluster and shares the local cluster position
     * - List.of() creates ImmutableCollections.List12 for 2-element lists
     * - 8-byte memory alignment padding applied where needed
     *
     * Breakdown:
     *
     * 1. SnapshotEntry object: ~32 bytes
     *    - Object header (mark + klass): 12 bytes
     *    - Position position: 4 bytes (reference)
     *    - List<ClusterEntry> clusters: 4 bytes (reference)
     *    - long distanceToPrevious: 8 bytes
     *    - SnapshotEntry next: 4 bytes (reference)
     *    - SnapshotEntry prev: 4 bytes (reference)
     *    - Alignment padding: 4 bytes
     *    Subtotal: 40 bytes
     *
     * 2. Position object (snapshot position): ~32 bytes
     *    - Object header: 12 bytes
     *    - long ledgerId: 8 bytes
     *    - long entryId: 8 bytes
     *    - Alignment padding: 4 bytes
     *    Subtotal: 32 bytes
     *
     * 3. ImmutableCollections.List12 (for 2 elements): ~32 bytes
     *    - Object header: 12 bytes
     *    - Object e0: 4 bytes (reference to first ClusterEntry)
     *    - Object e1: 4 bytes (reference to second ClusterEntry)
     *    - Alignment padding: 12 bytes
     *    Subtotal: 32 bytes
     *
     * 4. ClusterEntry objects (2 instances): ~64 bytes
     *    Each ClusterEntry (Java record): ~24 bytes
     *    - Object header: 12 bytes
     *    - String cluster: 4 bytes (reference, string itself is shared/interned)
     *    - Position position: 4 bytes (reference)
     *    - Alignment padding: 4 bytes
     *    Subtotal per entry: 24 bytes × 2 = 48 bytes
     *
     *    With alignment to 8 bytes: 48 → 48 bytes
     *    Actual total for both: 48 bytes
     *
     * 5. Additional Position object (for non-local cluster): ~32 bytes
     *    - Object header: 12 bytes
     *    - long ledgerId: 8 bytes
     *    - long entryId: 8 bytes
     *    - Alignment padding: 4 bytes
     *    Subtotal: 32 bytes
     *
     * Total per SnapshotEntry: 40 + 32 + 32 + 48 + 32 = ~184 bytes
     *
     * Rounded estimate: ~184-192 bytes per entry
     *
     * Note: Actual memory consumption may vary based on:
     * - JVM implementation and version
     * - Whether compressed OOPs are enabled
     * - Garbage collection and heap layout
     * - Runtime optimizations (escape analysis, object allocation elimination)
     * - Number of clusters per snapshot (this estimate assumes 2)
     */
    static class SnapshotEntry implements Comparable<SnapshotEntry> {
        private final Position position;
        private final List<ClusterEntry> clusters;
        private long distanceToPrevious;
        private SnapshotEntry next;
        private SnapshotEntry prev;

        SnapshotEntry(Position position, List<ClusterEntry> clusters, long distanceToPrevious) {
            this.position = position;
            this.clusters = clusters;
            this.distanceToPrevious = distanceToPrevious;
        }

        Position position() {
            return position;
        }

        List<ClusterEntry> clusters() {
            return clusters;
        }

        long distanceToPrevious() {
            return distanceToPrevious;
        }

        void setDistanceToPrevious(long distanceToPrevious) {
            this.distanceToPrevious = distanceToPrevious;
        }

        SnapshotEntry next() {
            return next;
        }

        void setNext(SnapshotEntry next) {
            this.next = next;
        }

        SnapshotEntry prev() {
            return prev;
        }

        void setPrev(SnapshotEntry prev) {
            this.prev = prev;
        }

        long totalDistance() {
            return distanceToPrevious + (next != null ? next.distanceToPrevious : 0L);
        }

        @Override
        public int compareTo(SnapshotEntry o) {
            int retval = Long.compare(totalDistance(), o.totalDistance());
            if (retval != 0) {
                return retval;
            }
            retval = position.compareTo(o.position);
            if (retval != 0) {
                return retval;
            }
            return Integer.compare(clusters.hashCode(), o.clusters.hashCode());
        }

        @Override
        public String toString() {
            return String.format("SnapshotEntry(position=%s, clusters=%s, distanceToPrevious=%d)", position, clusters,
                    distanceToPrevious);
        }
    }

    public record ClusterEntry(String cluster, Position position) {}

    public record SnapshotResult(Position position, List<ClusterEntry> clusters) {}

    public synchronized void addNewSnapshot(ReplicatedSubscriptionsSnapshot snapshot) {
        MarkersMessageIdData msgId = snapshot.getLocalMessageId();
        Position position = PositionFactory.create(msgId.getLedgerId(), msgId.getEntryId());


        if (tail != null && position.compareTo(tail.position) <= 0) {
            // clear the entries in the cache if the new snapshot is older than the last one
            // this means that the subscription has been resetted
            head = null;
            tail = null;
            numberOfSnapshots = 0;
            sortedSnapshots.clear();
            lastSortedEntry = null;
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
        SnapshotEntry entry = new SnapshotEntry(position, clusterEntryList,
                tail == null ? 0L : distanceFunction.applyAsLong(Range.open(tail.position, position)));

        if (log.isDebugEnabled()) {
            log.debug("[{}] Added new replicated-subscription snapshot at {} -- {}", subscription, position,
                    snapshot.getSnapshotId());
        }

        if (head == null) {
            head = entry;
            tail = entry;
        } else {
            tail.setNext(entry);
            entry.setPrev(tail);
            tail = entry;
        }
        numberOfSnapshots++;

        // Prune the cache
        if (numberOfSnapshots > maxSnapshotToCache) {
            removeSingleEntryWithMinimumTotalDistanceToPreviousAndNext();
        }
    }

    private void removeSingleEntryWithMinimumTotalDistanceToPreviousAndNext() {
        SnapshotEntry current = lastSortedEntry != null ? lastSortedEntry.next : head.next;
        while (current != null && current != tail) {
            sortedSnapshots.add(current);
            lastSortedEntry = current;
            current = current.next;
        }
        SnapshotEntry minEntry = sortedSnapshots.first();
        sortedSnapshots.remove(minEntry);
        SnapshotEntry minEntryNext = minEntry.next;
        SnapshotEntry minEntryPrevious = minEntry.prev;

        // remove minEntryPrevious and minEntryNext from the sorted set since the distance will be updated
        if (minEntryNext != tail) {
            sortedSnapshots.remove(minEntryNext);
        }
        if (minEntryPrevious != head) {
            sortedSnapshots.remove(minEntryPrevious);
        }

        // remove minEntry from the linked list
        minEntryPrevious.setNext(minEntryNext);
        minEntryNext.setPrev(minEntryPrevious);
        numberOfSnapshots--;
        if (lastSortedEntry == minEntry) {
            if (minEntryPrevious != head) {
                lastSortedEntry = minEntryPrevious;
            } else {
                lastSortedEntry = null;
            }
        }

        // update distanceToPrevious for the next entry
        minEntryNext.setDistanceToPrevious(minEntryNext.distanceToPrevious + minEntry.distanceToPrevious);

        // add entries back so that they are sorted
        if (minEntryNext != tail) {
            sortedSnapshots.add(minEntryNext);
        }
        if (minEntryPrevious != head) {
            sortedSnapshots.add(minEntryPrevious);
        }
    }

    /**
     * Signal that the mark-delete position on the subscription has been advanced. If there is a snapshot that
     * correspond to this position, it will returned, other it will return null.
     */
    public synchronized SnapshotResult advancedMarkDeletePosition(Position pos) {
        SnapshotEntry snapshot = null;

        SnapshotEntry current = head;

        while (current != null) {
            if (current.position.compareTo(pos) > 0) {
                // Snapshot is associated which an higher position, so it cannot be used now
                break;
            }
            // This snapshot is potentially good. Continue the search for to see if there is a higher snapshot we
            // can use
            snapshot = current;
            if (current == lastSortedEntry) {
                lastSortedEntry = null;
            }
            current = current.next;
            head = current;
            if (head != null) {
                sortedSnapshots.remove(head);
            }
            numberOfSnapshots--;
        }

        if (head == null) {
            tail = null;
        } else {
            head.setPrev(null);
            head.setDistanceToPrevious(0L);
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
    synchronized List<SnapshotEntry> getSnapshots() {
        List<SnapshotEntry> snapshots = new ArrayList<>(numberOfSnapshots);
        SnapshotEntry current = head;
        while (current != null) {
            snapshots.add(current);
            current = current.next;
        }
        return snapshots;
    }
}
