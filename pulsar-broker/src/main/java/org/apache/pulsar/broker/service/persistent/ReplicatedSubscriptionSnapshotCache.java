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

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;

/**
 * Store the last N snapshots that were scanned by a particular subscription.
 */
@Slf4j
public class ReplicatedSubscriptionSnapshotCache {
    private final String subscription;
    private final NavigableMap<Position, SnapshotEntry> snapshots;
    private final int maxSnapshotToCache;
    private final int snapshotFrequencyMillis;

    public ReplicatedSubscriptionSnapshotCache(String subscription, int maxSnapshotToCache,
                                               int snapshotFrequencyMillis) {
        this.subscription = subscription;
        this.snapshots = new TreeMap<>();
        this.maxSnapshotToCache = maxSnapshotToCache;
        this.snapshotFrequencyMillis = snapshotFrequencyMillis;
    }

    /**
     * Adds a new replicated subscription snapshot to the cache with time-based eviction policies.
     * This method handles 5 distinct cases for cache management:
     * <ol>
     *   <li><b>Empty Cache</b>: Directly inserts the snapshot if cache is empty.</li>
     *   <li><b>Position Precedes First Entry</b>: Clears entire cache and inserts new snapshot.</li>
     *   <li><b>Position in Middle</b>: Trims later entries and inserts new snapshot.</li>
     *   <li><b>Cache Not Full</b>: Inserts snapshot if cache has available space.</li>
     *   <li><b>Cache Full</b>: Evicts median-aged entry before insertion (time-window optimized).</li>
     * </ol>
     *
     * <p>Time-based eviction considers two conditions:
     * <ul>
     *   <li>Minimum frequency interval ({@code snapshotFrequencyMillis})</li>
     *   <li>Dynamic time window per slot ({@code timeWindowPerSlot})</li>
     * </ul>
     *
     * @param snapshot    The replicated subscription snapshot to add (non-null)
     * @param publishTime The ReplicatedSubscriptionsSnapshot marker message (entry),
     *                    the publishTime originates from the same broker, the time is reliable and clock skew is
     *                    not a problem.
     * @throws NullPointerException If snapshot argument is null
     * @see ReplicatedSubscriptionsSnapshot
     * @see #findPositionByIndex(int)  For median position calculation in eviction case
     */
    public synchronized void addNewSnapshot(ReplicatedSubscriptionsSnapshot snapshot, long publishTime) {
        SnapshotEntry snapshotEntry = new SnapshotEntry(snapshot, publishTime);
        MarkersMessageIdData msgId = snapshot.getLocalMessageId();
        Position position = PositionFactory.create(msgId.getLedgerId(), msgId.getEntryId());

        if (log.isDebugEnabled()) {
            log.debug("[{}] Added new replicated-subscription snapshot at {} -- {}", subscription, position,
                    snapshot.getSnapshotId());
        }
        // Case 1: cache if empty
        if (snapshots.lastEntry() == null) {
            snapshots.put(position, snapshotEntry);
            return;
        }

        // The time difference between the previous position and the earliest cache entry
        final long timeSinceFirstSnapshot = publishTime - snapshots.firstEntry().getValue().timestamp();
        // The time difference between the previous position and the lately cache entry
        final long timeSinceLastSnapshot = publishTime - snapshots.lastEntry().getValue().timestamp();
        // The time window length of each time slot, used for dynamic adjustment in the snapshot cache.
        // The larger the time slot, the slower the update.
        final long timeWindowPerSlot = timeSinceFirstSnapshot / snapshotFrequencyMillis / maxSnapshotToCache;

        if (position.compareTo(snapshots.firstKey()) < 0) {
            // Case 2: When executing 'pulsar-admin topics reset-cursor', reset position for subscription to a position
            // position precedes first entry
            snapshots.clear();
            snapshots.put(position, snapshotEntry);
            return;
        } else if (position.compareTo(snapshots.lastKey()) < 0) {
            // Case 3: When executing 'pulsar-admin topics reset-cursor', reset position for subscription to a position
            // the position is in the middle, delete the cache after that position
            while (position.compareTo(snapshots.lastKey()) < 0) {
                snapshots.pollLastEntry();
            }
            snapshots.put(position, snapshotEntry);
        }
        // omit cache
        // timeSinceLastSnapshot < snapshotFrequencyMillis, keep the same frequency
        // timeSinceLastSnapshot < timeWindowPerSlot, implementing dynamic adjustments
        if (timeSinceLastSnapshot < snapshotFrequencyMillis || timeSinceLastSnapshot < timeWindowPerSlot) {
            return;
        }
        if (snapshots.size() < maxSnapshotToCache) {
            // Case 4: Add to cache if not full
            snapshots.put(position, snapshotEntry);
        } else {
            // Case 5: Median-based eviction when cache is full
            int medianIndex = maxSnapshotToCache / 2;
            Position positionToRemove = findPositionByIndex(medianIndex);
            if (positionToRemove != null) {
                snapshots.remove(positionToRemove);
            }
            snapshots.put(position, snapshotEntry);
        }
    }

    /**
     * Find the Position in NavigableMap according to the target index.
     */
    private Position findPositionByIndex(int targetIndex) {
        Iterator<Position> it = snapshots.keySet().iterator();
        int currentIndex = 0;
        while (it.hasNext()) {
            Position position = it.next();
            if (currentIndex == targetIndex) {
                return position;
            }
            currentIndex++;
        }
        return null;
    }

    /**
     * Signal that the mark-delete position on the subscription has been advanced. If there is a snapshot that
     * correspond to this position, it will returned, other it will return null.
     */
    public synchronized ReplicatedSubscriptionsSnapshot advancedMarkDeletePosition(Position pos) {
        ReplicatedSubscriptionsSnapshot snapshot = null;
        while (!snapshots.isEmpty()) {
            Position first = snapshots.firstKey();
            if (first.compareTo(pos) > 0) {
                // Snapshot is associated which an higher position, so it cannot be used now
                break;
            } else {
                // This snapshot is potentially good. Continue the search for to see if there is a higher snapshot we
                // can use
                snapshot = snapshots.pollFirstEntry().getValue().snapshot();
            }
        }

        if (log.isDebugEnabled()) {
            if (snapshot != null) {
                log.debug("[{}] Advanced mark-delete position to {} -- found snapshot {} at {}:{}", subscription, pos,
                        snapshot.getSnapshotId(),
                        snapshot.getLocalMessageId().getLedgerId(),
                        snapshot.getLocalMessageId().getEntryId());
            } else {
                log.debug("[{}] Advanced mark-delete position to {} -- snapshot not found", subscription, pos);
            }
        }
        return snapshot;
    }

    private record SnapshotEntry(ReplicatedSubscriptionsSnapshot snapshot, long timestamp) {
    }
}
