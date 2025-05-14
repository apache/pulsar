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
import java.util.Map;
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
    private final NavigableMap<Position, ReplicatedSubscriptionsSnapshot> snapshots;
    private final NavigableMap<Position, Long> positionToTimestamp;
    private final int maxSnapshotToCache;
    private final int snapshotFrequencyMillis;

    public ReplicatedSubscriptionSnapshotCache(String subscription, int maxSnapshotToCache,
                                               int snapshotFrequencyMillis) {
        this.subscription = subscription;
        this.snapshots = new TreeMap<>();
        this.positionToTimestamp = new TreeMap<>();
        this.maxSnapshotToCache = maxSnapshotToCache;
        this.snapshotFrequencyMillis = snapshotFrequencyMillis;
    }

    public synchronized void addNewSnapshot(ReplicatedSubscriptionsSnapshot snapshot, long publishTime) {
        MarkersMessageIdData msgId = snapshot.getLocalMessageId();
        Position position = PositionFactory.create(msgId.getLedgerId(), msgId.getEntryId());

        if (log.isDebugEnabled()) {
            log.debug("[{}] Added new replicated-subscription snapshot at {} -- {}", subscription, position,
                    snapshot.getSnapshotId());
        }

        if (positionToTimestamp.lastEntry() == null) {
            snapshots.put(position, snapshot);
            positionToTimestamp.put(position, publishTime);
            return;
        }

        final long timeSinceFirstSnapshot = publishTime - positionToTimestamp.firstEntry().getValue();
        final long timeSinceLastSnapshot = publishTime - positionToTimestamp.lastEntry().getValue();
        final long timeWindowPerSlot = timeSinceFirstSnapshot / snapshotFrequencyMillis / maxSnapshotToCache;
        // reset cursor
        if (position.compareTo(positionToTimestamp.firstKey()) < 0) {
            positionToTimestamp.clear();
            snapshots.clear();
            snapshots.put(position, snapshot);
            positionToTimestamp.put(position, publishTime);
            return;
        } else if (position.compareTo(positionToTimestamp.lastKey()) < 0) {
            while (position.compareTo(positionToTimestamp.lastKey()) < 0) {
                positionToTimestamp.pollLastEntry();
                snapshots.pollLastEntry();
            }
            snapshots.put(position, snapshot);
            positionToTimestamp.put(position, publishTime);
        }

        if (timeSinceLastSnapshot < snapshotFrequencyMillis || timeSinceLastSnapshot < timeWindowPerSlot) {
            return;
        }
        if (snapshots.size() < maxSnapshotToCache) {
            snapshots.put(position, snapshot);
            positionToTimestamp.put(position, publishTime);
        } else {
            int medianIndex = maxSnapshotToCache / 2;
            Position positionToRemove = findPositionByIndex(medianIndex);
            if (positionToRemove != null) {
                positionToTimestamp.remove(positionToRemove);
                snapshots.remove(positionToRemove);
            }
            positionToTimestamp.put(position, publishTime);
            snapshots.put(position, snapshot);
        }
    }

    private Position findPositionByIndex(int targetIndex) {
        Iterator<Map.Entry<Position, Long>> it = positionToTimestamp.entrySet().iterator();
        int currentIndex = 0;
        while (it.hasNext()) {
            Map.Entry<Position, Long> entry = it.next();
            if (currentIndex == targetIndex) {
                return entry.getKey();
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
                snapshot = snapshots.pollFirstEntry().getValue();
                positionToTimestamp.pollFirstEntry();
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
}
