/**
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

import java.util.NavigableMap;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;

/**
 * Store the last N snapshots that were scanned by a particular subscription.
 */
@Slf4j
public class ReplicatedSubscriptionSnapshotCache {
    private final String subscription;
    private final NavigableMap<PositionImpl, ReplicatedSubscriptionsSnapshot> snapshots;
    private final int maxSnapshotToCache;

    public ReplicatedSubscriptionSnapshotCache(String subscription, int maxSnapshotToCache) {
        this.subscription = subscription;
        this.snapshots = new TreeMap<>();
        this.maxSnapshotToCache = maxSnapshotToCache;
    }

    public synchronized void addNewSnapshot(ReplicatedSubscriptionsSnapshot snapshot) {
        MarkersMessageIdData msgId = snapshot.getLocalMessageId();
        PositionImpl position = new PositionImpl(msgId.getLedgerId(), msgId.getEntryId());

        if (log.isDebugEnabled()) {
            log.debug("[{}] Added new replicated-subscription snapshot at {} -- {}", subscription, position,
                    snapshot.getSnapshotId());
        }

        snapshots.put(position, snapshot);

        // Prune the cache
        while (snapshots.size() > maxSnapshotToCache) {
            snapshots.pollFirstEntry();
        }
    }

    /**
     * Signal that the mark-delete position on the subscription has been advanced. If there is a snapshot that
     * correspond to this position, it will returned, other it will return null.
     */
    public synchronized ReplicatedSubscriptionsSnapshot advancedMarkDeletePosition(PositionImpl pos) {
        ReplicatedSubscriptionsSnapshot snapshot = null;
        while (!snapshots.isEmpty()) {
            PositionImpl first = snapshots.firstKey();
            if (first.compareTo(pos) > 0) {
                // Snapshot is associated which an higher position, so it cannot be used now
                break;
            } else {
                // This snapshot is potentially good. Continue the search for to see if there is a higher snapshot we
                // can use
                snapshot = snapshots.pollFirstEntry().getValue();
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
