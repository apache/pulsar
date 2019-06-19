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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.PulsarMarkers.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshot;
import org.testng.annotations.Test;

public class ReplicatedSubscriptionSnapshotCacheTest {
    @Test
    public void testSnashotCache() {
        ReplicatedSubscriptionSnapshotCache cache = new ReplicatedSubscriptionSnapshotCache("my-subscription", 10);

        assertNull(cache.advancedMarkDeletePosition(new PositionImpl(0, 0)));
        assertNull(cache.advancedMarkDeletePosition(new PositionImpl(100, 0)));

        cache.addNewSnapshot(ReplicatedSubscriptionsSnapshot.newBuilder()
                .setSnapshotId("snapshot-1")
                .setLocalMessageId(newMessageId(1, 1))
                .build());

        cache.addNewSnapshot(ReplicatedSubscriptionsSnapshot.newBuilder()
                .setSnapshotId("snapshot-2")
                .setLocalMessageId(newMessageId(2, 2))
                .build());

        cache.addNewSnapshot(ReplicatedSubscriptionsSnapshot.newBuilder()
                .setSnapshotId("snapshot-5")
                .setLocalMessageId(newMessageId(5, 5))
                .build());

        cache.addNewSnapshot(ReplicatedSubscriptionsSnapshot.newBuilder()
                .setSnapshotId("snapshot-7")
                .setLocalMessageId(newMessageId(7, 7))
                .build());

        assertNull(cache.advancedMarkDeletePosition(new PositionImpl(0, 0)));
        assertNull(cache.advancedMarkDeletePosition(new PositionImpl(1, 0)));
        ReplicatedSubscriptionsSnapshot snapshot = cache.advancedMarkDeletePosition(new PositionImpl(1, 1));
        assertNotNull(snapshot);
        assertEquals(snapshot.getSnapshotId(), "snapshot-1");

        snapshot = cache.advancedMarkDeletePosition(new PositionImpl(5, 6));
        assertNotNull(snapshot);
        assertEquals(snapshot.getSnapshotId(), "snapshot-5");

        // Snapshots should have been now removed
        assertNull(cache.advancedMarkDeletePosition(new PositionImpl(2, 2)));
        assertNull(cache.advancedMarkDeletePosition(new PositionImpl(5, 5)));
    }

    @Test
    public void testSnashotCachePruning() {
        ReplicatedSubscriptionSnapshotCache cache = new ReplicatedSubscriptionSnapshotCache("my-subscription", 3);

        cache.addNewSnapshot(ReplicatedSubscriptionsSnapshot.newBuilder()
                .setSnapshotId("snapshot-1")
                .setLocalMessageId(newMessageId(1, 1))
                .build());

        cache.addNewSnapshot(ReplicatedSubscriptionsSnapshot.newBuilder()
                .setSnapshotId("snapshot-2")
                .setLocalMessageId(newMessageId(2, 2))
                .build());

        cache.addNewSnapshot(ReplicatedSubscriptionsSnapshot.newBuilder()
                .setSnapshotId("snapshot-3")
                .setLocalMessageId(newMessageId(3, 3))
                .build());

        cache.addNewSnapshot(ReplicatedSubscriptionsSnapshot.newBuilder()
                .setSnapshotId("snapshot-4")
                .setLocalMessageId(newMessageId(4, 4))
                .build());

        // Snapshot-1 was already pruned
        assertNull(cache.advancedMarkDeletePosition(new PositionImpl(1, 1)));
        ReplicatedSubscriptionsSnapshot snapshot = cache.advancedMarkDeletePosition(new PositionImpl(2, 2));
        assertNotNull(snapshot);
        assertEquals(snapshot.getSnapshotId(), "snapshot-2");

        snapshot = cache.advancedMarkDeletePosition(new PositionImpl(5, 5));
        assertNotNull(snapshot);
        assertEquals(snapshot.getSnapshotId(), "snapshot-4");
    }

    private MessageIdData newMessageId(long ledgerId, long entryId) {
        return MessageIdData.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .build();
    }
}
