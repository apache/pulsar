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
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ReplicatedSubscriptionSnapshotCacheTest {

    @Test
    public void testSnapshotCache() {
        ReplicatedSubscriptionSnapshotCache cache = new ReplicatedSubscriptionSnapshotCache("my-subscription", 10);

        assertNull(cache.advancedMarkDeletePosition(new PositionImpl(0, 0)));
        assertNull(cache.advancedMarkDeletePosition(new PositionImpl(100, 0)));

        ReplicatedSubscriptionsSnapshot s1 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-1");
        s1.setLocalMessageId().setLedgerId(1).setEntryId(1);

        ReplicatedSubscriptionsSnapshot s2 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-2");
        s2.setLocalMessageId().setLedgerId(2).setEntryId(2);

        ReplicatedSubscriptionsSnapshot s5 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-5");
        s5.setLocalMessageId().setLedgerId(5).setEntryId(5);

        ReplicatedSubscriptionsSnapshot s7 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-7");
        s7.setLocalMessageId().setLedgerId(7 ).setEntryId(7);

        cache.addNewSnapshot(s1);
        cache.addNewSnapshot(s2);
        cache.addNewSnapshot(s5);
        cache.addNewSnapshot(s7);

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
    public void testSnapshotCachePruning() {
        ReplicatedSubscriptionSnapshotCache cache = new ReplicatedSubscriptionSnapshotCache("my-subscription", 3);

        ReplicatedSubscriptionsSnapshot s1 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-1");
        s1.setLocalMessageId().setLedgerId(1).setEntryId(1);

        ReplicatedSubscriptionsSnapshot s2 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-2");
        s2.setLocalMessageId().setLedgerId(2).setEntryId(2);

        ReplicatedSubscriptionsSnapshot s3 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-3");
        s3.setLocalMessageId().setLedgerId(3).setEntryId(3);

        ReplicatedSubscriptionsSnapshot s4 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-4");
        s4.setLocalMessageId().setLedgerId(4).setEntryId(4);

        cache.addNewSnapshot(s1);
        cache.addNewSnapshot(s2);
        cache.addNewSnapshot(s3);
        cache.addNewSnapshot(s4);

        // Snapshot-1 was already pruned
        assertNull(cache.advancedMarkDeletePosition(new PositionImpl(1, 1)));
        ReplicatedSubscriptionsSnapshot snapshot = cache.advancedMarkDeletePosition(new PositionImpl(2, 2));
        assertNotNull(snapshot);
        assertEquals(snapshot.getSnapshotId(), "snapshot-2");

        snapshot = cache.advancedMarkDeletePosition(new PositionImpl(5, 5));
        assertNotNull(snapshot);
        assertEquals(snapshot.getSnapshotId(), "snapshot-4");
    }
}
