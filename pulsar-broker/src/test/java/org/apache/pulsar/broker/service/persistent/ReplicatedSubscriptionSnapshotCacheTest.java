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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ReplicatedSubscriptionSnapshotCacheTest {
    int snapshotFrequencyMillis = 1000;

    @Test
    public void testSnapshotCache() {

        ReplicatedSubscriptionSnapshotCache cache =
                new ReplicatedSubscriptionSnapshotCache("my-subscription", 10, snapshotFrequencyMillis);

        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(0, 0)));
        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(100, 0)));

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
        long publishTime = System.currentTimeMillis();
        cache.addNewSnapshot(s1, publishTime);
        cache.addNewSnapshot(s2, publishTime + snapshotFrequencyMillis);
        cache.addNewSnapshot(s5, publishTime + 2L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s7, publishTime + 3L * snapshotFrequencyMillis);

        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(0, 0)));
        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(1, 0)));
        ReplicatedSubscriptionsSnapshot snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(1, 1));
        assertNotNull(snapshot);
        assertEquals(snapshot.getSnapshotId(), "snapshot-1");

        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(5, 6));
        assertNotNull(snapshot);
        assertEquals(snapshot.getSnapshotId(), "snapshot-5");

        // Snapshots should have been now removed
        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(2, 2)));
        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(5, 5)));
    }

    @Test
    public void testSnapshotCacheByRestCursor() {

        ReplicatedSubscriptionSnapshotCache cache =
                new ReplicatedSubscriptionSnapshotCache("my-subscription", 10, snapshotFrequencyMillis);
        // The rest cursor is smaller than the cache first position
        ReplicatedSubscriptionsSnapshot s1 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-1");
        s1.setLocalMessageId().setLedgerId(10).setEntryId(10);

        ReplicatedSubscriptionsSnapshot s2 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-2");
        s2.setLocalMessageId().setLedgerId(20).setEntryId(20);

        ReplicatedSubscriptionsSnapshot s3 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-3");
        s3.setLocalMessageId().setLedgerId(30).setEntryId(30);

        ReplicatedSubscriptionsSnapshot s4 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-4");
        s4.setLocalMessageId().setLedgerId(1).setEntryId(1);

        ReplicatedSubscriptionsSnapshot s5 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-5");
        s5.setLocalMessageId().setLedgerId(10).setEntryId(10);
        long publishTime = System.currentTimeMillis();
        cache.addNewSnapshot(s1, publishTime);
        cache.addNewSnapshot(s2, publishTime + 2L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s3, publishTime + 3L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s4, publishTime + 4L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s5, publishTime + 5L * snapshotFrequencyMillis);

        ReplicatedSubscriptionsSnapshot snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(1, 1));
        assertEquals(snapshot.getSnapshotId(), "snapshot-4");
        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(10, 10));
        assertEquals(snapshot.getSnapshotId(), "snapshot-5");
        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(20, 20)));
        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(30, 30)));

        // The rest cursor is smaller than the cache last position
        ReplicatedSubscriptionsSnapshot s6 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-6");
        s6.setLocalMessageId().setLedgerId(10).setEntryId(10);
        ReplicatedSubscriptionsSnapshot s7 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-7");
        s7.setLocalMessageId().setLedgerId(20).setEntryId(20);
        ReplicatedSubscriptionsSnapshot s8 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-8");
        s8.setLocalMessageId().setLedgerId(30).setEntryId(30);
        ReplicatedSubscriptionsSnapshot s9 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-9");
        s9.setLocalMessageId().setLedgerId(20).setEntryId(20);
        cache.addNewSnapshot(s6, publishTime + 6L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s7, publishTime + 7L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s8, publishTime + 8L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s9, publishTime + 9L * snapshotFrequencyMillis);
        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(10, 10));
        assertEquals(snapshot.getSnapshotId(), "snapshot-6");
        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(20, 20));
        assertEquals(snapshot.getSnapshotId(), "snapshot-9");
        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(30, 30)));
    }

    @Test
    public void testSnapshotCachePruning() {
        ReplicatedSubscriptionSnapshotCache cache =
                new ReplicatedSubscriptionSnapshotCache("my-subscription", 4, snapshotFrequencyMillis);

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
        ReplicatedSubscriptionsSnapshot s5 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-5");
        s5.setLocalMessageId().setLedgerId(5).setEntryId(5);
        ReplicatedSubscriptionsSnapshot s6 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-6");
        s6.setLocalMessageId().setLedgerId(6).setEntryId(6);
        ReplicatedSubscriptionsSnapshot s7 = new ReplicatedSubscriptionsSnapshot()
                .setSnapshotId("snapshot-7");
        s7.setLocalMessageId().setLedgerId(7).setEntryId(7);

        long publishTime = System.currentTimeMillis();
        cache.addNewSnapshot(s1, publishTime + snapshotFrequencyMillis);
        cache.addNewSnapshot(s2, publishTime + 2L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s3, publishTime + 3L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s4, publishTime + 4L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s5, publishTime + 5L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s6, publishTime + 5L * snapshotFrequencyMillis);
        cache.addNewSnapshot(s7, publishTime + 7L * snapshotFrequencyMillis);
        // snapshots = [s1, s2, s5, s7]
        cache.advancedMarkDeletePosition(PositionFactory.create(1, 1));
        ReplicatedSubscriptionsSnapshot snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(2, 2));
        assertEquals(snapshot.getSnapshotId(), "snapshot-2");
        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(3, 3));
        assertNull(snapshot);
        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(4, 4));
        assertNull(snapshot);
        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(5, 5));
        assertEquals(snapshot.getSnapshotId(), "snapshot-5");
        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(6, 6));
        assertNull(snapshot);
        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(7, 7));
        assertEquals(snapshot.getSnapshotId(), "snapshot-7");
    }
}
