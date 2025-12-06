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

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ReplicatedSubscriptionSnapshotCacheTest {

    @Test
    public void testSnapshotCache() {
        ReplicatedSubscriptionSnapshotCache cache =
                new ReplicatedSubscriptionSnapshotCache("my-subscription", 10, range -> 0);

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
        s7.setLocalMessageId().setLedgerId(7).setEntryId(7);

        cache.addNewSnapshot(s1);
        cache.addNewSnapshot(s2);
        cache.addNewSnapshot(s5);
        cache.addNewSnapshot(s7);

        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(0, 0)));
        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(1, 0)));
        ReplicatedSubscriptionSnapshotCache.SnapshotResult
                snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(1, 1));
        assertNotNull(snapshot);
        assertEquals(snapshot.position(), PositionFactory.create(1, 1));

        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(5, 6));
        assertNotNull(snapshot);
        assertEquals(snapshot.position(), PositionFactory.create(5, 5));

        // Snapshots should have been now removed
        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(2, 2)));
        assertNull(cache.advancedMarkDeletePosition(PositionFactory.create(5, 5)));
    }

    @Test
    public void testSnapshotCachePruning() {
        ReplicatedSubscriptionSnapshotCache cache =
                new ReplicatedSubscriptionSnapshotCache("my-subscription", 3, range -> 1);

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

        ReplicatedSubscriptionSnapshotCache.SnapshotResult
                snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(2, 2));
        assertNotNull(snapshot);
        // Snapshot-2 was already pruned
        assertEquals(snapshot.position(), PositionFactory.create(1, 1));

        snapshot = cache.advancedMarkDeletePosition(PositionFactory.create(5, 5));
        assertNotNull(snapshot);
        assertEquals(snapshot.position(), PositionFactory.create(4, 4));
    }


    @Test
    public void testSnapshotCachePruningByEqualDistance() {
        int maxSnapshotToCache = 10_000;
        int addSnapshotCount = 1_000_000;

        ReplicatedSubscriptionSnapshotCache cache =
                new ReplicatedSubscriptionSnapshotCache("my-subscription", maxSnapshotToCache,
                        range -> range.upperEndpoint().getEntryId() - range.lowerEndpoint().getEntryId());

        long ledgerIdCluster1 = 1;
        long entryIdCluster1 = 0;
        long ledgerIdCluster2 = 2;
        long entryIdCluster2 = 0;
        Random random = new Random();

        for (int i = 0; i < addSnapshotCount; i++) {
            ReplicatedSubscriptionsSnapshot snapshot = new ReplicatedSubscriptionsSnapshot()
                    .setSnapshotId(UUID.randomUUID().toString());
            snapshot.setLocalMessageId().setLedgerId(ledgerIdCluster1).setEntryId(entryIdCluster1);
            snapshot.addCluster().setCluster("cluster1").setMessageId().setLedgerId(ledgerIdCluster1)
                    .setEntryId(entryIdCluster1);
            snapshot.addCluster().setCluster("cluster2").setMessageId().setLedgerId(ledgerIdCluster2)
                    .setEntryId(entryIdCluster2);
            cache.addNewSnapshot(snapshot);
            entryIdCluster1 += 100 + random.nextInt(1000);
            entryIdCluster2 += 100 + random.nextInt(1000);
        }

        List<ReplicatedSubscriptionSnapshotCache.SnapshotEntry> snapshots = cache.getSnapshots();
        assertEquals(snapshots.size(), maxSnapshotToCache);
        ReplicatedSubscriptionSnapshotCache.SnapshotEntry second = snapshots.get(1);
        ReplicatedSubscriptionSnapshotCache.SnapshotEntry secondLast = snapshots.get(snapshots.size() - 2);
        long distance = secondLast.position().getEntryId() - second.position().getEntryId();
        long averageDistance = distance / snapshots.size();

        long maxDistance = 0;
        long minDistance = Long.MAX_VALUE;
        for (int i = 0; i < snapshots.size() - 1; i++) {
            Position position = snapshots.get(i).position();
            Position nextPosition = snapshots.get(i + 1).position();
            long distanceToNext = nextPosition.getEntryId() - position.getEntryId();
            System.out.println(i + ": " + position + " -> " + nextPosition + " distance to next: " + distanceToNext
                    + " to previous: " + snapshots.get(i).distanceToPrevious());
            maxDistance = Math.max(maxDistance, distanceToNext);
            minDistance = Math.min(minDistance, distanceToNext);
        }

        System.out.println("Min distance: " + minDistance);
        System.out.println("Average distance: " + averageDistance);
        System.out.println("Max distance: " + maxDistance);

        Position markDeletePosition =
                PositionFactory.create(ledgerIdCluster1, second.position().getEntryId() + random.nextLong(distance));

        assertThat(cache.advancedMarkDeletePosition(markDeletePosition)).satisfies(snapshotResult -> {
            long snapshotDistance = markDeletePosition.getEntryId() - snapshotResult.position().getEntryId();
            assertThat(snapshotDistance).describedAs("snapshot result: %s markDeletePosition: %s", snapshotResult,
                    markDeletePosition).isLessThanOrEqualTo(averageDistance * 2);
        });

    }
}
