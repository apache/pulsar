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
package org.apache.pulsar.broker.delayed.bucket;

import static org.apache.pulsar.broker.delayed.bucket.DelayedIndexQueue.COMPARATOR;
import java.util.ArrayList;
import java.util.List;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class DelayedIndexQueueTest {

    @Test
    public void testCompare() {
        DelayedIndex delayedIndex =
                new DelayedIndex().setTimestamp(1).setLedgerId(1L).setEntryId(1L);
        DelayedIndex delayedIndex2 =
                new DelayedIndex().setTimestamp(2).setLedgerId(2L).setEntryId(2L);
        Assert.assertTrue(COMPARATOR.compare(delayedIndex, delayedIndex2) < 0);

        delayedIndex =
                new DelayedIndex().setTimestamp(1).setLedgerId(1L).setEntryId(1L);
        delayedIndex2 =
                new DelayedIndex().setTimestamp(1).setLedgerId(2L).setEntryId(2L);
        Assert.assertTrue(COMPARATOR.compare(delayedIndex, delayedIndex2) < 0);

        delayedIndex = new DelayedIndex().setTimestamp(1).setLedgerId(1L).setEntryId(1L);
        delayedIndex2 = new DelayedIndex().setTimestamp(1).setLedgerId(1L).setEntryId(2L);
        Assert.assertTrue(COMPARATOR.compare(delayedIndex, delayedIndex2) < 0);
    }

    @Test
    public void testCombinedSegmentDelayedIndexQueue() {
        List<DelayedIndex> listA = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            DelayedIndex delayedIndex = new DelayedIndex().setTimestamp(i).setLedgerId(1L).setEntryId(1L);
            listA.add(delayedIndex);
        }
        SnapshotSegment snapshotSegmentA1 = new SnapshotSegment();
        snapshotSegmentA1.addAllIndexes(listA);

        List<DelayedIndex> listA2 = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            DelayedIndex delayedIndex = new DelayedIndex().setTimestamp(i).setLedgerId(1L).setEntryId(1L);
            listA2.add(delayedIndex);
        }
        SnapshotSegment snapshotSegmentA2 = new SnapshotSegment();
        snapshotSegmentA2.addAllIndexes(listA2);

        List<SnapshotSegment> segmentListA = new ArrayList<>();
        segmentListA.add(snapshotSegmentA1);
        segmentListA.add(snapshotSegmentA2);

        List<DelayedIndex> listB = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            DelayedIndex delayedIndex = new DelayedIndex().setTimestamp(i).setLedgerId(2L).setEntryId(1L);

            DelayedIndex delayedIndex2 = new DelayedIndex().setTimestamp(i).setLedgerId(2L).setEntryId(2L);
            listB.add(delayedIndex);
            listB.add(delayedIndex2);
        }

        SnapshotSegment snapshotSegmentB = new SnapshotSegment();
        snapshotSegmentB.addAllIndexes(listB);
        List<SnapshotSegment> segmentListB = new ArrayList<>();
        segmentListB.add(snapshotSegmentB);
        segmentListB.add(new SnapshotSegment());

        List<DelayedIndex> listC = new ArrayList<>();
        for (int i = 10; i < 30; i+=2) {
            DelayedIndex delayedIndex =
                    new DelayedIndex().setTimestamp(i).setLedgerId(2L).setEntryId(1L);

            DelayedIndex delayedIndex2 =
                    new DelayedIndex().setTimestamp(i).setLedgerId(2L).setEntryId(2L);
            listC.add(delayedIndex);
            listC.add(delayedIndex2);
        }

        SnapshotSegment snapshotSegmentC = new SnapshotSegment();
        snapshotSegmentC.addAllIndexes(listC);
        List<SnapshotSegment> segmentListC = new ArrayList<>();
        segmentListC.add(snapshotSegmentC);

        CombinedSegmentDelayedIndexQueue delayedIndexQueue =
                CombinedSegmentDelayedIndexQueue.wrap(List.of(segmentListA, segmentListB, segmentListC));

        int count = 0;
        while (!delayedIndexQueue.isEmpty()) {
            DelayedIndex pop = new DelayedIndex();
            delayedIndexQueue.popToObject(pop);
            log.info("{} , {}, {}", pop.getTimestamp(), pop.getLedgerId(), pop.getEntryId());
            count++;
            if (!delayedIndexQueue.isEmpty()) {
                DelayedIndex peek = delayedIndexQueue.peek();
                long timestamp = delayedIndexQueue.peekTimestamp();
                Assert.assertEquals(timestamp, peek.getTimestamp());
                Assert.assertTrue(COMPARATOR.compare(peek, pop) >= 0);
            }
        }
        Assert.assertEquals(58, count);
    }

    @Test
    public void TripleLongPriorityDelayedIndexQueueTest() {

        @Cleanup
        TripleLongPriorityQueue queue = new TripleLongPriorityQueue();
        for (int i = 0; i < 10; i++) {
            queue.add(i, 1, 1);
        }

        TripleLongPriorityDelayedIndexQueue delayedIndexQueue = TripleLongPriorityDelayedIndexQueue.wrap(queue);

        int count = 0;
        while (!delayedIndexQueue.isEmpty()) {
            DelayedIndex pop = new DelayedIndex();
            delayedIndexQueue.popToObject(pop);
            count++;
            if (!delayedIndexQueue.isEmpty()) {
                DelayedIndex peek = delayedIndexQueue.peek();
                long timestamp = delayedIndexQueue.peekTimestamp();
                Assert.assertEquals(timestamp, peek.getTimestamp());
                Assert.assertTrue(COMPARATOR.compare(peek, pop) >= 0);
            }
        }

        Assert.assertEquals(10, count);
    }
}
