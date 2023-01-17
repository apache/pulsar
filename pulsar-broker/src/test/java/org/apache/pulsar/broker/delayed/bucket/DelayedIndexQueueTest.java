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
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class DelayedIndexQueueTest {

    @Test
    public void testCompare() {
        DelayedIndex delayedIndex =
                DelayedIndex.newBuilder().setTimestamp(1).setLedgerId(1L).setEntryId(1L)
                        .build();
        DelayedIndex delayedIndex2 =
                DelayedIndex.newBuilder().setTimestamp(2).setLedgerId(2L).setEntryId(2L)
                        .build();
        Assert.assertTrue(COMPARATOR.compare(delayedIndex, delayedIndex2) < 0);

        delayedIndex =
                DelayedIndex.newBuilder().setTimestamp(1).setLedgerId(1L).setEntryId(1L)
                        .build();
        delayedIndex2 =
                DelayedIndex.newBuilder().setTimestamp(1).setLedgerId(2L).setEntryId(2L)
                        .build();
        Assert.assertTrue(COMPARATOR.compare(delayedIndex, delayedIndex2) < 0);

        delayedIndex =
                DelayedIndex.newBuilder().setTimestamp(1).setLedgerId(1L).setEntryId(1L)
                        .build();
        delayedIndex2 =
                DelayedIndex.newBuilder().setTimestamp(1).setLedgerId(1L).setEntryId(2L)
                        .build();
        Assert.assertTrue(COMPARATOR.compare(delayedIndex, delayedIndex2) < 0);
    }

    @Test
    public void testCombinedSegmentDelayedIndexQueue() {
        List<DelayedIndex> listA = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            DelayedIndex delayedIndex =
                    DelayedIndex.newBuilder().setTimestamp(i).setLedgerId(1L).setEntryId(1L)
                            .build();
            listA.add(delayedIndex);
        }
        SnapshotSegment snapshotSegmentA1 = SnapshotSegment.newBuilder().addAllIndexes(listA).build();

        List<DelayedIndex> listA2 = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            DelayedIndex delayedIndex =
                    DelayedIndex.newBuilder().setTimestamp(i).setLedgerId(1L).setEntryId(1L)
                            .build();
            listA2.add(delayedIndex);
        }
        SnapshotSegment snapshotSegmentA2 = SnapshotSegment.newBuilder().addAllIndexes(listA2).build();

        List<SnapshotSegment> segmentListA = new ArrayList<>();
        segmentListA.add(snapshotSegmentA1);
        segmentListA.add(snapshotSegmentA2);

        List<DelayedIndex> listB = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            DelayedIndex delayedIndex =
                    DelayedIndex.newBuilder().setTimestamp(i).setLedgerId(2L).setEntryId(1L)
                            .build();

            DelayedIndex delayedIndex2 =
                    DelayedIndex.newBuilder().setTimestamp(i).setLedgerId(2L).setEntryId(2L)
                            .build();
            listB.add(delayedIndex);
            listB.add(delayedIndex2);
        }

        SnapshotSegment snapshotSegmentB = SnapshotSegment.newBuilder().addAllIndexes(listB).build();
        List<SnapshotSegment> segmentListB = new ArrayList<>();
        segmentListB.add(snapshotSegmentB);
        segmentListB.add(SnapshotSegment.newBuilder().build());

        CombinedSegmentDelayedIndexQueue delayedIndexQueue =
                CombinedSegmentDelayedIndexQueue.wrap(segmentListA, segmentListB);

        int count = 0;
        while (!delayedIndexQueue.isEmpty()) {
            DelayedIndex pop = delayedIndexQueue.pop();
            log.info("{} , {}, {}", pop.getTimestamp(), pop.getLedgerId(), pop.getEntryId());
            count++;
            if (!delayedIndexQueue.isEmpty()) {
                DelayedIndex peek = delayedIndexQueue.peek();
                Assert.assertTrue(COMPARATOR.compare(peek, pop) >= 0);
            }
        }
        Assert.assertEquals(38, count);
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
            DelayedIndex pop = delayedIndexQueue.pop();
            count++;
            if (!delayedIndexQueue.isEmpty()) {
                DelayedIndex peek = delayedIndexQueue.peek();
                Assert.assertTrue(COMPARATOR.compare(peek, pop) >= 0);
            }
        }

        Assert.assertEquals(10, count);
    }
}
