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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AllArgsConstructor;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment;

@NotThreadSafe
class CombinedSegmentDelayedIndexQueue implements DelayedIndexQueue {

    @AllArgsConstructor
    static class Node {
        List<SnapshotSegment> segmentList;

        int segmentListCursor;

        int segmentCursor;
    }

    private static final Comparator<Node> COMPARATOR_NODE = (node1, node2) -> DelayedIndexQueue.COMPARATOR.compare(
            node1.segmentList.get(node1.segmentListCursor).getIndexes(node1.segmentCursor),
            node2.segmentList.get(node2.segmentListCursor).getIndexes(node2.segmentCursor));

    private final PriorityQueue<Node> kpq;

    private CombinedSegmentDelayedIndexQueue(List<List<SnapshotSegment>> segmentLists) {
        this.kpq = new PriorityQueue<>(segmentLists.size(), COMPARATOR_NODE);
        for (List<SnapshotSegment> segmentList : segmentLists) {
            Node node = new Node(segmentList, 0, 0);
            kpq.offer(node);
        }
    }

    public static CombinedSegmentDelayedIndexQueue wrap(List<List<SnapshotSegment>> segmentLists) {
        return new CombinedSegmentDelayedIndexQueue(segmentLists);
    }

    @Override
    public boolean isEmpty() {
        return kpq.isEmpty();
    }

    @Override
    public DelayedIndex peek() {
        return getValue(false);
    }

    @Override
    public DelayedIndex pop() {
        return getValue(true);
    }

    private DelayedIndex getValue(boolean needAdvanceCursor) {
        Node node = kpq.peek();
        Objects.requireNonNull(node);

        SnapshotSegment snapshotSegment = node.segmentList.get(node.segmentListCursor);
        DelayedIndex delayedIndex = snapshotSegment.getIndexes(node.segmentCursor);
        if (!needAdvanceCursor) {
            return delayedIndex;
        }

        kpq.poll();

        if (node.segmentCursor + 1 < snapshotSegment.getIndexesCount()) {
            node.segmentCursor++;
            kpq.offer(node);
        } else  {
            // help GC
            node.segmentList.set(node.segmentListCursor, null);
            while (node.segmentListCursor + 1 < node.segmentList.size()) {
                node.segmentListCursor++;
                node.segmentCursor = 0;

                // skip empty segment
                if (node.segmentList.get(node.segmentListCursor).getIndexesCount() > 0) {
                    kpq.offer(node);
                    break;
                }
            }
        }

        return delayedIndex;
    }
}
