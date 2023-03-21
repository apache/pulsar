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

import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment;

@NotThreadSafe
class CombinedSegmentDelayedIndexQueue implements DelayedIndexQueue {

    private final List<SnapshotSegment> segmentListA;
    private final List<SnapshotSegment> segmentListB;

    private int segmentListACursor = 0;
    private int segmentListBCursor = 0;
    private int segmentACursor = 0;
    private int segmentBCursor = 0;

    private CombinedSegmentDelayedIndexQueue(List<SnapshotSegment> segmentListA,
                                             List<SnapshotSegment> segmentListB) {
        this.segmentListA = segmentListA;
        this.segmentListB = segmentListB;
    }

    public static CombinedSegmentDelayedIndexQueue wrap(
            List<SnapshotSegment> segmentListA,
            List<SnapshotSegment> segmentListB) {
        return new CombinedSegmentDelayedIndexQueue(segmentListA, segmentListB);
    }

    @Override
    public boolean isEmpty() {
        return segmentListACursor >= segmentListA.size() && segmentListBCursor >= segmentListB.size();
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
        // skip empty segment
        while (segmentListACursor < segmentListA.size()
                && segmentListA.get(segmentListACursor).getIndexesCount() == 0) {
            segmentListACursor++;
        }
        while (segmentListBCursor < segmentListB.size()
                && segmentListB.get(segmentListBCursor).getIndexesCount() == 0) {
            segmentListBCursor++;
        }

        DelayedIndex delayedIndexA = null;
        DelayedIndex delayedIndexB = null;
        if (segmentListACursor >= segmentListA.size()) {
            delayedIndexB = segmentListB.get(segmentListBCursor).getIndexes(segmentBCursor);
        } else if (segmentListBCursor >= segmentListB.size()) {
            delayedIndexA = segmentListA.get(segmentListACursor).getIndexes(segmentACursor);
        } else {
            delayedIndexA = segmentListA.get(segmentListACursor).getIndexes(segmentACursor);
            delayedIndexB = segmentListB.get(segmentListBCursor).getIndexes(segmentBCursor);
        }

        DelayedIndex resultValue;
        if (delayedIndexB == null || (delayedIndexA != null && COMPARATOR.compare(delayedIndexA, delayedIndexB) < 0)) {
            resultValue = delayedIndexA;
            if (needAdvanceCursor) {
                if (++segmentACursor >= segmentListA.get(segmentListACursor).getIndexesCount()) {
                    segmentListA.set(segmentListACursor, null);
                    ++segmentListACursor;
                    segmentACursor = 0;
                }
            }
        } else {
            resultValue = delayedIndexB;
            if (needAdvanceCursor) {
                if (++segmentBCursor >= segmentListB.get(segmentListBCursor).getIndexesCount()) {
                    segmentListB.set(segmentListBCursor, null);
                    ++segmentListBCursor;
                    segmentBCursor = 0;
                }
            }
        }

        return resultValue;
    }
}
