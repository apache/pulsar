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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
class MutableBucket extends Bucket implements AutoCloseable {

    private final TripleLongPriorityQueue priorityQueue;

    MutableBucket(ManagedCursor cursor,
                  BucketSnapshotStorage bucketSnapshotStorage) {
        super(cursor, bucketSnapshotStorage, -1L, -1L);
        this.priorityQueue = new TripleLongPriorityQueue();
    }

    Pair<ImmutableBucket, DelayedIndex> sealBucketAndAsyncPersistent(
            long timeStepPerBucketSnapshotSegment,
            TripleLongPriorityQueue sharedQueue) {
        return createImmutableBucketAndAsyncPersistent(timeStepPerBucketSnapshotSegment, sharedQueue,
                TripleLongPriorityDelayedIndexQueue.wrap(priorityQueue), startLedgerId, endLedgerId);
    }

    Pair<ImmutableBucket, DelayedIndex> createImmutableBucketAndAsyncPersistent(
            final long timeStepPerBucketSnapshotSegment,
            TripleLongPriorityQueue sharedQueue, DelayedIndexQueue delayedIndexQueue, final long startLedgerId,
            final long endLedgerId) {
        if (delayedIndexQueue.isEmpty()) {
            return null;
        }
        long numMessages = 0;

        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();
        List<SnapshotSegmentMetadata> segmentMetadataList = new ArrayList<>();
        Map<Long, RoaringBitmap> bitMap = new HashMap<>();
        SnapshotSegment.Builder snapshotSegmentBuilder = SnapshotSegment.newBuilder();
        SnapshotSegmentMetadata.Builder segmentMetadataBuilder = SnapshotSegmentMetadata.newBuilder();

        long currentTimestampUpperLimit = 0;
        while (!delayedIndexQueue.isEmpty()) {
            DelayedIndex delayedIndex = delayedIndexQueue.peek();
            long timestamp = delayedIndex.getTimestamp();
            if (currentTimestampUpperLimit == 0) {
                currentTimestampUpperLimit = timestamp + timeStepPerBucketSnapshotSegment - 1;
            }

            long ledgerId = delayedIndex.getLedgerId();
            long entryId = delayedIndex.getEntryId();

            checkArgument(ledgerId >= startLedgerId && ledgerId <= endLedgerId);

            // Move first segment of bucket snapshot to sharedBucketPriorityQueue
            if (segmentMetadataList.size() == 0) {
                sharedQueue.add(timestamp, ledgerId, entryId);
            }

            delayedIndexQueue.pop();
            numMessages++;

            bitMap.computeIfAbsent(ledgerId, k -> new RoaringBitmap()).add(entryId, entryId + 1);

            snapshotSegmentBuilder.addIndexes(delayedIndex);

            if (delayedIndexQueue.isEmpty() || delayedIndexQueue.peek().getTimestamp() > currentTimestampUpperLimit) {
                segmentMetadataBuilder.setMaxScheduleTimestamp(timestamp);
                currentTimestampUpperLimit = 0;

                Iterator<Map.Entry<Long, RoaringBitmap>> iterator = bitMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Long, RoaringBitmap> entry = iterator.next();
                    byte[] array = new byte[entry.getValue().serializedSizeInBytes()];
                    entry.getValue().serialize(ByteBuffer.wrap(array));
                    segmentMetadataBuilder.putDelayedIndexBitMap(entry.getKey(), ByteString.copyFrom(array));
                    iterator.remove();
                }

                segmentMetadataList.add(segmentMetadataBuilder.build());
                segmentMetadataBuilder.clear();

                bucketSnapshotSegments.add(snapshotSegmentBuilder.build());
                snapshotSegmentBuilder.clear();
            }
        }

        SnapshotMetadata bucketSnapshotMetadata = SnapshotMetadata.newBuilder()
                .addAllMetadataList(segmentMetadataList)
                .build();

        final int lastSegmentEntryId = segmentMetadataList.size();

        ImmutableBucket bucket = new ImmutableBucket(cursor, bucketSnapshotStorage, startLedgerId, endLedgerId);
        bucket.setCurrentSegmentEntryId(1);
        bucket.setNumberBucketDelayedMessages(numMessages);
        bucket.setLastSegmentEntryId(lastSegmentEntryId);

        // Add the first snapshot segment last message to snapshotSegmentLastMessageTable
        checkArgument(!bucketSnapshotSegments.isEmpty());
        SnapshotSegment snapshotSegment = bucketSnapshotSegments.get(0);
        DelayedIndex lastDelayedIndex = snapshotSegment.getIndexes(snapshotSegment.getIndexesCount() - 1);
        Pair<ImmutableBucket, DelayedIndex> result = Pair.of(bucket, lastDelayedIndex);

        CompletableFuture<Long> future = asyncSaveBucketSnapshot(bucket,
                bucketSnapshotMetadata, bucketSnapshotSegments);
        bucket.setSnapshotCreateFuture(future);
        future.whenComplete((__, ex) -> {
            if (ex != null) {
                //TODO Record create snapshot failed
                log.error("Failed to create snapshot: ", ex);
            }
        });

        return result;
    }

    void moveScheduledMessageToSharedQueue(long cutoffTime, TripleLongPriorityQueue sharedBucketPriorityQueue) {
        while (!priorityQueue.isEmpty()) {
            long timestamp = priorityQueue.peekN1();
            if (timestamp > cutoffTime) {
                break;
            }

            long ledgerId = priorityQueue.peekN2();
            long entryId = priorityQueue.peekN3();
            sharedBucketPriorityQueue.add(timestamp, ledgerId, entryId);

            priorityQueue.pop();
        }
    }

    void resetLastMutableBucketRange() {
        this.startLedgerId = -1L;
        this.endLedgerId = -1L;
    }

    void clear() {
        this.resetLastMutableBucketRange();
        this.delayedIndexBitMap.clear();
    }

    public void close() {
        priorityQueue.close();
    }

    long getBufferMemoryUsage() {
        return priorityQueue.bytesCapacity();
    }

    boolean isEmpty() {
        return priorityQueue.isEmpty();
    }

    long nextDeliveryTime() {
        return priorityQueue.peekN1();
    }

    long size() {
        return priorityQueue.size();
    }

    void addMessage(long ledgerId, long entryId, long deliverAt) {
        priorityQueue.add(deliverAt, ledgerId, entryId);
        if (startLedgerId == -1L) {
            this.startLedgerId = ledgerId;
        }
        this.endLedgerId = ledgerId;
        putIndexBit(ledgerId, entryId);
    }
}
