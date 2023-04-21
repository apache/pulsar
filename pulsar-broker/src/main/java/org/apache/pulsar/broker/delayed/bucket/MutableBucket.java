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
import com.google.protobuf.UnsafeByteOperations;
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
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegmentMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
class MutableBucket extends Bucket implements AutoCloseable {

    private final TripleLongPriorityQueue priorityQueue;

    MutableBucket(String dispatcherName, ManagedCursor cursor, FutureUtil.Sequencer<Void> sequencer,
                  BucketSnapshotStorage bucketSnapshotStorage) {
        super(dispatcherName, cursor, sequencer, bucketSnapshotStorage, -1L, -1L);
        this.priorityQueue = new TripleLongPriorityQueue();
    }

    Pair<ImmutableBucket, DelayedIndex> sealBucketAndAsyncPersistent(
            long timeStepPerBucketSnapshotSegment,
            int maxIndexesPerBucketSnapshotSegment,
            TripleLongPriorityQueue sharedQueue) {
        return createImmutableBucketAndAsyncPersistent(timeStepPerBucketSnapshotSegment,
                maxIndexesPerBucketSnapshotSegment, sharedQueue,
                TripleLongPriorityDelayedIndexQueue.wrap(priorityQueue), startLedgerId, endLedgerId);
    }

    Pair<ImmutableBucket, DelayedIndex> createImmutableBucketAndAsyncPersistent(
            final long timeStepPerBucketSnapshotSegment, final int maxIndexesPerBucketSnapshotSegment,
            TripleLongPriorityQueue sharedQueue, DelayedIndexQueue delayedIndexQueue, final long startLedgerId,
            final long endLedgerId) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Creating bucket snapshot, startLedgerId: {}, endLedgerId: {}", dispatcherName,
                    startLedgerId, endLedgerId);
        }

        if (delayedIndexQueue.isEmpty()) {
            return null;
        }
        long numMessages = 0;

        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();
        List<SnapshotSegmentMetadata> segmentMetadataList = new ArrayList<>();
        Map<Long, RoaringBitmap> immutableBucketBitMap = new HashMap<>();

        Map<Long, RoaringBitmap> bitMap = new HashMap<>();
        SnapshotSegment snapshotSegment = new SnapshotSegment();
        SnapshotSegmentMetadata.Builder segmentMetadataBuilder = SnapshotSegmentMetadata.newBuilder();

        List<Long> firstScheduleTimestamps = new ArrayList<>();
        long currentTimestampUpperLimit = 0;
        long currentFirstTimestamp = 0L;
        while (!delayedIndexQueue.isEmpty()) {
            final long timestamp = delayedIndexQueue.peekTimestamp();
            if (currentTimestampUpperLimit == 0) {
                currentFirstTimestamp = timestamp;
                firstScheduleTimestamps.add(currentFirstTimestamp);
                currentTimestampUpperLimit = timestamp + timeStepPerBucketSnapshotSegment - 1;
            }

            DelayedIndex delayedIndex = snapshotSegment.addIndexe();
            delayedIndexQueue.popToObject(delayedIndex);

            final long ledgerId = delayedIndex.getLedgerId();
            final long entryId = delayedIndex.getEntryId();

            removeIndexBit(ledgerId, entryId);

            checkArgument(ledgerId >= startLedgerId && ledgerId <= endLedgerId);

            // Move first segment of bucket snapshot to sharedBucketPriorityQueue
            if (segmentMetadataList.size() == 0) {
                sharedQueue.add(timestamp, ledgerId, entryId);
            }

            bitMap.computeIfAbsent(ledgerId, k -> new RoaringBitmap()).add(entryId, entryId + 1);

            numMessages++;

            if (delayedIndexQueue.isEmpty() || delayedIndexQueue.peekTimestamp() > currentTimestampUpperLimit
                    || (maxIndexesPerBucketSnapshotSegment != -1
                    && snapshotSegment.getIndexesCount() >= maxIndexesPerBucketSnapshotSegment)) {
                segmentMetadataBuilder.setMaxScheduleTimestamp(timestamp);
                segmentMetadataBuilder.setMinScheduleTimestamp(currentFirstTimestamp);
                currentTimestampUpperLimit = 0;

                Iterator<Map.Entry<Long, RoaringBitmap>> iterator = bitMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    final var entry = iterator.next();
                    final var lId = entry.getKey();
                    final var bm = entry.getValue();
                    bm.runOptimize();
                    ByteBuffer byteBuffer = ByteBuffer.allocate(bm.serializedSizeInBytes());
                    bm.serialize(byteBuffer);
                    byteBuffer.flip();
                    segmentMetadataBuilder.putDelayedIndexBitMap(lId, UnsafeByteOperations.unsafeWrap(byteBuffer));
                    immutableBucketBitMap.compute(lId, (__, bm0) -> {
                        if (bm0 == null) {
                            return bm;
                        }
                        bm0.or(bm);
                        return bm0;
                    });
                    iterator.remove();
                }

                segmentMetadataList.add(segmentMetadataBuilder.build());
                segmentMetadataBuilder.clear();

                bucketSnapshotSegments.add(snapshotSegment);
                snapshotSegment = new SnapshotSegment();
            }
        }

        // optimize bm
        immutableBucketBitMap.values().forEach(RoaringBitmap::runOptimize);
        this.delayedIndexBitMap.values().forEach(RoaringBitmap::runOptimize);

        SnapshotMetadata bucketSnapshotMetadata = SnapshotMetadata.newBuilder()
                .addAllMetadataList(segmentMetadataList)
                .build();

        final int lastSegmentEntryId = segmentMetadataList.size();

        ImmutableBucket bucket = new ImmutableBucket(dispatcherName, cursor, sequencer, bucketSnapshotStorage,
                startLedgerId, endLedgerId);
        bucket.setCurrentSegmentEntryId(1);
        bucket.setNumberBucketDelayedMessages(numMessages);
        bucket.setLastSegmentEntryId(lastSegmentEntryId);
        bucket.setFirstScheduleTimestamps(firstScheduleTimestamps);
        bucket.setDelayedIndexBitMap(immutableBucketBitMap);

        // Skip first segment, because it has already been loaded
        List<SnapshotSegment> snapshotSegments = bucketSnapshotSegments.subList(1, bucketSnapshotSegments.size());
        bucket.setSnapshotSegments(snapshotSegments);

        // Add the first snapshot segment last message to snapshotSegmentLastMessageTable
        checkArgument(!bucketSnapshotSegments.isEmpty());
        SnapshotSegment firstSnapshotSegment = bucketSnapshotSegments.get(0);
        DelayedIndex lastDelayedIndex = firstSnapshotSegment.getIndexeAt(firstSnapshotSegment.getIndexesCount() - 1);
        Pair<ImmutableBucket, DelayedIndex> result = Pair.of(bucket, lastDelayedIndex);

        CompletableFuture<Long> future = asyncSaveBucketSnapshot(bucket,
                bucketSnapshotMetadata, bucketSnapshotSegments);
        bucket.setSnapshotCreateFuture(future);

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
        this.priorityQueue.clear();
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
