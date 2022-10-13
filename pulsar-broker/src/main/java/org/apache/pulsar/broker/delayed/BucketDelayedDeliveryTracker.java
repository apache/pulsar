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
package org.apache.pulsar.broker.delayed;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.mledger.util.Futures.executeWithRetry;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Table;
import com.google.common.collect.TreeRangeMap;
import com.google.protobuf.ByteString;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.SnapshotSegmentMetadata;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
@ThreadSafe
public class BucketDelayedDeliveryTracker extends InMemoryDelayedDeliveryTracker {

    protected static final int AsyncOperationTimeoutSeconds = 30;

    public static final String DELAYED_BUCKET_KEY_PREFIX = "#pulsar.internal.delayed.bucket";

    public static final String DELIMITER = "_";

    private final long minIndexCountPerBucket;

    private final long timeStepPerBucketSnapshotSegment;

    private final int maxNumBuckets;

    private final ManagedCursor cursor;

    public final BucketSnapshotStorage bucketSnapshotStorage;

    private long numberDelayedMessages;

    private final BucketState lastMutableBucketState;

    private final TripleLongPriorityQueue sharedBucketPriorityQueue;

    private final RangeMap<Long, BucketState> immutableBuckets;

    private final Table<Long, Long, BucketState> snapshotSegmentLastIndexTable;

    BucketDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher,
                                 Timer timer, long tickTimeMillis,
                                 boolean isDelayedDeliveryDeliverAtTimeStrict,
                                 long fixedDelayDetectionLookahead,
                                 BucketSnapshotStorage bucketSnapshotStorage,
                                 long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegment,
                                 int maxNumBuckets) {
        this(dispatcher, timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
                fixedDelayDetectionLookahead,
                bucketSnapshotStorage, minIndexCountPerBucket, timeStepPerBucketSnapshotSegment, maxNumBuckets);
    }

    BucketDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher,
                                 Timer timer, long tickTimeMillis, Clock clock,
                                 boolean isDelayedDeliveryDeliverAtTimeStrict,
                                 long fixedDelayDetectionLookahead,
                                 BucketSnapshotStorage bucketSnapshotStorage,
                                 long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegment,
                                 int maxNumBuckets) {
        super(dispatcher, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict,
                fixedDelayDetectionLookahead);
        this.minIndexCountPerBucket = minIndexCountPerBucket;
        this.timeStepPerBucketSnapshotSegment = timeStepPerBucketSnapshotSegment;
        this.maxNumBuckets = maxNumBuckets;
        this.cursor = dispatcher.getCursor();
        this.sharedBucketPriorityQueue = new TripleLongPriorityQueue();
        this.immutableBuckets = TreeRangeMap.create();
        this.snapshotSegmentLastIndexTable = HashBasedTable.create();

        this.bucketSnapshotStorage = bucketSnapshotStorage;

        this.numberDelayedMessages = 0L;

        this.lastMutableBucketState = new BucketState(-1L, -1L);
    }

    private void moveScheduledMessageToSharedQueue(long cutoffTime) {
        TripleLongPriorityQueue priorityQueue = getPriorityQueue();
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

    @Override
    public void run(Timeout timeout) throws Exception {
        synchronized (this) {
            if (timeout == null || timeout.isCancelled()) {
                return;
            }
            moveScheduledMessageToSharedQueue(getCutoffTime());
        }
        super.run(timeout);
    }

    private Optional<BucketState> findBucket(long ledgerId) {
        if (immutableBuckets.asMapOfRanges().isEmpty()) {
            return Optional.empty();
        }

        Range<Long> span = immutableBuckets.span();
        if (!span.contains(ledgerId)) {
            return Optional.empty();
        }
        return Optional.ofNullable(immutableBuckets.get(ledgerId));
    }

    private long getBucketId(BucketState bucketState) {
        Optional<Long> bucketIdOptional = bucketState.getBucketId();
        if (bucketIdOptional.isPresent()) {
            return bucketIdOptional.get();
        }

        String bucketIdStr = cursor.getCursorProperties().get(bucketState.bucketKey());
        long bucketId = Long.parseLong(bucketIdStr);
        bucketState.setBucketId(bucketId);
        return bucketId;
    }

    private BucketState createBucket(long startLedgerId, long endLedgerId) {
        BucketState bucketState = new BucketState(startLedgerId, endLedgerId);
        immutableBuckets.put(Range.closed(startLedgerId, endLedgerId), bucketState);
        return bucketState;
    }

    private CompletableFuture<Long> asyncSaveBucketSnapshot(
            BucketState bucketState, SnapshotMetadata snapshotMetadata,
            List<SnapshotSegment> bucketSnapshotSegments) {

        return bucketSnapshotStorage.createBucketSnapshot(snapshotMetadata, bucketSnapshotSegments)
                .thenApply(newBucketId -> {
                    bucketState.setBucketId(newBucketId);
                    String bucketKey = bucketState.bucketKey();
                    putBucketKeyId(bucketKey, newBucketId).exceptionally(ex -> {
                        log.warn("Failed to record bucketId to cursor property, bucketKey: {}", bucketKey);
                        return null;
                    });
                    return newBucketId;
                });
    }

    private CompletableFuture<Void> putBucketKeyId(String bucketKey, Long bucketId) {
        Objects.requireNonNull(bucketId);
        return executeWithRetry(() -> cursor.putCursorProperty(bucketKey, String.valueOf(bucketId)),
                ManagedLedgerException.BadVersionException.class);
    }

    private CompletableFuture<Long> asyncCreateBucketSnapshot() {
        TripleLongPriorityQueue priorityQueue = super.getPriorityQueue();
        if (priorityQueue.isEmpty()) {
            return CompletableFuture.completedFuture(-1L);
        }
        long numMessages = 0;

        final long startLedgerId = lastMutableBucketState.startLedgerId;
        final long endLedgerId = lastMutableBucketState.endLedgerId;

        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();
        List<SnapshotSegmentMetadata> segmentMetadataList = new ArrayList<>();
        Map<Long, RoaringBitmap> bitMap = new HashMap<>();
        SnapshotSegment.Builder snapshotSegmentBuilder = SnapshotSegment.newBuilder();
        SnapshotSegmentMetadata.Builder segmentMetadataBuilder = SnapshotSegmentMetadata.newBuilder();

        long currentTimestampUpperLimit = 0;
        while (!priorityQueue.isEmpty()) {
            long timestamp = priorityQueue.peekN1();
            if (currentTimestampUpperLimit == 0) {
                currentTimestampUpperLimit = timestamp + timeStepPerBucketSnapshotSegment - 1;
            }

            long ledgerId = priorityQueue.peekN2();
            long entryId = priorityQueue.peekN3();

            checkArgument(ledgerId >= startLedgerId && ledgerId <= endLedgerId);

            // Move first segment of bucket snapshot to sharedBucketPriorityQueue
            if (segmentMetadataList.size() == 0) {
                sharedBucketPriorityQueue.add(timestamp, ledgerId, entryId);
            }

            priorityQueue.pop();
            numMessages++;

            DelayedIndex delayedIndex = DelayedIndex.newBuilder()
                    .setTimestamp(timestamp)
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId).build();

            bitMap.computeIfAbsent(ledgerId, k -> new RoaringBitmap()).add(entryId, entryId + 1);

            snapshotSegmentBuilder.addIndexes(delayedIndex);

            if (priorityQueue.isEmpty() || priorityQueue.peekN1() > currentTimestampUpperLimit) {
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

        BucketState bucketState = this.createBucket(startLedgerId, endLedgerId);
        bucketState.setCurrentSegmentEntryId(1);
        bucketState.setNumberBucketDelayedMessages(numMessages);
        bucketState.setLastSegmentEntryId(lastSegmentEntryId);

        // Add the first snapshot segment last message to snapshotSegmentLastMessageTable
        checkArgument(!bucketSnapshotSegments.isEmpty());
        SnapshotSegment snapshotSegment = bucketSnapshotSegments.get(0);
        DelayedIndex delayedIndex = snapshotSegment.getIndexes(snapshotSegment.getIndexesCount() - 1);
        snapshotSegmentLastIndexTable.put(delayedIndex.getLedgerId(), delayedIndex.getEntryId(), bucketState);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Create bucket snapshot, bucketState: {}", dispatcher.getName(), bucketState);
        }

        CompletableFuture<Long> future = asyncSaveBucketSnapshot(bucketState,
                bucketSnapshotMetadata, bucketSnapshotSegments);
        bucketState.setSnapshotCreateFuture(future);
        future.whenComplete((__, ex) -> {
            if (ex == null) {
                bucketState.setSnapshotCreateFuture(null);
            } else {
                //TODO Record create snapshot failed
                log.error("Failed to create snapshot: ", ex);
            }
        });

        return future;
    }


    private CompletableFuture<Void> asyncLoadNextBucketSnapshotEntry(BucketState bucketState, boolean isRecover) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Load next bucket snapshot data, bucketState: {}", dispatcher.getName(), bucketState);
        }
        if (bucketState == null) {
            return CompletableFuture.completedFuture(null);
        }

        // Wait bucket snapshot create finish
        CompletableFuture<Long> snapshotCreateFuture =
                bucketState.getSnapshotCreateFuture().orElseGet(() -> CompletableFuture.completedFuture(-1L));

        return snapshotCreateFuture.thenCompose(__ -> {
            final long bucketId = getBucketId(bucketState);
            CompletableFuture<Integer> loadMetaDataFuture = new CompletableFuture<>();
            if (isRecover) {
                // TODO Recover bucket snapshot
            } else {
                loadMetaDataFuture.complete(bucketState.currentSegmentEntryId + 1);
            }

            return loadMetaDataFuture.thenCompose(nextSegmentEntryId -> {
                if (nextSegmentEntryId > bucketState.lastSegmentEntryId) {
                    // TODO Delete bucket snapshot
                    return CompletableFuture.completedFuture(null);
                }

                return bucketSnapshotStorage.getBucketSnapshotSegment(bucketId, nextSegmentEntryId, nextSegmentEntryId)
                        .thenAccept(bucketSnapshotSegments -> {
                            if (CollectionUtils.isEmpty(bucketSnapshotSegments)) {
                                return;
                            }

                            SnapshotSegment snapshotSegment = bucketSnapshotSegments.get(0);
                            List<DelayedIndex> indexList = snapshotSegment.getIndexesList();
                            DelayedIndex lastDelayedIndex = indexList.get(indexList.size() - 1);

                            this.snapshotSegmentLastIndexTable.put(lastDelayedIndex.getLedgerId(),
                                    lastDelayedIndex.getEntryId(), bucketState);

                            for (DelayedIndex index : indexList) {
                                sharedBucketPriorityQueue.add(index.getTimestamp(), index.getLedgerId(),
                                        index.getEntryId());
                            }

                            bucketState.setCurrentSegmentEntryId(nextSegmentEntryId);
                        });
            });
        });
    }

    private void resetLastMutableBucketRange() {
        lastMutableBucketState.setStartLedgerId(-1L);
        lastMutableBucketState.setEndLedgerId(-1L);
    }

    @Override
    public synchronized boolean addMessage(long ledgerId, long entryId, long deliverAt) {
        if (containsMessage(ledgerId, entryId)) {
            messagesHaveFixedDelay = false;
            return true;
        }

        if (deliverAt < 0 || deliverAt <= getCutoffTime()) {
            messagesHaveFixedDelay = false;
            return false;
        }

        boolean existBucket = findBucket(ledgerId).isPresent();

        // Create bucket snapshot
        if (ledgerId > lastMutableBucketState.endLedgerId && !getPriorityQueue().isEmpty()) {
            if (getPriorityQueue().size() >= minIndexCountPerBucket || existBucket) {
                asyncCreateBucketSnapshot();
                resetLastMutableBucketRange();
                if (immutableBuckets.asMapOfRanges().size() > maxNumBuckets) {
                    // TODO merge bucket snapshot (synchronize operate)
                }
            }
        }

        if (ledgerId < lastMutableBucketState.startLedgerId || existBucket) {
            // If (ledgerId < startLedgerId || existBucket) means that message index belong to previous bucket range,
            // enter sharedBucketPriorityQueue directly
            sharedBucketPriorityQueue.add(deliverAt, ledgerId, entryId);
        } else {
            checkArgument(ledgerId >= lastMutableBucketState.endLedgerId);

            getPriorityQueue().add(deliverAt, ledgerId, entryId);

            if (lastMutableBucketState.startLedgerId == -1L) {
                lastMutableBucketState.setStartLedgerId(ledgerId);
            }
            lastMutableBucketState.setEndLedgerId(ledgerId);
        }

        lastMutableBucketState.putIndexBit(ledgerId, entryId);
        numberDelayedMessages++;

        if (log.isDebugEnabled()) {
            log.debug("[{}] Add message {}:{} -- Delivery in {} ms ", dispatcher.getName(), ledgerId, entryId,
                    deliverAt - clock.millis());
        }

        updateTimer();

        checkAndUpdateHighest(deliverAt);

        return true;
    }

    @Override
    public synchronized boolean hasMessageAvailable() {
        long cutoffTime = getCutoffTime();

        boolean hasMessageAvailable = !getPriorityQueue().isEmpty() && getPriorityQueue().peekN1() <= cutoffTime;

        hasMessageAvailable = hasMessageAvailable
                || !sharedBucketPriorityQueue.isEmpty() && sharedBucketPriorityQueue.peekN1() <= cutoffTime;
        if (!hasMessageAvailable) {
            updateTimer();
        }
        return hasMessageAvailable;
    }

    @Override
    protected long nextDeliveryTime() {
        if (getPriorityQueue().isEmpty() && !sharedBucketPriorityQueue.isEmpty()) {
            return sharedBucketPriorityQueue.peekN1();
        } else if (sharedBucketPriorityQueue.isEmpty() && !getPriorityQueue().isEmpty()) {
            return getPriorityQueue().peekN1();
        }
        long timestamp = getPriorityQueue().peekN1();
        long bucketTimestamp = sharedBucketPriorityQueue.peekN1();
        return Math.min(timestamp, bucketTimestamp);
    }

    @Override
    public synchronized long getNumberOfDelayedMessages() {
        return numberDelayedMessages;
    }

    @Override
    public synchronized long getBufferMemoryUsage() {
        return getPriorityQueue().bytesCapacity() + sharedBucketPriorityQueue.bytesCapacity();
    }

    @Override
    public synchronized Set<PositionImpl> getScheduledMessages(int maxMessages) {
        long cutoffTime = getCutoffTime();

        moveScheduledMessageToSharedQueue(cutoffTime);

        Set<PositionImpl> positions = new TreeSet<>();
        int n = maxMessages;

        while (n > 0 && !sharedBucketPriorityQueue.isEmpty()) {
            long timestamp = sharedBucketPriorityQueue.peekN1();
            if (timestamp > cutoffTime) {
                break;
            }

            long ledgerId = sharedBucketPriorityQueue.peekN2();
            long entryId = sharedBucketPriorityQueue.peekN3();
            positions.add(new PositionImpl(ledgerId, entryId));

            sharedBucketPriorityQueue.pop();
            removeIndexBit(ledgerId, entryId);

            BucketState bucketState = snapshotSegmentLastIndexTable.remove(ledgerId, entryId);
            if (bucketState != null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Load next snapshot segment, bucketState: {}", dispatcher.getName(), bucketState);
                }
                // All message of current snapshot segment are scheduled, load next snapshot segment
                try {
                    asyncLoadNextBucketSnapshotEntry(bucketState, false).get(AsyncOperationTimeoutSeconds,
                            TimeUnit.SECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }

            --n;
            --numberDelayedMessages;
        }

        if (numberDelayedMessages <= 0) {
            // Reset to initial state
            highestDeliveryTimeTracked = 0;
            messagesHaveFixedDelay = true;
        }

        updateTimer();

        return positions;
    }

    @Override
    public synchronized void clear() {
        super.clear();
        cleanImmutableBuckets(true);
        sharedBucketPriorityQueue.clear();
        resetLastMutableBucketRange();
        lastMutableBucketState.delayedIndexBitMap.clear();
        snapshotSegmentLastIndexTable.clear();
        numberDelayedMessages = 0;
    }

    @Override
    public synchronized void close() {
        super.close();
        cleanImmutableBuckets(false);
        lastMutableBucketState.delayedIndexBitMap.clear();
        sharedBucketPriorityQueue.close();
    }

    private void cleanImmutableBuckets(boolean delete) {
        if (immutableBuckets != null) {
            Iterator<BucketState> iterator = immutableBuckets.asMapOfRanges().values().iterator();
            while (iterator.hasNext()) {
                BucketState bucketState = iterator.next();
                if (bucketState.delayedIndexBitMap != null) {
                    bucketState.delayedIndexBitMap.clear();
                }

                bucketState.getSnapshotCreateFuture().ifPresent(snapshotGenerateFuture -> {
                    if (delete) {
                        snapshotGenerateFuture.cancel(true);
                        // TODO delete bucket snapshot
                    } else {
                        try {
                            snapshotGenerateFuture.get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            log.warn("Failed wait to snapshot generate, bucketState: {}", bucketState);
                        }
                    }
                });
                iterator.remove();
            }
        }
    }

    private boolean removeIndexBit(long ledgerId, long entryId) {
        if (lastMutableBucketState.removeIndexBit(ledgerId, entryId)) {
            return true;
        }

        return findBucket(ledgerId).map(bucketState -> bucketState.removeIndexBit(ledgerId, entryId))
                .orElse(false);
    }

    @Override
    public boolean containsMessage(long ledgerId, long entryId) {
        if (lastMutableBucketState.containsMessage(ledgerId, entryId)) {
            return true;
        }

        return findBucket(ledgerId).map(bucketState -> bucketState.containsMessage(ledgerId, entryId))
                .orElse(false);
    }
}
