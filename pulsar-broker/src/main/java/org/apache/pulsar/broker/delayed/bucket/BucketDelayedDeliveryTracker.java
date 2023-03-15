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
import static org.apache.pulsar.broker.delayed.bucket.Bucket.DELAYED_BUCKET_KEY_PREFIX;
import static org.apache.pulsar.broker.delayed.bucket.Bucket.DELIMITER;
import static org.apache.pulsar.broker.delayed.bucket.Bucket.MaxRetryTimes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Table;
import com.google.common.collect.TreeRangeMap;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.time.Clock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.AbstractDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.DelayedIndex;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
@ThreadSafe
public class BucketDelayedDeliveryTracker extends AbstractDelayedDeliveryTracker {

    static final CompletableFuture<Long> NULL_LONG_PROMISE = CompletableFuture.completedFuture(null);

    static final int AsyncOperationTimeoutSeconds = 60;

    private final long minIndexCountPerBucket;

    private final long timeStepPerBucketSnapshotSegmentInMillis;

    private final int maxIndexesPerBucketSnapshotSegment;

    private final int maxNumBuckets;

    private long numberDelayedMessages;

    @Getter
    @VisibleForTesting
    private final MutableBucket lastMutableBucket;

    @Getter
    @VisibleForTesting
    private final TripleLongPriorityQueue sharedBucketPriorityQueue;

    @Getter
    @VisibleForTesting
    private final RangeMap<Long, ImmutableBucket> immutableBuckets;

    private final Table<Long, Long, ImmutableBucket> snapshotSegmentLastIndexTable;

    public BucketDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher,
                                 Timer timer, long tickTimeMillis,
                                 boolean isDelayedDeliveryDeliverAtTimeStrict,
                                 BucketSnapshotStorage bucketSnapshotStorage,
                                 long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                 int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets) {
        this(dispatcher, timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
                bucketSnapshotStorage, minIndexCountPerBucket, timeStepPerBucketSnapshotSegmentInMillis,
                maxIndexesPerBucketSnapshotSegment, maxNumBuckets);
    }

    public BucketDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher,
                                 Timer timer, long tickTimeMillis, Clock clock,
                                 boolean isDelayedDeliveryDeliverAtTimeStrict,
                                 BucketSnapshotStorage bucketSnapshotStorage,
                                 long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                 int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets) {
        super(dispatcher, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict);
        this.minIndexCountPerBucket = minIndexCountPerBucket;
        this.timeStepPerBucketSnapshotSegmentInMillis = timeStepPerBucketSnapshotSegmentInMillis;
        this.maxIndexesPerBucketSnapshotSegment = maxIndexesPerBucketSnapshotSegment;
        this.maxNumBuckets = maxNumBuckets;
        this.sharedBucketPriorityQueue = new TripleLongPriorityQueue();
        this.immutableBuckets = TreeRangeMap.create();
        this.snapshotSegmentLastIndexTable = HashBasedTable.create();
        this.lastMutableBucket = new MutableBucket(dispatcher.getName(), dispatcher.getCursor(), bucketSnapshotStorage);
        this.numberDelayedMessages = recoverBucketSnapshot();
    }

    private synchronized long recoverBucketSnapshot() throws RuntimeException {
        ManagedCursor cursor = this.lastMutableBucket.getCursor();
        Map<Range<Long>, ImmutableBucket> toBeDeletedBucketMap = new HashMap<>();
        cursor.getCursorProperties().keySet().forEach(key -> {
            if (key.startsWith(DELAYED_BUCKET_KEY_PREFIX)) {
                String[] keys = key.split(DELIMITER);
                checkArgument(keys.length == 3);
                ImmutableBucket immutableBucket =
                        new ImmutableBucket(dispatcher.getName(), cursor, this.lastMutableBucket.bucketSnapshotStorage,
                                Long.parseLong(keys[1]), Long.parseLong(keys[2]));
                putAndCleanOverlapRange(Range.closed(immutableBucket.startLedgerId, immutableBucket.endLedgerId),
                        immutableBucket, toBeDeletedBucketMap);
            }
        });

        Map<Range<Long>, ImmutableBucket> immutableBucketMap = immutableBuckets.asMapOfRanges();
        if (immutableBucketMap.isEmpty()) {
            log.info("[{}] Recover delayed message index bucket snapshot finish, don't find bucket snapshot",
                    dispatcher.getName());
            return 0;
        }

        Map<Range<Long>, CompletableFuture<List<DelayedIndex>>>
                futures = new HashMap<>(immutableBucketMap.size());
        for (Map.Entry<Range<Long>, ImmutableBucket> entry : immutableBucketMap.entrySet()) {
            Range<Long> key = entry.getKey();
            ImmutableBucket immutableBucket = entry.getValue();
            futures.put(key, immutableBucket.asyncRecoverBucketSnapshotEntry(this::getCutoffTime));
        }

        try {
            FutureUtil.waitForAll(futures.values()).get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }

        for (Map.Entry<Range<Long>, CompletableFuture<List<DelayedIndex>>> entry : futures.entrySet()) {
            Range<Long> key = entry.getKey();
            // the future will always be completed since it was waited for above
            List<DelayedIndex> indexList = entry.getValue().getNow(null);
            ImmutableBucket immutableBucket = immutableBucketMap.get(key);
            if (CollectionUtils.isEmpty(indexList)) {
                // Delete bucket snapshot if indexList is empty
                toBeDeletedBucketMap.put(key, immutableBucket);
            } else {
                DelayedIndex lastDelayedIndex = indexList.get(indexList.size() - 1);
                this.snapshotSegmentLastIndexTable.put(lastDelayedIndex.getLedgerId(),
                        lastDelayedIndex.getEntryId(), immutableBucket);
                for (DelayedIndex index : indexList) {
                    this.sharedBucketPriorityQueue.add(index.getTimestamp(), index.getLedgerId(),
                            index.getEntryId());
                }
            }
        }

        for (Map.Entry<Range<Long>, ImmutableBucket> mapEntry : toBeDeletedBucketMap.entrySet()) {
            Range<Long> key = mapEntry.getKey();
            ImmutableBucket immutableBucket = mapEntry.getValue();
            immutableBucketMap.remove(key);
            // delete asynchronously without waiting for completion
            immutableBucket.asyncDeleteBucketSnapshot();
        }

        MutableLong numberDelayedMessages = new MutableLong(0);
        immutableBucketMap.values().forEach(bucket -> {
            numberDelayedMessages.add(bucket.numberBucketDelayedMessages);
        });

        log.info("[{}] Recover delayed message index bucket snapshot finish, buckets: {}, numberDelayedMessages: {}",
                dispatcher.getName(), immutableBucketMap.size(), numberDelayedMessages.getValue());

        return numberDelayedMessages.getValue();
    }

    private synchronized void putAndCleanOverlapRange(Range<Long> range, ImmutableBucket immutableBucket,
                                                      Map<Range<Long>, ImmutableBucket> toBeDeletedBucketMap) {
        RangeMap<Long, ImmutableBucket> subRangeMap = immutableBuckets.subRangeMap(range);
        boolean canPut = false;
        if (!subRangeMap.asMapOfRanges().isEmpty()) {
            for (Map.Entry<Range<Long>, ImmutableBucket> rangeEntry : subRangeMap.asMapOfRanges().entrySet()) {
                if (range.encloses(rangeEntry.getKey())) {
                    toBeDeletedBucketMap.put(rangeEntry.getKey(), rangeEntry.getValue());
                    canPut = true;
                }
            }
        } else {
            canPut = true;
        }

        if (canPut) {
            immutableBuckets.put(range, immutableBucket);
        }
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        synchronized (this) {
            if (timeout == null || timeout.isCancelled()) {
                return;
            }
            lastMutableBucket.moveScheduledMessageToSharedQueue(getCutoffTime(), sharedBucketPriorityQueue);
        }
        super.run(timeout);
    }

    private Optional<ImmutableBucket> findImmutableBucket(long ledgerId) {
        if (immutableBuckets.asMapOfRanges().isEmpty()) {
            return Optional.empty();
        }

        return Optional.ofNullable(immutableBuckets.get(ledgerId));
    }

    private void afterCreateImmutableBucket(Pair<ImmutableBucket, DelayedIndex> immutableBucketDelayedIndexPair) {
        if (immutableBucketDelayedIndexPair != null) {
            ImmutableBucket immutableBucket = immutableBucketDelayedIndexPair.getLeft();
            immutableBuckets.put(Range.closed(immutableBucket.startLedgerId, immutableBucket.endLedgerId),
                    immutableBucket);

            DelayedIndex lastDelayedIndex = immutableBucketDelayedIndexPair.getRight();
            snapshotSegmentLastIndexTable.put(lastDelayedIndex.getLedgerId(), lastDelayedIndex.getEntryId(),
                    immutableBucket);

            immutableBucket.getSnapshotCreateFuture().ifPresent(createFuture -> {
                CompletableFuture<Long> future = createFuture.whenComplete((__, ex) -> {
                    if (ex == null) {
                        immutableBucket.setSnapshotSegments(null);
                        log.info("[{}] Creat bucket snapshot finish, bucketKey: {}", dispatcher.getName(),
                                immutableBucket.bucketKey());
                        return;
                    }

                    //TODO Record create snapshot failed
                    log.error("[{}] Failed to create bucket snapshot, bucketKey: {}",
                            dispatcher.getName(), immutableBucket.bucketKey(), ex);

                    // Put indexes back into the shared queue and downgrade to memory mode
                    synchronized (BucketDelayedDeliveryTracker.this) {
                        immutableBucket.getSnapshotSegments().ifPresent(snapshotSegments -> {
                            for (DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment snapshotSegment :
                                    snapshotSegments) {
                                for (DelayedIndex delayedIndex : snapshotSegment.getIndexesList()) {
                                    sharedBucketPriorityQueue.add(delayedIndex.getTimestamp(),
                                            delayedIndex.getLedgerId(), delayedIndex.getEntryId());
                                }
                            }
                            immutableBucket.setSnapshotSegments(null);
                        });

                        immutableBucket.setCurrentSegmentEntryId(immutableBucket.lastSegmentEntryId);
                        immutableBuckets.remove(
                                Range.closed(immutableBucket.startLedgerId, immutableBucket.endLedgerId));
                        snapshotSegmentLastIndexTable.remove(lastDelayedIndex.getLedgerId(),
                                lastDelayedIndex.getTimestamp());
                    }
                });
                immutableBucket.setSnapshotCreateFuture(future);
            });
        }
    }

    @Override
    public synchronized boolean addMessage(long ledgerId, long entryId, long deliverAt) {
        if (containsMessage(ledgerId, entryId)) {
            return true;
        }

        if (deliverAt < 0 || deliverAt <= getCutoffTime()) {
            return false;
        }

        boolean existBucket = findImmutableBucket(ledgerId).isPresent();

        // Create bucket snapshot
        if (!existBucket && ledgerId > lastMutableBucket.endLedgerId
                && lastMutableBucket.size() >= minIndexCountPerBucket
                && !lastMutableBucket.isEmpty()) {
            Pair<ImmutableBucket, DelayedIndex> immutableBucketDelayedIndexPair =
                    lastMutableBucket.sealBucketAndAsyncPersistent(
                            this.timeStepPerBucketSnapshotSegmentInMillis,
                            this.maxIndexesPerBucketSnapshotSegment,
                            this.sharedBucketPriorityQueue);
            afterCreateImmutableBucket(immutableBucketDelayedIndexPair);
            lastMutableBucket.resetLastMutableBucketRange();

            if (immutableBuckets.asMapOfRanges().size() > maxNumBuckets) {
                try {
                    asyncMergeBucketSnapshot().get(2 * AsyncOperationTimeoutSeconds * MaxRetryTimes, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // Ignore exception to merge bucket on the next schedule.
                    log.error("[{}] An exception occurs when merge bucket snapshot.", dispatcher.getName(), e);
                }
            }
        }

        if (ledgerId < lastMutableBucket.startLedgerId || existBucket) {
            // If (ledgerId < startLedgerId || existBucket) means that message index belong to previous bucket range,
            // enter sharedBucketPriorityQueue directly
            sharedBucketPriorityQueue.add(deliverAt, ledgerId, entryId);
        } else {
            checkArgument(ledgerId >= lastMutableBucket.endLedgerId);
            lastMutableBucket.addMessage(ledgerId, entryId, deliverAt);
        }

        numberDelayedMessages++;

        if (log.isDebugEnabled()) {
            log.debug("[{}] Add message {}:{} -- Delivery in {} ms ", dispatcher.getName(), ledgerId, entryId,
                    deliverAt - clock.millis());
        }

        updateTimer();

        return true;
    }

    private synchronized CompletableFuture<Void> asyncMergeBucketSnapshot() {
        List<ImmutableBucket> values = immutableBuckets.asMapOfRanges().values().stream().toList();
        long minNumberMessages = Long.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i + 1 < values.size(); i++) {
            ImmutableBucket bucketL = values.get(i);
            ImmutableBucket bucketR = values.get(i + 1);
            long numberMessages = bucketL.numberBucketDelayedMessages + bucketR.numberBucketDelayedMessages;
            if (numberMessages < minNumberMessages) {
                minNumberMessages = (int) numberMessages;
                if (bucketL.lastSegmentEntryId > bucketL.getCurrentSegmentEntryId()
                        && bucketR.lastSegmentEntryId > bucketR.getCurrentSegmentEntryId()
                        && bucketL.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE).isDone()
                        && bucketR.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE).isDone()) {
                    minIndex = i;
                }
            }
        }

        if (minIndex == -1) {
            log.warn("[{}] Can't find able merged bucket", dispatcher.getName());
            return CompletableFuture.completedFuture(null);
        }

        ImmutableBucket immutableBucketA = values.get(minIndex);
        ImmutableBucket immutableBucketB = values.get(minIndex + 1);

        if (log.isDebugEnabled()) {
            log.info("[{}] Merging bucket snapshot, bucketAKey: {}, bucketBKey: {}", dispatcher.getName(),
                    immutableBucketA.bucketKey(), immutableBucketB.bucketKey());
        }
        return asyncMergeBucketSnapshot(immutableBucketA, immutableBucketB).whenComplete((__, ex) -> {
            if (ex != null) {
                log.error("[{}] Failed to merge bucket snapshot, bucketAKey: {}, bucketBKey: {}",
                        dispatcher.getName(), immutableBucketA.bucketKey(), immutableBucketB.bucketKey(), ex);
            } else {
                log.info("[{}] Merge bucket snapshot finish, bucketAKey: {}, bucketBKey: {}",
                        dispatcher.getName(), immutableBucketA.bucketKey(), immutableBucketB.bucketKey());
            }
        });
    }

    private synchronized CompletableFuture<Void> asyncMergeBucketSnapshot(ImmutableBucket bucketA,
                                                                          ImmutableBucket bucketB) {
        CompletableFuture<List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment>> futureA =
                bucketA.getRemainSnapshotSegment();
        CompletableFuture<List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment>> futureB =
                bucketB.getRemainSnapshotSegment();
        return futureA.thenCombine(futureB, CombinedSegmentDelayedIndexQueue::wrap)
                .thenAccept(combinedDelayedIndexQueue -> {
                    Pair<ImmutableBucket, DelayedIndex> immutableBucketDelayedIndexPair =
                            lastMutableBucket.createImmutableBucketAndAsyncPersistent(
                                    timeStepPerBucketSnapshotSegmentInMillis, maxIndexesPerBucketSnapshotSegment,
                                    sharedBucketPriorityQueue, combinedDelayedIndexQueue, bucketA.startLedgerId,
                                    bucketB.endLedgerId);

                    // Merge bit map to new bucket
                    Map<Long, RoaringBitmap> delayedIndexBitMapA = bucketA.getDelayedIndexBitMap();
                    Map<Long, RoaringBitmap> delayedIndexBitMapB = bucketB.getDelayedIndexBitMap();
                    Map<Long, RoaringBitmap> delayedIndexBitMap = new HashMap<>(delayedIndexBitMapA);
                    delayedIndexBitMapB.forEach((ledgerId, bitMapB) -> {
                        delayedIndexBitMap.compute(ledgerId, (k, bitMapA) -> {
                            if (bitMapA == null) {
                                return bitMapB;
                            }

                            bitMapA.or(bitMapB);
                            return bitMapA;
                        });
                    });
                    immutableBucketDelayedIndexPair.getLeft().setDelayedIndexBitMap(delayedIndexBitMap);

                    afterCreateImmutableBucket(immutableBucketDelayedIndexPair);

                    immutableBucketDelayedIndexPair.getLeft().getSnapshotCreateFuture()
                            .orElse(NULL_LONG_PROMISE).thenCompose(___ -> {
                        CompletableFuture<Void> removeAFuture = bucketA.asyncDeleteBucketSnapshot();
                        CompletableFuture<Void> removeBFuture = bucketB.asyncDeleteBucketSnapshot();
                        return CompletableFuture.allOf(removeAFuture, removeBFuture);
                    });

                    immutableBuckets.remove(Range.closed(bucketA.startLedgerId, bucketA.endLedgerId));
                    immutableBuckets.remove(Range.closed(bucketB.startLedgerId, bucketB.endLedgerId));
                });
    }

    @Override
    public synchronized boolean hasMessageAvailable() {
        long cutoffTime = getCutoffTime();

        boolean hasMessageAvailable = getNumberOfDelayedMessages() > 0 && nextDeliveryTime() <= cutoffTime;
        if (!hasMessageAvailable) {
            updateTimer();
        }
        return hasMessageAvailable;
    }

    @Override
    protected long nextDeliveryTime() {
        if (lastMutableBucket.isEmpty() && !sharedBucketPriorityQueue.isEmpty()) {
            return sharedBucketPriorityQueue.peekN1();
        } else if (sharedBucketPriorityQueue.isEmpty() && !lastMutableBucket.isEmpty()) {
            return lastMutableBucket.nextDeliveryTime();
        }
        long timestamp = lastMutableBucket.nextDeliveryTime();
        long bucketTimestamp = sharedBucketPriorityQueue.peekN1();
        return Math.min(timestamp, bucketTimestamp);
    }

    @Override
    public synchronized long getNumberOfDelayedMessages() {
        return numberDelayedMessages;
    }

    @Override
    public synchronized long getBufferMemoryUsage() {
        return this.lastMutableBucket.getBufferMemoryUsage() + sharedBucketPriorityQueue.bytesCapacity();
    }

    @Override
    public synchronized NavigableSet<PositionImpl> getScheduledMessages(int maxMessages) {
        long cutoffTime = getCutoffTime();

        lastMutableBucket.moveScheduledMessageToSharedQueue(cutoffTime, sharedBucketPriorityQueue);

        NavigableSet<PositionImpl> positions = new TreeSet<>();
        int n = maxMessages;

        while (n > 0 && !sharedBucketPriorityQueue.isEmpty()) {
            long timestamp = sharedBucketPriorityQueue.peekN1();
            if (timestamp > cutoffTime) {
                break;
            }

            long ledgerId = sharedBucketPriorityQueue.peekN2();
            long entryId = sharedBucketPriorityQueue.peekN3();

            ImmutableBucket bucket = snapshotSegmentLastIndexTable.get(ledgerId, entryId);
            if (bucket != null && immutableBuckets.asMapOfRanges().containsValue(bucket)) {
                final int preSegmentEntryId = bucket.currentSegmentEntryId;
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Loading next bucket snapshot segment, bucketKey: {}, nextSegmentEntryId: {}",
                            dispatcher.getName(), bucket.bucketKey(), preSegmentEntryId + 1);
                }
                // All message of current snapshot segment are scheduled, load next snapshot segment
                // TODO make it asynchronous and not blocking this process
                try {
                    boolean createFutureDone = bucket.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE).isDone();

                    if (!createFutureDone) {
                        log.info("[{}] Skip load to wait for bucket snapshot create finish, bucketKey:{}",
                                dispatcher.getName(), bucket.bucketKey());
                        break;
                    }

                    if (bucket.currentSegmentEntryId == bucket.lastSegmentEntryId) {
                        immutableBuckets.remove(Range.closed(bucket.startLedgerId, bucket.endLedgerId));
                        bucket.asyncDeleteBucketSnapshot();
                        continue;
                    }

                    bucket.asyncLoadNextBucketSnapshotEntry().thenAccept(indexList -> {
                        if (CollectionUtils.isEmpty(indexList)) {
                            immutableBuckets.remove(Range.closed(bucket.startLedgerId, bucket.endLedgerId));
                            bucket.asyncDeleteBucketSnapshot();
                            return;
                        }
                        DelayedMessageIndexBucketSnapshotFormat.DelayedIndex
                                lastDelayedIndex = indexList.get(indexList.size() - 1);
                        this.snapshotSegmentLastIndexTable.put(lastDelayedIndex.getLedgerId(),
                                lastDelayedIndex.getEntryId(), bucket);
                        for (DelayedMessageIndexBucketSnapshotFormat.DelayedIndex index : indexList) {
                            sharedBucketPriorityQueue.add(index.getTimestamp(), index.getLedgerId(),
                                    index.getEntryId());
                        }
                    }).whenComplete((__, ex) -> {
                        if (ex != null) {
                            // Back bucket state
                            bucket.setCurrentSegmentEntryId(preSegmentEntryId);

                            log.error("[{}] Failed to load bucket snapshot segment, bucketKey: {}",
                                    dispatcher.getName(), bucket.bucketKey(), ex);
                        } else {
                            log.info("[{}] Load next bucket snapshot segment finish, bucketKey: {}, segmentEntryId: {}",
                                    dispatcher.getName(), bucket.bucketKey(), bucket.currentSegmentEntryId);
                        }
                    }).get(AsyncOperationTimeoutSeconds * MaxRetryTimes, TimeUnit.SECONDS);
                    snapshotSegmentLastIndexTable.remove(ledgerId, entryId);
                } catch (Exception e) {
                    // Ignore exception to reload this segment on the next schedule.
                    log.error("[{}] An exception occurs when load next bucket snapshot, bucketKey:{}",
                            dispatcher.getName(), bucket.bucketKey(), e);
                    break;
                }
            }

            positions.add(new PositionImpl(ledgerId, entryId));

            sharedBucketPriorityQueue.pop();
            removeIndexBit(ledgerId, entryId);

            --n;
            --numberDelayedMessages;
        }

        updateTimer();

        return positions;
    }

    @Override
    public boolean shouldPauseAllDeliveries() {
        return false;
    }

    @Override
    public synchronized void clear() {
        cleanImmutableBuckets(true);
        sharedBucketPriorityQueue.clear();
        lastMutableBucket.clear();
        snapshotSegmentLastIndexTable.clear();
        numberDelayedMessages = 0;
    }

    @Override
    public synchronized void close() {
        super.close();
        lastMutableBucket.close();
        cleanImmutableBuckets(false);
        sharedBucketPriorityQueue.close();
    }

    private void cleanImmutableBuckets(boolean delete) {
        if (immutableBuckets != null) {
            Iterator<ImmutableBucket> iterator = immutableBuckets.asMapOfRanges().values().iterator();
            while (iterator.hasNext()) {
                ImmutableBucket bucket = iterator.next();
                bucket.clear(delete);
                iterator.remove();
            }
        }
    }

    private boolean removeIndexBit(long ledgerId, long entryId) {
        if (lastMutableBucket.removeIndexBit(ledgerId, entryId)) {
            return true;
        }

        return findImmutableBucket(ledgerId).map(bucket -> bucket.removeIndexBit(ledgerId, entryId))
                .orElse(false);
    }

    @Override
    public boolean containsMessage(long ledgerId, long entryId) {
        if (lastMutableBucket.containsMessage(ledgerId, entryId)) {
            return true;
        }

        return findImmutableBucket(ledgerId).map(bucket -> bucket.containsMessage(ledgerId, entryId))
                .orElse(false);
    }
}
