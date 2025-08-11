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
import static org.apache.bookkeeper.mledger.ManagedCursor.CURSOR_INTERNAL_PROPERTY_PREFIX;
import static org.apache.pulsar.broker.delayed.bucket.Bucket.DELIMITER;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.AbstractDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
@ThreadSafe
public class BucketDelayedDeliveryTracker extends AbstractDelayedDeliveryTracker {

    /**
     * Record to represent a snapshot key with ledger ID and entry ID.
     * Avoids overhead of creating String instances for keys.
     */
    public static record SnapshotKey(long ledgerId, long entryId) {}

    public static final String DELAYED_BUCKET_KEY_PREFIX = CURSOR_INTERNAL_PROPERTY_PREFIX + "delayed.bucket";

    static final CompletableFuture<Long> NULL_LONG_PROMISE = CompletableFuture.completedFuture(null);

    static final int AsyncOperationTimeoutSeconds = 60;

    private static final Long INVALID_BUCKET_ID = -1L;

    private static final int MAX_MERGE_NUM = 4;

    private final long minIndexCountPerBucket;

    private final long timeStepPerBucketSnapshotSegmentInMillis;

    private final int maxIndexesPerBucketSnapshotSegment;

    private final int maxNumBuckets;

    private final AtomicLong numberDelayedMessages = new AtomicLong(0);

    // Thread safety locks
    private final StampedLock stampedLock = new StampedLock();

    @Getter
    @VisibleForTesting
    private final MutableBucket lastMutableBucket;

    @Getter
    @VisibleForTesting
    private final TripleLongPriorityQueue sharedBucketPriorityQueue;

    @Getter
    @VisibleForTesting
    private final RangeMap<Long, ImmutableBucket> immutableBuckets;

    private final ConcurrentHashMap<SnapshotKey, ImmutableBucket> snapshotSegmentLastIndexMap;

    private final BucketDelayedMessageIndexStats stats;

    private CompletableFuture<Void> pendingLoad = null;

    public BucketDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher,
                                        Timer timer, long tickTimeMillis,
                                        boolean isDelayedDeliveryDeliverAtTimeStrict,
                                        BucketSnapshotStorage bucketSnapshotStorage,
                                        long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                        int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets)
            throws RecoverDelayedDeliveryTrackerException {
        this(dispatcher, timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
                bucketSnapshotStorage, minIndexCountPerBucket, timeStepPerBucketSnapshotSegmentInMillis,
                maxIndexesPerBucketSnapshotSegment, maxNumBuckets);
    }

    public BucketDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher,
                                        Timer timer, long tickTimeMillis, Clock clock,
                                        boolean isDelayedDeliveryDeliverAtTimeStrict,
                                        BucketSnapshotStorage bucketSnapshotStorage,
                                        long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                        int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets)
            throws RecoverDelayedDeliveryTrackerException {
        super(dispatcher, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict);
        this.minIndexCountPerBucket = minIndexCountPerBucket;
        this.timeStepPerBucketSnapshotSegmentInMillis = timeStepPerBucketSnapshotSegmentInMillis;
        this.maxIndexesPerBucketSnapshotSegment = maxIndexesPerBucketSnapshotSegment;
        this.maxNumBuckets = maxNumBuckets;
        this.sharedBucketPriorityQueue = new TripleLongPriorityQueue();
        this.immutableBuckets = TreeRangeMap.create();
        this.snapshotSegmentLastIndexMap = new ConcurrentHashMap<>();
        this.lastMutableBucket =
                new MutableBucket(dispatcher.getName(), dispatcher.getCursor(), FutureUtil.Sequencer.create(),
                        bucketSnapshotStorage);
        this.stats = new BucketDelayedMessageIndexStats();

        // Close the tracker if failed to recover.
        try {
            long recoveredMessages = recoverBucketSnapshot();
            this.numberDelayedMessages.set(recoveredMessages);
        } catch (RecoverDelayedDeliveryTrackerException e) {
            close();
            throw e;
        }
    }

    private synchronized long recoverBucketSnapshot() throws RecoverDelayedDeliveryTrackerException {
        ManagedCursor cursor = this.lastMutableBucket.getCursor();
        Map<String, String> cursorProperties = cursor.getCursorProperties();
        if (MapUtils.isEmpty(cursorProperties)) {
            log.info("[{}] Recover delayed message index bucket snapshot finish, don't find bucket snapshot",
                    dispatcher.getName());
            return 0;
        }
        FutureUtil.Sequencer<Void> sequencer = this.lastMutableBucket.getSequencer();
        Map<Range<Long>, ImmutableBucket> toBeDeletedBucketMap = new HashMap<>();
        cursorProperties.keySet().forEach(key -> {
            if (key.startsWith(DELAYED_BUCKET_KEY_PREFIX)) {
                String[] keys = key.split(DELIMITER);
                checkArgument(keys.length == 3);
                ImmutableBucket immutableBucket =
                        new ImmutableBucket(dispatcher.getName(), cursor, sequencer,
                                this.lastMutableBucket.bucketSnapshotStorage,
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
            futures.put(key, handleRecoverBucketSnapshotEntry(entry.getValue()));
        }

        try {
            FutureUtil.waitForAll(futures.values()).get(AsyncOperationTimeoutSeconds * 5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("[{}] Failed to recover delayed message index bucket snapshot.", dispatcher.getName(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RecoverDelayedDeliveryTrackerException(e);
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
                this.snapshotSegmentLastIndexMap.put(
                        new SnapshotKey(lastDelayedIndex.getLedgerId(), lastDelayedIndex.getEntryId()),
                        immutableBucket);
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
            immutableBucket.asyncDeleteBucketSnapshot(stats);
        }

        MutableLong numberDelayedMessages = new MutableLong(0);
        immutableBucketMap.values().forEach(bucket -> {
            numberDelayedMessages.add(bucket.numberBucketDelayedMessages);
        });

        log.info("[{}] Recover delayed message index bucket snapshot finish, buckets: {}, numberDelayedMessages: {}",
                dispatcher.getName(), immutableBucketMap.size(), numberDelayedMessages.getValue());

        return numberDelayedMessages.getValue();
    }

    /**
     * Handle the BucketNotExistException when recover bucket snapshot entry.
     * The non exist bucket will be added to `toBeDeletedBucketMap` and deleted from `immutableBuckets`
     * in the next step.
     *
     * @param bucket
     * @return
     */
    private CompletableFuture<List<DelayedIndex>> handleRecoverBucketSnapshotEntry(ImmutableBucket bucket) {
        CompletableFuture<List<DelayedIndex>> f = new CompletableFuture<>();
        bucket.asyncRecoverBucketSnapshotEntry(this::getCutoffTime)
                .whenComplete((v, e) -> {
                    if (e == null) {
                        f.complete(v);
                    } else {
                        if (e instanceof BucketNotExistException) {
                            // If the bucket does not exist, return an empty list,
                            // the bucket will be deleted from `immutableBuckets` in the next step.
                            f.complete(Collections.emptyList());
                        } else {
                            f.completeExceptionally(e);
                        }
                    }
                });
        return f;
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

    private void afterCreateImmutableBucket(Pair<ImmutableBucket, DelayedIndex> immutableBucketDelayedIndexPair,
                                            long startTime) {
        if (immutableBucketDelayedIndexPair != null) {
            ImmutableBucket immutableBucket = immutableBucketDelayedIndexPair.getLeft();
            immutableBuckets.put(Range.closed(immutableBucket.startLedgerId, immutableBucket.endLedgerId),
                    immutableBucket);

            DelayedIndex lastDelayedIndex = immutableBucketDelayedIndexPair.getRight();
            snapshotSegmentLastIndexMap.put(
                    new SnapshotKey(lastDelayedIndex.getLedgerId(), lastDelayedIndex.getEntryId()),
                    immutableBucket);

            immutableBucket.getSnapshotCreateFuture().ifPresent(createFuture -> {
                CompletableFuture<Long> future = createFuture.handle((bucketId, ex) -> {
                    if (ex == null) {
                        immutableBucket.setSnapshotSegments(null);
                        immutableBucket.asyncUpdateSnapshotLength();
                        log.info("[{}] Create bucket snapshot finish, bucketKey: {}", dispatcher.getName(),
                                immutableBucket.bucketKey());

                        stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.create,
                                System.currentTimeMillis() - startTime);

                        return bucketId;
                    }

                    log.error("[{}] Failed to create bucket snapshot, bucketKey: {}", dispatcher.getName(),
                            immutableBucket.bucketKey(), ex);
                    stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.create);

                    // Put indexes back into the shared queue and downgrade to memory mode
                    synchronized (BucketDelayedDeliveryTracker.this) {
                        immutableBucket.getSnapshotSegments().ifPresent(snapshotSegments -> {
                            for (SnapshotSegment snapshotSegment : snapshotSegments) {
                                for (DelayedIndex delayedIndex : snapshotSegment.getIndexesList()) {
                                    sharedBucketPriorityQueue.add(delayedIndex.getTimestamp(),
                                            delayedIndex.getLedgerId(), delayedIndex.getEntryId());
                                }
                            }
                            immutableBucket.setSnapshotSegments(null);
                        });

                        immutableBucket.setCurrentSegmentEntryId(immutableBucket.lastSegmentEntryId);
                        immutableBuckets.asMapOfRanges().remove(
                                Range.closed(immutableBucket.startLedgerId, immutableBucket.endLedgerId));
                        snapshotSegmentLastIndexMap.remove(
                                new SnapshotKey(lastDelayedIndex.getLedgerId(), lastDelayedIndex.getEntryId()));
                    }
                    return INVALID_BUCKET_ID;
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
            long createStartTime = System.currentTimeMillis();
            stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.create);
            Pair<ImmutableBucket, DelayedIndex> immutableBucketDelayedIndexPair =
                    lastMutableBucket.sealBucketAndAsyncPersistent(
                            this.timeStepPerBucketSnapshotSegmentInMillis,
                            this.maxIndexesPerBucketSnapshotSegment,
                            this.sharedBucketPriorityQueue);
            afterCreateImmutableBucket(immutableBucketDelayedIndexPair, createStartTime);
            lastMutableBucket.resetLastMutableBucketRange();

            if (maxNumBuckets > 0 && immutableBuckets.asMapOfRanges().size() > maxNumBuckets) {
                asyncMergeBucketSnapshot();
            }
        }

        if (ledgerId < lastMutableBucket.startLedgerId || existBucket) {
            // If (ledgerId < startLedgerId || existBucket) means that message index belong to previous bucket range,
            // enter sharedBucketPriorityQueue directly
            sharedBucketPriorityQueue.add(deliverAt, ledgerId, entryId);
            lastMutableBucket.putIndexBit(ledgerId, entryId);
        } else {
            checkArgument(ledgerId >= lastMutableBucket.endLedgerId);
            lastMutableBucket.addMessage(ledgerId, entryId, deliverAt);
        }

        numberDelayedMessages.incrementAndGet();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Add message {}:{} -- Delivery in {} ms ", dispatcher.getName(), ledgerId, entryId,
                    deliverAt - clock.millis());
        }

        updateTimer();

        return true;
    }

    private synchronized List<ImmutableBucket> selectMergedBuckets(final List<ImmutableBucket> values, int mergeNum) {
        checkArgument(mergeNum < values.size());
        long minNumberMessages = Long.MAX_VALUE;
        long minScheduleTimestamp = Long.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i + (mergeNum - 1) < values.size(); i++) {
            List<ImmutableBucket> immutableBuckets = values.subList(i, i + mergeNum);
            if (immutableBuckets.stream().allMatch(bucket -> {
                // We should skip the bucket which last segment already been load to memory,
                // avoid record replicated index.
                return bucket.lastSegmentEntryId > bucket.currentSegmentEntryId && !bucket.merging;
            })) {
                long numberMessages = immutableBuckets.stream()
                        .mapToLong(bucket -> bucket.numberBucketDelayedMessages)
                        .sum();
                if (numberMessages <= minNumberMessages) {
                    minNumberMessages = numberMessages;
                    long scheduleTimestamp = immutableBuckets.stream()
                            .mapToLong(bucket -> bucket.firstScheduleTimestamps.get(bucket.currentSegmentEntryId + 1))
                            .min().getAsLong();
                    if (scheduleTimestamp < minScheduleTimestamp) {
                        minScheduleTimestamp = scheduleTimestamp;
                        minIndex = i;
                    }
                }
            }
        }

        if (minIndex >= 0) {
            return values.subList(minIndex, minIndex + mergeNum);
        } else if (mergeNum > 2){
            return selectMergedBuckets(values, mergeNum - 1);
        } else {
            return Collections.emptyList();
        }
    }

    private synchronized CompletableFuture<Void> asyncMergeBucketSnapshot() {
        List<ImmutableBucket> immutableBucketList = immutableBuckets.asMapOfRanges().values().stream().toList();
        List<ImmutableBucket> toBeMergeImmutableBuckets = selectMergedBuckets(immutableBucketList, MAX_MERGE_NUM);

        if (toBeMergeImmutableBuckets.isEmpty()) {
            log.warn("[{}] Can't find able merged buckets", dispatcher.getName());
            return CompletableFuture.completedFuture(null);
        }

        final String bucketsStr = toBeMergeImmutableBuckets.stream().map(Bucket::bucketKey).collect(
                Collectors.joining(",")).replaceAll(DELAYED_BUCKET_KEY_PREFIX + "_", "");
        if (log.isDebugEnabled()) {
            log.info("[{}] Merging bucket snapshot, bucketKeys: {}", dispatcher.getName(), bucketsStr);
        }

        for (ImmutableBucket immutableBucket : toBeMergeImmutableBuckets) {
            immutableBucket.merging = true;
        }

        long mergeStartTime = System.currentTimeMillis();
        stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.merge);
        return asyncMergeBucketSnapshot(toBeMergeImmutableBuckets).whenComplete((__, ex) -> {
            synchronized (this) {
                for (ImmutableBucket immutableBucket : toBeMergeImmutableBuckets) {
                    immutableBucket.merging = false;
                }
            }
            if (ex != null) {
                log.error("[{}] Failed to merge bucket snapshot, bucketKeys: {}",
                        dispatcher.getName(), bucketsStr, ex);

                stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.merge);
            } else {
                log.info("[{}] Merge bucket snapshot finish, bucketKeys: {}, bucketNum: {}",
                        dispatcher.getName(), bucketsStr, immutableBuckets.asMapOfRanges().size());

                stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.merge,
                        System.currentTimeMillis() - mergeStartTime);
            }
        });
    }

    private synchronized CompletableFuture<Void> asyncMergeBucketSnapshot(List<ImmutableBucket> buckets) {
        List<CompletableFuture<Long>> createFutures =
                buckets.stream().map(bucket -> bucket.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE))
                        .toList();

        return FutureUtil.waitForAll(createFutures).thenCompose(bucketId -> {
            if (createFutures.stream().anyMatch(future -> INVALID_BUCKET_ID.equals(future.join()))) {
                return FutureUtil.failedFuture(new RuntimeException("Can't merge buckets due to bucket create failed"));
            }

            List<CompletableFuture<List<SnapshotSegment>>> getRemainFutures =
                    buckets.stream().map(ImmutableBucket::getRemainSnapshotSegment).toList();

            return FutureUtil.waitForAll(getRemainFutures)
                    .thenApply(__ -> {
                        return CombinedSegmentDelayedIndexQueue.wrap(
                                getRemainFutures.stream().map(CompletableFuture::join).toList());
                    })
                    .thenAccept(combinedDelayedIndexQueue -> {
                        synchronized (BucketDelayedDeliveryTracker.this) {
                            long createStartTime = System.currentTimeMillis();
                            stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.create);
                            Pair<ImmutableBucket, DelayedIndex> immutableBucketDelayedIndexPair =
                                    lastMutableBucket.createImmutableBucketAndAsyncPersistent(
                                            timeStepPerBucketSnapshotSegmentInMillis,
                                            maxIndexesPerBucketSnapshotSegment,
                                            sharedBucketPriorityQueue, combinedDelayedIndexQueue,
                                            buckets.get(0).startLedgerId,
                                            buckets.get(buckets.size() - 1).endLedgerId);

                            // Merge bit map to new bucket
                            Map<Long, RoaringBitmap> delayedIndexBitMap =
                                    new HashMap<>(buckets.get(0).getDelayedIndexBitMap());
                            for (int i = 1; i < buckets.size(); i++) {
                                buckets.get(i).delayedIndexBitMap.forEach((ledgerId, bitMapB) -> {
                                    delayedIndexBitMap.compute(ledgerId, (k, bitMap) -> {
                                        if (bitMap == null) {
                                            return bitMapB;
                                        }

                                        bitMap.or(bitMapB);
                                        return bitMap;
                                    });
                                });
                            }

                            // optimize bm
                            delayedIndexBitMap.values().forEach(RoaringBitmap::runOptimize);
                            immutableBucketDelayedIndexPair.getLeft().setDelayedIndexBitMap(delayedIndexBitMap);

                            afterCreateImmutableBucket(immutableBucketDelayedIndexPair, createStartTime);

                            immutableBucketDelayedIndexPair.getLeft().getSnapshotCreateFuture()
                                    .orElse(NULL_LONG_PROMISE).thenCompose(___ -> {
                                        List<CompletableFuture<Void>> removeFutures =
                                                buckets.stream().map(bucket -> bucket.asyncDeleteBucketSnapshot(stats))
                                                        .toList();
                                        return FutureUtil.waitForAll(removeFutures);
                                    });

                            for (ImmutableBucket bucket : buckets) {
                                immutableBuckets.asMapOfRanges()
                                        .remove(Range.closed(bucket.startLedgerId, bucket.endLedgerId));
                            }
                        }
                    });
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
        // Use optimistic read for frequently called method
        long stamp = stampedLock.tryOptimisticRead();
        long result = nextDeliveryTimeUnsafe();


        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                result = nextDeliveryTimeUnsafe();
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }

    private long nextDeliveryTimeUnsafe() {
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
    public long getNumberOfDelayedMessages() {
        return numberDelayedMessages.get();
    }

    @Override
    public long getBufferMemoryUsage() {
        return this.lastMutableBucket.getBufferMemoryUsage() + sharedBucketPriorityQueue.bytesCapacity();
    }

    @Override
    public synchronized NavigableSet<Position> getScheduledMessages(int maxMessages) {
        if (!checkPendingLoadDone()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Skip getScheduledMessages to wait for bucket snapshot load finish.",
                        dispatcher.getName());
            }
            return Collections.emptyNavigableSet();
        }

        long cutoffTime = getCutoffTime();

        lastMutableBucket.moveScheduledMessageToSharedQueue(cutoffTime, sharedBucketPriorityQueue);

        NavigableSet<Position> positions = new TreeSet<>();
        int n = maxMessages;

        while (n > 0 && !sharedBucketPriorityQueue.isEmpty()) {
            long timestamp = sharedBucketPriorityQueue.peekN1();
            if (timestamp > cutoffTime) {
                break;
            }

            long ledgerId = sharedBucketPriorityQueue.peekN2();
            long entryId = sharedBucketPriorityQueue.peekN3();

            SnapshotKey snapshotKey = new SnapshotKey(ledgerId, entryId);

            ImmutableBucket bucket = snapshotSegmentLastIndexMap.get(snapshotKey);
            if (bucket != null && immutableBuckets.asMapOfRanges().containsValue(bucket)) {
                // All message of current snapshot segment are scheduled, try load next snapshot segment
                if (bucket.merging) {
                    log.info("[{}] Skip load to wait for bucket snapshot merge finish, bucketKey:{}",
                            dispatcher.getName(), bucket.bucketKey());
                    break;
                }

                final int preSegmentEntryId = bucket.currentSegmentEntryId;
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Loading next bucket snapshot segment, bucketKey: {}, nextSegmentEntryId: {}",
                            dispatcher.getName(), bucket.bucketKey(), preSegmentEntryId + 1);
                }
                boolean createFutureDone = bucket.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE).isDone();
                if (!createFutureDone) {
                    log.info("[{}] Skip load to wait for bucket snapshot create finish, bucketKey:{}",
                            dispatcher.getName(), bucket.bucketKey());
                    break;
                }

                long loadStartTime = System.currentTimeMillis();
                stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.load);
                CompletableFuture<Void> loadFuture = pendingLoad = bucket.asyncLoadNextBucketSnapshotEntry()
                        .thenAccept(indexList -> {
                    synchronized (BucketDelayedDeliveryTracker.this) {
                        this.snapshotSegmentLastIndexMap.remove(snapshotKey);
                        if (CollectionUtils.isEmpty(indexList)) {
                            immutableBuckets.asMapOfRanges()
                                    .remove(Range.closed(bucket.startLedgerId, bucket.endLedgerId));
                            bucket.asyncDeleteBucketSnapshot(stats);
                            return;
                        }
                        DelayedIndex
                                lastDelayedIndex = indexList.get(indexList.size() - 1);
                        this.snapshotSegmentLastIndexMap.put(
                                new SnapshotKey(lastDelayedIndex.getLedgerId(), lastDelayedIndex.getEntryId()),
                                bucket);
                        for (DelayedIndex index : indexList) {
                            sharedBucketPriorityQueue.add(index.getTimestamp(), index.getLedgerId(),
                                    index.getEntryId());
                        }
                    }
                }).whenComplete((__, ex) -> {
                    if (ex != null) {
                        // Back bucket state
                        bucket.setCurrentSegmentEntryId(preSegmentEntryId);

                        log.error("[{}] Failed to load bucket snapshot segment, bucketKey: {}, segmentEntryId: {}",
                                dispatcher.getName(), bucket.bucketKey(), preSegmentEntryId + 1, ex);

                        stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.load);
                    } else {
                        log.info("[{}] Load next bucket snapshot segment finish, bucketKey: {}, segmentEntryId: {}",
                                dispatcher.getName(), bucket.bucketKey(),
                                (preSegmentEntryId == bucket.lastSegmentEntryId) ? "-1" : preSegmentEntryId + 1);

                        stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.load,
                                System.currentTimeMillis() - loadStartTime);
                    }
                    synchronized (this) {
                        if (timeout != null) {
                            timeout.cancel();
                        }
                        timeout = timer.newTimeout(this, 0, TimeUnit.MILLISECONDS);
                    }
                });

                if (!checkPendingLoadDone() || loadFuture.isCompletedExceptionally()) {
                    break;
                }
            }

            positions.add(PositionFactory.create(ledgerId, entryId));

            sharedBucketPriorityQueue.pop();
            removeIndexBit(ledgerId, entryId);

            --n;
            numberDelayedMessages.decrementAndGet();
        }

        updateTimer();

        return positions;
    }

    private synchronized boolean checkPendingLoadDone() {
        if (pendingLoad == null || pendingLoad.isDone()) {
            pendingLoad = null;
            return true;
        }
        return false;
    }

    @Override
    public boolean shouldPauseAllDeliveries() {
        return false;
    }

    @Override
    public synchronized CompletableFuture<Void> clear() {
        CompletableFuture<Void> future = cleanImmutableBuckets();
        sharedBucketPriorityQueue.clear();
        lastMutableBucket.clear();
        snapshotSegmentLastIndexMap.clear();
        numberDelayedMessages.set(0);
        return future;
    }

    @Override
    public synchronized void close() {
        super.close();
        lastMutableBucket.close();
        sharedBucketPriorityQueue.close();
        try {
            List<CompletableFuture<Long>> completableFutures = immutableBuckets.asMapOfRanges().values().stream()
                    .map(bucket -> bucket.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE)).toList();
            FutureUtil.waitForAll(completableFutures).get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("[{}] Failed wait to snapshot generate", dispatcher.getName(), e);
        }
    }

    private CompletableFuture<Void> cleanImmutableBuckets() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Iterator<ImmutableBucket> iterator = immutableBuckets.asMapOfRanges().values().iterator();
        while (iterator.hasNext()) {
            ImmutableBucket bucket = iterator.next();
            futures.add(bucket.clear(stats));
            numberDelayedMessages.addAndGet(-bucket.getNumberBucketDelayedMessages());
            iterator.remove();
        }
        return FutureUtil.waitForAll(futures);
    }

    private boolean removeIndexBit(long ledgerId, long entryId) {
        if (lastMutableBucket.removeIndexBit(ledgerId, entryId)) {
            return true;
        }

        return findImmutableBucket(ledgerId).map(bucket -> bucket.removeIndexBit(ledgerId, entryId))
                .orElse(false);
    }

    public boolean containsMessage(long ledgerId, long entryId) {
        // Try optimistic read first for best performance
        long stamp = stampedLock.tryOptimisticRead();
        boolean result = containsMessageUnsafe(ledgerId, entryId);


        if (!stampedLock.validate(stamp)) {
            // Fall back to read lock if validation fails
            stamp = stampedLock.readLock();
            try {
                result = containsMessageUnsafe(ledgerId, entryId);
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }

    private boolean containsMessageUnsafe(long ledgerId, long entryId) {
        if (lastMutableBucket.containsMessage(ledgerId, entryId)) {
            return true;
        }

        return findImmutableBucket(ledgerId).map(bucket -> bucket.containsMessage(ledgerId, entryId))
                .orElse(false);
    }


    public Map<String, TopicMetricBean> genTopicMetricMap() {
        stats.recordNumOfBuckets(immutableBuckets.asMapOfRanges().size() + 1);
        stats.recordDelayedMessageIndexLoaded(this.sharedBucketPriorityQueue.size() + this.lastMutableBucket.size());
        MutableLong totalSnapshotLength = new MutableLong();
        immutableBuckets.asMapOfRanges().values().forEach(immutableBucket -> {
            totalSnapshotLength.add(immutableBucket.getSnapshotLength());
        });
        stats.recordBucketSnapshotSizeBytes(totalSnapshotLength.longValue());
        return stats.genTopicMetricMap();
    }
}
