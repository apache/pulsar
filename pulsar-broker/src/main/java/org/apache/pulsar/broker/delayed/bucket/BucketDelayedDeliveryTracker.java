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
import static org.apache.pulsar.broker.delayed.bucket.ImmutableBucket.DELIMITER;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.github.merlimat.slog.Logger;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.AbstractDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.DelayedDeliveryContext;
import org.apache.pulsar.broker.delayed.DispatcherDelayedDeliveryContext;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.LongBitmap;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;

@ThreadSafe
public class BucketDelayedDeliveryTracker extends AbstractDelayedDeliveryTracker {

    private static final Logger LOG = Logger.get(BucketDelayedDeliveryTracker.class);
    protected final Logger log;

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

    @Getter
    @VisibleForTesting
    private final BucketContext ctx;

    @Getter
    @VisibleForTesting
    private final MutableBucket lastMutableBucket;

    @Getter
    @VisibleForTesting
    private final TripleLongPriorityQueue sharedBucketPriorityQueue;

    @Getter
    @VisibleForTesting
    private final RangeMap<Long, ImmutableBucket> immutableBuckets;

    @Getter
    @VisibleForTesting
    private final BucketDelayedMessageIndex index = new BucketDelayedMessageIndex();

    @Getter
    @VisibleForTesting
    private final AtomicLong bucketsCount = new AtomicLong(0);

    @Getter
    @VisibleForTesting
    private final AtomicLong totalSnapshotLengthBytes = new AtomicLong(0);

    private final ConcurrentHashMap<SnapshotKey, ImmutableBucket> snapshotSegmentLastIndexMap;

    private final BucketDelayedMessageIndexStats stats;

    private CompletableFuture<Void> pendingLoad = null;

    private volatile CompletableFuture<Void> trimFuture;

    public BucketDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher,
                                        Timer timer, long tickTimeMillis,
                                        boolean isDelayedDeliveryDeliverAtTimeStrict,
                                        BucketSnapshotStorage bucketSnapshotStorage,
                                        long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                        int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets)
            throws RecoverDelayedDeliveryTrackerException {
        this(new DispatcherDelayedDeliveryContext(dispatcher), timer, tickTimeMillis, Clock.systemUTC(),
                isDelayedDeliveryDeliverAtTimeStrict, bucketSnapshotStorage, minIndexCountPerBucket,
                timeStepPerBucketSnapshotSegmentInMillis, maxIndexesPerBucketSnapshotSegment, maxNumBuckets);
    }

    public BucketDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher,
                                        Timer timer, long tickTimeMillis, Clock clock,
                                        boolean isDelayedDeliveryDeliverAtTimeStrict,
                                        BucketSnapshotStorage bucketSnapshotStorage,
                                        long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                        int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets)
            throws RecoverDelayedDeliveryTrackerException {
        this(new DispatcherDelayedDeliveryContext(dispatcher), timer, tickTimeMillis, clock,
                isDelayedDeliveryDeliverAtTimeStrict, bucketSnapshotStorage, minIndexCountPerBucket,
                timeStepPerBucketSnapshotSegmentInMillis, maxIndexesPerBucketSnapshotSegment, maxNumBuckets);
    }

    @VisibleForTesting
    public BucketDelayedDeliveryTracker(DelayedDeliveryContext context,
                                        Timer timer, long tickTimeMillis, Clock clock,
                                        boolean isDelayedDeliveryDeliverAtTimeStrict,
                                        BucketSnapshotStorage bucketSnapshotStorage,
                                        long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegmentInMillis,
                                        int maxIndexesPerBucketSnapshotSegment, int maxNumBuckets)
            throws RecoverDelayedDeliveryTrackerException {
        super(context, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict);
        this.log = LOG.with().ctx(super.log).build();
        this.minIndexCountPerBucket = minIndexCountPerBucket;
        this.timeStepPerBucketSnapshotSegmentInMillis = timeStepPerBucketSnapshotSegmentInMillis;
        this.maxIndexesPerBucketSnapshotSegment = maxIndexesPerBucketSnapshotSegment;
        this.maxNumBuckets = maxNumBuckets;
        this.sharedBucketPriorityQueue = new TripleLongPriorityQueue();
        this.immutableBuckets = TreeRangeMap.create();
        this.snapshotSegmentLastIndexMap = new ConcurrentHashMap<>();
        this.ctx = new BucketContext(context.getName(), context.getCursor(), FutureUtil.Sequencer.create(),
                bucketSnapshotStorage);
        this.lastMutableBucket = new MutableBucket(ctx);
        this.stats = new BucketDelayedMessageIndexStats();

        // Close the tracker if failed to recover.
        try {
            recoverBucketSnapshot();
        } catch (RecoverDelayedDeliveryTrackerException e) {
            close();
            throw e;
        }
    }

    private synchronized long recoverBucketSnapshot() throws RecoverDelayedDeliveryTrackerException {
        ManagedCursor cursor = ctx.cursor();
        Map<String, String> cursorProperties = cursor.getCursorProperties();
        if (MapUtils.isEmpty(cursorProperties)) {
            log.info("Recover delayed message index bucket snapshot finish, don't find bucket snapshot");
            return 0;
        }
        Map<Range<Long>, ImmutableBucket> toBeDeletedBucketMap = new HashMap<>();
        cursorProperties.keySet().forEach(key -> {
            if (key.startsWith(DELAYED_BUCKET_KEY_PREFIX)) {
                String[] keys = key.split(DELIMITER);
                checkArgument(keys.length == 3);
                ImmutableBucket immutableBucket =
                        new ImmutableBucket(ctx, Long.parseLong(keys[1]), Long.parseLong(keys[2]));
                putAndCleanOverlapRange(Range.closed(immutableBucket.getStartLedgerId(),
                        immutableBucket.getEndLedgerId()), immutableBucket, toBeDeletedBucketMap);
            }
        });

        Map<Range<Long>, ImmutableBucket> immutableBucketMap = immutableBuckets.asMapOfRanges();
        if (immutableBucketMap.isEmpty()) {
            log.info("Recover delayed message index bucket snapshot finish, don't find bucket snapshot");
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
            log.error()
                    .exception(e)
                    .log("Failed to recover delayed message index bucket snapshot.");
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
            removeBucket(key);
            // delete asynchronously without waiting for completion
            immutableBucket.asyncDeleteBucketSnapshot(stats);
        }

        long totalLength = 0;
        for (ImmutableBucket bucket : immutableBucketMap.values()) {
            index.restore(bucket.getDelayedIndexBitMap());
            totalLength += bucket.getSnapshotLength();
        }
        totalSnapshotLengthBytes.set(totalLength);
        bucketsCount.set(immutableBuckets.asMapOfRanges().size());

        log.info()
                .attr("buckets", immutableBucketMap.size())
                .attr("numberDelayedMessages", index.size())
                .log("Recover delayed message index bucket snapshot finish");

        return index.size();
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
        Map<Range<Long>, ImmutableBucket> subRangeMap = immutableBuckets.subRangeMap(range).asMapOfRanges();
        boolean canPut = false;
        if (!subRangeMap.isEmpty()) {
            for (Map.Entry<Range<Long>, ImmutableBucket> rangeEntry : subRangeMap.entrySet()) {
                // Use original key instead of truncated key for encloses check
                ImmutableBucket bucket = rangeEntry.getValue();
                Range<Long> originalKey = Range.closed(bucket.getStartLedgerId(), bucket.getEndLedgerId());

                if (range.encloses(originalKey)) {
                    toBeDeletedBucketMap.put(originalKey, bucket);
                    canPut = true;
                }
            }
        } else {
            canPut = true;
        }

        if (canPut) {
            putBucket(range, immutableBucket);
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

    private ImmutableBucket findImmutableBucket(long ledgerId) {
        return immutableBuckets.get(ledgerId);
    }

    private void afterCreateImmutableBucket(Pair<ImmutableBucket, DelayedIndex> immutableBucketDelayedIndexPair,
                                            long startTime) {
        if (immutableBucketDelayedIndexPair != null) {
            ImmutableBucket immutableBucket = immutableBucketDelayedIndexPair.getLeft();
            putBucket(Range.closed(immutableBucket.getStartLedgerId(), immutableBucket.getEndLedgerId()),
                    immutableBucket);

            DelayedIndex lastDelayedIndex = immutableBucketDelayedIndexPair.getRight();
            snapshotSegmentLastIndexMap.put(
                    new SnapshotKey(lastDelayedIndex.getLedgerId(), lastDelayedIndex.getEntryId()),
                    immutableBucket);

            immutableBucket.getSnapshotCreateFuture().ifPresent(createFuture -> {
                CompletableFuture<Long> future = createFuture.handle((bucketId, ex) -> {
                    if (ex == null) {
                        immutableBucket.setSnapshotSegments(null);
                        immutableBucket.asyncUpdateSnapshotLength()
                                .thenAccept(newLength -> {
                                    synchronized (BucketDelayedDeliveryTracker.this) {
                                        updateBucketSnapshotLength(immutableBucket, newLength);
                                    }
                                });
                        log.info()
                                .attr("bucketKey", immutableBucket.bucketKey())
                                .log("Create bucket snapshot finish, bucketKey");

                        stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.create,
                                System.currentTimeMillis() - startTime);

                        return bucketId;
                    }

                    log.error()
                            .attr("bucketKey", immutableBucket.bucketKey())
                            .exception(ex)
                            .log("Failed to create bucket snapshot");
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

                        immutableBucket.setCurrentSegmentEntryId(immutableBucket.getLastSegmentEntryId());
                        removeBucket(
                                Range.closed(immutableBucket.getStartLedgerId(), immutableBucket.getEndLedgerId()));
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
        if (deliverAt < 0 || deliverAt <= getCutoffTime()) {
            removeIndexBit(ledgerId, entryId);
            return false;
        }

        if (containsMessage(ledgerId, entryId)) {
            return true;
        }

        boolean existBucket = findImmutableBucket(ledgerId) != null;

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

            if (maxNumBuckets > 0 && bucketsCount.get() > maxNumBuckets
                    && (trimFuture == null || trimFuture.isDone())) {
                trimFuture = asyncTrimImmutableBuckets()
                        .thenCompose(ignore -> asyncMergeBucketSnapshot())
                        .whenComplete((ignore, t) -> {
                            if (t != null) {
                                log.warn().exception(t).log("Failed to trim or merge bucket snapshots");
                            }
                        });
            }
        }

        if (ledgerId >= lastMutableBucket.endLedgerId && !existBucket) {
            lastMutableBucket.addMessage(ledgerId, entryId, deliverAt);
        } else {
            // Message index belongs to previous bucket range or the current mutable bucket range,
            // enter sharedBucketPriorityQueue directly
            sharedBucketPriorityQueue.add(deliverAt, ledgerId, entryId);
        }
        index.track(ledgerId, entryId);
            log.debug()
                    .attr("ledgerId", ledgerId)
                    .attr("entryId", entryId)
                    .attr("deliveryInMs", deliverAt - clock.millis())
                    .log("Add message");
                updateTimer();

        return true;
    }

    @VisibleForTesting
    synchronized List<ImmutableBucket> selectMergedBuckets(final List<ImmutableBucket> values, int mergeNum) {
        if (values.size() < 2 || mergeNum < 2) {
            return Collections.emptyList();
        }
        int actualMergeNum = Math.min(mergeNum, values.size());
        long minNumberMessages = Long.MAX_VALUE;
        long minScheduleTimestamp = Long.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i + (actualMergeNum - 1) < values.size(); i++) {
            List<ImmutableBucket> immutableBuckets = values.subList(i, i + actualMergeNum);
            if (immutableBuckets.stream().allMatch(bucket -> {
                // We should skip the bucket which last segment already been load to memory,
                // avoid record replicated index.
                return bucket.getLastSegmentEntryId() > bucket.getCurrentSegmentEntryId() && !bucket.merging;
            })) {
                long numberMessages = immutableBuckets.stream()
                        .mapToLong(bucket -> bucket.getNumberBucketDelayedMessages())
                        .sum();
                if (numberMessages <= minNumberMessages) {
                    minNumberMessages = numberMessages;
                    // Snapshot segment IDs start at 1 while the timestamp list is zero-based.
                    // The next unloaded segment is currentSegmentEntryId + 1, at list index
                    // currentSegmentEntryId.
                    long scheduleTimestamp = immutableBuckets.stream()
                            .mapToLong(bucket -> bucket.getFirstScheduleTimestamps()
                                       .get(bucket.getCurrentSegmentEntryId()))
                            .min().getAsLong();
                    if (scheduleTimestamp < minScheduleTimestamp) {
                        minScheduleTimestamp = scheduleTimestamp;
                        minIndex = i;
                    }
                }
            }
        }

        if (minIndex >= 0) {
            return values.subList(minIndex, minIndex + actualMergeNum);
        } else if (actualMergeNum > 2) {
            return selectMergedBuckets(values, actualMergeNum - 1);
        } else {
            return Collections.emptyList();
        }
    }

    private synchronized CompletableFuture<Void> asyncMergeBucketSnapshot() {
        if (maxNumBuckets <= 0 || bucketsCount.get() <= maxNumBuckets) {
            return CompletableFuture.completedFuture(null);
        }
        List<ImmutableBucket> immutableBucketList = immutableBuckets.asMapOfRanges().values().stream().toList();
        List<ImmutableBucket> toBeMergeImmutableBuckets = selectMergedBuckets(immutableBucketList, MAX_MERGE_NUM);

        if (toBeMergeImmutableBuckets.isEmpty()) {
            log.warn("Can't find able merged buckets");
            return CompletableFuture.completedFuture(null);
        }

        final String bucketsStr = toBeMergeImmutableBuckets.stream().map(ImmutableBucket::bucketKey).collect(
                Collectors.joining(",")).replaceAll(DELAYED_BUCKET_KEY_PREFIX + "_", "");
            log.info()
                    .attr("bucketKeys", bucketsStr)
                    .log("Merging bucket snapshot, bucketKeys");
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
                log.error()
                        .attr("bucketKeys", bucketsStr)
                        .exception(ex)
                        .log("Failed to merge bucket snapshot, bucketKeys");

                stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.merge);
            } else {
                log.info()
                        .attr("bucketKeys", bucketsStr)
                        .attr("bucketNum", bucketsCount.get())
                        .log("Merge bucket snapshot finish");

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

            List<CompletableFuture<List<SnapshotSegment>>> getAllSnapshotFutures =
                    buckets.stream().map(ImmutableBucket::getAllSnapshotSegments).toList();

            return FutureUtil.waitForAll(getAllSnapshotFutures)
                    .thenApply(__ -> {
                        return CombinedSegmentDelayedIndexQueue.wrap(
                                getAllSnapshotFutures.stream().map(CompletableFuture::join).toList());
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
                                            buckets.get(0).getStartLedgerId(),
                                            buckets.get(buckets.size() - 1).getEndLedgerId());

                            // Merge bit map to new bucket
                            Long2ObjectMap<LongBitmap> delayedIndexBitMap =
                                    new Long2ObjectOpenHashMap<>(buckets.get(0).getDelayedIndexBitMap());
                            for (int i = 1; i < buckets.size(); i++) {
                                buckets.get(i).getDelayedIndexBitMap().forEach((ledgerId, bitMapB) -> {
                                    delayedIndexBitMap.compute(ledgerId, (k, bitMap) -> {
                                        if (bitMap == null) {
                                            return bitMapB;
                                        }

                                        bitMap.or(bitMapB);
                                        return bitMap;
                                    });
                                });
                            }

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
                                removeBucket(Range.closed(bucket.getStartLedgerId(), bucket.getEndLedgerId()));
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
    protected synchronized long nextDeliveryTime() {
        if (lastMutableBucket.isEmpty() && !sharedBucketPriorityQueue.isEmpty()) {
            return sharedBucketPriorityQueue.peekN1();
        } else if (sharedBucketPriorityQueue.isEmpty() && !lastMutableBucket.isEmpty()) {
            return lastMutableBucket.nextDeliveryTime();
        } else if (lastMutableBucket.isEmpty() && sharedBucketPriorityQueue.isEmpty()) {
            // numberDelayedMessages can be > 0 while both queues are empty (e.g. remaining
            // messages live in not-yet-loaded snapshot segments). Returning Long.MAX_VALUE
            // signals "no imminent delivery" without throwing on the empty queues.
            return Long.MAX_VALUE;
        }
        long timestamp = lastMutableBucket.nextDeliveryTime();
        long bucketTimestamp = sharedBucketPriorityQueue.peekN1();
        return Math.min(timestamp, bucketTimestamp);
    }

    @Override
    public long getNumberOfDelayedMessages() {
        return index.size();
    }

    @Override
    public long getBufferMemoryUsage() {
        return this.lastMutableBucket.getBufferMemoryUsage() + sharedBucketPriorityQueue.bytesCapacity();
    }

    @Override
    public synchronized NavigableSet<Position> getScheduledMessages(int maxMessages) {
        if (!checkPendingLoadDone()) {
            log.debug("Skip getScheduledMessages to wait for bucket snapshot load finish");
            return Collections.emptyNavigableSet();
        }

        long cutoffTime = getCutoffTime();
        Long firstLiveLedgerId = firstActiveLedgerId();

        lastMutableBucket.moveScheduledMessageToSharedQueue(cutoffTime, sharedBucketPriorityQueue);

        NavigableSet<Position> positions = new TreeSet<>();
        int n = maxMessages;

        while (n > 0 && !sharedBucketPriorityQueue.isEmpty()) {
            long timestamp = sharedBucketPriorityQueue.peekN1();
            long ledgerId = sharedBucketPriorityQueue.peekN2();
            long entryId = sharedBucketPriorityQueue.peekN3();
            if (firstLiveLedgerId != null && ledgerId < firstLiveLedgerId) {
                sharedBucketPriorityQueue.pop();
                removeIndexBit(ledgerId, entryId);
                continue;
            }
            if (timestamp > cutoffTime) {
                break;
            }

            SnapshotKey snapshotKey = new SnapshotKey(ledgerId, entryId);

            ImmutableBucket bucket = snapshotSegmentLastIndexMap.get(snapshotKey);
            if (bucket != null && immutableBuckets.asMapOfRanges().containsValue(bucket)) {
                // All message of current snapshot segment are scheduled, try load next snapshot segment
                if (bucket.merging) {
                    log.info()
                            .attr("bucketKey", bucket.bucketKey())
                            .log("Skip load to wait for bucket snapshot merge finish");
                    break;
                }

                final int preSegmentEntryId = bucket.getCurrentSegmentEntryId();
                    log.debug()
                            .attr("bucketKey", bucket.bucketKey())
                            .attr("nextSegmentEntryId", preSegmentEntryId + 1)
                            .log("Loading next bucket snapshot segment");
                                boolean createFutureDone = bucket.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE)
                                        .isDone();
                if (!createFutureDone) {
                    log.info()
                            .attr("bucketKey", bucket.bucketKey())
                            .log("Skip load to wait for bucket snapshot create finish");
                    break;
                }

                long loadStartTime = System.currentTimeMillis();
                stats.recordTriggerEvent(BucketDelayedMessageIndexStats.Type.load);
                CompletableFuture<Void> loadFuture = pendingLoad = bucket.asyncLoadNextBucketSnapshotEntry()
                        .thenAccept(indexList -> {
                    synchronized (BucketDelayedDeliveryTracker.this) {
                        this.snapshotSegmentLastIndexMap.remove(snapshotKey);
                        if (CollectionUtils.isEmpty(indexList)) {
                            removeBucket(Range.closed(bucket.getStartLedgerId(), bucket.getEndLedgerId()));
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

                        log.error()
                                .attr("bucketKey", bucket.bucketKey())
                                .attr("segmentEntryId", preSegmentEntryId + 1)
                                .exception(ex)
                                .log("Failed to load bucket snapshot segment");

                        stats.recordFailEvent(BucketDelayedMessageIndexStats.Type.load);
                    } else {
                        log.info()
                                .attr("bucketKey", bucket.bucketKey())
                                .attr("segmentEntryId",
                                        (preSegmentEntryId == bucket.getLastSegmentEntryId()) ? "-1" : preSegmentEntryId
                                        + 1)
                                .log("Load next bucket snapshot segment finish");

                        stats.recordSuccessEvent(BucketDelayedMessageIndexStats.Type.load,
                                System.currentTimeMillis() - loadStartTime);
                    }
                    rescheduleTimer(0);
                });

                if (!checkPendingLoadDone() || loadFuture.isCompletedExceptionally()) {
                    break;
                }
            }

            sharedBucketPriorityQueue.pop();
            // Dedup: queue may carry the same position twice (initial seal + merge); only the
            // first delivery of each position decrements the counter via removeIndexBit.
            if (removeIndexBit(ledgerId, entryId)) {
                positions.add(PositionFactory.create(ledgerId, entryId));
                --n;
            }
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
        // Wait for any in-flight trim+merge to settle, then clear.
        // Reuse trimFuture to block new triggers until the clear chain completes.
        CompletableFuture<Void> before = trimFuture != null && !trimFuture.isDone()
                ? trimFuture : CompletableFuture.completedFuture(null);
        trimFuture = before
                .exceptionally(t -> {
                    log.warn().exception(t).log("Trim/merge buckets failed, but still clear");
                    return null;
                })
                .thenCompose(__ -> {
                    synchronized (BucketDelayedDeliveryTracker.this) {
                        CompletableFuture<Void> future = cleanImmutableBuckets();
                        sharedBucketPriorityQueue.clear();
                        index.clear();
                        lastMutableBucket.clear();
                        snapshotSegmentLastIndexMap.clear();
                        return future;
                    }
                });
        return trimFuture;
    }

    @Override
    public void close() {
        // Block for AutoCloseable / synchronous callers; asynchronous callers should use closeAsync().
        closeAsync().join();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        List<CompletableFuture<Long>> completableFutures;
        synchronized (this) {
            super.close();
            lastMutableBucket.close();
            sharedBucketPriorityQueue.close();
            completableFutures = immutableBuckets.asMapOfRanges().values().stream()
                    .map(bucket -> bucket.getSnapshotCreateFuture().orElse(NULL_LONG_PROMISE)).toList();
        }
        return FutureUtil.waitForAll(completableFutures)
                .exceptionally(e -> {
                    log.warn().exception(e).log("Failed wait to snapshot generate");
                    return null;
                });
    }

    private CompletableFuture<Void> cleanImmutableBuckets() {
        Map<Range<Long>, ImmutableBucket> bucketsToDelete =
                new HashMap<>(immutableBuckets.asMapOfRanges());

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        bucketsToDelete.forEach((range, bucket) -> {
            removeBucket(range);
            futures.add(bucket.clear(stats));
        });

        return FutureUtil.waitForAll(futures);
    }

    private boolean removeIndexBit(long ledgerId, long entryId) {
        return index.untrack(ledgerId, entryId);
    }

    public synchronized boolean containsMessage(long ledgerId, long entryId) {
        return index.contains(ledgerId, entryId);
    }

    public Map<String, TopicMetricBean> genTopicMetricMap() {
        stats.recordNumOfBuckets((int) (bucketsCount.get() + 1));
        stats.recordDelayedMessageIndexLoaded(this.sharedBucketPriorityQueue.size() + this.lastMutableBucket.size());
        stats.recordBucketSnapshotSizeBytes(totalSnapshotLengthBytes.get());
        return stats.genTopicMetricMap();
    }

    /**
     * Delete orphaned bucket snapshots whose ledger range lies entirely before the earliest
     * surviving ledger. Buckets are deleted sequentially; the chain stops on first failure
     * to avoid wasted work when storage is unavailable.
     */
    private synchronized CompletableFuture<Void> asyncTrimImmutableBuckets() {
        Long firstLedgerId = firstActiveLedgerId();
        if (null == firstLedgerId) {
            return CompletableFuture.completedFuture(null);
        }
        ManagedLedger ledger = context.getCursor().getManagedLedger();

        Map<Range<Long>, ImmutableBucket> toBeDeletedBuckets = new HashMap<>();
        // subRangeMap returns clipped intersection ranges. Snapshot deletion must use the original
        // bucket range, so only select buckets whose complete range precedes the first live ledger.
        immutableBuckets.asMapOfRanges().forEach((range, bucket) -> {
            if (range.upperEndpoint() < firstLedgerId) {
                toBeDeletedBuckets.put(range, bucket);
            }
        });

        if (toBeDeletedBuckets.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String ledgerName = ledger.getName();
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (Map.Entry<Range<Long>, ImmutableBucket> entry : toBeDeletedBuckets.entrySet()) {
            chain = chain.thenCompose(__ ->
                    deleteBucketSnapshot(ledgerName, entry.getKey(), entry.getValue()));
        }
        return chain;
    }

    private CompletableFuture<Void> deleteBucketSnapshot(String ledgerName,
                                                          Range<Long> range, ImmutableBucket bucket) {
        return bucket.asyncDeleteBucketSnapshot(stats)
                .handle((__, t) -> {
                    if (t != null) {
                        log.warn().attr("LedgerName", ledgerName)
                                .attr("BucketKey", bucket.bucketKey())
                                .log("Failed to delete bucket snapshot");
                        throw new CompletionException(t);
                    }
                    synchronized (this) {
                        snapshotSegmentLastIndexMap.entrySet().removeIf(entry -> entry.getValue() == bucket);
                        removeBucket(range);
                        bucket.getDelayedIndexBitMap().forEach((ledgerId, bitmap) ->
                                bitmap.forEachLong(entryId -> index.untrack(ledgerId, entryId)));
                    }
                    return null;
                });
    }

    private Long firstActiveLedgerId() {
        ManagedCursor cursor = context.getCursor();
        Position mdp = cursor.getMarkDeletedPosition();
        return mdp == null ? null : mdp.getLedgerId();
    }

    private void putBucket(Range<Long> range, ImmutableBucket bucket) {
        long removedLength = immutableBuckets.subRangeMap(range).asMapOfRanges().values().stream()
                .mapToLong(ImmutableBucket::getSnapshotLength)
                .sum();

        immutableBuckets.put(range, bucket);
        bucketsCount.set(immutableBuckets.asMapOfRanges().size());
        totalSnapshotLengthBytes.addAndGet(bucket.getSnapshotLength() - removedLength);
    }

    private void removeBucket(Range<Long> range) {
        // Use exact key matching - all callers should provide exact keys
        ImmutableBucket bucket = immutableBuckets.asMapOfRanges().get(range);

        if (bucket != null) {
            // Remove even if snapshot length is 0 (for newly created buckets)
            immutableBuckets.asMapOfRanges().remove(range);
            bucketsCount.set(immutableBuckets.asMapOfRanges().size());
            totalSnapshotLengthBytes.addAndGet(-bucket.getSnapshotLength());
        }
    }

    private void updateBucketSnapshotLength(ImmutableBucket bucket, long newLength) {
        if (!immutableBuckets.asMapOfRanges().containsValue(bucket)) {
            return;
        }
        long oldLength = bucket.getSnapshotLength();
        bucket.setSnapshotLength(newLength);
        totalSnapshotLengthBytes.addAndGet(newLength - oldLength);
    }
}
