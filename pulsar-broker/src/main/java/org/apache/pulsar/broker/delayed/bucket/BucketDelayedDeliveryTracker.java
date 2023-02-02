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

@Slf4j
@ThreadSafe
public class BucketDelayedDeliveryTracker extends AbstractDelayedDeliveryTracker {

    static final int AsyncOperationTimeoutSeconds = 30;

    private final long minIndexCountPerBucket;

    private final long timeStepPerBucketSnapshotSegment;

    private final int maxNumBuckets;

    private long numberDelayedMessages;

    private final MutableBucket lastMutableBucket;

    private final TripleLongPriorityQueue sharedBucketPriorityQueue;

    @Getter
    @VisibleForTesting
    private final RangeMap<Long, ImmutableBucket> immutableBuckets;

    private final Table<Long, Long, ImmutableBucket> snapshotSegmentLastIndexTable;

    public BucketDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher,
                                 Timer timer, long tickTimeMillis,
                                 boolean isDelayedDeliveryDeliverAtTimeStrict,
                                 BucketSnapshotStorage bucketSnapshotStorage,
                                 long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegment,
                                 int maxNumBuckets) {
        this(dispatcher, timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
                bucketSnapshotStorage, minIndexCountPerBucket, timeStepPerBucketSnapshotSegment, maxNumBuckets);
    }

    public BucketDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher,
                                 Timer timer, long tickTimeMillis, Clock clock,
                                 boolean isDelayedDeliveryDeliverAtTimeStrict,
                                 BucketSnapshotStorage bucketSnapshotStorage,
                                 long minIndexCountPerBucket, long timeStepPerBucketSnapshotSegment,
                                 int maxNumBuckets) {
        super(dispatcher, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict);
        this.minIndexCountPerBucket = minIndexCountPerBucket;
        this.timeStepPerBucketSnapshotSegment = timeStepPerBucketSnapshotSegment;
        this.maxNumBuckets = maxNumBuckets;
        this.sharedBucketPriorityQueue = new TripleLongPriorityQueue();
        this.immutableBuckets = TreeRangeMap.create();
        this.snapshotSegmentLastIndexTable = HashBasedTable.create();
        ManagedCursor cursor = dispatcher.getCursor();
        this.lastMutableBucket = new MutableBucket(cursor, bucketSnapshotStorage);
        this.numberDelayedMessages = recoverBucketSnapshot();
    }

    private synchronized long recoverBucketSnapshot() throws RuntimeException {
        ManagedCursor cursor = this.lastMutableBucket.cursor;
        Map<Range<Long>, ImmutableBucket> toBeDeletedBucketMap = new HashMap<>();
        cursor.getCursorProperties().keySet().forEach(key -> {
            if (key.startsWith(DELAYED_BUCKET_KEY_PREFIX)) {
                String[] keys = key.split(DELIMITER);
                checkArgument(keys.length == 3);
                ImmutableBucket immutableBucket =
                        new ImmutableBucket(cursor, this.lastMutableBucket.bucketSnapshotStorage,
                                Long.parseLong(keys[1]), Long.parseLong(keys[2]));
                putAndCleanOverlapRange(Range.closed(immutableBucket.startLedgerId, immutableBucket.endLedgerId),
                        immutableBucket, toBeDeletedBucketMap);
            }
        });

        Map<Range<Long>, ImmutableBucket> immutableBucketMap = immutableBuckets.asMapOfRanges();
        if (immutableBucketMap.isEmpty()) {
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
            if (log.isDebugEnabled()) {
                log.debug("[{}] Create bucket snapshot, bucket: {}", dispatcher.getName(),
                        lastMutableBucket);
            }
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
                    lastMutableBucket.sealBucketAndAsyncPersistent(this.timeStepPerBucketSnapshotSegment,
                            this.sharedBucketPriorityQueue);
            afterCreateImmutableBucket(immutableBucketDelayedIndexPair);
            lastMutableBucket.resetLastMutableBucketRange();

            if (immutableBuckets.asMapOfRanges().size() > maxNumBuckets) {
                try {
                    asyncMergeBucketSnapshot().get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    throw new RuntimeException(e);
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
                minIndex = i;
            }
        }
        return asyncMergeBucketSnapshot(values.get(minIndex), values.get(minIndex + 1));
    }

    private synchronized CompletableFuture<Void> asyncMergeBucketSnapshot(ImmutableBucket bucketA,
                                                                          ImmutableBucket bucketB) {
        immutableBuckets.remove(Range.closed(bucketA.startLedgerId, bucketA.endLedgerId));
        immutableBuckets.remove(Range.closed(bucketB.startLedgerId, bucketB.endLedgerId));

        CompletableFuture<Long> snapshotCreateFutureA =
                bucketA.getSnapshotCreateFuture().orElse(CompletableFuture.completedFuture(null));
        CompletableFuture<Long> snapshotCreateFutureB =
                bucketB.getSnapshotCreateFuture().orElse(CompletableFuture.completedFuture(null));

        return CompletableFuture.allOf(snapshotCreateFutureA, snapshotCreateFutureB).thenCompose(__ -> {
            CompletableFuture<List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment>> futureA =
                    bucketA.getRemainSnapshotSegment();
            CompletableFuture<List<DelayedMessageIndexBucketSnapshotFormat.SnapshotSegment>> futureB =
                    bucketB.getRemainSnapshotSegment();
            return futureA.thenCombine(futureB, CombinedSegmentDelayedIndexQueue::wrap)
                    .thenAccept(combinedDelayedIndexQueue -> {
                        Pair<ImmutableBucket, DelayedIndex> immutableBucketDelayedIndexPair =
                                lastMutableBucket.createImmutableBucketAndAsyncPersistent(
                                        timeStepPerBucketSnapshotSegment, sharedBucketPriorityQueue,
                                        combinedDelayedIndexQueue, bucketA.startLedgerId, bucketB.endLedgerId);
                        afterCreateImmutableBucket(immutableBucketDelayedIndexPair);

                        immutableBucketDelayedIndexPair.getLeft().getSnapshotCreateFuture()
                                .orElse(CompletableFuture.completedFuture(null)).thenCompose(___ -> {
                                    CompletableFuture<Void> removeAFuture = bucketA.asyncDeleteBucketSnapshot();
                                    CompletableFuture<Void> removeBFuture = bucketB.asyncDeleteBucketSnapshot();
                                    return CompletableFuture.allOf(removeAFuture, removeBFuture);
                                });
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
            positions.add(new PositionImpl(ledgerId, entryId));

            sharedBucketPriorityQueue.pop();
            removeIndexBit(ledgerId, entryId);

            ImmutableBucket bucket = snapshotSegmentLastIndexTable.remove(ledgerId, entryId);
            if (bucket != null && immutableBuckets.asMapOfRanges().containsValue(bucket)) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Load next snapshot segment, bucket: {}", dispatcher.getName(), bucket);
                }
                // All message of current snapshot segment are scheduled, load next snapshot segment
                // TODO make it asynchronous and not blocking this process
                try {
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
                    }).get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    // TODO make this segment load again
                    throw new RuntimeException(e);
                }
            }

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
