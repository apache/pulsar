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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Table;
import com.google.common.collect.TreeRangeMap;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.time.Clock;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.AbstractDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat;
import org.apache.pulsar.broker.delayed.proto.DelayedMessageIndexBucketSnapshotFormat.DelayedIndex;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
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
        this.numberDelayedMessages = 0L;
        ManagedCursor cursor = dispatcher.getCursor();
        this.lastMutableBucket = new MutableBucket(cursor, bucketSnapshotStorage);
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

    private void sealBucket() {
        Pair<ImmutableBucket, DelayedIndex> immutableBucketDelayedIndexPair =
                lastMutableBucket.sealBucketAndAsyncPersistent(this.timeStepPerBucketSnapshotSegment,
                        this.sharedBucketPriorityQueue);
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
            sealBucket();
            lastMutableBucket.resetLastMutableBucketRange();

            if (immutableBuckets.asMapOfRanges().size() > maxNumBuckets) {
                // TODO merge bucket snapshot (synchronize operate)
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
            if (bucket != null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Load next snapshot segment, bucket: {}", dispatcher.getName(), bucket);
                }
                // All message of current snapshot segment are scheduled, load next snapshot segment
                // TODO make it asynchronous and not blocking this process
                try {
                    bucket.asyncLoadNextBucketSnapshotEntry(false).thenAccept(indexList -> {
                        if (CollectionUtils.isEmpty(indexList)) {
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
