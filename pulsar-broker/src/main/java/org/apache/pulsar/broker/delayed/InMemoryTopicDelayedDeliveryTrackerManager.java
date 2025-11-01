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
package org.apache.pulsar.broker.delayed;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * In-memory implementation of topic-level delayed delivery tracker manager.
 * This manager maintains a single global delayed message index per topic that is shared by all
 * subscriptions, significantly reducing memory usage in multi-subscription scenarios.
 */
@Slf4j
public class InMemoryTopicDelayedDeliveryTrackerManager implements TopicDelayedDeliveryTrackerManager, TimerTask {

    // Global delayed message index: timestamp -> ledgerId -> entryId bitmap
    // Outer: sorted by timestamp for efficient finding of earliest bucket
    // Inner: per-ledger bitmaps of entry-ids
    private final ConcurrentSkipListMap<Long, Long2ObjectRBTreeMap<Roaring64Bitmap>> delayedMessageMap =
            new ConcurrentSkipListMap<>();

    // Subscription registry: subscription name -> subscription context
    private final ConcurrentHashMap<String, SubContext> subscriptionContexts = new ConcurrentHashMap<>();

    // Timer for delayed delivery
    private final Timer timer;
    private Timeout timeout;
    private long currentTimeoutTarget = -1;
    // Last time the TimerTask was triggered
    private long lastTickRun = 0L;

    // Configuration
    private long tickTimeMillis;
    private final boolean isDelayedDeliveryDeliverAtTimeStrict;
    private final long fixedDelayDetectionLookahead;
    private final Clock clock;

    // Statistics
    private final AtomicLong delayedMessagesCount = new AtomicLong(0);
    private final AtomicLong bufferMemoryBytes = new AtomicLong(0);

    // Prune throttling
    // Last pruning time
    private final AtomicLong lastPruneNanos = new AtomicLong(0);
    // Minimum interval between prunes
    private final long minPruneIntervalNanos;

    // Fixed-delay detection (parity with legacy behavior)
    private volatile long highestDeliveryTimeTracked = 0;
    private volatile boolean messagesHaveFixedDelay = true;

    // Timestamp precision for memory optimization
    // TODO: Due to the dynamic adjustment of tickTimeMillis, the same message Position(ledgerId, entryId)
    //  may be bucketed into different timestamp buckets under different time precisions,
    //  causing "cross-bucket duplicate indexes" and thus duplicate memory occupancy and duplicate returns.
    private volatile int timestampPrecisionBitCnt;

    // Per-bucket locks (timestamp -> lock) for fine-grained concurrency
    private final ConcurrentHashMap<Long, ReentrantLock> bucketLocks = new ConcurrentHashMap<>();

    // Timer state guard
    private final ReentrantLock timerLock = new ReentrantLock();

    /**
     * Subscription context that holds per-subscription state.
     */
    @Getter
    static class SubContext {
        private final AbstractPersistentDispatcherMultipleConsumers dispatcher;
        private final String subscriptionName;
        private volatile long tickTimeMillis;
        private final boolean isDelayedDeliveryDeliverAtTimeStrict;
        private final long fixedDelayDetectionLookahead;
        private final Clock clock;
        private volatile Position markDeletePosition;

        SubContext(AbstractPersistentDispatcherMultipleConsumers dispatcher, long tickTimeMillis,
                   boolean isDelayedDeliveryDeliverAtTimeStrict, long fixedDelayDetectionLookahead,
                   Clock clock) {
            this.dispatcher = dispatcher;
            this.subscriptionName = dispatcher.getSubscription().getName();
            this.tickTimeMillis = tickTimeMillis;
            this.isDelayedDeliveryDeliverAtTimeStrict = isDelayedDeliveryDeliverAtTimeStrict;
            this.fixedDelayDetectionLookahead = fixedDelayDetectionLookahead;
            this.clock = clock;
        }

        void updateMarkDeletePosition(Position position) {
            this.markDeletePosition = position;
        }

        long getCutoffTime() {
            long now = clock.millis();
            return isDelayedDeliveryDeliverAtTimeStrict ? now : now + tickTimeMillis;
        }
    }

    private final Runnable onEmptyCallback;

    public InMemoryTopicDelayedDeliveryTrackerManager(Timer timer, long tickTimeMillis,
                                                      boolean isDelayedDeliveryDeliverAtTimeStrict,
                                                      long fixedDelayDetectionLookahead) {
        this(timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
             fixedDelayDetectionLookahead, null);
    }

    public InMemoryTopicDelayedDeliveryTrackerManager(Timer timer, long tickTimeMillis, Clock clock,
                                                      boolean isDelayedDeliveryDeliverAtTimeStrict,
                                                      long fixedDelayDetectionLookahead) {
        this(timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict, fixedDelayDetectionLookahead, null);
    }

    public InMemoryTopicDelayedDeliveryTrackerManager(Timer timer, long tickTimeMillis, Clock clock,
                                                      boolean isDelayedDeliveryDeliverAtTimeStrict,
                                                      long fixedDelayDetectionLookahead,
                                                      Runnable onEmptyCallback) {
        this.timer = timer;
        this.tickTimeMillis = tickTimeMillis;
        this.clock = clock;
        this.isDelayedDeliveryDeliverAtTimeStrict = isDelayedDeliveryDeliverAtTimeStrict;
        this.fixedDelayDetectionLookahead = fixedDelayDetectionLookahead;
        this.timestampPrecisionBitCnt = calculateTimestampPrecisionBitCnt(tickTimeMillis);
        this.onEmptyCallback = onEmptyCallback;
        // Default prune throttle interval: clamp to [5ms, 50ms] using tickTimeMillis as hint
        // TODO: make configurable if needed
        long pruneMs = Math.max(5L, Math.min(50L, tickTimeMillis));
        this.minPruneIntervalNanos = TimeUnit.MILLISECONDS.toNanos(pruneMs);
    }

    private static int calculateTimestampPrecisionBitCnt(long tickTimeMillis) {
        int bitCnt = 0;
        while (tickTimeMillis > 0) {
            tickTimeMillis >>= 1;
            bitCnt++;
        }
        return bitCnt > 0 ? bitCnt - 1 : 0;
    }

    private static long trimLowerBit(long timestamp, int bits) {
        return timestamp & (-1L << bits);
    }

    @Override
    public DelayedDeliveryTracker createOrGetView(AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        String subscriptionName = dispatcher.getSubscription().getName();

        SubContext subContext = subscriptionContexts.computeIfAbsent(subscriptionName,
                k -> new SubContext(dispatcher, tickTimeMillis, isDelayedDeliveryDeliverAtTimeStrict,
                        fixedDelayDetectionLookahead, clock));
        return new InMemoryTopicDelayedDeliveryTrackerView(this, subContext);
    }

    @Override
    public void unregister(AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        String subscriptionName = dispatcher.getSubscription().getName();

        subscriptionContexts.remove(subscriptionName);
        // If no more subscriptions, proactively free index and release memory
        if (subscriptionContexts.isEmpty()) {
            timerLock.lock();
            try {
                if (timeout != null) {
                    timeout.cancel();
                    timeout = null;
                }
                currentTimeoutTarget = -1;
            } finally {
                timerLock.unlock();
            }
            delayedMessageMap.clear();
            bucketLocks.clear();
            delayedMessagesCount.set(0);
            bufferMemoryBytes.set(0);
            if (onEmptyCallback != null) {
                try {
                    onEmptyCallback.run();
                } catch (Throwable t) {
                    if (log.isDebugEnabled()) {
                        log.debug("onEmptyCallback failed", t);
                    }
                }
            }
        }
    }

    @Override
    public void onTickTimeUpdated(long newTickTimeMillis) {
        if (this.tickTimeMillis == newTickTimeMillis) {
            return;
        }
        this.tickTimeMillis = newTickTimeMillis;
        // Update precision bits for new tick time (accept old/new buckets co-exist)
        this.timestampPrecisionBitCnt = calculateTimestampPrecisionBitCnt(newTickTimeMillis);
        // Propagate to all subscriptions
        for (SubContext sc : subscriptionContexts.values()) {
            sc.tickTimeMillis = newTickTimeMillis;
        }
        // Re-evaluate timer scheduling with new tick time
        timerLock.lock();
        try {
            updateTimerLocked();
        } finally {
            timerLock.unlock();
        }
        if (log.isDebugEnabled()) {
            log.debug("Updated tickTimeMillis for topic-level delayed delivery manager to {} ms", newTickTimeMillis);
        }
    }

    @Override
    public long topicBufferMemoryBytes() {
        return bufferMemoryBytes.get();
    }

    @Override
    public long topicDelayedMessages() {
        return delayedMessagesCount.get();
    }

    @Override
    public void close() {
        timerLock.lock();
        try {
            if (timeout != null) {
                timeout.cancel();
                timeout = null;
            }
            currentTimeoutTarget = -1;
        } finally {
            timerLock.unlock();
        }
        delayedMessageMap.clear();
        bucketLocks.clear();
        subscriptionContexts.clear();
        delayedMessagesCount.set(0);
        bufferMemoryBytes.set(0);
    }

    // Internal methods for subscription views

    /**
     * Add a message to the global delayed message index.
     */
    boolean addMessageForSub(SubContext subContext, long ledgerId, long entryId, long deliverAt) {
        if (deliverAt < 0 || deliverAt <= subContext.getCutoffTime()) {
            return false;
        }

        long timestamp = trimLowerBit(deliverAt, timestampPrecisionBitCnt);
        ReentrantLock bLock = bucketLocks.computeIfAbsent(timestamp, k -> new ReentrantLock());
        bLock.lock();
        try {
            Long2ObjectRBTreeMap<Roaring64Bitmap> ledgerMap =
                    delayedMessageMap.computeIfAbsent(timestamp, k -> new Long2ObjectRBTreeMap<>());
            Roaring64Bitmap entryIds = ledgerMap.get(ledgerId);
            if (entryIds == null) {
                entryIds = new Roaring64Bitmap();
                ledgerMap.put(ledgerId, entryIds);
            }
            long before = entryIds.getLongSizeInBytes();
            if (!entryIds.contains(entryId)) {
                entryIds.add(entryId);
                delayedMessagesCount.incrementAndGet();
                long after = entryIds.getLongSizeInBytes();
                bufferMemoryBytes.addAndGet(after - before);
            }
        } finally {
            bLock.unlock();
        }

        // Timer update and fixed delay detection
        timerLock.lock();
        try {
            updateTimerLocked();
        } finally {
            timerLock.unlock();
        }
        checkAndUpdateHighest(deliverAt);
        return true;
    }

    private void checkAndUpdateHighest(long deliverAt) {
        if (deliverAt < (highestDeliveryTimeTracked - tickTimeMillis)) {
            messagesHaveFixedDelay = false;
        }
        highestDeliveryTimeTracked = Math.max(highestDeliveryTimeTracked, deliverAt);
    }

    /**
     * Check if there are messages available for a subscription.
     */
    boolean hasMessageAvailableForSub(SubContext subContext) {
        if (delayedMessageMap.isEmpty()) {
            return false;
        }
        // Use firstEntry() to avoid NoSuchElementException on concurrent empty map
        Map.Entry<Long, Long2ObjectRBTreeMap<Roaring64Bitmap>> first = delayedMessageMap.firstEntry();
        if (first == null) {
            return false;
        }
        long cutoffTime = subContext.getCutoffTime();
        long firstTs = first.getKey();
        return firstTs <= cutoffTime;
    }

    /**
     * Get scheduled messages for a subscription.
     */
    NavigableSet<Position> getScheduledMessagesForSub(SubContext subContext, int maxMessages) {
        NavigableSet<Position> positions = new TreeSet<>();
        int remaining = maxMessages;

        long cutoffTime = subContext.getCutoffTime();
        Position markDelete = subContext.getMarkDeletePosition();

        // Snapshot of buckets up to cutoff and iterate per-bucket with bucket locks
        List<Long> tsList = new ArrayList<>(delayedMessageMap.headMap(cutoffTime, true).keySet());
        for (Long ts : tsList) {
            if (remaining <= 0) {
                break;
            }
            ReentrantLock bLock = bucketLocks.get(ts);
            if (bLock == null) {
                continue;
            }
            bLock.lock();
            try {
                Long2ObjectRBTreeMap<Roaring64Bitmap> ledgerMap = delayedMessageMap.get(ts);
                if (ledgerMap == null) {
                    continue;
                }
                for (Long2ObjectMap.Entry<Roaring64Bitmap> ledgerEntry : ledgerMap.long2ObjectEntrySet()) {
                    if (remaining <= 0) {
                        break;
                    }
                    long ledgerId = ledgerEntry.getLongKey();
                    Roaring64Bitmap entryIds = ledgerEntry.getValue();
                    if (markDelete != null && ledgerId < markDelete.getLedgerId()) {
                        continue;
                    }
                    LongIterator it = entryIds.getLongIterator();
                    while (it.hasNext() && remaining > 0) {
                        long entryId = it.next();
                        if (markDelete != null && ledgerId == markDelete.getLedgerId()
                                && entryId <= markDelete.getEntryId()) {
                            continue;
                        }
                        positions.add(PositionFactory.create(ledgerId, entryId));
                        remaining--;
                    }
                }
            } finally {
                bLock.unlock();
            }
        }

        // Throttled prune: avoid heavy prune on every hot-path call
        if (!positions.isEmpty()) {
            maybePruneByTime();
        }

        return positions;
    }

    /**
     * Check if deliveries should be paused for a subscription.
     */
    boolean shouldPauseAllDeliveriesForSub(SubContext subContext) {
        // Parity with legacy: pause if all observed delays are fixed and backlog is large enough
        return subContext.getFixedDelayDetectionLookahead() > 0
                && messagesHaveFixedDelay
                && getNumberOfVisibleDelayedMessagesForSub(subContext) >= subContext.getFixedDelayDetectionLookahead()
                && !hasMessageAvailableForSub(subContext);
    }

    /**
     * Clear delayed messages for a subscription (no-op for topic-level manager).
     */
    void clearForSub() {
        // No-op: we don't clear global index for individual subscriptions
    }

    /**
     * Update mark delete position for a subscription.
     */
    void updateMarkDeletePosition(SubContext subContext, Position position) {
        // Event-driven update from dispatcher; keep it lightweight (no prune here)
        subContext.updateMarkDeletePosition(position);
    }

    // Private helper methods

    private void updateTimerLocked() {
        // Use firstEntry() to avoid NoSuchElementException on concurrent empty map
        Map.Entry<Long, Long2ObjectRBTreeMap<Roaring64Bitmap>> first = delayedMessageMap.firstEntry();
        if (first == null) {
            if (timeout != null) {
                currentTimeoutTarget = -1;
                timeout.cancel();
                timeout = null;
            }
            return;
        }
        long nextDeliveryTime = first.getKey();
        if (nextDeliveryTime == currentTimeoutTarget) {
            return;
        }
        if (timeout != null) {
            timeout.cancel();
        }
        long now = clock.millis();
        long delayMillis = nextDeliveryTime - now;
        if (delayMillis < 0) {
            return;
        }
        long remainingTickDelayMillis = lastTickRun + tickTimeMillis - now;
        long calculatedDelayMillis = Math.max(delayMillis, remainingTickDelayMillis);
        currentTimeoutTarget = nextDeliveryTime;
        timeout = timer.newTimeout(this, calculatedDelayMillis, TimeUnit.MILLISECONDS);
    }

    private void updateBufferMemoryEstimate() {
        // No-op in incremental mode (kept for compatibility)
    }

    private long getNumberOfVisibleDelayedMessagesForSub(SubContext subContext) {
        // Simplified implementation - returns total count
        // Could be enhanced to count only messages visible to this subscription
        return delayedMessagesCount.get();
    }

    private void pruneByMinMarkDelete() {
        // Find the minimum mark delete position across all subscriptions
        Position minMarkDelete = null;
        for (SubContext subContext : subscriptionContexts.values()) {
            Position markDelete = subContext.getMarkDeletePosition();
            if (markDelete != null) {
                if (minMarkDelete == null || markDelete.compareTo(minMarkDelete) < 0) {
                    minMarkDelete = markDelete;
                }
            }
        }

        if (minMarkDelete == null) {
            return;
        }

        // No idempotency set to clean (Option A): rely on per-bitmap removal below

        // Prune per bucket under bucket lock
        for (Long ts : new ArrayList<>(delayedMessageMap.keySet())) {
            ReentrantLock bLock = bucketLocks.get(ts);
            if (bLock == null) {
                continue;
            }
            bLock.lock();
            try {
                Long2ObjectRBTreeMap<Roaring64Bitmap> ledgerMap = delayedMessageMap.get(ts);
                if (ledgerMap == null) {
                    continue;
                }
                ArrayList<Long> ledgersToRemove = new ArrayList<>();
                for (Long2ObjectMap.Entry<Roaring64Bitmap> ledgerEntry : ledgerMap.long2ObjectEntrySet()) {
                    long ledgerId = ledgerEntry.getLongKey();
                    Roaring64Bitmap entryIds = ledgerEntry.getValue();
                    if (ledgerId < minMarkDelete.getLedgerId()) {
                        long bytes = entryIds.getLongSizeInBytes();
                        delayedMessagesCount.addAndGet(-entryIds.getLongCardinality());
                        bufferMemoryBytes.addAndGet(-bytes);
                        ledgersToRemove.add(ledgerId);
                    } else if (ledgerId == minMarkDelete.getLedgerId()) {
                        long before = entryIds.getLongSizeInBytes();
                        long removedCount = 0;
                        LongIterator it = entryIds.getLongIterator();
                        ArrayList<Long> toRemove = new ArrayList<>();
                        while (it.hasNext()) {
                            long e = it.next();
                            if (e <= minMarkDelete.getEntryId()) {
                                toRemove.add(e);
                            }
                        }
                        for (Long e : toRemove) {
                            entryIds.removeLong(e);
                            removedCount++;
                        }
                        long after = entryIds.getLongSizeInBytes();
                        delayedMessagesCount.addAndGet(-removedCount);
                        bufferMemoryBytes.addAndGet(after - before);
                        if (entryIds.isEmpty()) {
                            ledgersToRemove.add(ledgerId);
                        }
                    }
                }
                for (Long ledgerId : ledgersToRemove) {
                    ledgerMap.remove(ledgerId);
                }
                if (ledgerMap.isEmpty()) {
                    delayedMessageMap.remove(ts);
                    bucketLocks.remove(ts);
                }
            } finally {
                bLock.unlock();
            }
        }
    }

    private void maybePruneByTime() {
        long now = System.nanoTime();
        long last = lastPruneNanos.get();
        if (now - last >= minPruneIntervalNanos) {
            if (lastPruneNanos.compareAndSet(last, now)) {
                pruneByMinMarkDelete();
            }
        }
    }

    // idempotency set removed per Option A

    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout == null || timeout.isCancelled()) {
            return;
        }

        // Clear timer state
        timerLock.lock();
        try {
            currentTimeoutTarget = -1;
            this.timeout = null;
            lastTickRun = clock.millis();
        } finally {
            timerLock.unlock();
        }

        ArrayList<AbstractPersistentDispatcherMultipleConsumers> toTrigger = new ArrayList<>();
        // Use firstEntry() to avoid NoSuchElementException on concurrent empty map
        Map.Entry<Long, Long2ObjectRBTreeMap<Roaring64Bitmap>> first = delayedMessageMap.firstEntry();
        if (first != null) {
            long earliestTs = first.getKey();
            for (SubContext subContext : subscriptionContexts.values()) {
                long cutoff = subContext.getCutoffTime();
                if (earliestTs <= cutoff) {
                    toTrigger.add(subContext.getDispatcher());
                }
            }

            // If a significant portion of subscriptions are eligible, opportunistically prune (throttled)
            int subs = subscriptionContexts.size();
            int eligible = toTrigger.size();
            // majority by default
            int threshold = Math.max(1, subs / 2);
            if (eligible >= threshold) {
                // Not under timerLock or any bucket lock; prune uses per-bucket locks and is safe here
                maybePruneByTime();
            }
        }

        // Invoke callbacks outside of locks
        for (AbstractPersistentDispatcherMultipleConsumers d : toTrigger) {
            d.readMoreEntriesAsync();
        }
    }

    private void refreshMarkDeletePosition(SubContext subContext) {
        // Deprecated: mark-delete is now updated via event-driven callbacks from dispatcher.
        // Intentionally no-op to keep API surface; will be removed in a later cleanup.
    }
}
