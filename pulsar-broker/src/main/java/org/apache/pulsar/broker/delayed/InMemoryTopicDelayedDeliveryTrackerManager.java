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
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * In-memory implementation of topic-level delayed delivery tracker manager.
 * This manager maintains a single global delayed message index per topic that is shared by all
 * subscriptions, significantly reducing memory usage in multi-subscription scenarios.
 */
@Slf4j
public class InMemoryTopicDelayedDeliveryTrackerManager implements TopicDelayedDeliveryTrackerManager, TimerTask {

    // Global delayed message index: timestamp -> ledgerId -> entryId bitmap
    private final Long2ObjectSortedMap<Long2ObjectSortedMap<Roaring64Bitmap>> delayedMessageMap =
            new Long2ObjectRBTreeMap<>();

    // Subscription registry: subscription name -> subscription context
    private final Map<String, SubContext> subscriptionContexts = new HashMap<>();

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

    // Fixed-delay detection (parity with legacy behavior)
    private long highestDeliveryTimeTracked = 0;
    private boolean messagesHaveFixedDelay = true;

    // Timestamp precision for memory optimization
    private int timestampPrecisionBitCnt;

    /**
     * Subscription context that holds per-subscription state.
     */
    @Getter
    static class SubContext {
        private final AbstractPersistentDispatcherMultipleConsumers dispatcher;
        private final String subscriptionName;
        private long tickTimeMillis;
        private final boolean isDelayedDeliveryDeliverAtTimeStrict;
        private final long fixedDelayDetectionLookahead;
        private final Clock clock;
        private Position markDeletePosition;

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

    public InMemoryTopicDelayedDeliveryTrackerManager(Timer timer, long tickTimeMillis,
                                                      boolean isDelayedDeliveryDeliverAtTimeStrict,
                                                      long fixedDelayDetectionLookahead) {
        this(timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
             fixedDelayDetectionLookahead);
    }

    public InMemoryTopicDelayedDeliveryTrackerManager(Timer timer, long tickTimeMillis, Clock clock,
                                                      boolean isDelayedDeliveryDeliverAtTimeStrict,
                                                      long fixedDelayDetectionLookahead) {
        this.timer = timer;
        this.tickTimeMillis = tickTimeMillis;
        this.clock = clock;
        this.isDelayedDeliveryDeliverAtTimeStrict = isDelayedDeliveryDeliverAtTimeStrict;
        this.fixedDelayDetectionLookahead = fixedDelayDetectionLookahead;
        this.timestampPrecisionBitCnt = calculateTimestampPrecisionBitCnt(tickTimeMillis);
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

        synchronized (this) {
            SubContext subContext = subscriptionContexts.computeIfAbsent(subscriptionName,
                k -> new SubContext(dispatcher, tickTimeMillis, isDelayedDeliveryDeliverAtTimeStrict,
                                   fixedDelayDetectionLookahead, clock));

            return new InMemoryTopicDelayedDeliveryTrackerView(this, subContext);
        }
    }

    @Override
    public void unregister(AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        String subscriptionName = dispatcher.getSubscription().getName();

        synchronized (this) {
            subscriptionContexts.remove(subscriptionName);
            // If no more subscriptions, proactively free index and close the manager to release memory
            if (subscriptionContexts.isEmpty()) {
                delayedMessageMap.clear();
                delayedMessagesCount.set(0);
                bufferMemoryBytes.set(0);
                close();
            }
        }
    }

    @Override
    public void onTickTimeUpdated(long newTickTimeMillis) {
        synchronized (this) {
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
            updateTimer();
            if (log.isDebugEnabled()) {
                log.debug("Updated tickTimeMillis for topic-level delayed delivery manager to {} ms",
                        newTickTimeMillis);
            }
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
        synchronized (this) {
            if (timeout != null) {
                timeout.cancel();
                timeout = null;
            }
            delayedMessageMap.clear();
            subscriptionContexts.clear();
            delayedMessagesCount.set(0);
            bufferMemoryBytes.set(0);
        }
    }

    // Internal methods for subscription views

    /**
     * Add a message to the global delayed message index.
     */
    boolean addMessageForSub(SubContext subContext, long ledgerId, long entryId, long deliverAt) {
        synchronized (this) {
            if (deliverAt < 0 || deliverAt <= subContext.getCutoffTime()) {
                return false;
            }

            long timestamp = trimLowerBit(deliverAt, timestampPrecisionBitCnt);
            Long2ObjectSortedMap<Roaring64Bitmap> ledgerMap = delayedMessageMap.computeIfAbsent(
                timestamp, k -> new Long2ObjectRBTreeMap<>());
            Roaring64Bitmap entryIds = ledgerMap.computeIfAbsent(ledgerId, k -> new Roaring64Bitmap());

            // Incremental memory accounting: measure size delta on change
            long before = entryIds.getLongSizeInBytes();
            boolean existed = entryIds.contains(entryId);
            if (!existed) {
                entryIds.add(entryId);
                delayedMessagesCount.incrementAndGet();
                long after = entryIds.getLongSizeInBytes();
                bufferMemoryBytes.addAndGet(after - before);
            }

            updateTimer();
            // Update global fixed-delay detection
            checkAndUpdateHighest(deliverAt);
            return true;
        }
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
        synchronized (this) {
            refreshMarkDeletePosition(subContext);
            if (delayedMessageMap.isEmpty()) {
                return false;
            }

            long cutoffTime = subContext.getCutoffTime();
            long firstTimestamp = delayedMessageMap.firstLongKey();

            if (firstTimestamp > cutoffTime) {
                return false;
            }

            // Quick check: if there's any message in the earliest time bucket that's after mark delete
            Long2ObjectSortedMap<Roaring64Bitmap> ledgerMap = delayedMessageMap.get(firstTimestamp);
            if (ledgerMap != null) {
                Position markDelete = subContext.getMarkDeletePosition();
                if (markDelete == null) {
                    return true; // No mark delete means all messages are available
                }

                for (var entry : ledgerMap.long2ObjectEntrySet()) {
                    long ledgerId = entry.getLongKey();
                    Roaring64Bitmap entryIds = entry.getValue();

                    if (ledgerId > markDelete.getLedgerId()) {
                        return true;
                    } else if (ledgerId == markDelete.getLedgerId()) {
                        // Check if there are any entry IDs after mark delete
                        if (entryIds.stream().anyMatch(entryId -> entryId > markDelete.getEntryId())) {
                            return true;
                        }
                    }
                }
            }

            return false;
        }
    }

    /**
     * Get scheduled messages for a subscription.
     */
    NavigableSet<Position> getScheduledMessagesForSub(SubContext subContext, int maxMessages) {
        synchronized (this) {
            refreshMarkDeletePosition(subContext);
            int remaining = maxMessages;
            NavigableSet<Position> positions = new TreeSet<>();
            long cutoffTime = subContext.getCutoffTime();
            Position markDelete = subContext.getMarkDeletePosition();

            // Iterate through time buckets
            var iterator = delayedMessageMap.long2ObjectEntrySet().iterator();
            while (iterator.hasNext() && remaining > 0) {
                var timeEntry = iterator.next();
                long timestamp = timeEntry.getLongKey();

                if (timestamp > cutoffTime) {
                    break;
                }

                Long2ObjectSortedMap<Roaring64Bitmap> ledgerMap = timeEntry.getValue();

                // Iterate through ledgers in this time bucket
                var ledgerIterator = ledgerMap.long2ObjectEntrySet().iterator();
                while (ledgerIterator.hasNext() && remaining > 0) {
                    var ledgerEntry = ledgerIterator.next();
                    long ledgerId = ledgerEntry.getLongKey();
                    Roaring64Bitmap entryIds = ledgerEntry.getValue();

                    // Fast skip if entire ledger is before mark-delete
                    if (markDelete != null && ledgerId < markDelete.getLedgerId()) {
                        continue;
                    }

                    // Iterate over entry ids without materializing array
                    var it = entryIds.iterator();
                    while (it.hasNext() && remaining > 0) {
                        long entryId = it.next();

                        // Skip entries that are before or at mark delete
                        if (markDelete != null) {
                            if (ledgerId == markDelete.getLedgerId() && entryId <= markDelete.getEntryId()) {
                                continue;
                            }
                        }

                        positions.add(PositionFactory.create(ledgerId, entryId));
                        remaining--;
                    }
                }
            }

            // Prune global index based on min mark-delete across all subscriptions
            pruneByMinMarkDelete();

            return positions;
        }
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
        synchronized (this) {
            subContext.updateMarkDeletePosition(position);
            // Trigger pruning if needed
            pruneByMinMarkDelete();
        }
    }

    // Private helper methods

    private void updateTimer() {
        if (delayedMessageMap.isEmpty()) {
            if (timeout != null) {
                currentTimeoutTarget = -1;
                timeout.cancel();
                timeout = null;
            }
            return;
        }

        long nextDeliveryTime = delayedMessageMap.firstLongKey();
        if (nextDeliveryTime == currentTimeoutTarget) {
            return;
        }

        if (timeout != null) {
            timeout.cancel();
        }

        long now = clock.millis();
        long delayMillis = nextDeliveryTime - now;

        if (delayMillis < 0) {
            // Messages are ready; avoid retriggering timer, dispatcher will pick them on next read
            return;
        }

        // Align with tick window like AbstractDelayedDeliveryTracker
        long remainingTickDelayMillis = lastTickRun + tickTimeMillis - now;
        long calculatedDelayMillis = Math.max(delayMillis, remainingTickDelayMillis);

        currentTimeoutTarget = nextDeliveryTime;
        timeout = timer.newTimeout(this, calculatedDelayMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
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

        // Prune entries that are before min mark delete
        var iterator = delayedMessageMap.long2ObjectEntrySet().iterator();
        while (iterator.hasNext()) {
            var timeEntry = iterator.next();
            Long2ObjectSortedMap<Roaring64Bitmap> ledgerMap = timeEntry.getValue();

            var ledgerIterator = ledgerMap.long2ObjectEntrySet().iterator();
            while (ledgerIterator.hasNext()) {
                var ledgerEntry = ledgerIterator.next();
                long ledgerId = ledgerEntry.getLongKey();
                Roaring64Bitmap entryIds = ledgerEntry.getValue();

                if (ledgerId < minMarkDelete.getLedgerId()) {
                    // Entire ledger can be removed
                    long bytes = entryIds.getLongSizeInBytes();
                    delayedMessagesCount.addAndGet(-entryIds.getLongCardinality());
                    bufferMemoryBytes.addAndGet(-bytes);
                    ledgerIterator.remove();
                } else if (ledgerId == minMarkDelete.getLedgerId()) {
                    // Remove entries <= mark delete entry ID
                    long removedCount = 0;
                    long before = entryIds.getLongSizeInBytes();
                    var entryIterator = entryIds.iterator();
                    while (entryIterator.hasNext()) {
                        long entryId = entryIterator.next();
                        if (entryId <= minMarkDelete.getEntryId()) {
                            entryIterator.remove();
                            removedCount++;
                        }
                    }
                    long after = entryIds.getLongSizeInBytes();
                    delayedMessagesCount.addAndGet(-removedCount);
                    bufferMemoryBytes.addAndGet(after - before);

                    if (entryIds.isEmpty()) {
                        ledgerIterator.remove();
                    }
                }
            }

            if (ledgerMap.isEmpty()) {
                iterator.remove();
            }
        }

        updateBufferMemoryEstimate();
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout == null || timeout.isCancelled()) {
            return;
        }

        java.util.ArrayList<AbstractPersistentDispatcherMultipleConsumers> toTrigger = new java.util.ArrayList<>();
        synchronized (this) {
            currentTimeoutTarget = -1;
            this.timeout = null;
            lastTickRun = clock.millis();

            // Decide which dispatchers to trigger while holding the lock
            for (SubContext subContext : subscriptionContexts.values()) {
                if (hasMessageAvailableForSub(subContext)) {
                    toTrigger.add(subContext.getDispatcher());
                }
            }
        }
        // Invoke callbacks outside the manager lock to reduce contention
        for (AbstractPersistentDispatcherMultipleConsumers d : toTrigger) {
            d.readMoreEntriesAsync();
        }
    }

    private void refreshMarkDeletePosition(SubContext subContext) {
        try {
            Position pos = subContext.getDispatcher().getCursor().getMarkDeletedPosition();
            if (pos != null) {
                Position current = subContext.getMarkDeletePosition();
                if (current == null || pos.compareTo(current) > 0) {
                    subContext.updateMarkDeletePosition(pos);
                }
            }
        } catch (Throwable t) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to refresh mark-delete position for subscription {}",
                        subContext.getSubscriptionName(), t);
            }
        }
    }
}
