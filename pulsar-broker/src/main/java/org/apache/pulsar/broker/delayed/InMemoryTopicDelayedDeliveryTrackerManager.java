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

    // Configuration
    private final long tickTimeMillis;
    private final boolean isDelayedDeliveryDeliverAtTimeStrict;
    private final long fixedDelayDetectionLookahead;
    private final Clock clock;

    // Statistics
    private final AtomicLong delayedMessagesCount = new AtomicLong(0);
    private final AtomicLong bufferMemoryBytes = new AtomicLong(0);

    // Timestamp precision for memory optimization
    private final int timestampPrecisionBitCnt;

    /**
     * Subscription context that holds per-subscription state.
     */
    @Getter
    static class SubContext {
        private final AbstractPersistentDispatcherMultipleConsumers dispatcher;
        private final String subscriptionName;
        private final long tickTimeMillis;
        private final boolean isDelayedDeliveryDeliverAtTimeStrict;
        private final long fixedDelayDetectionLookahead;
        private Position markDeletePosition;

        SubContext(AbstractPersistentDispatcherMultipleConsumers dispatcher, long tickTimeMillis,
                   boolean isDelayedDeliveryDeliverAtTimeStrict, long fixedDelayDetectionLookahead) {
            this.dispatcher = dispatcher;
            this.subscriptionName = dispatcher.getSubscription().getName();
            this.tickTimeMillis = tickTimeMillis;
            this.isDelayedDeliveryDeliverAtTimeStrict = isDelayedDeliveryDeliverAtTimeStrict;
            this.fixedDelayDetectionLookahead = fixedDelayDetectionLookahead;
        }

        void updateMarkDeletePosition(Position position) {
            this.markDeletePosition = position;
        }

        long getCutoffTime() {
            return isDelayedDeliveryDeliverAtTimeStrict ? System.currentTimeMillis() :
                   System.currentTimeMillis() + tickTimeMillis;
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
                                   fixedDelayDetectionLookahead));

            return new InMemoryTopicDelayedDeliveryTrackerView(this, subContext);
        }
    }

    @Override
    public void unregister(AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        String subscriptionName = dispatcher.getSubscription().getName();

        synchronized (this) {
            subscriptionContexts.remove(subscriptionName);

            // If no more subscriptions, close the manager
            if (subscriptionContexts.isEmpty() && delayedMessageMap.isEmpty()) {
                close();
            }
        }
    }

    @Override
    public void onTickTimeUpdated(long newTickTimeMillis) {
        // For now, tick time updates are not supported after initialization
        // This could be enhanced to update all subscription contexts
        log.warn("Tick time updates are not currently supported for topic-level delayed delivery managers");
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

            // Check if this entry already exists (deduplication)
            if (!entryIds.contains(entryId)) {
                entryIds.add(entryId);
                delayedMessagesCount.incrementAndGet();
                updateBufferMemoryEstimate();
            }

            updateTimer();
            return true;
        }
    }

    /**
     * Check if there are messages available for a subscription.
     */
    boolean hasMessageAvailableForSub(SubContext subContext) {
        synchronized (this) {
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

                    // Filter entries based on mark delete position
                    long[] entryIdArray = entryIds.toArray();
                    for (long entryId : entryIdArray) {
                        if (remaining <= 0) {
                            break;
                        }

                        // Skip entries that are before or at mark delete
                        if (markDelete != null) {
                            if (ledgerId < markDelete.getLedgerId()) {
                                continue;
                            }
                            if (ledgerId == markDelete.getLedgerId() && entryId <= markDelete.getEntryId()) {
                                continue;
                            }
                        }

                        positions.add(PositionFactory.create(ledgerId, entryId));
                        remaining--;
                    }
                }
            }

            // Note: We don't remove messages from the global index here
            // Pruning will be handled separately based on min mark delete across all subscriptions

            return positions;
        }
    }

    /**
     * Check if deliveries should be paused for a subscription.
     */
    boolean shouldPauseAllDeliveriesForSub(SubContext subContext) {
        // Simplified implementation - could be enhanced with fixed delay detection
        return fixedDelayDetectionLookahead > 0
                && getNumberOfVisibleDelayedMessagesForSub(subContext) >= fixedDelayDetectionLookahead
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
            // Messages are ready, but we don't need to keep retriggering
            return;
        }

        currentTimeoutTarget = nextDeliveryTime;
        timeout = timer.newTimeout(this, delayMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void updateBufferMemoryEstimate() {
        // Simplified memory estimation
        long estimatedBytes = delayedMessageMap.values().stream()
            .mapToLong(ledgerMap -> ledgerMap.values().stream()
                .mapToLong(Roaring64Bitmap::getLongSizeInBytes).sum())
            .sum();
        bufferMemoryBytes.set(estimatedBytes);
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
                    delayedMessagesCount.addAndGet(-entryIds.getLongCardinality());
                    ledgerIterator.remove();
                } else if (ledgerId == minMarkDelete.getLedgerId()) {
                    // Remove entries <= mark delete entry ID
                    long removedCount = 0;
                    var entryIterator = entryIds.iterator();
                    while (entryIterator.hasNext()) {
                        long entryId = entryIterator.next();
                        if (entryId <= minMarkDelete.getEntryId()) {
                            entryIterator.remove();
                            removedCount++;
                        }
                    }
                    delayedMessagesCount.addAndGet(-removedCount);

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

        synchronized (this) {
            currentTimeoutTarget = -1;
            this.timeout = null;

            // Trigger read more entries for all subscriptions
            for (SubContext subContext : subscriptionContexts.values()) {
                subContext.getDispatcher().readMoreEntriesAsync();
            }
        }
    }
}