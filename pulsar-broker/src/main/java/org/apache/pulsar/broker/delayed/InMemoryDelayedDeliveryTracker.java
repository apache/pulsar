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

import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.netty.util.Timer;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.time.Clock;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.common.util.collections.LongBitmap;
import org.apache.pulsar.common.util.collections.LongBitmaps;

public class InMemoryDelayedDeliveryTracker extends AbstractDelayedDeliveryTracker {

    private static final Logger LOG = Logger.get(InMemoryDelayedDeliveryTracker.class);
    protected final Logger log;

    // timestamp -> ledgerId -> entryId
    // AVL tree -> OpenHashMap -> LongBitmap
    protected final Long2ObjectSortedMap<Long2ObjectSortedMap<LongBitmap>>
            delayedMessageMap = new Long2ObjectAVLTreeMap<>();

    // The same pending positions, indexed by ledger for the managed-ledger read-skip predicate. Membership reads
    // run on ledger threads, so access to all indexes is serialized on this tracker, not on the dispatcher.
    private final Long2ObjectMap<LongBitmap> delayedMessageIndex = new Long2ObjectOpenHashMap<>();

    // Positions handed to replay while a rewound normal read can still encounter them. They are no longer
    // delayed, but must remain hidden from normal reads until the cursor catches up (or rewinds again).
    private final Long2ObjectMap<LongBitmap> replayedMessageIndex = new Long2ObjectOpenHashMap<>();
    private boolean retainReplayedMessages;

    // If we detect that all messages have fixed delay time, such that the delivery is
    // always going to be in FIFO order, then we can avoid pulling all the messages in
    // tracker. Instead, we use the lookahead for detection and pause the read from
    // the cursor if the delays are fixed.
    @Getter
    @VisibleForTesting
    private final long fixedDelayDetectionLookahead;

    // This is the timestamp of the message with the highest delivery time
    // If new added messages are lower than this, it means the delivery is requested
    // to be out-of-order. It gets reset to 0, once the tracker is emptied.
    private long highestDeliveryTimeTracked = 0;

    // Track whether we have seen all messages with fixed delay so far.
    private boolean messagesHaveFixedDelay = true;

    // The bit count to trim to reduce memory occupation.
    private final int timestampPrecisionBitCnt;
    private final long precisionMillis;

    // Count of delayed messages in the tracker.
    private final AtomicLong delayedMessagesCount = new AtomicLong(0);

    // Cached memory usage of the delayed message bitmaps, maintained via delta on each mutation.
    private final AtomicLong memoryUsage = new AtomicLong(0);

    InMemoryDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                   long tickTimeMillis,
                                   boolean isDelayedDeliveryDeliverAtTimeStrict,
                                   long fixedDelayDetectionLookahead) {
        this(new DispatcherDelayedDeliveryContext(dispatcher), timer, tickTimeMillis, Clock.systemUTC(),
                isDelayedDeliveryDeliverAtTimeStrict, fixedDelayDetectionLookahead);
    }

    @VisibleForTesting
    public InMemoryDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                          long tickTimeMillis, Clock clock,
                                          boolean isDelayedDeliveryDeliverAtTimeStrict,
                                          long fixedDelayDetectionLookahead) {
        this(new DispatcherDelayedDeliveryContext(dispatcher), timer, tickTimeMillis, clock,
                isDelayedDeliveryDeliverAtTimeStrict, fixedDelayDetectionLookahead);
    }

    private InMemoryDelayedDeliveryTracker(DelayedDeliveryContext context, Timer timer,
                                           long tickTimeMillis, Clock clock,
                                           boolean isDelayedDeliveryDeliverAtTimeStrict,
                                           long fixedDelayDetectionLookahead) {
        super(context, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict);
        this.log = LOG.with().ctx(super.log).build();
        this.fixedDelayDetectionLookahead = fixedDelayDetectionLookahead;
        this.timestampPrecisionBitCnt = calculateTimestampPrecisionBitCnt(tickTimeMillis);
        this.precisionMillis = 1L << timestampPrecisionBitCnt;
    }

    /**
     * The tick time is used to determine the precision of the delivery time. As the redelivery time
     * is not accurate, we can bucket the delivery time and group multiple message ids into the same
     * bucket to reduce the memory usage. THe default value is 1 second, which means we accept 1 second
     * deviation for the delivery time, so that we can trim the lower 9 bits of the delivery time, because
     * 2**9ms = 512ms < 1s, 2**10ms = 1024ms > 1s.
     * @param tickTimeMillis
     * @return
     */
    private static int calculateTimestampPrecisionBitCnt(long tickTimeMillis) {
        int bitCnt = 0;
        while (tickTimeMillis > 0) {
            tickTimeMillis >>= 1;
            bitCnt++;
        }
        return bitCnt > 0 ? bitCnt - 1 : 0;
    }

    /**
     * trim the lower bits of the timestamp to reduce the memory usage.
     * @param timestamp
     * @param bits
     * @return
     */
    private static long trimLowerBit(long timestamp, int bits) {
        return timestamp & (-1L << bits);
    }

    @Override
    public synchronized boolean addMessage(long ledgerId, long entryId, long deliverAt) {
        if (containsMessage(delayedMessageIndex, ledgerId, entryId)) {
            return true;
        }
        if (deliverAt < 0 || deliverAt <= getCutoffTime()) {
            messagesHaveFixedDelay = false;
            return false;
        }

        log.debug()
                .attr("ledgerId", ledgerId)
                .attr("entryId", entryId)
                .attr("deliveryInMs", () -> deliverAt - clock.millis())
                .log("Add message");
        long timestamp = roundTimestamp(deliverAt);

        LongBitmap bitmap = delayedMessageMap.computeIfAbsent(timestamp, k -> new Long2ObjectRBTreeMap<>())
            .computeIfAbsent(ledgerId, k -> LongBitmaps.create());

        long oldSize = bitmap.serializedSize();
        if (bitmap.checkedAdd(entryId)) {
            long newSize = bitmap.serializedSize();
            memoryUsage.addAndGet(newSize - oldSize);
            delayedMessagesCount.incrementAndGet();

            LongBitmap index = delayedMessageIndex.computeIfAbsent(ledgerId, k -> LongBitmaps.create());
            long oldIndexSize = index.serializedSize();
            index.add(entryId);
            memoryUsage.addAndGet(index.serializedSize() - oldIndexSize);
        }

        updateTimer();

        checkAndUpdateHighest(deliverAt);

        return true;
    }

    /**
     * Round the deliverAt timestamp to the bucket boundary used as the key in {@link #delayedMessageMap}, so that
     * all messages within the same bucket share a single map entry to reduce memory usage.
     *
     * In strict delivery mode the timestamp is rounded up: a bucket then becomes due only after every deliverAt
     * time inside it has passed, so messages are delivered up to one bucket (less than tickTimeMillis) late, but
     * never before their deliverAt time. Rounding down instead would let {@link #getScheduledMessages(int)} hand a
     * message to the dispatcher before its deliverAt time; the dispatcher would put it back and re-trigger reads
     * in a loop until the deliverAt time is reached (see issue #25996).
     *
     * In non-strict mode the timestamp is rounded down, since delivering up to tickTimeMillis early is allowed.
     * Availability checks account for the full bucket width so that the original deliverAt is still within that
     * allowed window.
     */
    private long roundTimestamp(long deliverAt) {
        if (isDeliverAtTimeStrict()) {
            // round up, saturating at Long.MAX_VALUE instead of overflowing for deliverAt close to Long.MAX_VALUE
            long roundedUp = deliverAt + precisionMillis - 1;
            return trimLowerBit(roundedUp < deliverAt ? Long.MAX_VALUE : roundedUp, timestampPrecisionBitCnt);
        }
        return trimLowerBit(deliverAt, timestampPrecisionBitCnt);
    }

    private long getBucketCutoffTime() {
        return isDeliverAtTimeStrict() ? getCutoffTime() : getCutoffTime() - precisionMillis + 1;
    }

    /**
     * Check that new delivery time comes after the current highest, or at
     * least within a single tick time interval of 1 second.
     */
    private void checkAndUpdateHighest(long deliverAt) {
        if (deliverAt < (highestDeliveryTimeTracked - tickTimeMillis)) {
            messagesHaveFixedDelay = false;
        }

        highestDeliveryTimeTracked = Math.max(highestDeliveryTimeTracked, deliverAt);
    }

    /**
     * Return true if there's at least a message that is scheduled to be delivered already.
     */
    @Override
    public synchronized boolean hasMessageAvailable() {
        boolean hasMessageAvailable = !delayedMessageMap.isEmpty()
                && delayedMessageMap.firstLongKey() <= getBucketCutoffTime();
        if (!hasMessageAvailable) {
            updateTimer();
        }
        return hasMessageAvailable;
    }

    /**
     * Get a set of position of messages that have already reached.
     */
    @Override
    public synchronized NavigableSet<Position> getScheduledMessages(int maxMessages) {
        return getMessages(maxMessages, getBucketCutoffTime());
    }

    /**
     * Release indexed messages immediately when delayed delivery is disabled, using the same replay handoff
     * as due messages so normal reads cannot dispatch a second copy.
     */
    public synchronized NavigableSet<Position> drainAllMessages(int maxMessages) {
        return getMessages(maxMessages, Long.MAX_VALUE);
    }

    private NavigableSet<Position> getMessages(int maxMessages, long cutoffTime) {
        int n = maxMessages;
        NavigableSet<Position> positions = new TreeSet<>();

        while (n > 0 && !delayedMessageMap.isEmpty()) {
            long timestamp = delayedMessageMap.firstLongKey();
            if (timestamp > cutoffTime) {
                break;
            }

            LongSet ledgerIdToDelete = new LongOpenHashSet();
            Long2ObjectSortedMap<LongBitmap> ledgerMap = delayedMessageMap.get(timestamp);
            for (Long2ObjectMap.Entry<LongBitmap> ledgerEntry : ledgerMap.long2ObjectEntrySet()) {
                long ledgerId = ledgerEntry.getLongKey();
                LongBitmap entryIds = ledgerEntry.getValue();
                LongBitmap index = delayedMessageIndex.get(ledgerId);
                long cardinality = entryIds.cardinality();
                long oldSize = entryIds.serializedSize();
                long oldIndexSize = index.serializedSize();
                long drained = entryIds.drainTo(n, entryId -> {
                    positions.add(PositionFactory.create(ledgerId, entryId));
                    index.remove(entryId);
                    if (retainReplayedMessages) {
                        LongBitmap replayed = replayedMessageIndex.computeIfAbsent(ledgerId, k -> LongBitmaps.create());
                        long oldReplayedSize = replayed.serializedSize();
                        replayed.add(entryId);
                        memoryUsage.addAndGet(replayed.serializedSize() - oldReplayedSize);
                    }
                });
                long newSize = entryIds.serializedSize();
                memoryUsage.addAndGet(newSize - oldSize + index.serializedSize() - oldIndexSize);
                if (index.isEmpty()) {
                    delayedMessageIndex.remove(ledgerId);
                }
                delayedMessagesCount.addAndGet(-drained);
                n -= drained;
                if (drained == cardinality) {
                    // Bitmap is now empty; the entry will be removed from the ledger map below.
                    ledgerIdToDelete.add(ledgerId);
                }
                if (n <= 0) {
                    break;
                }
            }
            for (long ledgerId : ledgerIdToDelete) {
                ledgerMap.remove(ledgerId);
            }
            if (ledgerMap.isEmpty()) {
                delayedMessageMap.remove(timestamp);
            }
        }
        log.debug()
                .attr("messagesCount", positions.size())
                .log("Get scheduled messages");
        if (delayedMessageMap.isEmpty()) {
            resetFixedDelayDetection();
            if (delayedMessagesCount.get() != 0) {
                log.warn()
                        .attr("delayedMessagesCount", delayedMessagesCount.get())
                        .log("Delayed message tracker is empty, but delayedMessagesCount is non-zero");
            }
        }

        updateTimer();
        return positions;
    }

    @Override
    public synchronized CompletableFuture<Void> clear() {
        this.delayedMessageMap.clear();
        this.delayedMessageIndex.clear();
        this.replayedMessageIndex.clear();
        this.retainReplayedMessages = false;
        this.delayedMessagesCount.set(0);
        this.memoryUsage.set(0);
        resetFixedDelayDetection();
        updateTimer();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Whether a normal read must skip this position: it is either delayed or has been handed to replay while
     * the normal cursor is still catching up after a rewind. Replay reads do not use this predicate.
     */
    public synchronized boolean containsMessage(long ledgerId, long entryId) {
        return containsMessage(delayedMessageIndex, ledgerId, entryId)
                || containsMessage(replayedMessageIndex, ledgerId, entryId);
    }

    private static boolean containsMessage(Long2ObjectMap<LongBitmap> messages, long ledgerId, long entryId) {
        LongBitmap index = messages.get(ledgerId);
        return index != null && index.contains(entryId);
    }

    /**
     * A new rewind must allow previously replayed, unacknowledged messages to be read again.
     */
    public synchronized void startRewind() {
        endRewind();
        retainReplayedMessages = true;
    }

    /**
     * Once normal reads have caught up, their replay suppression markers are no longer needed.
     */
    public synchronized void endRewind() {
        replayedMessageIndex.values().forEach(bitmap -> {
            long oldSize = bitmap.serializedSize();
            bitmap.clear();
            memoryUsage.addAndGet(bitmap.serializedSize() - oldSize);
        });
        replayedMessageIndex.clear();
        retainReplayedMessages = false;
    }

    /**
     * Drop positions acknowledged or expired while consumers were disconnected before reusing the index.
     */
    public synchronized void removeMessagesUpTo(Position position) {
        var iterator = delayedMessageMap.values().iterator();
        while (iterator.hasNext()) {
            Long2ObjectSortedMap<LongBitmap> ledgerMap = iterator.next();
            delayedMessagesCount.addAndGet(-removeMessagesUpTo(ledgerMap, position));
            if (ledgerMap.isEmpty()) {
                iterator.remove();
            }
        }
        removeMessagesUpTo(delayedMessageIndex, position);
        removeMessagesUpTo(replayedMessageIndex, position);
        if (delayedMessageMap.isEmpty()) {
            resetFixedDelayDetection();
        }
        updateTimer();
    }

    private long removeMessagesUpTo(Long2ObjectMap<LongBitmap> index, Position position) {
        long removed = 0;
        var iterator = index.long2ObjectEntrySet().iterator();
        while (iterator.hasNext()) {
            Long2ObjectMap.Entry<LongBitmap> entry = iterator.next();
            if (entry.getLongKey() > position.getLedgerId()) {
                continue;
            }
            LongBitmap bitmap = entry.getValue();
            long oldCount = bitmap.cardinality();
            long oldSize = bitmap.serializedSize();
            if (entry.getLongKey() < position.getLedgerId() || position.getEntryId() >= bitmap.lastPresentValue()) {
                bitmap.clear();
            } else {
                bitmap.remove(0, position.getEntryId() + 1);
            }
            removed += oldCount - bitmap.cardinality();
            memoryUsage.addAndGet(bitmap.serializedSize() - oldSize);
            if (bitmap.isEmpty()) {
                iterator.remove();
            }
        }
        return removed;
    }

    /**
     * Go back to the initial state of the fixed delay detection. It must be done whenever the tracker is emptied,
     * since the messages added afterwards are not related to the ones that were tracked before.
     */
    private void resetFixedDelayDetection() {
        highestDeliveryTimeTracked = 0;
        messagesHaveFixedDelay = true;
    }

    @Override
    public long getNumberOfDelayedMessages() {
        return delayedMessagesCount.get();
    }

    /**
     * Estimates memory usage of all bitmaps in the tracker.
     * Uses serialized size as an approximation of memory usage.
     * @return estimated memory usage in bytes
     */
    @Override
    public long getBufferMemoryUsage() {
        return memoryUsage.get();
    }

    @Override
    public synchronized void close() {
        clear();
        super.close();
    }

    @Override
    public synchronized boolean shouldPauseAllDeliveries() {
        // Pause deliveries if we know all delays are fixed within the lookahead window
        return fixedDelayDetectionLookahead > 0
                && messagesHaveFixedDelay
                && getNumberOfDelayedMessages() >= fixedDelayDetectionLookahead
                && !hasMessageAvailable();
    }

    protected synchronized long nextDeliveryTime() {
        return delayedMessageMap.firstLongKey();
    }
}
