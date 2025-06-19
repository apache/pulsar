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
import io.netty.util.Timer;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.time.Clock;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.roaringbitmap.longlong.Roaring64Bitmap;

@Slf4j
public class InMemoryDelayedDeliveryTracker extends AbstractDelayedDeliveryTracker {

    // timestamp -> ledgerId -> entryId
    // AVL tree -> OpenHashMap -> RoaringBitmap
    protected final Long2ObjectSortedMap<Long2ObjectMap<Roaring64Bitmap>>
            delayedMessageMap = new Long2ObjectAVLTreeMap<>();

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

    InMemoryDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                   long tickTimeMillis,
                                   boolean isDelayedDeliveryDeliverAtTimeStrict,
                                   long fixedDelayDetectionLookahead) {
        this(dispatcher, timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
                fixedDelayDetectionLookahead);
    }

    public InMemoryDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                          long tickTimeMillis, Clock clock,
                                          boolean isDelayedDeliveryDeliverAtTimeStrict,
                                          long fixedDelayDetectionLookahead) {
        super(dispatcher, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict);
        this.fixedDelayDetectionLookahead = fixedDelayDetectionLookahead;
        this.timestampPrecisionBitCnt = calculateTimestampPrecisionBitCnt(tickTimeMillis);
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
    public boolean addMessage(long ledgerId, long entryId, long deliverAt) {
        if (deliverAt < 0 || deliverAt <= getCutoffTime()) {
            messagesHaveFixedDelay = false;
            return false;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Add message {}:{} -- Delivery in {} ms ", dispatcher.getName(), ledgerId, entryId,
                    deliverAt - clock.millis());
        }

        long timestamp = trimLowerBit(deliverAt, timestampPrecisionBitCnt);
        delayedMessageMap.computeIfAbsent(timestamp, k -> new Long2ObjectOpenHashMap<>())
                .computeIfAbsent(ledgerId, k -> new Roaring64Bitmap())
                .add(entryId);
        updateTimer();

        checkAndUpdateHighest(deliverAt);

        return true;
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
    public boolean hasMessageAvailable() {
        boolean hasMessageAvailable = !delayedMessageMap.isEmpty()
                && delayedMessageMap.firstLongKey() <= getCutoffTime();
        if (!hasMessageAvailable) {
            updateTimer();
        }
        return hasMessageAvailable;
    }

    /**
     * Get a set of position of messages that have already reached.
     */
    @Override
    public NavigableSet<Position> getScheduledMessages(int maxMessages) {
        int n = maxMessages;
        NavigableSet<Position> positions = new TreeSet<>();
        long cutoffTime = getCutoffTime();

        while (n > 0 && !delayedMessageMap.isEmpty()) {
            long timestamp = delayedMessageMap.firstLongKey();
            if (timestamp > cutoffTime) {
                break;
            }

            LongSet ledgerIdToDelete = new LongOpenHashSet();
            Long2ObjectMap<Roaring64Bitmap> ledgerMap = delayedMessageMap.get(timestamp);
            for (Long2ObjectMap.Entry<Roaring64Bitmap> ledgerEntry : ledgerMap.long2ObjectEntrySet()) {
                long ledgerId = ledgerEntry.getLongKey();
                Roaring64Bitmap entryIds = ledgerEntry.getValue();
                int cardinality = (int) entryIds.getLongCardinality();
                if (cardinality <= n) {
                    entryIds.forEach(entryId -> {
                        positions.add(PositionFactory.create(ledgerId, entryId));
                    });
                    n -= cardinality;
                    ledgerIdToDelete.add(ledgerId);
                } else {
                    long[] entryIdsArray = entryIds.toArray();
                    for (int i = 0; i < n; i++) {
                        positions.add(PositionFactory.create(ledgerId, entryIdsArray[i]));
                        entryIds.removeLong(entryIdsArray[i]);
                    }
                    n = 0;
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

        if (log.isDebugEnabled()) {
            log.debug("[{}] Get scheduled messages - found {}", dispatcher.getName(), positions.size());
        }

        if (delayedMessageMap.isEmpty()) {
            // Reset to initial state
            highestDeliveryTimeTracked = 0;
            messagesHaveFixedDelay = true;
        }

        updateTimer();
        return positions;
    }

    @Override
    public CompletableFuture<Void> clear() {
        this.delayedMessageMap.clear();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getNumberOfDelayedMessages() {
        return delayedMessageMap.values().stream().mapToLong(
                ledgerMap -> ledgerMap.values().stream().mapToLong(
                        Roaring64Bitmap::getLongCardinality).sum()).sum();
    }

    /**
     * This method rely on Roaring64Bitmap::getLongSizeInBytes to calculate the memory usage of the buffer.
     * The memory usage of the buffer is not accurate, because Roaring64Bitmap::getLongSizeInBytes will
     * overestimate the memory usage of the buffer a lot.
     * @return the memory usage of the buffer
     */
    @Override
    public long getBufferMemoryUsage() {
        return delayedMessageMap.values().stream().mapToLong(
                ledgerMap -> ledgerMap.values().stream().mapToLong(
                        Roaring64Bitmap::getLongSizeInBytes).sum()).sum();
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public boolean shouldPauseAllDeliveries() {
        // Pause deliveries if we know all delays are fixed within the lookahead window
        return fixedDelayDetectionLookahead > 0
                && messagesHaveFixedDelay
                && getNumberOfDelayedMessages() >= fixedDelayDetectionLookahead
                && !hasMessageAvailable();
    }

    protected long nextDeliveryTime() {
        return delayedMessageMap.firstLongKey();
    }
}
