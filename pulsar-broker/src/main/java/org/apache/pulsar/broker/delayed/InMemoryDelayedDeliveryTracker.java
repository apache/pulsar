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
import java.time.Clock;
import java.util.NavigableSet;
import java.util.TreeSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;

@Slf4j
public class InMemoryDelayedDeliveryTracker extends AbstractDelayedDeliveryTracker {

    protected final TripleLongPriorityQueue priorityQueue = new TripleLongPriorityQueue();

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

    InMemoryDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher, Timer timer, long tickTimeMillis,
                                   boolean isDelayedDeliveryDeliverAtTimeStrict,
                                   long fixedDelayDetectionLookahead) {
        this(dispatcher, timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
                fixedDelayDetectionLookahead);
    }

    public InMemoryDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                   long tickTimeMillis, Clock clock,
                                   boolean isDelayedDeliveryDeliverAtTimeStrict,
                                   long fixedDelayDetectionLookahead) {
        super(dispatcher, timer, tickTimeMillis, clock, isDelayedDeliveryDeliverAtTimeStrict);
        this.fixedDelayDetectionLookahead = fixedDelayDetectionLookahead;
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

        priorityQueue.add(deliverAt, ledgerId, entryId);
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
        boolean hasMessageAvailable = !priorityQueue.isEmpty() && priorityQueue.peekN1() <= getCutoffTime();
        if (!hasMessageAvailable) {
            updateTimer();
        }
        return hasMessageAvailable;
    }

    /**
     * Get a set of position of messages that have already reached.
     */
    @Override
    public NavigableSet<PositionImpl> getScheduledMessages(int maxMessages) {
        int n = maxMessages;
        NavigableSet<PositionImpl> positions = new TreeSet<>();
        long cutoffTime = getCutoffTime();

        while (n > 0 && !priorityQueue.isEmpty()) {
            long timestamp = priorityQueue.peekN1();
            if (timestamp > cutoffTime) {
                break;
            }

            long ledgerId = priorityQueue.peekN2();
            long entryId = priorityQueue.peekN3();
            positions.add(new PositionImpl(ledgerId, entryId));

            priorityQueue.pop();
            --n;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Get scheduled messages - found {}", dispatcher.getName(), positions.size());
        }

        if (priorityQueue.isEmpty()) {
            // Reset to initial state
            highestDeliveryTimeTracked = 0;
            messagesHaveFixedDelay = true;
        }

        updateTimer();
        return positions;
    }

    @Override
    public void clear() {
        this.priorityQueue.clear();
    }

    @Override
    public long getNumberOfDelayedMessages() {
        return priorityQueue.size();
    }

    @Override
    public long getBufferMemoryUsage() {
        return priorityQueue.bytesCapacity();
    }

    @Override
    public void close() {
        super.close();
        priorityQueue.close();
    }

    @Override
    public boolean shouldPauseAllDeliveries() {
        // Pause deliveries if we know all delays are fixed within the lookahead window
        return fixedDelayDetectionLookahead > 0
                && messagesHaveFixedDelay
                && getNumberOfDelayedMessages() >= fixedDelayDetectionLookahead
                && !hasMessageAvailable();
    }

    @Override
    public boolean containsMessage(long ledgerId, long entryId) {
        return false;
    }

    protected long nextDeliveryTime() {
        return priorityQueue.peekN1();
    }
}
