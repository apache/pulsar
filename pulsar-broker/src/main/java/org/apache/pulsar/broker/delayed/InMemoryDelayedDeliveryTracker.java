/**
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
import java.time.Clock;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;

@Slf4j
public class InMemoryDelayedDeliveryTracker implements DelayedDeliveryTracker, TimerTask {

    protected final TripleLongPriorityQueue priorityQueue = new TripleLongPriorityQueue();

    private final PersistentDispatcherMultipleConsumers dispatcher;

    // Reference to the shared (per-broker) timer for delayed delivery
    private final Timer timer;

    // Current timeout or null if not set
    protected Timeout timeout;

    // Timestamp at which the timeout is currently set
    private long currentTimeoutTarget;

    // Last time the TimerTask was triggered for this class
    private long lastTickRun;

    private long tickTimeMillis;

    private final Clock clock;

    private final boolean isDelayedDeliveryDeliverAtTimeStrict;

    // If we detect that all messages have fixed delay time, such that the delivery is
    // always going to be in FIFO order, then we can avoid pulling all the messages in
    // tracker. Instead, we use the lookahead for detection and pause the read from
    // the cursor if the delays are fixed.
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

    InMemoryDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                   long tickTimeMillis, Clock clock,
                                   boolean isDelayedDeliveryDeliverAtTimeStrict,
                                   long fixedDelayDetectionLookahead) {
        this.dispatcher = dispatcher;
        this.timer = timer;
        this.tickTimeMillis = tickTimeMillis;
        this.clock = clock;
        this.isDelayedDeliveryDeliverAtTimeStrict = isDelayedDeliveryDeliverAtTimeStrict;
        this.fixedDelayDetectionLookahead = fixedDelayDetectionLookahead;
    }

    /**
     * When {@link #isDelayedDeliveryDeliverAtTimeStrict} is false, we allow for early delivery by as much as the
     * {@link #tickTimeMillis} because it is a slight optimization to let messages skip going back into the delay
     * tracker for a brief amount of time when we're already trying to dispatch to the consumer.
     *
     * When {@link #isDelayedDeliveryDeliverAtTimeStrict} is true, we use the current time to determine when messages
     * can be delivered. As a consequence, there are two delays that will affect delivery. The first is the
     * {@link #tickTimeMillis} and the second is the {@link Timer}'s granularity.
     *
     * @return the cutoff time to determine whether a message is ready to deliver to the consumer
     */
    private long getCutoffTime() {
        return isDelayedDeliveryDeliverAtTimeStrict ? clock.millis() : clock.millis() + tickTimeMillis;
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

        // Check that new delivery time comes after the current highest, or at
        // least within a single tick time interval of 1 second.
        if (deliverAt < (highestDeliveryTimeTracked - tickTimeMillis)) {
            messagesHaveFixedDelay = false;
        }

        highestDeliveryTimeTracked = Math.max(highestDeliveryTimeTracked, deliverAt);

        return true;
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
    public void resetTickTime(long tickTime) {

        if (this.tickTimeMillis != tickTime) {
            this.tickTimeMillis = tickTime;
        }
    }

    @Override
    public void clear() {
        this.priorityQueue.clear();
    }

    @Override
    public long getNumberOfDelayedMessages() {
        return priorityQueue.size();
    }

    /**
     * Update the scheduled timer task such that:
     * 1. If there are no delayed messages, return and do not schedule a timer task.
     * 2. If the next message in the queue has the same deliverAt time as the timer task, return and leave existing
     *    timer task in place.
     * 3. If the deliverAt time for the next delayed message has already passed (i.e. the delay is negative), return
     *    without scheduling a timer task since the subscription is backlogged.
     * 4. Else, schedule a timer task where the delay is the greater of these two: the next message's deliverAt time or
     *    the last tick time plus the tickTimeMillis (to ensure we do not schedule the task more frequently than the
     *    tickTimeMillis).
     */
    private void updateTimer() {
        if (priorityQueue.isEmpty()) {
            if (timeout != null) {
                currentTimeoutTarget = -1;
                timeout.cancel();
                timeout = null;
            }
            return;
        }

        long timestamp = priorityQueue.peekN1();
        if (timestamp == currentTimeoutTarget) {
            // The timer is already set to the correct target time
            return;
        }

        if (timeout != null) {
            timeout.cancel();
        }

        long now = clock.millis();
        long delayMillis = timestamp - now;

        if (delayMillis < 0) {
            // There are messages that are already ready to be delivered. If
            // the dispatcher is not getting them is because the consumer is
            // either not connected or slow.
            // We don't need to keep retriggering the timer. When the consumer
            // catches up, the dispatcher will do the readMoreEntries() and
            // get these messages
            return;
        }

        // Compute the earliest time that we schedule the timer to run.
        long remainingTickDelayMillis = lastTickRun + tickTimeMillis - now;
        long calculatedDelayMillis = Math.max(delayMillis, remainingTickDelayMillis);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Start timer in {} millis", dispatcher.getName(), calculatedDelayMillis);
        }

        // Even though we may delay longer than this timestamp because of the tick delay, we still track the
        // current timeout with reference to the next message's timestamp.
        currentTimeoutTarget = timestamp;
        timeout = timer.newTimeout(this, calculatedDelayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Timer triggered", dispatcher.getName());
        }
        if (timeout == null || timeout.isCancelled()) {
            return;
        }

        synchronized (dispatcher) {
            lastTickRun = clock.millis();
            currentTimeoutTarget = -1;
            this.timeout = null;
            dispatcher.readMoreEntries();
        }
    }

    @Override
    public void close() {
        if (timeout != null) {
            timeout.cancel();
            timeout = null;
        }
        priorityQueue.close();
    }

    @Override
    public boolean shouldPauseAllDeliveries() {
        // Pause deliveries if we know all delays are fixed within the lookahead window
        return fixedDelayDetectionLookahead > 0
                && messagesHaveFixedDelay
                && priorityQueue.size() >= fixedDelayDetectionLookahead
                && !hasMessageAvailable();
    }
}
