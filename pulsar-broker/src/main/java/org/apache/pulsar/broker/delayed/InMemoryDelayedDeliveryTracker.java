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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;

@Slf4j
public class InMemoryDelayedDeliveryTracker implements DelayedDeliveryTracker, TimerTask {

    private final TripleLongPriorityQueue priorityQueue = new TripleLongPriorityQueue();

    private final PersistentDispatcherMultipleConsumers dispatcher;

    // Reference to the shared (per-broker) timer for delayed delivery
    private final Timer timer;

    // Current timeout or null if not set
    private Timeout timeout;

    // Timestamp at which the timeout is currently set
    private long currentTimeoutTarget;

    private long tickTimeMillis;

    private final Clock clock;

    InMemoryDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher, Timer timer, long tickTimeMillis) {
        this(dispatcher, timer, tickTimeMillis, Clock.systemUTC());
    }

    InMemoryDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                   long tickTimeMillis, Clock clock) {
        this.dispatcher = dispatcher;
        this.timer = timer;
        this.tickTimeMillis = tickTimeMillis;
        this.clock = clock;
    }

    @Override
    public boolean addMessage(long ledgerId, long entryId, long deliveryAt) {
        long now = clock.millis();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Add message {}:{} -- Delivery in {} ms ", dispatcher.getName(), ledgerId, entryId,
                    deliveryAt - now);
        }
        if (deliveryAt < (now + tickTimeMillis)) {
            // It's already about time to deliver this message. We add the buffer of
            // `tickTimeMillis` because messages can be extracted from the tracker
            // slightly before the expiration time. We don't want the messages to
            // go back into the delay tracker (for a brief amount of time) when we're
            // trying to dispatch to the consumer.
            return false;
        }

        priorityQueue.add(deliveryAt, ledgerId, entryId);
        updateTimer();
        return true;
    }

    /**
     * Return true if there's at least a message that is scheduled to be delivered already.
     */
    @Override
    public boolean hasMessageAvailable() {
        // Avoid the TimerTask run before reach the timeout.
        long cutOffTime = clock.millis() + tickTimeMillis;
        boolean hasMessageAvailable = !priorityQueue.isEmpty() && priorityQueue.peekN1() <= cutOffTime;
        if (!hasMessageAvailable) {
            // prevent the first delay message later than cutoffTime
            updateTimer();
        }
        return hasMessageAvailable;
    }

    /**
     * Get a set of position of messages that have already reached.
     */
    @Override
    public Set<PositionImpl> getScheduledMessages(int maxMessages) {
        int n = maxMessages;
        Set<PositionImpl> positions = new TreeSet<>();
        long now = clock.millis();
        // Pick all the messages that will be ready within the tick time period.
        // This is to avoid keeping rescheduling the timer for each message at
        // very short delay
        long cutoffTime = now + tickTimeMillis;

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
        updateTimer();
        return positions;
    }

    @Override
    public void resetTickTime(long tickTime) {
        if (this.tickTimeMillis != tickTime){
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

        long delayMillis = timestamp - clock.millis();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Start timer in {} millis", dispatcher.getName(), delayMillis);
        }

        if (delayMillis < 0) {
            // There are messages that are already ready to be delivered. If
            // the dispatcher is not getting them is because the consumer is
            // either not connected or slow.
            // We don't need to keep retriggering the timer. When the consumer
            // catches up, the dispatcher will do the readMoreEntries() and
            // get these messages
            return;
        }

        currentTimeoutTarget = timestamp;
        timeout = timer.newTimeout(this, delayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Timer triggered", dispatcher.getName());
        }
        if (timeout.isCancelled()) {
            return;
        }

        synchronized (dispatcher) {
            currentTimeoutTarget = -1;
            timeout = null;
            dispatcher.readMoreEntries();
        }
    }

    @Override
    public void close() {
        priorityQueue.close();
        if (timeout != null) {
            timeout.cancel();
        }
    }
}
