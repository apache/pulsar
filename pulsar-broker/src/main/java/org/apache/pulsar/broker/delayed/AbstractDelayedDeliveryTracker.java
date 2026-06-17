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

import io.github.merlimat.slog.Logger;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;

public abstract class AbstractDelayedDeliveryTracker implements DelayedDeliveryTracker, TimerTask {

    private static final Logger LOG = Logger.get(AbstractDelayedDeliveryTracker.class);
    protected final Logger log;

    protected final DelayedDeliveryContext context;

    // Reference to the shared (per-broker) timer for delayed delivery
    protected final Timer timer;

    // Current timeout or null if not set. Guarded by timeoutLock.
    private Timeout timeout;

    // Timestamp at which the timeout is currently set. Guarded by timeoutLock.
    private long currentTimeoutTarget;

    // Last time the TimerTask was triggered for this class. Guarded by timeoutLock.
    private long lastTickRun;

    // Updated through resetTickTime() from dispatcher threads and read on the timer thread.
    protected volatile long tickTimeMillis;

    protected final Clock clock;

    private final boolean isDelayedDeliveryDeliverAtTimeStrict;

    private final Object triggerLock;
    // Guards the timer state (timeout, currentTimeoutTarget, lastTickRun) against concurrent access from
    // dispatcher threads (updateTimer/rescheduleTimer/close) and the timer thread (run). It is a leaf lock:
    // no subclass method is invoked while holding it.
    private final Object timeoutLock = new Object();

    public AbstractDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                          long tickTimeMillis,
                                          boolean isDelayedDeliveryDeliverAtTimeStrict) {
        this(new DispatcherDelayedDeliveryContext(dispatcher), timer, tickTimeMillis,
                Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict);
    }

    public AbstractDelayedDeliveryTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                          long tickTimeMillis, Clock clock,
                                          boolean isDelayedDeliveryDeliverAtTimeStrict) {
        this(new DispatcherDelayedDeliveryContext(dispatcher), timer, tickTimeMillis,
                clock, isDelayedDeliveryDeliverAtTimeStrict);
    }

    public AbstractDelayedDeliveryTracker(DelayedDeliveryContext context, Timer timer,
                                          long tickTimeMillis,
                                          boolean isDelayedDeliveryDeliverAtTimeStrict) {
        this(context, timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict);
    }

    public AbstractDelayedDeliveryTracker(DelayedDeliveryContext context, Timer timer,
                                          long tickTimeMillis, Clock clock,
                                          boolean isDelayedDeliveryDeliverAtTimeStrict) {
        this.context = context;
        this.triggerLock = context.getTriggerLock();
        this.timer = timer;
        this.tickTimeMillis = tickTimeMillis;
        this.clock = clock;
        this.isDelayedDeliveryDeliverAtTimeStrict = isDelayedDeliveryDeliverAtTimeStrict;
        this.log = LOG.with().attr("dispatcher", context.getName()).build();
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
    protected long getCutoffTime() {
        return isDelayedDeliveryDeliverAtTimeStrict ? clock.millis() : clock.millis() + tickTimeMillis;
    }

    protected boolean isDeliverAtTimeStrict() {
        return isDelayedDeliveryDeliverAtTimeStrict;
    }

    public void resetTickTime(long tickTime) {
        if (this.tickTimeMillis != tickTime) {
            this.tickTimeMillis = tickTime;
        }
    }

    /**
     * Update the delivery timer to fire when the next message in the tracker becomes due.
     *
     * Callers are expected to serialize all tracker state mutations (at the dispatcher or tracker level), so the
     * snapshot of {@link #getNumberOfDelayedMessages()} and {@link #nextDeliveryTime()} is taken before acquiring
     * timeoutLock. This keeps timeoutLock a leaf lock that never calls into subclass methods, ruling out lock
     * ordering deadlocks with subclasses that synchronize those methods on the tracker instance.
     */
    protected final void updateTimer() {
        long numberOfDelayedMessages = getNumberOfDelayedMessages();
        long nextDeliveryTimestamp = numberOfDelayedMessages > 0 ? nextDeliveryTime() : -1;
        synchronized (timeoutLock) {
            doUpdateTimer(numberOfDelayedMessages, nextDeliveryTimestamp);
        }
    }

    private void doUpdateTimer(long numberOfDelayedMessages, long timestamp) {
        if (numberOfDelayedMessages == 0) {
            if (timeout != null) {
                currentTimeoutTarget = -1;
                timeout.cancel();
                timeout = null;
            }
            return;
        }

        if (timestamp == currentTimeoutTarget) {
            // The timer is already set to the correct target time
            return;
        }

        if (timeout != null) {
            timeout.cancel();
            timeout = null;
        }
        // Reset the tracked state so a subsequent updateTimer() call cannot short-circuit on a stale
        // currentTimeoutTarget while no live timer remains. See #25996.
        currentTimeoutTarget = -1;

        long now = clock.millis();
        long delayMillis = timestamp - now;

        if (delayMillis <= 0) {
            // There are messages that are already ready to be delivered. If
            // the dispatcher is not getting them is because the consumer is
            // either not connected or slow.
            // We don't need to keep retriggering the timer. When the consumer
            // catches up, the dispatcher will do the readMoreEntries() and
            // get these messages.
            return;
        }

        // Compute the earliest time that we schedule the timer to run.
        long remainingTickDelayMillis = lastTickRun + tickTimeMillis - now;
        long calculatedDelayMillis = Math.max(delayMillis, remainingTickDelayMillis);
        log.debug().attr("delayMillis", calculatedDelayMillis)
                .log("Start timer");
        // Even though we may delay longer than this timestamp because of the tick delay, we still track the
        // current timeout with reference to the next message's timestamp.
        currentTimeoutTarget = timestamp;
        timeout = timer.newTimeout(this, calculatedDelayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run(Timeout triggeredTimeout) throws Exception {
        log.debug("Timer triggered");

        if (triggeredTimeout == null || triggeredTimeout.isCancelled()) {
            return;
        }

        synchronized (timeoutLock) {
            lastTickRun = clock.millis();
            // Only reset the timer state if the triggered timeout is the currently armed one. A timeout that
            // was already superseded by updateTimer()/rescheduleTimer() may still fire if it passed its
            // isCancelled() check before being cancelled; it must not clear the state of the newer timer.
            if (triggeredTimeout == this.timeout) {
                currentTimeoutTarget = -1;
                this.timeout = null;
            }
        }

        synchronized (triggerLock) {
            context.triggerReadMoreEntries();
        }
    }

    /**
     * Cancel the current timer (if any) and schedule the timer task to run after the given delay. Used by
     * subclasses to trigger a dispatch round from asynchronous completions instead of mutating the timer
     * state directly.
     */
    protected final void rescheduleTimer(long delayMillis) {
        synchronized (timeoutLock) {
            if (timeout != null) {
                timeout.cancel();
            }
            currentTimeoutTarget = -1;
            timeout = timer.newTimeout(this, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        synchronized (timeoutLock) {
            if (timeout != null) {
                timeout.cancel();
                timeout = null;
            }
            currentTimeoutTarget = -1;
        }
    }

    protected abstract long nextDeliveryTime();
}
