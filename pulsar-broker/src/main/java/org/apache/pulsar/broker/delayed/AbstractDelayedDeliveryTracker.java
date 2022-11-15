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
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;

@Slf4j
public abstract class AbstractDelayedDeliveryTracker implements DelayedDeliveryTracker, TimerTask {

    protected final PersistentDispatcherMultipleConsumers dispatcher;

    // Reference to the shared (per-broker) timer for delayed delivery
    protected final Timer timer;

    // Current timeout or null if not set
    protected Timeout timeout;

    // Timestamp at which the timeout is currently set
    private long currentTimeoutTarget;

    // Last time the TimerTask was triggered for this class
    private long lastTickRun;

    protected long tickTimeMillis;

    protected final Clock clock;

    private final boolean isDelayedDeliveryDeliverAtTimeStrict;

    public AbstractDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                          long tickTimeMillis,
                                          boolean isDelayedDeliveryDeliverAtTimeStrict) {
        this(dispatcher, timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict);
    }

    public AbstractDelayedDeliveryTracker(PersistentDispatcherMultipleConsumers dispatcher, Timer timer,
                                          long tickTimeMillis, Clock clock,
                                          boolean isDelayedDeliveryDeliverAtTimeStrict) {
        this.dispatcher = dispatcher;
        this.timer = timer;
        this.tickTimeMillis = tickTimeMillis;
        this.clock = clock;
        this.isDelayedDeliveryDeliverAtTimeStrict = isDelayedDeliveryDeliverAtTimeStrict;
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

    public void resetTickTime(long tickTime) {
        if (this.tickTimeMillis != tickTime) {
            this.tickTimeMillis = tickTime;
        }
    }

    protected void updateTimer() {
        if (getNumberOfDelayedMessages() == 0) {
            if (timeout != null) {
                currentTimeoutTarget = -1;
                timeout.cancel();
                timeout = null;
            }
            return;
        }
        long timestamp = nextDeliveryTime();
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
    }

    protected abstract long nextDeliveryTime();
}
