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

    // Current timeout or null if not set
    protected Timeout timeout;

    // Timestamp at which the timeout is currently set
    private long currentTimeoutTarget;

    // Last time the TimerTask was triggered for this class
    private long lastTickRun;

    protected long tickTimeMillis;

    protected final Clock clock;

    private final boolean isDelayedDeliveryDeliverAtTimeStrict;
    private final Object triggerLock;

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
            log.debug().attr("delayMillis", calculatedDelayMillis)
                    .log("Start timer");
                // Even though we may delay longer than this timestamp because of the tick delay, we still track the
        // current timeout with reference to the next message's timestamp.
        currentTimeoutTarget = timestamp;
        timeout = timer.newTimeout(this, calculatedDelayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run(Timeout timeout) throws Exception {
            log.debug("Timer triggered");
                if (timeout == null || timeout.isCancelled()) {
            return;
        }

        synchronized (triggerLock) {
            lastTickRun = clock.millis();
            currentTimeoutTarget = -1;
            this.timeout = null;
            context.triggerReadMoreEntries();
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
