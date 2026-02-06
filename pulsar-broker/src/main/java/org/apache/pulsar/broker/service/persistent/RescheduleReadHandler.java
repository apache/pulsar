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
package org.apache.pulsar.broker.service.persistent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Reschedules reads so that the possible pending read is cancelled if it's waiting for more entries.
 * This will prevent the dispatcher in getting blocked when there are entries in the replay queue
 * that should be handled. This will also batch multiple calls together to reduce the number of
 * operations.
 */
@Slf4j
class RescheduleReadHandler {
    private static final int UNSET = -1;
    private static final int NO_PENDING_READ = 0;
    private final AtomicLong maxReadOpCounter = new AtomicLong(UNSET);
    private final LongSupplier readIntervalMsSupplier;
    private final ScheduledExecutorService executor;
    private final Runnable cancelPendingRead;
    private final Runnable rescheduleReadImmediately;
    private final BooleanSupplier hasPendingReadRequestThatMightWait;
    private final LongSupplier readOpCounterSupplier;
    private final BooleanSupplier hasEntriesInReplayQueue;

    RescheduleReadHandler(LongSupplier readIntervalMsSupplier,
                          ScheduledExecutorService executor, Runnable cancelPendingRead,
                          Runnable rescheduleReadImmediately, BooleanSupplier hasPendingReadRequestThatMightWait,
                          LongSupplier readOpCounterSupplier,
                          BooleanSupplier hasEntriesInReplayQueue) {
        this.readIntervalMsSupplier = readIntervalMsSupplier;
        this.executor = executor;
        this.cancelPendingRead = cancelPendingRead;
        this.rescheduleReadImmediately = rescheduleReadImmediately;
        this.hasPendingReadRequestThatMightWait = hasPendingReadRequestThatMightWait;
        this.readOpCounterSupplier = readOpCounterSupplier;
        this.hasEntriesInReplayQueue = hasEntriesInReplayQueue;
    }

    public void rescheduleRead() {
        long readOpCountWhenPendingRead =
                hasPendingReadRequestThatMightWait.getAsBoolean() ? readOpCounterSupplier.getAsLong() : NO_PENDING_READ;
        if (maxReadOpCounter.compareAndSet(UNSET, readOpCountWhenPendingRead)) {
            Runnable runnable = () -> {
                // Read the current value of maxReadOpCounter and set it to UNSET, this will allow scheduling a next
                // runnable
                long maxReadOpCount = maxReadOpCounter.getAndSet(UNSET);
                // Cancel a possible pending read if it's been waiting for more entries since the runnable was
                // scheduled. This is detected by checking that the value of the readOpCounter has not changed
                // since the runnable was scheduled. Canceling the read request will only be needed if there
                // are entries in the replay queue.
                if (maxReadOpCount != NO_PENDING_READ && readOpCounterSupplier.getAsLong() == maxReadOpCount
                        && hasEntriesInReplayQueue.getAsBoolean()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Cancelling pending read request because it's waiting for more entries");
                    }
                    cancelPendingRead.run();
                }
                // Re-schedule read immediately, or join the next scheduled read
                if (log.isDebugEnabled()) {
                    log.debug("Triggering read");
                }
                rescheduleReadImmediately.run();
            };
            long rescheduleDelay = readIntervalMsSupplier.getAsLong();
            if (rescheduleDelay > 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Scheduling after {} ms", rescheduleDelay);
                }
                executor.schedule(runnable, rescheduleDelay, TimeUnit.MILLISECONDS);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Running immediately");
                }
                runnable.run();
            }
        } else {
            // When there's a scheduled read, update the maxReadOpCounter to carry the state when the later scheduled
            // read was done
            long updatedValue = maxReadOpCounter.updateAndGet(
                    // Ignore updating if the value is UNSET
                    current -> current == UNSET ? UNSET :
                            // Prefer keeping NO_PENDING_READ if the latest value is NO_PENDING_READ
                            (readOpCountWhenPendingRead == NO_PENDING_READ ? NO_PENDING_READ :
                                    // Otherwise, keep the maximum value
                                    Math.max(current, readOpCountWhenPendingRead)));
            // If the value was unset, it means that the runnable was already run and retrying is needed
            // so that we don't miss any entries
            if (updatedValue == UNSET) {
                // Retry
                rescheduleRead();
            }
        }
    }
}
