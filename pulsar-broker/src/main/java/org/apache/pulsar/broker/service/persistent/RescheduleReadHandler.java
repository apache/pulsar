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
import org.apache.bookkeeper.mledger.ManagedCursor;

/**
 * Reschedules reads so that the possible pending read is cancelled if it's waiting for more entries.
 * This will prevent the dispatcher in getting blocked when there are entries in the replay queue.
 */
class RescheduleReadHandler {
    private static final int UNSET = -1;
    private static final int NO_PENDING_READ = 0;
    private final AtomicLong maxReadOpCounter = new AtomicLong(UNSET);
    private final ManagedCursor cursor;
    private final LongSupplier readIntervalMsSupplier;
    private final ScheduledExecutorService executor;
    private final Runnable cancelPendingRead;
    private final Runnable rescheduleReadImmediately;
    private final BooleanSupplier hasEntriesInReplayQueue;

    RescheduleReadHandler(ManagedCursor cursor, LongSupplier readIntervalMsSupplier,
                          ScheduledExecutorService executor, Runnable cancelPendingRead,
                          Runnable rescheduleReadImmediately, BooleanSupplier hasEntriesInReplayQueue) {
        this.cursor = cursor;
        this.readIntervalMsSupplier = readIntervalMsSupplier;
        this.executor = executor;
        this.cancelPendingRead = cancelPendingRead;
        this.rescheduleReadImmediately = rescheduleReadImmediately;
        this.hasEntriesInReplayQueue = hasEntriesInReplayQueue;
    }

    public void rescheduleRead() {
        long readOpCountWhenPendingRead = cursor.hasPendingReadRequest() ? cursor.getReadOpCount() : NO_PENDING_READ;
        if (maxReadOpCounter.compareAndSet(UNSET, readOpCountWhenPendingRead)) {
            Runnable runnable = () -> {
                long maxReadOpCount = maxReadOpCounter.getAndSet(UNSET);
                if (maxReadOpCount != NO_PENDING_READ && cursor.getReadOpCount() == maxReadOpCount
                    && hasEntriesInReplayQueue.getAsBoolean()) {
                    cancelPendingRead.run();
                }
                // re-schedule read immediately, or join the next scheduled read
                rescheduleReadImmediately.run();
            };
            long rescheduleDelay = readIntervalMsSupplier.getAsLong();
            if (rescheduleDelay > 0) {
                executor.schedule(runnable, rescheduleDelay, TimeUnit.MILLISECONDS);
            } else {
                runnable.run();
            }
        } else {
            long updatedValue = maxReadOpCounter.updateAndGet(
                    current -> current != UNSET ? Math.max(current, readOpCountWhenPendingRead) : UNSET);
            if (updatedValue == UNSET) {
                // retry
                rescheduleRead();
            }
        }
    }
}
