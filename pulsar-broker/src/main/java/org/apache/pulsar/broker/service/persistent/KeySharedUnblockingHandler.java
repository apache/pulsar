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
import java.util.function.LongSupplier;
import org.apache.bookkeeper.mledger.ManagedCursor;

class KeySharedUnblockingHandler {
    private static final int UNSET = -1;
    private static final int NO_PENDING_READ = 0;
    private final AtomicLong keySharedUnblockingMaxReadOpCounter = new AtomicLong(UNSET);
    private final ManagedCursor cursor;
    private final LongSupplier keySharedUnblockingIntervalMsSupplier;
    private final ScheduledExecutorService executor;
    private final Runnable cancelPendingRead;
    private final Runnable rescheduleReadImmediately;

    KeySharedUnblockingHandler(ManagedCursor cursor, LongSupplier keySharedUnblockingIntervalMsSupplier,
                               ScheduledExecutorService executor, Runnable cancelPendingRead,
                               Runnable rescheduleReadImmediately) {
        this.cursor = cursor;
        this.keySharedUnblockingIntervalMsSupplier = keySharedUnblockingIntervalMsSupplier;
        this.executor = executor;
        this.cancelPendingRead = cancelPendingRead;
        this.rescheduleReadImmediately = rescheduleReadImmediately;
    }

    public void unblock() {
        long readOpCountWhenPendingRead = cursor.hasPendingReadRequest() ? cursor.getReadOpCount() : NO_PENDING_READ;
        if (keySharedUnblockingMaxReadOpCounter.compareAndSet(UNSET, readOpCountWhenPendingRead)) {
            Runnable runnable = () -> {
                long maxReadOpCount = keySharedUnblockingMaxReadOpCounter.getAndSet(UNSET);
                if (maxReadOpCount != NO_PENDING_READ && cursor.getReadOpCount() == maxReadOpCount) {
                    cancelPendingRead.run();
                }
                // re-schedule read immediately, or join the next scheduled read
                rescheduleReadImmediately.run();
            };
            long unblockingDelay = keySharedUnblockingIntervalMsSupplier.getAsLong();
            if (unblockingDelay > 0) {
                executor.schedule(runnable, unblockingDelay, TimeUnit.MILLISECONDS);
            } else {
                runnable.run();
            }
        } else {
            long updatedValue = keySharedUnblockingMaxReadOpCounter.updateAndGet(
                    current -> current != UNSET ? Math.max(current, readOpCountWhenPendingRead) : UNSET);
            if (updatedValue == UNSET) {
                // retry
                unblock();
            }
        }
    }
}
