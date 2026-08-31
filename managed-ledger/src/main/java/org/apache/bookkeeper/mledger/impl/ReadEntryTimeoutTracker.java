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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.jctools.queues.MpscUnboundedArrayQueue;

@CustomLog
class ReadEntryTimeoutTracker implements AutoCloseable {
    private static final int READ_TIMEOUT_QUEUE_CHUNK_SIZE = 128 * 1024;
    private static final int CHECK_INTERVAL_SECONDS = 1;

    private final MpscUnboundedArrayQueue<ReadTimeoutWrapper> timeoutQueue = new MpscUnboundedArrayQueue<>(
            READ_TIMEOUT_QUEUE_CHUNK_SIZE);
    private final AtomicInteger timeoutQueueSize = new AtomicInteger();
    private final ScheduledFuture<?> timeoutTask;

    ReadEntryTimeoutTracker(OrderedScheduler scheduledExecutor) {
        this.timeoutTask = scheduledExecutor.scheduleAtFixedRate(catchingAndLoggingThrowables(this::checkTimeouts),
                CHECK_INTERVAL_SECONDS, CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    ReadTimeoutWrapper add(ManagedLedgerImpl.ReadEntryCallbackWrapper callback, long readOpCount,
                           long timeoutAtNanos) {
        ReadTimeoutWrapper timeout = new ReadTimeoutWrapper(readOpCount, timeoutAtNanos, callback);
        timeoutQueue.offer(timeout);
        timeoutQueueSize.incrementAndGet();
        return timeout;
    }

    @VisibleForTesting
    synchronized void checkTimeouts() {
        long now = System.nanoTime();
        int entriesToProcess = timeoutQueueSize.get();
        for (int i = 0; i < entriesToProcess; i++) {
            ReadTimeoutWrapper timeout = timeoutQueue.poll();
            if (timeout == null) {
                return;
            }
            timeoutQueueSize.decrementAndGet();
            ManagedLedgerImpl.ReadEntryCallbackWrapper callback = timeout.getCallback();
            if (callback == null) {
                continue;
            }
            if (!callback.shouldTriggerReadTimeout()) {
                timeout.clearCallback(callback);
                continue;
            }
            if (timeout.timeoutAtNanos > now) {
                requeue(timeout);
                continue;
            }
            callback = timeout.clearCallback();
            if (callback != null) {
                log.warn()
                        .attr("overdueNanos", now - timeout.timeoutAtNanos)
                        .log("Read entry timeout");
                callback.readFailed(createManagedLedgerException(BKException.Code.TimeoutException),
                        timeout.readOpCount);
            }
        }
    }

    private void requeue(ReadTimeoutWrapper timeout) {
        timeoutQueue.offer(timeout);
        timeoutQueueSize.incrementAndGet();
    }

    @Override
    public void close() {
        timeoutTask.cancel(false);
    }

    @VisibleForTesting
    int pendingTimeoutCount() {
        return timeoutQueueSize.get();
    }

    static final class ReadTimeoutWrapper {
        private static final AtomicReferenceFieldUpdater<ReadTimeoutWrapper,
                ManagedLedgerImpl.ReadEntryCallbackWrapper> CALLBACK_UPDATER = AtomicReferenceFieldUpdater
                .newUpdater(ReadTimeoutWrapper.class, ManagedLedgerImpl.ReadEntryCallbackWrapper.class, "callback");

        final long readOpCount;
        final long timeoutAtNanos;
        volatile ManagedLedgerImpl.ReadEntryCallbackWrapper callback;

        ReadTimeoutWrapper(long readOpCount, long timeoutAtNanos,
                           ManagedLedgerImpl.ReadEntryCallbackWrapper callback) {
            this.readOpCount = readOpCount;
            this.timeoutAtNanos = timeoutAtNanos;
            this.callback = callback;
        }

        ManagedLedgerImpl.ReadEntryCallbackWrapper getCallback() {
            return callback;
        }

        ManagedLedgerImpl.ReadEntryCallbackWrapper clearCallback() {
            return CALLBACK_UPDATER.getAndSet(this, null);
        }

        void clearCallback(ManagedLedgerImpl.ReadEntryCallbackWrapper callback) {
            CALLBACK_UPDATER.compareAndSet(this, callback, null);
        }
    }
}
