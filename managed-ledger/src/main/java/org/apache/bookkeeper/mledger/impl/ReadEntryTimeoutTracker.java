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
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.jctools.queues.MpscUnboundedArrayQueue;

@CustomLog
class ReadEntryTimeoutTracker implements AutoCloseable {
    private static final int READ_TIMEOUT_QUEUE_CHUNK_SIZE = 128 * 1024;
    private static final int CHECK_INTERVAL_SECONDS = 1;

    private final MpscUnboundedArrayQueue<ManagedLedgerImpl.ReadEntryCallbackWrapper> timeoutQueue =
            new MpscUnboundedArrayQueue<>(READ_TIMEOUT_QUEUE_CHUNK_SIZE);
    private final AtomicInteger timeoutQueueSize = new AtomicInteger();
    private final ScheduledFuture<?> timeoutTask;

    ReadEntryTimeoutTracker(OrderedScheduler scheduledExecutor) {
        this.timeoutTask = scheduledExecutor.scheduleAtFixedRate(catchingAndLoggingThrowables(this::checkTimeouts),
                CHECK_INTERVAL_SECONDS, CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    void add(ManagedLedgerImpl.ReadEntryCallbackWrapper callback) {
        timeoutQueue.offer(callback);
        timeoutQueueSize.incrementAndGet();
    }

    @VisibleForTesting
    synchronized void checkTimeouts() {
        long now = System.nanoTime();
        // This is intentionally O(all tracked reads). Read-entry timeout is disabled by default, the
        // tracker runs once per second, and the MPSC queue keeps this maintenance path cheap while
        // avoiding per-ledger timeout tasks. The assumption is that deployments enabling this feature
        // prefer one lightweight broker-level scan over creating a scheduled task per ManagedLedger or
        // per read. Expired callbacks are handed back to the owning ledger's executor, so this shared
        // scan does not serialize completion callbacks across unrelated ledgers.
        int entriesToProcess = timeoutQueueSize.get();
        for (int i = 0; i < entriesToProcess; i++) {
            ManagedLedgerImpl.ReadEntryCallbackWrapper callback = timeoutQueue.poll();
            if (callback == null) {
                return;
            }
            timeoutQueueSize.decrementAndGet();
            if (callback.isCompleted()) {
                continue;
            }
            if (callback.timeoutAtNanos > now) {
                requeue(callback);
                continue;
            }
            if (callback.triggerReadTimeout(createManagedLedgerException(BKException.Code.TimeoutException))) {
                log.warn()
                        .attr("ledgerName", callback.managedLedgerName)
                        .attr("ledgerId", callback.ledgerId)
                        .attr("entryId", callback.entryId)
                        .attr("overdueNanos", now - callback.timeoutAtNanos)
                        .log("Read entry timeout");
            }
        }
    }

    private void requeue(ManagedLedgerImpl.ReadEntryCallbackWrapper callback) {
        timeoutQueue.offer(callback);
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
}
