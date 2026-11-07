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
package org.apache.pulsar.broker.transaction.timeout;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;

/**
 * A timer-task implementation of {@link TransactionTimeoutTracker}.
 */
public class TransactionTimeoutTrackerImpl implements TransactionTimeoutTracker, TimerTask {

    private final Timer timer;
    private final TransactionTimeoutQueue priorityQueue = new TransactionTimeoutQueue();
    private final long tickTimeMillis;
    private final Clock clock;
    private final long tcId;
    private final TransactionMetadataStoreService transactionMetadataStoreService;

    // All mutable state is guarded by this tracker, including replay and close.
    private Timeout currentTimeout;
    private long nowTaskTimeoutTime;
    private boolean closed;

    TransactionTimeoutTrackerImpl(long tcId, Timer timer, long tickTimeMillis,
                                  TransactionMetadataStoreService transactionMetadataStoreService) {
        this(tcId, timer, tickTimeMillis, transactionMetadataStoreService, Clock.systemUTC());
    }

    @VisibleForTesting
    TransactionTimeoutTrackerImpl(long tcId, Timer timer, long tickTimeMillis,
                                  TransactionMetadataStoreService transactionMetadataStoreService, Clock clock) {
        this.tcId = tcId;
        this.transactionMetadataStoreService = transactionMetadataStoreService;
        this.timer = timer;
        this.tickTimeMillis = tickTimeMillis;
        this.clock = clock;
    }

    @Override
    public void addTransaction(long sequenceId, long timeout) {
        synchronized (this) {
            if (closed) {
                return;
            }
            if (timeout >= tickTimeMillis) {
                priorityQueue.add(sequenceId, clock.millis() + timeout);
                scheduleTimeout();
                return;
            }
        }
        transactionMetadataStoreService.endTransactionForTimeout(new TxnID(tcId, sequenceId));
    }

    @Override
    public synchronized void removeTransaction(long sequenceId) {
        if (!closed && priorityQueue.remove(sequenceId)) {
            scheduleTimeout();
        }
    }

    @Override
    public synchronized void replayAddTransaction(long sequenceId, long timeout) {
        if (!closed) {
            priorityQueue.add(sequenceId, timeout);
        }
    }

    @Override
    public synchronized void start() {
        if (!closed) {
            scheduleTimeout();
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        cancelTimeout();
        priorityQueue.clear();
    }

    /** Called with the tracker lock held. Keep an earlier timer to avoid rescheduling on every completion. */
    private void scheduleTimeout() {
        if (priorityQueue.isEmpty()) {
            // Keep one coordinator-level wakeup to reuse if another transaction arrives. It retains no txn record.
            return;
        }
        long nextTimeoutTime = priorityQueue.peekTimeout();
        // An expired timer may be waiting for this lock or draining due transactions outside it.
        if (currentTimeout != null && !currentTimeout.isCancelled()
                && nowTaskTimeoutTime <= nextTimeoutTime) {
            return;
        }
        cancelTimeout();
        nowTaskTimeoutTime = nextTimeoutTime;
        currentTimeout = timer.newTimeout(this, Math.max(0, nextTimeoutTime - clock.millis()), TimeUnit.MILLISECONDS);
    }

    private void cancelTimeout() {
        if (currentTimeout != null) {
            currentTimeout.cancel();
            currentTimeout = null;
        }
    }

    @Override
    public void run(Timeout timeout) {
        try {
            while (true) {
                long sequenceId;
                synchronized (this) {
                    // A cancelled timer may already be running when a new timer replaces it.
                    if (closed || timeout != currentTimeout || priorityQueue.isEmpty()
                            || priorityQueue.peekTimeout() > clock.millis()) {
                        return;
                    }
                    sequenceId = priorityQueue.peekSequenceId();
                    priorityQueue.pop();
                }
                // Completion can synchronously cancel another transaction. Remove first and call outside the lock.
                transactionMetadataStoreService.endTransactionForTimeout(new TxnID(tcId, sequenceId));
            }
        } finally {
            synchronized (this) {
                if (timeout == currentTimeout) {
                    currentTimeout = null;
                    if (!closed) {
                        scheduleTimeout();
                    }
                }
            }
        }
    }

    @VisibleForTesting
    synchronized int getTrackedTransactionCount() {
        return priorityQueue.size();
    }
}
