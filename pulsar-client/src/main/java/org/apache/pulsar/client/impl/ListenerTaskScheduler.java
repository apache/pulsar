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
package org.apache.pulsar.client.impl;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Coalesces listener-drain requests on an asynchronous, serial FIFO executor.
 * An additional trigger requires a later turn: its arrival task may be queued behind the current drain.
 * Arrival tasks must be enqueued before their corresponding triggers. Rejected submissions reset the state
 * for a later notification to retry; delivery is not guaranteed while the executor rejects work.
 */
final class ListenerTaskScheduler implements Runnable {
    private static final int IDLE = 0;
    private static final int SCHEDULED = 1;
    private static final int TRIGGERED_AGAIN = 2;
    private static final AtomicIntegerFieldUpdater<ListenerTaskScheduler> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ListenerTaskScheduler.class, "state");

    private final Executor executor;
    private final Runnable drain;
    private volatile int state;

    ListenerTaskScheduler(Executor executor, Runnable drain) {
        this.executor = executor;
        this.drain = drain;
    }

    void trigger() {
        int previous = STATE_UPDATER.getAndUpdate(this,
                current -> current == IDLE ? SCHEDULED : TRIGGERED_AGAIN);
        if (previous == IDLE) {
            try {
                executor.execute(this);
            } catch (RejectedExecutionException error) {
                // A later notification can retry if the executor becomes available again.
                STATE_UPDATER.set(this, IDLE);
                throw error;
            }
        }
    }

    @Override
    public void run() {
        try {
            drain.run();
        } finally {
            if (STATE_UPDATER.getAndSet(this, IDLE) == TRIGGERED_AGAIN) {
                // Enqueue after pending arrival tasks, rather than draining again in this turn.
                // A concurrent trigger after the reset either schedules this turn or marks it for a later turn.
                trigger();
            }
        }
    }
}
