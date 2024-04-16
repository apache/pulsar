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

package org.apache.pulsar.broker.qos;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link MonotonicSnapshotClock}.
 *
 * Starts a daemon thread that updates the snapshot value periodically with a configured interval. The close method
 * should be called to stop the thread.
 */
public class DefaultMonotonicSnapshotClock implements MonotonicSnapshotClock, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultMonotonicSnapshotClock.class);
    private final long sleepMillis;
    private final int sleepNanos;
    private final LongSupplier clockSource;
    private final Thread thread;
    private volatile long snapshotTickNanos;

    public DefaultMonotonicSnapshotClock(long snapshotIntervalNanos, LongSupplier clockSource) {
        if (snapshotIntervalNanos < TimeUnit.MILLISECONDS.toNanos(1)) {
            throw new IllegalArgumentException("snapshotIntervalNanos must be at least 1 millisecond");
        }
        this.sleepMillis = TimeUnit.NANOSECONDS.toMillis(snapshotIntervalNanos);
        this.sleepNanos = (int) (snapshotIntervalNanos - TimeUnit.MILLISECONDS.toNanos(sleepMillis));
        this.clockSource = clockSource;
        updateSnapshotTickNanos();
        thread = new Thread(this::snapshotLoop, getClass().getSimpleName() + "-update-loop");
        thread.setDaemon(true);
        thread.start();
    }

    /** {@inheritDoc} */
    @Override
    public long getTickNanos(boolean requestSnapshot) {
        if (requestSnapshot) {
            updateSnapshotTickNanos();
        }
        return snapshotTickNanos;
    }

    private void updateSnapshotTickNanos() {
        snapshotTickNanos = clockSource.getAsLong();
    }

    private void snapshotLoop() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                updateSnapshotTickNanos();
                try {
                    Thread.sleep(sleepMillis, sleepNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (Throwable t) {
            // report unexpected error since this would be a fatal error when the clock doesn't progress anymore
            // this is very unlikely to happen, but it's better to log it in any case
            LOG.error("Unexpected fatal error that stopped the clock.", t);
        }
    }

    @Override
    public void close() {
        thread.interrupt();
    }
}
