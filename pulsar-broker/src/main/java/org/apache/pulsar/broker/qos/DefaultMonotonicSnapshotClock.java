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

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link MonotonicSnapshotClock}.
 *
 * Starts a daemon thread that updates the snapshot value periodically with a configured interval. The close method
 * should be called to stop the thread.
 * A single thread is used to update the monotonic clock value so that the snapshot value is always increasing,
 * even if the clock source is not strictly monotonic across all CPUs. This might be the case in some virtualized
 * environments.
 */
public class DefaultMonotonicSnapshotClock implements MonotonicSnapshotClock, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultMonotonicSnapshotClock.class);
    private final long sleepMillis;
    private final int sleepNanos;
    private final LongSupplier clockSource;
    private final TickUpdaterThread tickUpdaterThread;
    private final long snapshotIntervalNanos;
    private volatile long snapshotTickNanos;

    public DefaultMonotonicSnapshotClock(long snapshotIntervalNanos, LongSupplier clockSource) {
        if (snapshotIntervalNanos < TimeUnit.MILLISECONDS.toNanos(1)) {
            throw new IllegalArgumentException("snapshotIntervalNanos must be at least 1 millisecond");
        }
        this.sleepMillis = TimeUnit.NANOSECONDS.toMillis(snapshotIntervalNanos);
        this.sleepNanos = (int) (snapshotIntervalNanos - TimeUnit.MILLISECONDS.toNanos(sleepMillis));
        this.clockSource = Objects.requireNonNull(clockSource, "clockSource must not be null");
        this.snapshotIntervalNanos = snapshotIntervalNanos;
        tickUpdaterThread = new TickUpdaterThread();
        tickUpdaterThread.start();
    }

    /** {@inheritDoc} */
    @Override
    public long getTickNanos(boolean requestSnapshot) {
        if (requestSnapshot) {
            tickUpdaterThread.requestUpdate();
        }
        return snapshotTickNanos;
    }

    /**
     * A thread that updates snapshotTickNanos value periodically with a configured interval.
     * The thread is started when the DefaultMonotonicSnapshotClock is created and runs until the close method is
     * called.
     * A single thread is used to read the clock source value since on some hardware of virtualized platforms,
     * System.nanoTime() isn't strictly monotonic across all CPUs. Reading by a single thread will improve the
     * stability of the read value since a single thread is scheduled on a single CPU. If the thread is migrated
     * to another CPU, the clock source value might leap backward or forward, but logic in this class will handle it.
     */
    private class TickUpdaterThread extends Thread {
        private final Object tickUpdateDelayMonitor = new Object();
        private final Object tickUpdatedMonitor = new Object();
        private final long maxDelta;
        private long referenceClockSourceValue;
        private long baseSnapshotTickNanos;
        private long previousSnapshotTickNanos;
        private volatile boolean running;
        private boolean tickUpdateDelayMonitorNotified;
        private long requestCount;

        TickUpdaterThread() {
            super(DefaultMonotonicSnapshotClock.class.getSimpleName() + "-update-loop");
            // set as daemon thread so that it doesn't prevent the JVM from exiting
            setDaemon(true);
            // set the highest priority
            setPriority(MAX_PRIORITY);
            this.maxDelta = 2 * snapshotIntervalNanos;
        }

        @Override
        public void run() {
            try {
                running = true;
                long updatedForRequestCount = -1;
                while (!isInterrupted()) {
                    try {
                        boolean snapshotRequested = false;
                        // sleep for the configured interval on a monitor that can be notified to stop the sleep
                        // and update the tick value immediately. This is used in requestUpdate method.
                        synchronized (tickUpdateDelayMonitor) {
                            tickUpdateDelayMonitorNotified = false;
                            // only wait if no explicit request has been made since the last update
                            if (requestCount == updatedForRequestCount) {
                                // if no request has been made, sleep for the configured interval
                                tickUpdateDelayMonitor.wait(sleepMillis, sleepNanos);
                                snapshotRequested = tickUpdateDelayMonitorNotified;
                            }
                            updatedForRequestCount = requestCount;
                        }
                        updateSnapshotTickNanos(snapshotRequested);
                        notifyAllTickUpdated();
                    } catch (InterruptedException e) {
                        interrupt();
                        break;
                    }
                }
            } catch (Throwable t) {
                // report unexpected error since this would be a fatal error when the clock doesn't progress anymore
                // this is very unlikely to happen, but it's better to log it in any case
                LOG.error("Unexpected fatal error that stopped the clock.", t);
            } finally {
                LOG.info("DefaultMonotonicSnapshotClock's TickUpdaterThread stopped. {},tid={}", this, getId());
                running = false;
                notifyAllTickUpdated();
            }
        }

        private void updateSnapshotTickNanos(boolean snapshotRequested) {
            long clockValue = clockSource.getAsLong();

            // Initialization
            if (referenceClockSourceValue == 0) {
                referenceClockSourceValue = clockValue;
                baseSnapshotTickNanos = clockValue;
                snapshotTickNanos = clockValue;
                previousSnapshotTickNanos = clockValue;
                return;
            }

            // calculate the duration since the reference clock source value
            // so that the snapshot value is always increasing and tolerates it when the clock source is not strictly
            // monotonic across all CPUs and leaps backward or forward
            long durationSinceReference = clockValue - referenceClockSourceValue;
            long newSnapshotTickNanos = baseSnapshotTickNanos + durationSinceReference;

            // reset the reference clock source value if the clock source value leaps backward or forward
            if (newSnapshotTickNanos < previousSnapshotTickNanos - maxDelta
                    || newSnapshotTickNanos > previousSnapshotTickNanos + maxDelta) {
                referenceClockSourceValue = clockValue;
                baseSnapshotTickNanos = previousSnapshotTickNanos;
                if (!snapshotRequested) {
                    // if the snapshot value is not requested, increment by the snapshot interval
                    baseSnapshotTickNanos += snapshotIntervalNanos;
                }
                newSnapshotTickNanos = baseSnapshotTickNanos;
            }

            // update snapshotTickNanos value if the new value is greater than the previous value
            if (newSnapshotTickNanos > previousSnapshotTickNanos) {
                snapshotTickNanos = newSnapshotTickNanos;
                // store into a field so that we don't need to do a volatile read to find out the previous value
                previousSnapshotTickNanos = newSnapshotTickNanos;
            }
        }

        private void notifyAllTickUpdated() {
            synchronized (tickUpdatedMonitor) {
                // notify all threads that are waiting for the tick value to be updated
                tickUpdatedMonitor.notifyAll();
            }
        }

        public void requestUpdate() {
            if (!running) {
                // thread has stopped running, fallback to update the value directly without any optimizations
                snapshotTickNanos = clockSource.getAsLong();
                return;
            }
            synchronized (tickUpdatedMonitor) {
                // notify the thread to stop waiting and update the tick value
                synchronized (tickUpdateDelayMonitor) {
                    tickUpdateDelayMonitorNotified = true;
                    requestCount++;
                    tickUpdateDelayMonitor.notify();
                }
                // wait until the tick value has been updated
                try {
                    tickUpdatedMonitor.wait();
                } catch (InterruptedException e) {
                    currentThread().interrupt();
                }
            }
        }

        @Override
        public synchronized void start() {
            super.start();
            // wait until the thread is started and the tick value has been updated
            synchronized (tickUpdatedMonitor) {
                try {
                    tickUpdatedMonitor.wait();
                } catch (InterruptedException e) {
                    currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void close() {
        tickUpdaterThread.interrupt();
    }
}
