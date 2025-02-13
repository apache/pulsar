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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link MonotonicSnapshotClock} optimized for use with {@link AsyncTokenBucket}.
 *
 * <p>
 * This class provides a monotonic snapshot value that consistently increases, ensuring reliable behavior
 * even in environments where the underlying clock source may not be strictly monotonic across all CPUs,
 * such as certain virtualized platforms.
 * </p>
 *
 * <p>
 * Upon instantiation, a daemon thread is launched to periodically update the snapshot value at a configured
 * interval. It is essential to invoke the {@link #close()} method to gracefully terminate this thread when it is
 * no longer needed.
 * </p>
 *
 * <p>
 * The {@link AsyncTokenBucket} utilizes this clock to obtain tick values. It does not require a consistent value on
 * every retrieval. However, when a consistent snapshot is necessary, the {@link #getTickNanos(boolean)} method
 * is called with the {@code requestSnapshot} parameter set to {@code true}.
 * </p>
 *
 * <p>
 * By employing a single thread to update the monotonic clock value, this implementation ensures that the snapshot
 * value remains strictly increasing. This approach mitigates potential inconsistencies that may arise from clock
 * source discrepancies across different CPUs.
 * </p>
 */
public class DefaultMonotonicSnapshotClock implements MonotonicSnapshotClock, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultMonotonicSnapshotClock.class);
    private final TickUpdaterThread tickUpdaterThread;
    private volatile long snapshotTickNanos;

    public DefaultMonotonicSnapshotClock(long snapshotIntervalNanos, LongSupplier clockSource) {
        if (snapshotIntervalNanos < TimeUnit.MILLISECONDS.toNanos(1)) {
            throw new IllegalArgumentException("snapshotIntervalNanos must be at least 1 millisecond");
        }
        tickUpdaterThread = new TickUpdaterThread(snapshotIntervalNanos,
                Objects.requireNonNull(clockSource, "clockSource must not be null"), this::setSnapshotTickNanos);
        tickUpdaterThread.start();
    }

    private void setSnapshotTickNanos(long snapshotTickNanos) {
        this.snapshotTickNanos = snapshotTickNanos;
    }

    /** {@inheritDoc} */
    @Override
    public long getTickNanos(boolean requestSnapshot) {
        if (requestSnapshot) {
            tickUpdaterThread.requestUpdateAndWait();
        }
        return snapshotTickNanos;
    }

    @Override
    public void close() {
        tickUpdaterThread.interrupt();
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
    private static class TickUpdaterThread extends Thread {
        private final Object tickUpdateDelayMonitor = new Object();
        private final Object tickUpdatedMonitor = new Object();
        private final MonotonicLeapDetectingTickUpdater tickUpdater;
        private volatile boolean running;
        private boolean tickUpdateDelayMonitorNotified;
        private AtomicLong requestCount = new AtomicLong();
        private final long sleepMillis;
        private final int sleepNanos;

        TickUpdaterThread(long snapshotIntervalNanos, LongSupplier clockSource, LongConsumer setSnapshotTickNanos) {
            super(DefaultMonotonicSnapshotClock.class.getSimpleName() + "-update-loop");
            // set as daemon thread so that it doesn't prevent the JVM from exiting
            setDaemon(true);
            // set the highest priority
            setPriority(MAX_PRIORITY);
            this.sleepMillis = TimeUnit.NANOSECONDS.toMillis(snapshotIntervalNanos);
            this.sleepNanos = (int) (snapshotIntervalNanos - TimeUnit.MILLISECONDS.toNanos(sleepMillis));
            tickUpdater = new MonotonicLeapDetectingTickUpdater(clockSource, setSnapshotTickNanos,
                    snapshotIntervalNanos);
        }

        @Override
        public void run() {
            try {
                running = true;
                long updatedForRequestCount = -1;
                while (!isInterrupted()) {
                    try {
                        // track if the thread has waited for the whole duration of the snapshot interval
                        // before updating the tick value
                        boolean waitedSnapshotInterval = false;
                        // sleep for the configured interval on a monitor that can be notified to stop the sleep
                        // and update the tick value immediately. This is used in requestUpdate method.
                        synchronized (tickUpdateDelayMonitor) {
                            tickUpdateDelayMonitorNotified = false;
                            // only wait if no explicit request has been made since the last update
                            if (requestCount.get() == updatedForRequestCount) {
                                // if no request has been made, sleep for the configured interval
                                tickUpdateDelayMonitor.wait(sleepMillis, sleepNanos);
                                waitedSnapshotInterval = !tickUpdateDelayMonitorNotified;
                            }
                        }
                        updatedForRequestCount = requestCount.get();
                        // update the tick value using the tick updater which will tolerate leaps backward
                        tickUpdater.update(waitedSnapshotInterval);
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

        private void notifyAllTickUpdated() {
            synchronized (tickUpdatedMonitor) {
                // notify all threads that are waiting for the tick value to be updated
                tickUpdatedMonitor.notifyAll();
            }
        }

        public void requestUpdateAndWait() {
            if (!running) {
                synchronized (tickUpdater) {
                    // thread has stopped running, fallback to update the value directly without optimizations
                    tickUpdater.update(false);
                }
                return;
            }
            // increment the request count that ensures that the thread will update the tick value after this request
            // was made also when there's a race condition between the request and the update
            // this solution doesn't prevent all races, and it's not guaranteed that the tick value is always updated
            // it will prevent the request having to wait for the delayed update cycle. This is sufficient for the
            // use case.
            requestCount.incrementAndGet();
            synchronized (tickUpdatedMonitor) {
                // notify the thread to stop waiting and update the tick value
                synchronized (tickUpdateDelayMonitor) {
                    tickUpdateDelayMonitorNotified = true;
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
            // wait until the thread is started and the tick value has been updated
            synchronized (tickUpdatedMonitor) {
                super.start();
                try {
                    tickUpdatedMonitor.wait();
                } catch (InterruptedException e) {
                    currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Handles updating the tick value in a monotonic way so that the value is always increasing,
     * regardless of leaps backward in the clock source value.
     */
    static class MonotonicLeapDetectingTickUpdater {
        private final LongSupplier clockSource;
        private final long snapshotInternalNanos;
        private final long maxDeltaNanosForLeapDetection;
        private final LongConsumer tickUpdatedCallback;
        private long referenceClockSourceValue = Long.MIN_VALUE;
        private long baseSnapshotTickNanos;
        private long previousSnapshotTickNanos;

        MonotonicLeapDetectingTickUpdater(LongSupplier clockSource, LongConsumer tickUpdatedCallback,
                                          long snapshotInternalNanos) {
            this.clockSource = clockSource;
            this.snapshotInternalNanos = snapshotInternalNanos;
            this.maxDeltaNanosForLeapDetection = 2 * snapshotInternalNanos;
            this.tickUpdatedCallback = tickUpdatedCallback;
        }

        /**
         * Updates the snapshot tick value. The tickUpdatedCallback is called if the value has changed.
         * The value is updated in a monotonic way so that the value is always increasing, regardless of leaps backward
         * in the clock source value.
         * Leap detection is done by comparing the new value with the previous value and the maximum delta value.
         *
         * @param waitedSnapshotInterval if true, the method has waited for the snapshot interval since the previous
         *                               call.
         */
        public void update(boolean waitedSnapshotInterval) {
            // get the current clock source value
            long clockValue = clockSource.getAsLong();

            // Initialization on first call
            if (referenceClockSourceValue == Long.MIN_VALUE) {
                referenceClockSourceValue = clockValue;
                baseSnapshotTickNanos = clockValue;
                previousSnapshotTickNanos = clockValue;
                // update the tick value using the callback
                tickUpdatedCallback.accept(clockValue);
                return;
            }

            // calculate the duration since the reference clock source value
            // so that the snapshot value is always increasing and tolerates it when the clock source is not strictly
            // monotonic across all CPUs and leaps backward
            long durationSinceReference = clockValue - referenceClockSourceValue;
            // calculate the new snapshot tick value as a duration since the reference clock source value
            // and add it to the base snapshot tick value
            long newSnapshotTickNanos = baseSnapshotTickNanos + durationSinceReference;

            // reset the reference clock source value if the clock source value leaps backward
            // more than the maximum delta value
            if (newSnapshotTickNanos < previousSnapshotTickNanos - maxDeltaNanosForLeapDetection) {
                // when the clock source value leaps backward, reset the reference value to the new value
                // for future duration calculations
                referenceClockSourceValue = clockValue;
                // if the updater thread has waited for the snapshot interval since the previous call,
                // increment the base snapshot tick value by the snapshot interval value
                long incrementWhenLeapDetected = waitedSnapshotInterval ? snapshotInternalNanos : 0;
                // set the base snapshot tick value to the new value
                baseSnapshotTickNanos = previousSnapshotTickNanos + incrementWhenLeapDetected;
                // set the new snapshot tick value to the base value
                newSnapshotTickNanos = baseSnapshotTickNanos;
            }

            // update snapshotTickNanos value if the new value is greater than the previous value
            if (newSnapshotTickNanos > previousSnapshotTickNanos) {
                // store the previous value
                previousSnapshotTickNanos = newSnapshotTickNanos;
                // update the tick value using the callback
                tickUpdatedCallback.accept(newSnapshotTickNanos);
            }
        }
    }
}
