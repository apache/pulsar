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

/**
 * A clock source that is optimized for performance by updating the returned low precision monotonic
 * time in a separate thread with a configurable resolution.
 * This resolves a performance bottleneck on platforms where calls to System.nanoTime() are relatively
 * costly. For example, this happens on MacOS with Apple silicon CPUs (M1,M2,M3).
 * Instantiating this class creates a daemon thread that updates the monotonic time. The close method
 * should be called to stop the thread.
 */
public class GranularMonotonicClockSource implements MonotonicClockSource, AutoCloseable {
    private final long sleepMillis;
    private final int sleepNanos;
    private final LongSupplier clockSource;
    private final Thread thread;
    private volatile long lastNanos;

    public GranularMonotonicClockSource(long granularityNanos, LongSupplier clockSource) {
        this.sleepMillis = TimeUnit.NANOSECONDS.toMillis(granularityNanos);
        this.sleepNanos = (int) (granularityNanos - TimeUnit.MILLISECONDS.toNanos(sleepMillis));
        this.clockSource = clockSource;
        this.lastNanos = clockSource.getAsLong();
        thread = new Thread(this::updateLoop, getClass().getSimpleName() + "-update-loop");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public long getNanos(boolean highPrecision) {
        if (highPrecision) {
            long currentNanos = clockSource.getAsLong();
            lastNanos = currentNanos;
            return currentNanos;
        }
        return lastNanos;
    }

    private void updateLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            lastNanos = clockSource.getAsLong();
            try {
                Thread.sleep(sleepMillis, sleepNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public void close() {
        thread.interrupt();
    }
}
