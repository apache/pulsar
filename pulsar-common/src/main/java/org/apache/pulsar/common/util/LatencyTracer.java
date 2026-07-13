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
package org.apache.pulsar.common.util;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class LatencyTracer {

    private static final AtomicLongFieldUpdater<LatencyTracer> END_TIME_UPDATER = AtomicLongFieldUpdater.newUpdater(
            LatencyTracer.class, "endNs");
    private final Queue<Timepoint> timepoints;
    private final NanoTimeSupplier nanoTimeSupplier;
    private final long startNs;
    private volatile long endNs = -1L;

    public LatencyTracer(Queue<Timepoint> timepoints, NanoTimeSupplier nanoTimeSupplier) {
        this.timepoints = timepoints;
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.startNs = nanoTimeSupplier.getNanos();
    }

    public <T> CompletableFuture<T> trace(String message, CompletableFuture<T> future) {
        if (future.isDone()) {
            return future;
        }
        return future.whenComplete((__, ___) -> trace(message));
    }

    public void trace(String action) {
        timepoints.add(new Timepoint(action, nanoTimeSupplier.getNanos()));
    }

    public long latencyInMillis() {
        ensureEndTimeSet();
        return TimeUnit.NANOSECONDS.toMillis(endNs >= 0L ? endNs - startNs : System.nanoTime() - startNs);
    }

    public String latencyString() {
        ensureEndTimeSet();
        StringBuilder sb = new StringBuilder();
        sb.append("total: ").append(latencyInMillis()).append(" ms");
        long prevNs = startNs;
        for (final var tp : timepoints) {
            sb.append(", ").append(tp.name).append(": ");
            long latencyMs = TimeUnit.NANOSECONDS.toMillis(tp.timeInNanos - prevNs);
            if (latencyMs > 0) {
                sb.append(latencyMs).append(" ms");
            } else {
                sb.append(TimeUnit.NANOSECONDS.toMicros(tp.timeInNanos - prevNs)).append(" us");
            }
            prevNs = tp.timeInNanos;
        }
        if (prevNs != startNs) {
            sb.append(", done: ");
            final var latencyMs = TimeUnit.NANOSECONDS.toMillis(endNs - prevNs);
            if (latencyMs > 0) {
                sb.append(latencyMs).append(" ms");
            } else {
                sb.append(TimeUnit.NANOSECONDS.toMicros(endNs - prevNs)).append(" us");
            }
        }
        return sb.toString();
    }

    private void ensureEndTimeSet() {
        // An optimized approach to update end time only once:
        //  -1: the initial invalid value
        //  -2: only one thread can perform the CAS successfully and modify the end time to -2
        // Then this thread will use the system call to get timestamp only once.
        if (END_TIME_UPDATER.compareAndSet(this, -1L, -2L)) {
            endNs = nanoTimeSupplier.getNanos();
        }
    }

    public interface NanoTimeSupplier {

        long getNanos();
    }

    public record Timepoint(String name, long timeInNanos) {
    }
}
