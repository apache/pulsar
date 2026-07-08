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

package org.apache.pulsar.utils;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class LatencyTracer {

    private final long startNs;
    private final List<Timepoint> timepoints;

    public LatencyTracer(int capacity) {
        this(System.nanoTime(), capacity);
    }

    @VisibleForTesting
    LatencyTracer(long startNs, int capacity) {
        this.startNs = startNs;
        this.timepoints = new ArrayList<>(capacity);
    }

    public <T> CompletableFuture<T> trace(String message, CompletableFuture<T> future) {
        if (future.isDone()) {
            return future;
        }
        return future.whenComplete((__, ___) -> trace(message, System.nanoTime()));
    }

    public void trace(String action) {
       trace(action, System.nanoTime());
    }

    @VisibleForTesting
    void trace(String action, long nanos) {
        timepoints.add(new Timepoint(action, nanos));
    }

    public long latencyInMillis() {
        if (timepoints.isEmpty()) {
            return 0;
        }
        return TimeUnit.NANOSECONDS.toMillis(timepoints.get(timepoints.size() - 1).timeInNanos - startNs);
    }

    public String latencyString() {
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
        return sb.toString();
    }

    private record Timepoint(String name, long timeInNanos) {
    }
}
