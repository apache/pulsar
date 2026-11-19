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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LatencyTracer {

    private static final long PENDING = -1;

    protected final List<TracePoint> tracePoints;
    private final NanoTimeSupplier nanoTimeSupplier;
    private final long startNs;
    private final long startTimeInMillis;
    private volatile long endTimeInMillis;
    private final AtomicReference<String> failureReason = new AtomicReference<>();
    private final Deque<Runnable> pendingActionsCompletions = new ArrayDeque<>();
    private volatile boolean closed;

    /**
     * Creates a latency tracer.
     *
     * <p><strong>Compatibility note:</strong> This changes the common constructor API introduced in Pulsar 4.2.4.
     * Callers must no longer provide a {@code Queue<Timepoint>} and can retrieve trace points using
     * {@link #getTracePoints()}.
     */
    public LatencyTracer(NanoTimeSupplier nanoTimeSupplier) {
        this(nanoTimeSupplier, 16);
    }

    /**
     * Creates a latency tracer with space for the expected number of trace points.
     *
     * <p>Subclasses that know their maximum number of actions should provide it to avoid growing the trace point
     * list while tracing.
     */
    protected LatencyTracer(NanoTimeSupplier nanoTimeSupplier, int initialTracePointCapacity) {
        this.tracePoints = new ArrayList<>(initialTracePointCapacity);
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.startNs = nanoTimeSupplier.getNanos();
        this.startTimeInMillis = System.currentTimeMillis();
        this.endTimeInMillis = startTimeInMillis;
    }

    public <T> CompletableFuture<T> trace(String message, CompletableFuture<T> future) {
        if (closed) {
            return future;
        }
        TracePoint tracePoint = startTrace(message);
        if (tracePoint == null) {
            return future;
        }
        return future.whenComplete((__, throwable) -> finishTrace(tracePoint, throwable));
    }

    private void observeFailure(TracePoint tracePoint) {
        String reason = resolveFailureReason(tracePoint);
        if (reason != null) {
            setFailureReason(reason);
        }
    }

    protected String resolveFailureReason(TracePoint tracePoint) {
        StringBuilder reason = new StringBuilder(tracePoint.name());
        Throwable throwable = getTracePointFailure(tracePoint);
        String message = throwable.getMessage() == null ? throwable.getClass().getSimpleName() : throwable.getMessage();
        return reason.append(": ").append(message).toString();
    }

    protected Throwable getTracePointFailure(TracePoint tracePoint) {
        return tracePoint.failure;
    }

    public TracePoint startTrace(String action) {
        if (closed) {
            return null;
        }
        return startTraceInternal(action);
    }

    private synchronized TracePoint startTraceInternal(String action) {
        if (closed) {
            return null;
        }
        return addTracePoint(action, nanoTimeSupplier.getNanos(), PENDING);
    }

    public void finishTrace(TracePoint tracePoint, Throwable throwable) {
        if (tracePoint == null) {
            return;
        }
        synchronized (this) {
            finishTraceInternal(tracePoint, throwable);
        }
    }

    private void finishTraceInternal(TracePoint tracePoint, Throwable throwable) {
        if (throwable != null) {
            tracePoint.failure = throwable;
            observeFailure(tracePoint);
        }
        tracePoint.endNs = nanoTimeSupplier.getNanos();
        // A completion callback can reentrantly start a new action. Recheck pending actions before every callback so
        // subsequent callbacks wait for that action to complete.
        while (tracePoints.stream().noneMatch(TracePoint::isPending) && !pendingActionsCompletions.isEmpty()) {
            runRunnable(pendingActionsCompletions.removeFirst());
        }
    }

    public synchronized boolean isTracePending(String action) {
        return tracePoints.stream().anyMatch(tracePoint -> tracePoint.name.equals(action)
                && tracePoint.endNs == PENDING);
    }

    /**
     * Runs {@code runnable} after all pending actions have completed. Actions started before the runnable is executed
     * also delay it. If no action is pending, the runnable runs immediately.
     */
    public synchronized void runAfterPendingActionsComplete(Runnable runnable) {
        if (tracePoints.stream().anyMatch(TracePoint::isPending)) {
            pendingActionsCompletions.addLast(runnable);
        } else {
            runRunnable(runnable);
        }
    }

    private static void runRunnable(Runnable runnable) {
        try {
            runnable.run();
        } catch (RuntimeException e) {
            // Continue running the remaining completion callbacks.
        }
    }

    /**
     * Returns a copy of all trace points, including pending points.
     */
    public synchronized List<TracePoint> getTracePoints() {
        return copyTracePoints(tracePoints);
    }

    protected synchronized List<TracePoint> getPendingTracePoints() {
        return copyTracePoints(tracePoints.stream().filter(TracePoint::isPending).toList());
    }

    private static List<TracePoint> copyTracePoints(List<TracePoint> source) {
        List<TracePoint> result = new ArrayList<>(source.size());
        for (TracePoint tracePoint : source) {
            result.add(tracePoint.copy());
        }
        return result;
    }

    protected void setFailureReason(String reason) {
        failureReason.compareAndSet(null, reason);
    }

    protected String getFailureReason() {
        return failureReason.get();
    }

    protected Long getTimeoutTimeInMillis() {
        return null;
    }

    /**
     * Stops tracing. Subsequent trace requests are ignored without acquiring the tracer monitor.
     */
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    private TracePoint addTracePoint(String action, long startNs, long endNs) {
        TracePoint tracePoint = new TracePoint(action, startNs, endNs);
        tracePoints.add(tracePoint);
        return tracePoint;
    }

    public synchronized Snapshot getSnapshot() {
        final StringBuilder details = new StringBuilder();
        final StringBuilder pendingSteps = new StringBuilder();
        long latestEndNs = startNs;
        for (TracePoint tracePoint : tracePoints) {
            if (tracePoint.isPending()) {
                if (pendingSteps.length() > 0) {
                    pendingSteps.append(", ");
                }
                pendingSteps.append(tracePoint.name());
            } else {
                details.append(", ").append(tracePoint.name()).append(": ");
                long latencyNs = tracePoint.endNs - tracePoint.startNs;
                long latencyMs = TimeUnit.NANOSECONDS.toMillis(latencyNs);
                if (latencyMs > 0) {
                    details.append(latencyMs).append(" ms");
                } else {
                    details.append(TimeUnit.NANOSECONDS.toMicros(latencyNs)).append(" us");
                }
                latestEndNs = Math.max(latestEndNs, tracePoint.endNs);
            }
        }
        if (pendingSteps.length() > 0) {
            latestEndNs = nanoTimeSupplier.getNanos();
        }
        String failureReason = getFailureReason();
        Long timeoutTimeInMillis = getTimeoutTimeInMillis();
        long totalLatencyMs = TimeUnit.NANOSECONDS.toMillis(latestEndNs - startNs);
        endTimeInMillis = startTimeInMillis + totalLatencyMs;
        boolean completed = pendingSteps.length() == 0;
        boolean success = completed && failureReason == null && timeoutTimeInMillis == null;
        String state;
        if (failureReason != null || timeoutTimeInMillis != null) {
            state = "failure";
        } else if (!completed) {
            state = "still in progress";
        } else {
            state = "succeed";
        }
        StringBuilder description = new StringBuilder("state: ").append(state)
                .append(", start timestamp: ").append(startTimeInMillis)
                .append(", end timestamp: ").append(endTimeInMillis)
                .append(", total: ").append(totalLatencyMs).append(" ms").append(details);
        if (pendingSteps.length() > 0) {
            description.append(", pending steps: ").append(pendingSteps);
        }
        if (failureReason != null) {
            description.append(", failure reason: ").append(failureReason);
        }
        if (timeoutTimeInMillis != null) {
            description.append(", timeout timestamp: ").append(timeoutTimeInMillis);
        }
        return new Snapshot(completed, success, latestEndNs, totalLatencyMs, description.toString());
    }

    public interface NanoTimeSupplier {

        long getNanos();
    }

    public static final class TracePoint {
        private final String name;
        private final long startNs;
        private long endNs;
        private Throwable failure;

        private TracePoint(String name, long startNs, long endNs) {
            this.name = name;
            this.startNs = startNs;
            this.endNs = endNs;
        }

        public String name() {
            return name;
        }

        public long startTimeInNanos() {
            return startNs;
        }

        public long endTimeInNanos() {
            return endNs;
        }

        public boolean isPending() {
            return endNs == PENDING;
        }

        private TracePoint copy() {
            TracePoint copy = new TracePoint(name, startNs, endNs);
            copy.failure = failure;
            return copy;
        }
    }

    /**
     * A snapshot of the latency tracer at a given moment.
     *
     * <p><strong>Compatibility note:</strong> This changes the Pulsar 4.2.4 common API and makes the previous
     * canonical constructor unavailable.
     *
     * @param completed whether all trace points have completed
     * @param success whether all trace points have completed without a failure reason or timeout
     * @param endTimeInNanos the latest traced timestamp in nanoseconds
     * @param elapsedInMillis the elapsed time in milliseconds from when the tracer was created
     * @param description the trace summary, e.g. "state: succeed, start timestamp: 1000, end timestamp: 1100,
     *                    total: 100 ms, A: 60 ms, B: 40 ms"
     */
    public record Snapshot(boolean completed, boolean success, long endTimeInNanos, long elapsedInMillis,
                           String description) {
    }
}
