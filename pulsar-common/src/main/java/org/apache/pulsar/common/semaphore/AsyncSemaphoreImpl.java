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
package org.apache.pulsar.common.semaphore;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import org.apache.pulsar.common.util.Runnables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of AsyncSemaphore with timeout and queue size limits.
 */
public class AsyncSemaphoreImpl implements AsyncSemaphore, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AsyncSemaphoreImpl.class);

    private final AtomicLong availablePermits;
    private final Queue<PendingRequest> queue;
    private final long maxPermits;
    private final long timeoutMillis;
    private final ScheduledExecutorService executor;
    private final boolean shutdownExecutor;
    private final LongConsumer queueLatencyRecorder;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Runnable processQueueRunnable = Runnables.catchingAndLoggingThrowables(this::internalProcessQueue);

    /**
     * Creates an AsyncSemaphoreImpl with the given parameters.
     * @param maxPermits max number of permits available for acquisition
     * @param maxQueueSize max number of requests that can be queued
     * @param timeoutMillis timeout in milliseconds for acquiring permits
     */
    public AsyncSemaphoreImpl(long maxPermits, int maxQueueSize, long timeoutMillis) {
        this(maxPermits, maxQueueSize, timeoutMillis, maxPermits > 0 ? createExecutor() : null, maxPermits > 0, null);
    }

    /**
     * Creates an AsyncSemaphoreImpl with the given parameters.
     * @param maxPermits max number of permits available for acquisition
     * @param maxQueueSize max number of requests that can be queued
     * @param timeoutMillis timeout in milliseconds for acquiring permits
     * @param executor executor service to use for scheduling timeouts, it is expected to be single threaded
     * @param queueLatencyRecorder consumer to record queue latency, Long.MAX_VALUE is used for requests that timed out
     */
    public AsyncSemaphoreImpl(long maxPermits, int maxQueueSize, long timeoutMillis,
                              ScheduledExecutorService executor, LongConsumer queueLatencyRecorder) {
        this(maxPermits, maxQueueSize, timeoutMillis, executor, false, queueLatencyRecorder);
    }

    AsyncSemaphoreImpl(long maxPermits, int maxQueueSize, long timeoutMillis, ScheduledExecutorService executor,
                       boolean shutdownExecutor, LongConsumer queueLatencyRecorder) {
        this.availablePermits = new AtomicLong(maxPermits);
        this.maxPermits = maxPermits;
        this.queue = new ArrayBlockingQueue<>(maxQueueSize);
        this.timeoutMillis = timeoutMillis;
        this.executor = executor;
        this.shutdownExecutor = shutdownExecutor;
        this.queueLatencyRecorder = queueLatencyRecorder;
    }

    private static ScheduledExecutorService createExecutor() {
        return Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("async-semaphore-executor"));
    }

    @Override
    public CompletableFuture<AsyncSemaphorePermit> acquire(long permits, BooleanSupplier isCancelled) {
        return internalAcquire(permits, permits, isCancelled);
    }

    private CompletableFuture<AsyncSemaphorePermit> internalAcquire(long permits, long acquirePermits,
                                                                    BooleanSupplier isCancelled) {
        if (permits < 0 || (permits > maxPermits && !isUnbounded())) {
            throw new IllegalArgumentException("Invalid permits value: " + permits);
        }

        // if maximum permits is <= 0, then the semaphore is unbounded
        if (isUnbounded()) {
            return CompletableFuture.completedFuture(new SemaphorePermit(permits));
        }

        CompletableFuture<AsyncSemaphorePermit> future = new CompletableFuture<>();

        if (closed.get()) {
            future.completeExceptionally(new PermitAcquireAlreadyClosedException("Semaphore is closed"));
            return future;
        }

        PendingRequest request = new PendingRequest(permits, acquirePermits, future, isCancelled);
        if (!queue.offer(request)) {
            future.completeExceptionally(new PermitAcquireQueueFullException(
                    "Semaphore queue is full"));
            return future;
        }
        // Schedule timeout
        ScheduledFuture<?> timeoutTask = executor.schedule(() -> {
            if (!request.future.isDone() && queue.remove(request)) {
                // timeout is recorded with Long.MAX_VALUE as the age
                recordQueueLatency(Long.MAX_VALUE);
                // also record the time in the queue
                recordQueueLatency(request.getAgeNanos());
                future.completeExceptionally(new PermitAcquireTimeoutException(
                        "Permit acquisition timed out"));
                // the next request might have smaller permits and that might be processed
                processQueue();
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);
        request.setTimeoutTask(timeoutTask);

        processQueue();
        return future;
    }

    private boolean isUnbounded() {
        return maxPermits <= 0;
    }

    private void recordQueueLatency(long ageNanos) {
        if (queueLatencyRecorder != null) {
            queueLatencyRecorder.accept(ageNanos);
        }
    }

    @Override
    public CompletableFuture<AsyncSemaphorePermit> update(AsyncSemaphorePermit permit, long newPermits,
                                                          BooleanSupplier isCancelled) {
        if (newPermits < 0 || (newPermits > maxPermits && !isUnbounded())) {
            throw new IllegalArgumentException("Invalid permits value: " + newPermits);
        }
        if (isUnbounded()) {
            return CompletableFuture.completedFuture(new SemaphorePermit(newPermits));
        }
        long oldPermits = permit.getPermits();
        long additionalPermits = newPermits - oldPermits;
        // mark the old permits as released without adding the permits to availablePermits
        castToImplementation(permit).releasePermits();
        if (additionalPermits > 0) {
            return internalAcquire(newPermits, additionalPermits, isCancelled);
        } else {
            // new permits are less than the old ones, so we return the difference
            availablePermits.addAndGet(-additionalPermits);
            processQueue();
            // return the new permits immediately
            return CompletableFuture.completedFuture(new SemaphorePermit(newPermits));
        }
    }

    @Override
    public void release(AsyncSemaphorePermit permit) {
        if (isUnbounded()) {
            return;
        }
        availablePermits.addAndGet(castToImplementation(permit).releasePermits());
        processQueue();
    }

    @Override
    public long getAvailablePermits() {
        if (isUnbounded()) {
            return Long.MAX_VALUE;
        }
        return availablePermits.get();
    }

    @Override
    public long getAcquiredPermits() {
        if (isUnbounded()) {
            return 0;
        }
        return maxPermits - availablePermits.get();
    }

    @Override
    public int getQueueSize() {
        return queue.size();
    }

    private SemaphorePermit castToImplementation(AsyncSemaphorePermit permit) {
        if (permit instanceof SemaphorePermit semaphorePermit) {
            return semaphorePermit;
        } else {
            throw new IllegalArgumentException("Invalid permit type");
        }
    }

    private void processQueue() {
        if (closed.get()) {
            return;
        }
        executor.execute(processQueueRunnable);
    }

    private synchronized void internalProcessQueue() {
        while (!closed.get()) {
            long current = availablePermits.get();
            if (current <= 0) {
                break;
            }

            PendingRequest request = queue.peek();
            if (request == null) {
                break;
            }

            if (request.isCancelled.getAsBoolean()) {
                request.cancelTimeoutTask();
                queue.remove(request);
                request.future.completeExceptionally(
                        new PermitAcquireCancelledException("Permit acquisition was cancelled"));
                continue;
            }

            if (request.acquirePermits <= current) {
                availablePermits.addAndGet(-request.acquirePermits);
                request.cancelTimeoutTask();
                queue.remove(request);
                SemaphorePermit permit = new SemaphorePermit(request.permits);
                recordQueueLatency(request.getAgeNanos());
                boolean futureCompleted = request.future.complete(permit);
                if (!futureCompleted){
                    // request was already cancelled, return permits
                    availablePermits.addAndGet(request.acquirePermits);
                }
            } else {
                break;
            }
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            while (!queue.isEmpty()) {
                PendingRequest request = queue.poll();
                request.cancelTimeoutTask();
                request.future.completeExceptionally(new PermitAcquireAlreadyClosedException("Semaphore is closed"));
            }
            if (shutdownExecutor) {
                executor.shutdownNow();
            }
        }
    }

    private static class PendingRequest {
        final long permits;
        private final long acquirePermits;
        final CompletableFuture<AsyncSemaphorePermit> future;
        private final BooleanSupplier isCancelled;
        private volatile ScheduledFuture<?> timeoutTask;
        private final long requestCreatedNanos = System.nanoTime();

        PendingRequest(long permits, long acquirePermits, CompletableFuture<AsyncSemaphorePermit> future,
                       BooleanSupplier isCancelled) {
            this.permits = permits;
            this.acquirePermits = acquirePermits;
            this.future = future;
            this.isCancelled = isCancelled;
        }

        void setTimeoutTask(ScheduledFuture<?> timeoutTask) {
            this.timeoutTask = timeoutTask;
        }

        void cancelTimeoutTask() {
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
                timeoutTask = null;
            }
        }

        long getAgeNanos() {
            return System.nanoTime() - requestCreatedNanos;
        }
    }

    private static class SemaphorePermit implements AsyncSemaphorePermit {
        private static final AtomicLongFieldUpdater<SemaphorePermit> PERMITS_UPDATER = AtomicLongFieldUpdater
                .newUpdater(SemaphorePermit.class, "permits");
        private volatile long permits;

        SemaphorePermit(long permits) {
            this.permits = permits;
        }

        @Override
        public long getPermits() {
            return permits;
        }

        public long releasePermits() {
            return PERMITS_UPDATER.getAndSet(this, 0);
        }
    }
}
