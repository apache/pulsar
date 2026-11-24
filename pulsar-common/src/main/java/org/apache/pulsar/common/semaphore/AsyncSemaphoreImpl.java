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

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import lombok.CustomLog;
import org.apache.pulsar.common.util.Runnables;

/**
 * Implementation of AsyncSemaphore with timeout and queue size limits.
 *
 * <p>Permits are granted on the calling thread when no other requests are waiting. Waiting requests are processed
 * on the executor, which also handles the timeouts. Their futures are completed outside the lock on the completion
 * executor, so the callers' continuations don't run on the executor thread. {@link AsyncDualMemoryLimiterImpl}
 * provides a dedicated completion executor. Otherwise {@link ForkJoinPool#commonPool()} is used, or the executor when
 * the common pool's parallelism is 1 or less, since a disabled common pool can accept tasks without running them.
 */
@CustomLog
public class AsyncSemaphoreImpl implements AsyncSemaphore, AutoCloseable {
    // Same threshold as CompletableFuture and OpReadEntry: parallelism=0 reports 1 and never runs the tasks
    @VisibleForTesting
    static final boolean USE_COMMON_POOL = ForkJoinPool.getCommonPoolParallelism() > 1;

    private final AtomicLong availablePermits;
    private final Queue<PendingRequest> queue;
    private final long maxPermits;
    private final long timeoutMillis;
    private final ScheduledExecutorService executor;
    private final boolean shutdownExecutor;
    private final Executor completionExecutor;
    private final LongConsumer queueLatencyRecorder;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    // true while a processQueue task is pending, so that at most one such task is queued in the executor
    private final AtomicBoolean processQueueScheduled = new AtomicBoolean(false);
    private final Runnable processQueueRunnable = Runnables.catchingAndLoggingThrowables(() -> {
        processQueueScheduled.set(false);
        internalProcessQueue();
    });
    private final ScheduledFuture<?> processQueueScheduledFuture;

    /**
     * Creates an AsyncSemaphoreImpl with the given parameters.
     * @param maxPermits max number of permits available for acquisition, set to <= 0 for unbounded semaphore (not
     *                   recommended)
     * @param maxQueueSize max number of requests that can be queued, set to <= 0 for unbounded queue (not recommended)
     * @param timeoutMillis timeout in milliseconds for acquiring permits
     */
    public AsyncSemaphoreImpl(long maxPermits, int maxQueueSize, long timeoutMillis) {
        this(maxPermits, maxQueueSize, timeoutMillis,
                maxPermits > 0 ? createExecutor("async-semaphore-executor") : null, maxPermits > 0, null);
    }

    /**
     * Creates an AsyncSemaphoreImpl with the given parameters.
     * @param maxPermits max number of permits available for acquisition, set to <= 0 for unbounded semaphore (not
     *                   recommended)
     * @param maxQueueSize max number of requests that can be queued, set to <= 0 for unbounded queue (not recommended)
     * @param timeoutMillis timeout in milliseconds for acquiring permits
     * @param executor executor service to use for scheduling timeouts, it is expected to be single threaded and to
     *                 remove cancelled tasks, see {@link ScheduledThreadPoolExecutor#setRemoveOnCancelPolicy(boolean)}
     * @param queueLatencyRecorder consumer to record queue latency, Long.MAX_VALUE is used for requests that timed out
     */
    public AsyncSemaphoreImpl(long maxPermits, int maxQueueSize, long timeoutMillis,
                              ScheduledExecutorService executor, LongConsumer queueLatencyRecorder) {
        this(maxPermits, maxQueueSize, timeoutMillis, executor, false, queueLatencyRecorder);
    }

    AsyncSemaphoreImpl(long maxPermits, int maxQueueSize, long timeoutMillis, ScheduledExecutorService executor,
                       boolean shutdownExecutor, LongConsumer queueLatencyRecorder) {
        this(maxPermits, maxQueueSize, timeoutMillis, executor, shutdownExecutor,
                USE_COMMON_POOL ? ForkJoinPool.commonPool() : executor, queueLatencyRecorder);
    }

    AsyncSemaphoreImpl(long maxPermits, int maxQueueSize, long timeoutMillis, ScheduledExecutorService executor,
                       boolean shutdownExecutor, Executor completionExecutor, LongConsumer queueLatencyRecorder) {
        this.availablePermits = new AtomicLong(maxPermits);
        this.maxPermits = maxPermits;
        this.queue = maxQueueSize > 0 ? new ArrayBlockingQueue<>(maxQueueSize) : new LinkedBlockingQueue<>();
        this.timeoutMillis = timeoutMillis;
        this.executor = executor;
        this.shutdownExecutor = shutdownExecutor;
        this.completionExecutor = completionExecutor;
        this.queueLatencyRecorder = queueLatencyRecorder;
        // scheduled task that runs the processQueue method every half of the timeout
        // this is to support cancellation in cases where the head of the queue request is blocking others
        // from proceeding and it happens to already be cancelled.
        this.processQueueScheduledFuture = executor != null
                ? executor.schedule(processQueueRunnable, timeoutMillis / 2, TimeUnit.MILLISECONDS) : null;
    }

    /**
     * Creates a single threaded executor for the semaphore. Timeout tasks get cancelled when permits are granted, so
     * cancelled tasks are removed from the queue right away instead of being retained until their delay expires.
     */
    static ScheduledThreadPoolExecutor createExecutor(String threadNamePrefix) {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(1, new DefaultThreadFactory(threadNamePrefix));
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    @Override
    public CompletableFuture<AsyncSemaphorePermit> acquire(long permits, BooleanSupplier isCancelled) {
        return internalAcquire(permits, null, isCancelled);
    }

    private CompletableFuture<AsyncSemaphorePermit> internalAcquire(long permits, SemaphorePermit previousPermit,
                                                                    BooleanSupplier isCancelled) {
        validatePermits(permits);

        // if maximum permits is <= 0, then the semaphore is unbounded
        if (isUnbounded()) {
            return CompletableFuture.completedFuture(new SemaphorePermit(permits));
        }

        CompletableFuture<AsyncSemaphorePermit> future = new CompletableFuture<>();

        if (closed.get()) {
            future.completeExceptionally(new PermitAcquireAlreadyClosedException("Semaphore is closed"));
            return future;
        }

        if (isCancelled.getAsBoolean()) {
            future.completeExceptionally(new PermitAcquireCancelledException("Permit acquisition was cancelled"));
            return future;
        }

        PendingRequest request = new PendingRequest(permits, previousPermit, future, isCancelled);
        if (tryAcquireWithoutQueueing(request)) {
            recordQueueLatency(request.getAgeNanos());
            future.complete(new SemaphorePermit(permits));
            return future;
        }
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
                completeAsync(() -> future.completeExceptionally(new PermitAcquireTimeoutException(
                        "Permit acquisition timed out")));
                // the next request might have smaller permits and that might be processed
                processQueue();
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);
        request.setTimeoutTask(timeoutTask);

        processQueue();
        return future;
    }

    private void validatePermits(long permits) {
        if (permits < 0) {
            throw new IllegalArgumentException("Invalid negative permits value: " + permits);
        }
        if (!isUnbounded() && permits > maxPermits) {
            throw new IllegalArgumentException(
                    "Requested permits=" + permits + " is larger than maxPermits=" + maxPermits);
        }
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
        validatePermits(newPermits);
        if (isUnbounded()) {
            return CompletableFuture.completedFuture(new SemaphorePermit(newPermits));
        }
        long oldPermits = permit.getPermits();
        long additionalPermits = newPermits - oldPermits;
        if (additionalPermits > 0) {
            return internalAcquire(newPermits, castToImplementation(permit), isCancelled);
        }
        // mark the old permits as released without adding the permits to availablePermits
        long leftoverPermits = castToImplementation(permit).releasePermits() - newPermits;
        if (leftoverPermits >= 0) {
            if (leftoverPermits > 0) {
                // new permits are less than the old ones, so we return the difference
                availablePermits.addAndGet(leftoverPermits);
                processQueue();
            }
            // return the new permits immediately
            return CompletableFuture.completedFuture(new SemaphorePermit(newPermits));
        } else {
            return acquire(newPermits, isCancelled);
        }
    }

    @Override
    public void release(AsyncSemaphorePermit permit) {
        if (isUnbounded()) {
            return;
        }
        long releasedPermits = castToImplementation(permit).releasePermits();
        if (releasedPermits > 0) {
            availablePermits.addAndGet(releasedPermits);
            processQueue();
        }
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
        // a request is added to the queue before this is called, so skipping an empty queue doesn't lose a wakeup
        if (closed.get() || queue.isEmpty()) {
            return;
        }
        // a pending task also handles the changes made before this call since it hasn't started yet
        if (processQueueScheduled.compareAndSet(false, true)) {
            executor.execute(processQueueRunnable);
        }
    }

    private synchronized boolean tryAcquireWithoutQueueing(PendingRequest request) {
        // permits are granted in FIFO order, so a request can't bypass the waiting requests
        return queue.isEmpty() && tryAcquirePermits(request);
    }

    // must be called while holding the lock
    private boolean tryAcquirePermits(PendingRequest request) {
        long current = availablePermits.get();
        if (current <= 0 || request.getRequiredPermits() > current) {
            return false;
        }
        availablePermits.addAndGet(-request.getRequiredPermitsReusingPrevious());
        return true;
    }

    private void internalProcessQueue() {
        List<Runnable> completions = new ArrayList<>();
        try {
            synchronized (this) {
                while (!closed.get()) {
                    PendingRequest request = queue.peek();
                    if (request == null) {
                        break;
                    }

                    if (request.isCancelled.getAsBoolean()) {
                        request.cancelTimeoutTask();
                        queue.remove(request);
                        completions.add(() -> request.future.completeExceptionally(
                                new PermitAcquireCancelledException("Permit acquisition was cancelled")));
                        continue;
                    }

                    // request future has been completed by user code cancellation, remove it from the queue
                    if (request.future.isDone()) {
                        request.cancelTimeoutTask();
                        queue.remove(request);
                        continue;
                    }

                    if (!tryAcquirePermits(request)) {
                        break;
                    }
                    request.cancelTimeoutTask();
                    queue.remove(request);
                    completions.add(() -> completeAcquiredRequest(request));
                }
            }
        } finally {
            // complete outside the lock, also when an isCancelled supplier throws, so that granted permits don't leak
            completions.forEach(this::completeAsync);
        }
    }

    private void completeAcquiredRequest(PendingRequest request) {
        recordQueueLatency(request.getAgeNanos());
        if (!request.future.complete(new SemaphorePermit(request.permits))) {
            // request was cancelled by user code, return all permits, including those taken over from the
            // previous permit, which was released when the request was granted
            availablePermits.addAndGet(request.permits);
            processQueue();
        }
    }

    // the continuations of waiting requests shouldn't run on the executor, otherwise they'd delay all other requests
    private void completeAsync(Runnable completion) {
        Runnable task = Runnables.catchingAndLoggingThrowables(completion);
        try {
            completionExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            // the executors are shut down when closing, complete inline so that a granted request isn't lost
            task.run();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (processQueueScheduledFuture != null) {
                processQueueScheduledFuture.cancel(false);
            }
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
        private final SemaphorePermit previousPermit;
        final CompletableFuture<AsyncSemaphorePermit> future;
        private final BooleanSupplier isCancelled;
        private volatile ScheduledFuture<?> timeoutTask;
        private final long requestCreatedNanos = System.nanoTime();

        PendingRequest(long permits, SemaphorePermit previousPermit, CompletableFuture<AsyncSemaphorePermit> future,
                       BooleanSupplier isCancelled) {
            this.permits = permits;
            this.previousPermit = previousPermit;
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

        long getRequiredPermits() {
            return previousPermit == null ? permits : permits - previousPermit.getPermits();
        }

        long getRequiredPermitsReusingPrevious() {
            return previousPermit == null ? permits : permits - previousPermit.releasePermits();
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

        @Override
        public String toString() {
            return "SemaphorePermit@" + System.identityHashCode(this) + "[permits=" + permits + "]";
        }
    }
}
