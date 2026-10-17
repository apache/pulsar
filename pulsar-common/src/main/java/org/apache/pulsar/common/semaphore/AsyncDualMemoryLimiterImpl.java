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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.BooleanSupplier;
import lombok.CustomLog;

/**
 * Implementation of AsyncDualMemoryLimiter with separate limits for heap and direct memory.
 *
 * <p>Requests that had to wait for permits are completed on a dedicated pool, so their continuations don't run on
 * the executor that handles the permits and timeouts, also when the common pool isn't usable.
 */
@CustomLog
public class AsyncDualMemoryLimiterImpl implements AsyncDualMemoryLimiter, AutoCloseable {
    private final ScheduledExecutorService executor;
    private final boolean shutdownExecutor;
    private final ExecutorService completionExecutor;
    private final AsyncSemaphoreImpl heapLimiter;
    private final AsyncSemaphoreImpl directLimiter;

    /**
     * Creates an AsyncDualMemoryLimiterImpl with the given parameters.
     * @param maxHeapMemory max heap memory available for allocation
     * @param maxHeapQueueSize max number of requests that can be queued for heap memory allocation
     * @param heapTimeoutMillis timeout in milliseconds for heap memory allocation
     * @param maxDirectMemory max direct memory available for allocation
     * @param maxDirectQueueSize max number of requests that can be queued for direct memory allocation
     * @param directTimeoutMillis timeout in milliseconds for direct memory allocation
     * @param executor executor service to use for scheduling timeouts, it is expected to be single threaded and to
     *                 remove cancelled tasks, see {@link ScheduledThreadPoolExecutor#setRemoveOnCancelPolicy(boolean)}
     */
    public AsyncDualMemoryLimiterImpl(long maxHeapMemory, int maxHeapQueueSize, long heapTimeoutMillis,
                               long maxDirectMemory, int maxDirectQueueSize, long directTimeoutMillis,
                               ScheduledExecutorService executor) {
        this(maxHeapMemory, maxHeapQueueSize, heapTimeoutMillis, maxDirectMemory, maxDirectQueueSize,
                directTimeoutMillis, executor, false);
    }

    /**
     * Creates an AsyncDualMemoryLimiterImpl with the given parameters.
     * @param maxHeapMemory max heap memory available for allocation
     * @param maxHeapQueueSize max number of requests that can be queued for heap memory allocation
     * @param heapTimeoutMillis timeout in milliseconds for heap memory allocation
     * @param maxDirectMemory max direct memory available for allocation
     * @param maxDirectQueueSize max number of requests that can be queued for direct memory allocation
     * @param directTimeoutMillis timeout in milliseconds for direct memory allocation
     */
    public AsyncDualMemoryLimiterImpl(long maxHeapMemory, int maxHeapQueueSize, long heapTimeoutMillis,
                                      long maxDirectMemory, int maxDirectQueueSize, long directTimeoutMillis) {
        this(maxHeapMemory, maxHeapQueueSize, heapTimeoutMillis, maxDirectMemory, maxDirectQueueSize,
                directTimeoutMillis, AsyncSemaphoreImpl.createExecutor("async-dual-memory-limiter"), true);
    }

    AsyncDualMemoryLimiterImpl(long maxHeapMemory, int maxHeapQueueSize, long heapTimeoutMillis,
                               long maxDirectMemory, int maxDirectQueueSize, long directTimeoutMillis,
                               ScheduledExecutorService executor, boolean shutdownExecutor) {
        this.executor = executor;
        this.shutdownExecutor = shutdownExecutor;
        // threads are only started when a request had to wait for permits
        this.completionExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new DefaultThreadFactory("async-dual-memory-limiter-completion"));
        this.heapLimiter = new AsyncSemaphoreImpl(maxHeapMemory, maxHeapQueueSize, heapTimeoutMillis, executor,
                false, completionExecutor, this::recordHeapWaitTime);
        this.directLimiter = new AsyncSemaphoreImpl(maxDirectMemory, maxDirectQueueSize, directTimeoutMillis, executor,
                false, completionExecutor, this::recordDirectWaitTime);
    }

    @Override
    public CompletableFuture<AsyncDualMemoryLimiterPermit> acquire(long memorySize, LimitType limitType,
                                                                   BooleanSupplier isCancelled) {
        AsyncSemaphore limiter = getLimiter(limitType);
        return limiter.acquire(memorySize, isCancelled).thenApply(result ->
                new DualMemoryLimiterPermit(limitType, result));
    }

    @VisibleForTesting
    public AsyncSemaphore getLimiter(LimitType limitType) {
        switch (limitType) {
        case HEAP_MEMORY:
            return heapLimiter;
        case DIRECT_MEMORY:
            return directLimiter;
        default:
            throw new IllegalArgumentException("Unsupported limit type: " + limitType);
        }
    }

    @Override
    public CompletableFuture<AsyncDualMemoryLimiterPermit> update(AsyncDualMemoryLimiterPermit permit,
                                                                  long newMemorySize, BooleanSupplier isCancelled) {
        AsyncSemaphore limiter = getLimiter(permit.getLimitType());
        return limiter.update(castToImplementation(permit).getUnderlyingPermit(), newMemorySize, isCancelled)
                .thenApply(updatedPermit -> new DualMemoryLimiterPermit(permit.getLimitType(), updatedPermit));
    }

    @Override
    public void release(AsyncDualMemoryLimiterPermit permit) {
        AsyncSemaphore limiter = getLimiter(permit.getLimitType());
        limiter.release(castToImplementation(permit).getUnderlyingPermit());
    }

    private DualMemoryLimiterPermit castToImplementation(AsyncDualMemoryLimiterPermit permit) {
        if (permit instanceof DualMemoryLimiterPermit dualMemoryLimiterPermit) {
            return dualMemoryLimiterPermit;
        } else {
            throw new IllegalArgumentException("Invalid permit type");
        }
    }

    /**
     * Record the wait time for a heap memory allocation permit.
     * @param waitTimeNanos wait time in nanoseconds, or Long.MAX_VALUE if the allocation timed out
     */
    protected void recordHeapWaitTime(long waitTimeNanos) {

    }

    /**
     * Record the wait time for a direct memory allocation permit.
     * @param waitTimeNanos wait time in nanoseconds, or Long.MAX_VALUE if the allocation timed out
     */
    protected void recordDirectWaitTime(long waitTimeNanos) {

    }

    @Override
    public void close() {
        heapLimiter.close();
        directLimiter.close();
        if (shutdownExecutor) {
            executor.shutdown();
        }
        // already submitted completions still run after shutdown()
        completionExecutor.shutdown();
    }

    private static class DualMemoryLimiterPermit implements AsyncDualMemoryLimiterPermit {
        private final LimitType limitType;
        private final AsyncSemaphore.AsyncSemaphorePermit underlyingPermit;

        DualMemoryLimiterPermit(LimitType limitType, AsyncSemaphore.AsyncSemaphorePermit underlyingPermit) {
            this.limitType = limitType;
            this.underlyingPermit = underlyingPermit;
        }

        @Override
        public long getPermits() {
            return underlyingPermit.getPermits();
        }

        @Override
        public LimitType getLimitType() {
            return limitType;
        }

        public AsyncSemaphore.AsyncSemaphorePermit getUnderlyingPermit() {
            return underlyingPermit;
        }

        @Override
        public String toString() {
            return "DualMemoryLimiterPermit@" + System.identityHashCode(this) + "{" + "limitType=" + limitType
                    + ", permits=" + underlyingPermit.getPermits() + '}';
        }
    }
}
