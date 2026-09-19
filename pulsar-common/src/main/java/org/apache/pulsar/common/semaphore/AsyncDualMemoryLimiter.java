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

import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * An abstraction for an asynchronous memory semaphore that tracks separate limits for heap and direct memory.
 */
public interface AsyncDualMemoryLimiter {
    enum LimitType {
        HEAP_MEMORY,    // For heap memory allocation
        DIRECT_MEMORY   // For direct memory allocation
    }

    /**
     * Acquire permits for the specified memory size.
     * Returned future completes when memory permits are available.
     * It will complete exceptionally with AsyncSemaphore.PermitAcquireTimeoutException on timeout
     * and exceptionally with AsyncSemaphore.PermitAcquireQueueFullException when queue full
     *
     * @param memorySize  the size of memory to acquire permits for
     * @param limitType   the type of memory limit (HEAP_MEMORY or DIRECT_MEMORY)
     * @param isCancelled supplier that returns true if acquisition should be cancelled
     * @return CompletableFuture that completes with permit when available
     */
    CompletableFuture<AsyncDualMemoryLimiterPermit> acquire(long memorySize, LimitType limitType,
                                                            BooleanSupplier isCancelled);

    /**
     * Acquire or release permits for previously acquired permits by updating the requested memory size.
     * Returns a future that completes when permits are available.
     * It will complete exceptionally with AsyncSemaphore.PermitAcquireTimeoutException on timeout
     * and exceptionally with AsyncSemaphore.PermitAcquireQueueFullException when queue full
     * The provided permit is released when the permits are successfully acquired and the returned updated
     * permit replaces the old instance.
     *
     * @param permit        the previously acquired permit to update
     * @param newMemorySize the new memory size to update to
     * @param isCancelled   supplier that returns true if update should be cancelled
     * @return CompletableFuture that completes with permit when available
     */
    CompletableFuture<AsyncDualMemoryLimiterPermit> update(AsyncDualMemoryLimiterPermit permit, long newMemorySize,
                                                           BooleanSupplier isCancelled);

    /**
     * Release previously acquired permit.
     * Must be called to prevent memory permit leaks.
     *
     * @param permit the permit to release
     */
    void release(AsyncDualMemoryLimiterPermit permit);
    /**
     * Execute the specified function with acquired permits and release the permits after the returned future completes.
     * @param memorySize memory size to acquire permits for
     * @param limitType memory limit type to acquire permits for
     * @param function function to execute with acquired permits
     * @return result of the function
     * @param <T> type of the CompletableFuture returned by the function
     */
    default <T> CompletableFuture<T> withAcquiredPermits(long memorySize, LimitType limitType,
                                                         BooleanSupplier isCancelled,
                                                         Function<AsyncDualMemoryLimiterPermit,
                                                                 CompletableFuture<T>> function,
                                                         Function<Throwable, CompletableFuture<T>>
                                                                 permitAcquireErrorHandler) {
        return AsyncDualMemoryLimiterUtil.withPermitsFuture(acquire(memorySize, limitType, isCancelled), function,
                permitAcquireErrorHandler, this::release);
    }

    /**
     * Executed the specified function with updated permits and release the permits after the returned future completes.
     * @param initialPermit initial permit to update
     * @param newMemorySize new memory size to update to
     * @param function function to execute with updated permits
     * @return result of the function
     * @param <T> type of the CompletableFuture returned by the function
     */
    default <T> CompletableFuture<T> withUpdatedPermits(AsyncDualMemoryLimiterPermit initialPermit, long newMemorySize,
                                                        BooleanSupplier isCancelled,
                                                        Function<AsyncDualMemoryLimiterPermit,
                                                                CompletableFuture<T>> function,
                                                        Function<Throwable, CompletableFuture<T>>
                                                                permitAcquireErrorHandler) {
        return AsyncDualMemoryLimiterUtil.withPermitsFuture(update(initialPermit, newMemorySize, isCancelled), function,
                permitAcquireErrorHandler, this::release);
    }

    /**
     * Represents an acquired permit for memory limiting that can be updated or released.
     */
    interface AsyncDualMemoryLimiterPermit {
        long getPermits();
        LimitType getLimitType();
    }
}
