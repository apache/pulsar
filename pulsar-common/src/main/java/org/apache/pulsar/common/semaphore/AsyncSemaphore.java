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

/**
 * An abstraction for a generic asynchronous semaphore.
 */
public interface AsyncSemaphore {
    /**
     * Acquire permits from the semaphore.
     * Returned future completes when permits are available.
     * It will complete exceptionally with AsyncSemaphore.PermitAcquireTimeoutException on timeout
     * and exceptionally with AsyncSemaphore.PermitAcquireQueueFullException when queue full
     *
     * @param permits     number of permits to acquire
     * @param isCancelled supplier that returns true if acquisition should be cancelled
     * @return CompletableFuture that completes with permit when available
     */
    CompletableFuture<AsyncSemaphorePermit> acquire(long permits, BooleanSupplier isCancelled);

    /**
     * Acquire or release permits for previously acquired permits by updating the permits.
     * Returns a future that completes when permits are available.
     * The provided permit is released when the permits are successfully acquired and the returned updated
     * permit replaces the old instance.
     * It will complete exceptionally with AsyncSemaphore.PermitAcquireTimeoutException on timeout
     * and exceptionally with AsyncSemaphore.PermitAcquireQueueFullException when queue full
     *
     * @param permit      previously acquired permit to update
     * @param newPermits  new number of permits to update to
     * @param isCancelled supplier that returns true if update should be cancelled
     * @return CompletableFuture that completes with permit when available
     */
    CompletableFuture<AsyncSemaphorePermit> update(AsyncSemaphorePermit permit, long newPermits,
                                                   BooleanSupplier isCancelled);

    /**
     * Release previously acquired permit.
     * Must be called to prevent permit leaks.
     *
     * @param permit permit to release
     */
    void release(AsyncSemaphorePermit permit);
    /**
     * Get the number of available permits.
     */
    long getAvailablePermits();

    /**
     * Get the number of acquired permits.
     */
    long getAcquiredPermits();

    /**
     * Get the current size of queued requests.
     */
    int getQueueSize();

    /**
     * Abstract base class for all exceptions thrown by acquire or update.
     */
    abstract class PermitAcquireException extends RuntimeException {
        public PermitAcquireException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when permit acquisition times out.
     */
    class PermitAcquireTimeoutException extends PermitAcquireException {
        public PermitAcquireTimeoutException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when permit acquisition queue is full.
     */
    class PermitAcquireQueueFullException extends PermitAcquireException {
        public PermitAcquireQueueFullException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when permit acquisition is attempted on a closed semaphore.
     */
    class PermitAcquireAlreadyClosedException extends PermitAcquireException {
        public PermitAcquireAlreadyClosedException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when permit acquisition is cancelled.
     */
    class PermitAcquireCancelledException extends PermitAcquireException {
        public PermitAcquireCancelledException(String message) {
            super(message);
        }
    }

    /**
     * Represents an acquired permit that can be updated or released.
     */
    interface AsyncSemaphorePermit {
        long getPermits();
    }
}
