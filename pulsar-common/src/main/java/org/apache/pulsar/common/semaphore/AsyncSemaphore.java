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
     * It will complete exceptionally with AsyncSemaphorePermitAcquireTimeoutException on timeout
     * and exceptionally with AsyncSemaphorePermitAcquireQueueFullException when queue full
     * @return CompletableFuture that completes with permit when available
     */
    CompletableFuture<AsyncSemaphorePermit> acquire(long permits, BooleanSupplier isCancelled);

    /**
     * Acquire or release permits for previously acquired permits by updating the permits.
     * Returns a future that completes when permits are available.
     * It will complete exceptionally with AsyncSemaphorePermitAcquireTimeoutException on timeout
     * and exceptionally with AsyncSemaphorePermitAcquireQueueFullException when queue full
     * @return CompletableFuture that completes with permit when available
     */
    CompletableFuture<AsyncSemaphorePermit> update(AsyncSemaphorePermit permit, long newPermits,
                                                   BooleanSupplier isCancelled);
    /**
     * Release previously acquired permit.
     * Must be called to prevent permit leaks.
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

    class PermitAcquireAlreadyClosedException extends PermitAcquireException {
        public PermitAcquireAlreadyClosedException(String message) {
            super(message);
        }
    }

    class PermitAcquireCancelledException extends PermitAcquireException {
        public PermitAcquireCancelledException(String message) {
            super(message);
        }
    }

    /**
     * Represents a permit that can be updated or released.
     */
    interface AsyncSemaphorePermit {
        long getPermits();
    }
}
