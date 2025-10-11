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
package org.apache.pulsar.common.util.memory;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for asynchronous dual memory limiting.
 * Manages limits for both heap and direct memory usage.
 */
public interface AsyncDualMemoryLimiter {

    /**
     * Enum representing the type of memory limit.
     */
    enum LimitType {
        HEAP_MEMORY,    // For heap memory allocation
        DIRECT_MEMORY   // For direct memory allocation
    }

    /**
     * Acquire memory permits asynchronously.
     *
     * @param memorySize the amount of memory to acquire in bytes
     * @param limitType the type of memory limit
     * @return CompletableFuture containing the memory permit
     */
    CompletableFuture<AsyncDualMemoryLimiterPermit> acquire(long memorySize, LimitType limitType);

    /**
     * Update an existing permit with a new memory size.
     *
     * @param permit the existing permit to update
     * @param newMemorySize the new memory size in bytes
     * @return CompletableFuture containing the updated permit
     */
    CompletableFuture<AsyncDualMemoryLimiterPermit> update(AsyncDualMemoryLimiterPermit permit, long newMemorySize);

    /**
     * Release a memory permit back to the limiter.
     *
     * @param permit the permit to release
     */
    void release(AsyncDualMemoryLimiterPermit permit);
}
