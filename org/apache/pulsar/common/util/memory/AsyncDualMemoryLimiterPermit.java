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

/**
 * Represents a permit acquired from an AsyncDualMemoryLimiter.
 * Contains information about the acquired memory allocation.
 */
public interface AsyncDualMemoryLimiterPermit {

    /**
     * Get the memory size allocated by this permit.
     *
     * @return the memory size in bytes
     */
    long getMemorySize();

    /**
     * Get the limit type associated with this permit.
     *
     * @return the limit type
     */
    AsyncDualMemoryLimiter.LimitType getLimitType();

    /**
     * Check if this permit is still valid.
     *
     * @return true if the permit is valid, false otherwise
     */
    boolean isValid();
}
