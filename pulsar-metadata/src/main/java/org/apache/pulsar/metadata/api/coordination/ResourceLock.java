/**
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
package org.apache.pulsar.metadata.api.coordination;

import java.util.concurrent.CompletableFuture;

/**
 * Represent a lock that the current process has on a shared resource.
 */
public interface ResourceLock<T> {

    /**
     * @return the path associated with the lock
     */
    String getPath();

    /**
     * @return the value associated with the lock
     */
    T getValue();

    /**
     * Update the value of the lock.
     *
     * @return a future to track when the release operation is complete
     */
    CompletableFuture<Void> updateValue(T newValue);

    /**
     * Release the lock on the resource.
     *
     * @return a future to track when the release operation is complete
     */
    CompletableFuture<Void> release();

    /**
     * Get a future that can be used to get notified when the lock is expired or it gets released.
     *
     * @return a future to get notification if the lock is expired
     */
    CompletableFuture<Void> getLockExpiredFuture();
}
