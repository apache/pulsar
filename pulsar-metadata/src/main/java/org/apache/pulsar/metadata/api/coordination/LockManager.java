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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;

/**
 * Controller for acquiring distributed lock on resources.
 */
public interface LockManager<T> extends AutoCloseable {
    /**
     * Read the content of an existing lock.
     *
     * If the lock is already taken, this operation will fail immediately.
     *
     * Warning: because of the distributed nature of the lock, having acquired a lock will never provide a strong
     * guarantee that no one else also think it owns the same resource. The caller will have to deal with these race
     * conditions when using the resource itself (eg. using compareAndSet() or fencing mechanisms).
     *
     * @param path
     *            the path of the resource on which to acquire the lock
     * @return a future that will track the completion of the operation
     * @throws NotFoundException
     *             if the lock is not taken
     * @throws MetadataStoreException
     *             if there's a failure in reading the lock
     */
    CompletableFuture<Optional<T>> readLock(String path);

    /**
     * Acquire a lock on a shared resource.
     *
     * If the lock is already taken, this operation will fail immediately.
     *
     * Warning: because of the distributed nature of the lock, having acquired a lock will never provide a strong
     * guarantee that no one else also think it owns the same resource. The caller will have to deal with these race
     * conditions when using the resource itself (eg. using compareAndSet() or fencing mechanisms).
     *
     * @param path
     *            the path of the resource on which to acquire the lock
     * @param value
     *            the value of the lock
     * @return a future that will track the completion of the operation
     * @throws LockBusyException
     *             if the lock is already taken
     * @throws MetadataStoreException
     *             if there's a failure in acquiring the lock
     */
    CompletableFuture<ResourceLock<T>> acquireLock(String path, T value);

    /**
     * List all the locks that are children of a specific path.
     *
     * For example, given locks: <code>/a/b/lock-1</code> and <code>/a/b/lock-2</code>, the
     * <code>listLocks("/a/b")</code> will return a list of <code>["lock-1", "lock-2"]</code>.
     *
     * @param path
     *            the prefix path to get the list of locks
     * @return a future that will track the completion of the operation
     * @throws MetadataStoreException
     *             if there's a failure in getting the list of locks
     */
    CompletableFuture<List<String>> listLocks(String path);

    /**
     * Close the LockManager and release all the locks.
     *
     * @return a future that will track the completion of the operation
     * @throws MetadataStoreException
     *             if there's a failure in closing the LockManager
     */
    CompletableFuture<Void> asyncClose();

}
