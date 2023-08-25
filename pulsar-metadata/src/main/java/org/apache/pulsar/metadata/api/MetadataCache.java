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
package org.apache.pulsar.metadata.api;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;

/**
 * Represent the caching layer access for a specific type of objects.
 */
public interface MetadataCache<T> {

    /**
     * Tries to fetch one item from the cache or fallback to the store if not present.
     * <p>
     * If the key is not found, the {@link Optional} will be empty.
     *
     * @param path
     *            the path of the object in the metadata store
     * @return a future to track the completion of the operation
     */
    CompletableFuture<Optional<T>> get(String path);

    /**
     * Tries to fetch one item from the cache or fallback to the store if not present.
     * <p>
     * If the key is not found, the {@link Optional} will be empty.
     *
     * @param path
     *            the path of the object in the metadata store
     * @return a future to track the completion of the operation
     */
    CompletableFuture<Optional<CacheGetResult<T>>> getWithStats(String path);

    /**
     * Check if an object is present in cache without triggering a load from the metadata store.
     *
     * @param path
     *            the path of the object in the metadata store
     * @return the cached object or an empty {@link Optional} is the cache doesn't have the object
     */
    Optional<T> getIfCached(String path);

    /**
     * Return all the nodes (lexicographically sorted) that are children to the specific path.
     *
     * If the path itself does not exist, it will return an empty list.
     *
     * @param path
     *            the path of the key to get from the store
     * @return a future to track the async request
     */
    CompletableFuture<List<String>> getChildren(String path);

    /**
     * Read whether a specific path exists.
     *
     * Note: In case of keys with multiple levels (eg: '/a/b/c'), checking the existence of a parent (eg. '/a') might
     * not necessarily return true, unless the key had been explicitly created.
     *
     * @param path
     *            the path of the key to check on the store
     * @return a future to track the async request
     */
    CompletableFuture<Boolean> exists(String path);

    /**
     * Perform an atomic read-modify-update of the value.
     * <p>
     * The modify function can potentially be called multiple times if there are concurrent updates happening.
     * <p>
     * If the object does not exist yet, the <code>modifyFunction</code> will get passed an {@link Optional#empty()}
     * object.
     *
     * @param path
     *            the path of the object in the metadata store
     * @param modifyFunction
     *            a function that will be passed the current value and returns a modified value to be stored
     * @return a future to track the completion of the operation
     */
    CompletableFuture<T> readModifyUpdateOrCreate(String path, Function<Optional<T>, T> modifyFunction);

    /**
     * Perform an atomic read-modify-update of the value.
     * <p>
     * The modify function can potentially be called multiple times if there are concurrent updates happening.
     *
     * @param path
     *            the path of the object in the metadata store
     * @param modifyFunction
     *            a function that will be passed the current value and returns a modified value to be stored
     * @return a future to track the completion of the operation
     */
    CompletableFuture<T> readModifyUpdate(String path, Function<T, T> modifyFunction);

    /**
     * Create a new object in the metadata store.
     * <p>
     * This operation will make sure to keep the cache consistent.
     *
     * @param path
     *            the path of the object in the metadata store
     * @param value
     *            the object to insert in metadata store
     * @return a future to track the completion of the operation
     * @throws AlreadyExistsException
     *             If the object is already present.
     */
    CompletableFuture<Void> create(String path, T value);

    /**
     * Delete an object from the metadata store.
     * <p>
     * This operation will make sure to keep the cache consistent.
     *
     * @param path
     *            the path of the object in the metadata store
     * @return a future to track the completion of the operation
     * @throws NotFoundException
     *             if the object is not present in the metadata store.
     */
    CompletableFuture<Void> delete(String path);

    /**
     * Force the invalidation of an object in the metadata cache.
     *
     * @param path the path of the object in the metadata store
     */
    void invalidate(String path);

    /**
     * Force the invalidation of all object in the metadata cache.
     */
    void invalidateAll();

    /**
     * Invalidate and reload an object in the metadata cache.
     *
     * @param path the path of the object in the metadata store
     */
    void refresh(String path);
}
