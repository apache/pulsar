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

package org.apache.pulsar.metadata.api;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Defines metadata store tableview.
 * MetadataStoreTableView initially fills existing items to its local tableview and eventually
 * synchronize remote updates to its local tableview from the remote metadata store.
 * This abstraction can help replicate metadata in memory from metadata store.
 */
public interface MetadataStoreTableView<T> {

    class ConflictException extends RuntimeException {
        public ConflictException(String msg) {
            super(msg);
        }
    }

    /**
     * Starts the tableview by filling existing items to its local tableview from the remote metadata store.
     */
    void start() throws MetadataStoreException;

    /**
     * Gets one item from the local tableview.
     * <p>
     * If the key is not found, return null.
     *
     * @param key the key to check
     * @return value if exists. Otherwise, null.
     */
    T get(String key);

    /**
     * Tries to put the item in the persistent store.
     * All peer tableviews (including the local one) will be notified and be eventually consistent with this put value.
     * <p>
     * This operation can fail if the input value conflicts with the existing one.
     *
     * @param key the key to check on the tableview
     * @return a future to track the completion of the operation
     * @throws MetadataStoreTableView.ConflictException
     *             if the input value conflicts with the existing one.
     */
    CompletableFuture<Void> put(String key, T value);

    /**
     * Tries to delete the item from the persistent store.
     * All peer tableviews (including the local one) will be notified and be eventually consistent with this deletion.
     * <p>
     * This can fail if the item is not present in the metadata store.
     *
     * @param key the key to check on the tableview
     * @return a future to track the completion of the operation
     * @throws MetadataStoreException.NotFoundException
     *             if the key is not present in the metadata store.
     */
    CompletableFuture<Void> delete(String key);

    /**
     * Returns the entry set of the items in the local tableview.
     * @return entry set
     */
    Set<Map.Entry<String, T>> entrySet();
}

