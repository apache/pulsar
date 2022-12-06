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
package org.apache.pulsar.broker.loadbalance.extensions.store;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * The load data store interface.
 *
 * @param <T> The Load data type.
 */
public interface LoadDataStore<T> extends Closeable {

    /**
     * Async push load data to store.
     *
     * @param key The load data key.
     * @param loadData The load data.
     */
    CompletableFuture<Void> pushAsync(String key, T loadData);

    /**
     * Async remove load data from store.
     *
     * @param key The load data key to remove.
     */
    CompletableFuture<Void> removeAsync(String key);

    /**
     * Get load data by key.
     *
     * @param key The load data key.
     */
    Optional<T> get(String key);

    /**
     * Performs the given action for each entry in this map until all entries
     * have been processed or the action throws an exception.
     *
     * @param action The action to be performed for each entry
     */
    void forEach(BiConsumer<String, T> action);

    /**
     * Returns a Set view of the mappings contained in this map.
     *
     * @return a set view of the mappings contained in this map
     */
    Set<Map.Entry<String, T>> entrySet();

    /**
     * The load data key count.
     */
    int size();

}
