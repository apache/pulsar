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

package org.apache.pulsar.client.api;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public interface TableView<T> extends Closeable {

    /**
     * Returns the number of key-value mappings in the {@link TableView}.
     *
     * @return the number of key-value mappings in this TableView
     */
    int size();

    /**
     * Returns {@code true} if this {@link TableView} contains no key-value mappings.
     *
     * @return true if this TableView contains no key-value mappings
     */
    boolean isEmpty();

    /**
     * Returns {@code true} if this {@link TableView} contains a mapping for the specified
     * key.
     *
     * @param key key whose presence in this map is to be tested
     * @return true if this map contains a mapping for the specified key
     */
    boolean containsKey(String key);

    /**
     * Returns the value to which the specified key is mapped, or null if this map contains
     * no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with the key or null if the keys was not found
     */
    T get(String key);

    /**
     * Returns a Set view of the mappings contained in this map.
     *
     * @return a set view of the mappings contained in this map
     */
    Set<Map.Entry<String, T>> entrySet();

    /**
     * Returns a {@link Set} view of the keys contained in this {@link TableView}.
     *
     * @return a set view of the keys contained in this map
     */
    Set<String> keySet();

    /**
     * Returns a Collection view of the values contained in this {@link TableView}.
     *
     * @return a collection view of the values contained in this TableView
     */
    Collection<T> values();

    /**
     * Performs the given action for each entry in this map until all entries
     * have been processed or the action throws an exception.
     *
     * @param action The action to be performed for each entry
     */
    void forEach(BiConsumer<String, T> action);

    /**
     * Performs the give action for each entry in this map until all entries
     * have been processed or the action throws an exception.
     *
     * @param action The action to be performed for each entry
     */
    void forEachAndListen(BiConsumer<String, T> action);

    /**
     * Close the table view and releases resources allocated.
     *
     * @return a future that can used to track when the table view has been closed.
     */
    CompletableFuture<Void> closeAsync();
}
