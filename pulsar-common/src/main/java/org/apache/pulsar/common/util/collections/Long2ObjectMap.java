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
package org.apache.pulsar.common.util.collections;

import java.util.Collection;
import java.util.function.LongFunction;

/**
 * A map with primitive {@code long} keys and object values.
 *
 * <p>Using primitive keys avoids the overhead of boxing {@code long} values into
 * {@link Long} objects, reducing both memory usage and GC pressure compared to
 * {@code Map<Long, V>}.
 *
 * @param <V> the type of mapped values
 */
public interface Long2ObjectMap<V> {

    /**
     * Returns the value associated with the given key, or {@code null} if not present.
     *
     * @param key the key
     * @return the value, or {@code null}
     */
    V get(long key);

    /**
     * Associates the given value with the given key.
     *
     * @param key   the key
     * @param value the value
     * @return the previous value, or {@code null} if there was no mapping
     */
    V put(long key, V value);

    /**
     * Removes the mapping for the given key.
     *
     * @param key the key
     * @return the previous value, or {@code null} if there was no mapping
     */
    V remove(long key);

    /**
     * If the key is not already present, computes its value using the given function
     * and inserts it.
     *
     * @param key             the key
     * @param mappingFunction the function to compute a value
     * @return the current (existing or computed) value
     */
    V computeIfAbsent(long key, LongFunction<? extends V> mappingFunction);

    /**
     * Returns {@code true} if this map contains no entries.
     */
    boolean isEmpty();

    /**
     * Returns the number of entries in this map.
     */
    int size();

    /**
     * Removes all entries from this map.
     */
    void clear();

    /**
     * Iterates over all entries, calling the consumer with primitive long keys
     * (no boxing).
     *
     * @param consumer the consumer to call for each entry
     */
    void forEach(LongObjConsumer<? super V> consumer);

    /**
     * Returns a {@link Collection} view of the values in this map.
     * The collection supports iteration and {@code stream()} operations.
     */
    Collection<V> values();
}
