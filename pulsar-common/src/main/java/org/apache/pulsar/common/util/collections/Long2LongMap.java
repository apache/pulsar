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

import java.util.function.LongUnaryOperator;

/**
 * A map with primitive {@code long} keys and primitive {@code long} values.
 *
 * <p>The default return value for missing keys is {@code 0}. Use {@link #getOrDefault(long, long)}
 * or {@link #containsKey(long)} when {@code 0} is a valid mapped value.
 */
public interface Long2LongMap {

    @FunctionalInterface
    interface EntryConsumer {
        void accept(long key, long value);
    }

    @FunctionalInterface
    interface EntryPredicate {
        boolean test(long key, long value);
    }

    /**
     * Returns the value for the given key, or {@code 0} if not present.
     *
     * @param key the key
     * @return the mapped value, or {@code 0}
     */
    long get(long key);

    /**
     * Associates the given value with the given key.
     *
     * @param key the key
     * @param value the value
     * @return the previous value, or {@code 0} if there was no mapping
     */
    long put(long key, long value);

    /**
     * Removes the mapping for the given key.
     *
     * @param key the key
     * @return the previous value, or {@code 0} if there was no mapping
     */
    long remove(long key);

    /**
     * Returns the value for the given key, or the specified default if not present.
     *
     * @param key the key
     * @param defaultValue the default value to return if the key is absent
     * @return the mapped value, or {@code defaultValue}
     */
    long getOrDefault(long key, long defaultValue);

    /**
     * If the key is not already present, computes its value using the given function and inserts it.
     *
     * @param key the key
     * @param mappingFunction the function to compute a value
     * @return the current (existing or computed) value
     */
    long computeIfAbsent(long key, LongUnaryOperator mappingFunction);

    /**
     * Returns {@code true} if this map contains the given key.
     *
     * @param key the key
     * @return {@code true} if this map contains the key
     */
    boolean containsKey(long key);

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
     * Iterates over all entries, calling the consumer with primitive long keys and values.
     *
     * @param consumer the consumer to call for each entry
     */
    void forEach(EntryConsumer consumer);

    /**
     * Removes each entry that matches the predicate.
     *
     * @param predicate the predicate to test entries
     * @return the number of removed entries
     */
    int removeIf(EntryPredicate predicate);
}
