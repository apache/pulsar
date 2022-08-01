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
package org.apache.pulsar.common.util.collections;

import java.util.Set;
import java.util.function.Function;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet.LongPair;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet.LongPairConsumer;

/**
 * Hash set where values are composed of pairs of longs.
 */
public interface LongPairSet {

    /**
     * Adds composite value of item1 and item2 to set.
     *
     * @param item1
     * @param item2
     * @return
     */
    boolean add(long item1, long item2);

    /**
     * Removes composite value of item1 and item2 from set.
     *
     * @param item1
     * @param item2
     * @return
     */
    boolean remove(long item1, long item2);

    /**
     * Removes composite value of item1 and item2 from set if provided predicate {@link LongPairPredicate} matches.
     *
     * @param filter
     * @return
     */
    int removeIf(LongPairPredicate filter);

    /**
     * Execute {@link LongPairConsumer} processor for each entry in the set.
     *
     * @param processor
     */
    void forEach(LongPairConsumer processor);

    /**
     * @return a new list of all keys (makes a copy)
     */
    Set<LongPair> items();

    /**
     * @return a new list of keys with max provided numberOfItems (makes a copy)
     */
    Set<LongPair> items(int numberOfItems);

    /**
     *
     * @param numberOfItems
     * @param longPairConverter
     *            converts (long,long) pair to <T> object
     *
     * @return a new list of keys with max provided numberOfItems
     */
    <T> Set<T> items(int numberOfItems, LongPairFunction<T> longPairConverter);

    /**
     * Check if set is empty.
     *
     * @return
     */
    boolean isEmpty();

    /**
     * Removes all items from set.
     */
    void clear();

    /**
     * Predicate to checks for a key-value pair where both of them have long types.
     */
    public interface LongPairPredicate {
        boolean test(long v1, long v2);
    }

    /**
     * Returns size of the set.
     *
     * @return
     */
    long size();

    /**
     * Returns capacity of the set.
     *
     * @return
     */
    long capacity();

    /**
     * Checks if given (item1,item2) composite value exists into set.
     *
     * @param item1
     * @param item2
     * @return
     */
    boolean contains(long item1, long item2);

    /**
     * Represents a function that accepts two long arguments and produces a result. This is the two-arity specialization
     * of {@link Function}.
     *
     * @param <T>
     *            the type of the result of the function
     *
     */
    @FunctionalInterface
    public interface LongPairFunction<T> {

        /**
         * Applies this function to the given arguments.
         *
         * @param item1
         *            the first function argument
         * @param item2
         *            the second function argument
         * @return the function result
         */
        T apply(long item1, long item2);
    }
}
