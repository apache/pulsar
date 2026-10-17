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
package org.apache.pulsar.broker.topiclistlimit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;

/**
 * A cache for storing and managing topic list size estimates in namespaces.
 * This class provides functionality to:
 * - Cache and retrieve topic list size estimates for different namespaces and modes
 * - Handle concurrent requests for topic list sizes efficiently
 * - Maintain and update size estimates based on actual topic list sizes
 * - Prevent thundering herd problems when multiple concurrent requests for a namespace are made without
 *   a previous size estimate
 * The cache uses namespace name and topic list mode (PERSISTENT/NON_PERSISTENT/ALL) as keys
 * and maintains size estimates that are refined with actual usage.
 */
public class TopicListSizeResultCache {
    // 10kB initial estimate for topic list heap size
    private static final long INITIAL_TOPIC_LIST_HEAP_SIZE = 10 * 1024;

    private Cache<CacheKey, ResultHolder> topicListSizeCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .build();

    record CacheKey(String namespaceName, CommandGetTopicsOfNamespace.Mode mode) {
    }

    /**
     * Holds the topic list size estimate and future for the topic list size.
     * The size is returned by calling {@link #getSizeAsync()} method which is asynchronous.
     * The estimate is updated by calling {@link #updateSize(long)} method.
     */
    public static class ResultHolder {
        private final AtomicReference<CompletableFuture<Long>> topicListSizeFuture =
                new AtomicReference<>(null);

        /**
         * Get the topic list size estimate. The first request will return the initial estimate
         * and update the estimate based on the returned size of the topic list. Other concurrent requests
         * will wait for the first request to complete and use the estimate of the first request.
         * Subsequent requests will use estimate which gets updated based on the returned size of the topic list
         * of each request.
         * @return a future that will return the topic list size estimate
         */
        public CompletableFuture<Long> getSizeAsync() {
            if (topicListSizeFuture.compareAndSet(null, new CompletableFuture<>())) {
                // let the first request proceed with the initial estimate
                return CompletableFuture.completedFuture(INITIAL_TOPIC_LIST_HEAP_SIZE);
            } else {
                // all other requests wait for the first one to complete
                return topicListSizeFuture.get();
            }
        }

        /**
         * Update the topic list size estimate. The last changed value will be used.
         *
         * @param actualSize the actual size of the topic list
         */
        public void updateSize(long actualSize) {
            CompletableFuture<Long> currentFuture = topicListSizeFuture.get();
            if (currentFuture != null && !currentFuture.isDone()) {
                // complete the future if it's not done yet
                currentFuture.complete(actualSize);
            } else if (currentFuture == null || currentFuture.getNow(0L).longValue() != actualSize) {
                // only update the future if the current value is different from the existing value
                topicListSizeFuture.compareAndSet(currentFuture, CompletableFuture.completedFuture(actualSize));
            }
        }

        /**
         * After errors, it's necessary to call this method to ensure that the instance isn't left in a state
         * where concurrent requests are waiting for the first request to complete the future by calling updateSize.
         */
        public void resetIfInitializing() {
            CompletableFuture<Long> currentFuture = topicListSizeFuture.getAndUpdate(value -> {
                if (value != null && !value.isDone()) {
                    return null;
                } else {
                    return value;
                }
            });
            // let all current requests proceed with the initial estimate if this were to happen
            if (currentFuture != null && !currentFuture.isDone()) {
                currentFuture.complete(INITIAL_TOPIC_LIST_HEAP_SIZE);
            }
        }
    }

    /**
     * Get the topic list size result holder for the given namespace and mode.
     * @param namespaceName the namespace name in the format of "tenant/namespace"
     * @param mode the mode of the topic list request (PERSISTENT, NON_PERSISTENT, ALL)
     * @return the topic list size result holder
     */
    public ResultHolder getTopicListSize(String namespaceName,
                                         CommandGetTopicsOfNamespace.Mode mode) {
        return topicListSizeCache.asMap()
                .computeIfAbsent(new CacheKey(namespaceName, mode), __ -> new ResultHolder());
    }
}
