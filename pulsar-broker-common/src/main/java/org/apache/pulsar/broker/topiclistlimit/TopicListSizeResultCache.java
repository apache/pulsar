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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
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
 * The cache uses the topic listing ({@link TopicListing}: namespace name, topic list mode
 * (PERSISTENT/NON_PERSISTENT/ALL) and listing type) and the optional topic listing properties (PIP-452) as keys
 * and maintains size estimates that are refined with actual usage.
 */
public class TopicListSizeResultCache {
    // 10kB initial estimate for topic list heap size
    private static final long INITIAL_TOPIC_LIST_HEAP_SIZE = 10 * 1024;
    // how often a listing whose estimate is larger than the limit is allowed to measure the topic list again
    private static final Duration DEFAULT_OVERSIZED_REMEASURE_INTERVAL = Duration.ofMinutes(1);

    private final Cache<CacheKey, ResultHolder> topicListSizeCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .build();
    private final long sizeLimit;
    private final long oversizedRemeasureIntervalNanos;

    /**
     * Creates a cache without a size limit.
     */
    public TopicListSizeResultCache() {
        this(0);
    }

    /**
     * Creates a cache for listings that are limited by the given topic list heap memory limit.
     *
     * @param sizeLimit the topic list heap memory limit, use {@code <= 0} for no limit. A topic list larger than
     *                  the limit can't be served. Requests for a listing whose estimate is larger than the limit get
     *                  the estimate as-is, so that they fail without waiting for permits. Once per interval, a
     *                  request gets the limit as the estimate instead, so that it can list the topics and measure
     *                  the topic list again, which lets the listing recover after the topic list has become
     *                  smaller.
     */
    public TopicListSizeResultCache(long sizeLimit) {
        this(sizeLimit, DEFAULT_OVERSIZED_REMEASURE_INTERVAL);
    }

    @VisibleForTesting
    TopicListSizeResultCache(long sizeLimit, Duration oversizedRemeasureInterval) {
        this.sizeLimit = sizeLimit > 0 ? sizeLimit : Long.MAX_VALUE;
        this.oversizedRemeasureIntervalNanos = oversizedRemeasureInterval.toNanos();
    }

    /**
     * Cache key. The topic listing properties are stored as a hash so that the client provided property values
     * aren't retained in the cache. A hash collision only results in sharing a size estimate.
     */
    record CacheKey(TopicListing listing, HashCode propertiesHash) {
        CacheKey(String namespaceName, CommandGetTopicsOfNamespace.Mode mode) {
            this(TopicListing.namespaceTopics(namespaceName, mode), (HashCode) null);
        }

        CacheKey(TopicListing listing, Map<String, String> properties) {
            this(listing, hashProperties(properties));
        }

        private static HashCode hashProperties(Map<String, String> properties) {
            if (properties == null || properties.isEmpty()) {
                return null;
            }
            Hasher hasher = Hashing.murmur3_128().newHasher();
            properties.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey(Comparator.nullsFirst(Comparator.naturalOrder())))
                    .forEach(entry -> {
                        putString(hasher, entry.getKey());
                        putString(hasher, entry.getValue());
                    });
            return hasher.hash();
        }

        private static void putString(Hasher hasher, String value) {
            if (value == null) {
                hasher.putInt(-1);
            } else {
                // length prefix keeps the encoding of the entries unambiguous
                hasher.putInt(value.length());
                hasher.putString(value, StandardCharsets.UTF_8);
            }
        }
    }

    /**
     * Holds the topic list size estimate and future for the topic list size.
     * The size is returned by calling {@link #getSizeAsync()} method which is asynchronous.
     * The estimate is updated by calling {@link #updateSize(long)} method.
     */
    public static class ResultHolder {
        private final AtomicReference<CompletableFuture<Long>> topicListSizeFuture =
                new AtomicReference<>(null);
        private final long sizeLimit;
        private final long oversizedRemeasureIntervalNanos;
        // the earliest time when a request for an oversized listing may measure the topic list again
        private final AtomicLong nextOversizedRemeasureNanos;

        public ResultHolder() {
            this(Long.MAX_VALUE, 0);
        }

        ResultHolder(long sizeLimit, long oversizedRemeasureIntervalNanos) {
            this.sizeLimit = sizeLimit;
            this.oversizedRemeasureIntervalNanos = oversizedRemeasureIntervalNanos;
            this.nextOversizedRemeasureNanos = new AtomicLong(System.nanoTime());
        }

        /**
         * Get the topic list size estimate. The first request will return the initial estimate
         * and update the estimate based on the returned size of the topic list. Other concurrent requests
         * will wait for the first request to complete and use the estimate of the first request.
         * Subsequent requests will use estimate which gets updated based on the returned size of the topic list
         * of each request.
         * When the estimate is larger than the size limit, it's returned as-is so that the request fails without
         * waiting for permits, except for one request per interval, which gets the size limit so that it can
         * measure the topic list again.
         * @return a future that will return the topic list size estimate
         */
        public CompletableFuture<Long> getSizeAsync() {
            CompletableFuture<Long> estimate;
            if (topicListSizeFuture.compareAndSet(null, new CompletableFuture<>())) {
                // let the first request proceed with the initial estimate
                estimate = CompletableFuture.completedFuture(Math.min(INITIAL_TOPIC_LIST_HEAP_SIZE, sizeLimit));
            } else {
                // all other requests wait for the first one to complete
                estimate = topicListSizeFuture.get();
            }
            return sizeLimit == Long.MAX_VALUE ? estimate : estimate.thenApply(this::limitOversizedEstimate);
        }

        private long limitOversizedEstimate(long estimate) {
            if (estimate <= sizeLimit) {
                return estimate;
            }
            long now = System.nanoTime();
            long next = nextOversizedRemeasureNanos.get();
            if (now - next >= 0
                    && nextOversizedRemeasureNanos.compareAndSet(next, now + oversizedRemeasureIntervalNanos)) {
                // a request that can be granted, so that the topic list gets measured again
                return sizeLimit;
            }
            // acquiring permits for an estimate larger than the limit fails right away. Requesting all permits
            // instead would make the request wait until no other topic list is in flight, and other requests
            // would wait behind it.
            return estimate;
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
        return getTopicListSize(namespaceName, mode, null);
    }

    /**
     * Get the topic list size result holder for the given namespace, mode and topic listing properties.
     * The properties are passed to a customized topic listing implementation (PIP-452) which can return
     * a different topic list for different properties. Therefore, the estimate is kept separately for each
     * distinct set of properties.
     * @param namespaceName the namespace name in the format of "tenant/namespace"
     * @param mode the mode of the topic list request (PERSISTENT, NON_PERSISTENT, ALL)
     * @param properties the topic listing properties, null or empty when not provided
     * @return the topic list size result holder
     */
    public ResultHolder getTopicListSize(String namespaceName,
                                         CommandGetTopicsOfNamespace.Mode mode,
                                         Map<String, String> properties) {
        return getTopicListSize(TopicListing.namespaceTopics(namespaceName, mode), properties);
    }

    /**
     * Get the topic list size result holder for the given topic listing and topic listing properties.
     * @param listing the topic listing
     * @param properties the topic listing properties (PIP-452), null or empty when not provided
     * @return the topic list size result holder
     */
    public ResultHolder getTopicListSize(TopicListing listing, Map<String, String> properties) {
        return topicListSizeCache.asMap()
                .computeIfAbsent(new CacheKey(listing, properties),
                        __ -> new ResultHolder(sizeLimit, oversizedRemeasureIntervalNanos));
    }
}
