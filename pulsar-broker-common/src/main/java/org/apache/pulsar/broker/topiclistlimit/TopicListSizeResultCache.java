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
import java.util.function.BooleanSupplier;
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
    // how soon after measuring an oversized topic list the listing may measure it again while the limiter is idle
    private static final Duration DEFAULT_OVERSIZED_REMEASURE_INTERVAL = Duration.ofMinutes(1);
    // how soon after measuring an oversized topic list the listing may measure it again even if the limiter is busy
    private static final Duration DEFAULT_OVERSIZED_FORCED_REMEASURE_INTERVAL = Duration.ofMinutes(10);
    // how long a re-measuring request blocks other re-measuring requests unless it completes before
    private static final Duration DEFAULT_REMEASURE_GATE_TIMEOUT = Duration.ofMinutes(1);

    private final Cache<CacheKey, ResultHolder> topicListSizeCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .build();
    private final OversizedRemeasurePolicy remeasurePolicy;

    /**
     * Creates a cache without a size limit.
     */
    public TopicListSizeResultCache() {
        this(0, () -> true);
    }

    /**
     * Creates a cache for listings that are limited by the given topic list heap memory limit.
     *
     * <p>A topic list larger than the limit can't be served. Requests for a listing whose estimate is larger than
     * the limit get the estimate as-is, so that they fail without waiting for permits. To let the listing recover
     * after its topic list has become smaller, a request may get the limit as its estimate so that it lists the
     * topics and measures the list again. That request needs all permits, and permits are granted in order, so
     * while it waits for the permits of other listings, all later listings wait behind it. Therefore:
     * <ul>
     *     <li>a listing is measured again at the earliest one interval after its oversized size was measured,</li>
     *     <li>only one listing at a time is measured again,</li>
     *     <li>and only while no other topic list is in flight or waiting for permits. When the limiter hasn't been
     *     idle for a longer interval, the listing is measured again anyway, so that it can recover on a broker that
     *     is never idle.</li>
     * </ul>
     *
     * @param sizeLimit the topic list heap memory limit, use {@code <= 0} for no limit
     * @param limiterIdle supplier that returns true when no topic list permits are acquired or requested
     */
    public TopicListSizeResultCache(long sizeLimit, BooleanSupplier limiterIdle) {
        this(sizeLimit, DEFAULT_OVERSIZED_REMEASURE_INTERVAL, DEFAULT_OVERSIZED_FORCED_REMEASURE_INTERVAL,
                limiterIdle);
    }

    @VisibleForTesting
    TopicListSizeResultCache(long sizeLimit, Duration oversizedRemeasureInterval,
                             Duration oversizedForcedRemeasureInterval, BooleanSupplier limiterIdle) {
        this.remeasurePolicy = new OversizedRemeasurePolicy(sizeLimit > 0 ? sizeLimit : Long.MAX_VALUE,
                oversizedRemeasureInterval.toNanos(), oversizedForcedRemeasureInterval.toNanos(),
                DEFAULT_REMEASURE_GATE_TIMEOUT.toNanos(), limiterIdle, new AtomicReference<>(),
                new AtomicLong(System.nanoTime()));
    }

    /**
     * Returns the topic list heap memory limit, {@link Long#MAX_VALUE} when there's no limit. An estimate larger
     * than the limit can't be granted.
     */
    public long getSizeLimit() {
        return remeasurePolicy.sizeLimit();
    }

    /**
     * The listing that is being measured again, and the time when it stops blocking other listings from being
     * measured again unless it records its size before that.
     */
    private record RemeasureGate(ResultHolder owner, long expiresAtNanos) {
    }

    /**
     * The size limit and the rules for measuring an oversized topic list again.
     *
     * @param gate the listing that is being measured again. Only one listing is measured again at a time.
     * @param nextForcedRemeasureNanos the earliest time when any listing may be measured again while the limiter
     *                                 is busy. The listings have separate estimates for each set of properties,
     *                                 which come from the clients, so the number of forced re-measures is limited
     *                                 for the whole broker.
     */
    private record OversizedRemeasurePolicy(long sizeLimit, long intervalNanos, long forcedIntervalNanos,
                                            long gateTimeoutNanos, BooleanSupplier limiterIdle,
                                            AtomicReference<RemeasureGate> gate,
                                            AtomicLong nextForcedRemeasureNanos) {
        static final OversizedRemeasurePolicy UNLIMITED = new OversizedRemeasurePolicy(Long.MAX_VALUE, 0, 0, 0,
                () -> true, new AtomicReference<>(), new AtomicLong());

        boolean tryForcedRemeasure(long now) {
            long next = nextForcedRemeasureNanos.get();
            return now - next >= 0 && nextForcedRemeasureNanos.compareAndSet(next, now + forcedIntervalNanos);
        }

        boolean tryEnterGate(ResultHolder holder, long now) {
            RemeasureGate current = gate.get();
            return (current == null || now - current.expiresAtNanos() >= 0)
                    && gate.compareAndSet(current, new RemeasureGate(holder, now + gateTimeoutNanos));
        }

        void exitGate(ResultHolder holder) {
            RemeasureGate current = gate.get();
            if (current != null && current.owner() == holder) {
                gate.compareAndSet(current, null);
            }
        }
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
        private final OversizedRemeasurePolicy policy;
        private final long sizeLimit;
        private final long initialEstimate;
        // the earliest time when a request for an oversized listing may measure the topic list again
        private volatile long nextRemeasureNanos;
        // the earliest time when the topic list is measured again even if the limiter is busy
        private volatile long nextForcedRemeasureNanos;

        public ResultHolder() {
            this(OversizedRemeasurePolicy.UNLIMITED, 0);
        }

        private ResultHolder(OversizedRemeasurePolicy policy, long knownSizeEstimate) {
            this.policy = policy;
            this.sizeLimit = policy.sizeLimit();
            this.initialEstimate = Math.max(Math.min(INITIAL_TOPIC_LIST_HEAP_SIZE, sizeLimit), knownSizeEstimate);
            long now = System.nanoTime();
            // a listing that starts from an oversized known estimate hasn't been measured yet
            this.nextRemeasureNanos = now;
            this.nextForcedRemeasureNanos = now + policy.forcedIntervalNanos();
        }

        /**
         * Get the topic list size estimate. The first request will return the initial estimate
         * and update the estimate based on the returned size of the topic list. Other concurrent requests
         * will wait for the first request to complete and use the estimate of the first request.
         * Subsequent requests will use estimate which gets updated based on the returned size of the topic list
         * of each request.
         * When the estimate is larger than the size limit, it's returned as-is so that the request fails without
         * waiting for permits, except for a request that gets the size limit so that it can measure the topic
         * list again (see {@link TopicListSizeResultCache#TopicListSizeResultCache(long, BooleanSupplier)}).
         * @return a future that will return the topic list size estimate
         */
        public CompletableFuture<Long> getSizeAsync() {
            CompletableFuture<Long> estimate;
            if (topicListSizeFuture.compareAndSet(null, new CompletableFuture<>())) {
                // let the first request proceed with the initial estimate
                estimate = CompletableFuture.completedFuture(initialEstimate);
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
            // Requesting all permits while other topic lists are in flight would make the request wait for them,
            // and all later requests would wait behind it, since the permits are granted in order. Measure the
            // list again only when no other topic list is in flight, or when the limiter hasn't been idle for a
            // longer time, and only one listing at a time.
            boolean limiterIdle = policy.limiterIdle().getAsBoolean();
            if (now - nextRemeasureNanos >= 0
                    && (limiterIdle || now - nextForcedRemeasureNanos >= 0)
                    && policy.tryEnterGate(this, now)) {
                if (limiterIdle || policy.tryForcedRemeasure(now)) {
                    nextRemeasureNanos = now + policy.intervalNanos();
                    nextForcedRemeasureNanos = now + policy.forcedIntervalNanos();
                    // a request that can be granted, so that the topic list gets measured again
                    return sizeLimit;
                }
                policy.exitGate(this);
            }
            // acquiring permits for an estimate larger than the limit fails right away
            return estimate;
        }

        /**
         * Update the topic list size estimate. The last changed value will be used.
         *
         * @param actualSize the actual size of the topic list
         */
        public void updateSize(long actualSize) {
            if (actualSize > sizeLimit) {
                // the topic list can be measured again after the interval
                long now = System.nanoTime();
                nextRemeasureNanos = now + policy.intervalNanos();
                nextForcedRemeasureNanos = now + policy.forcedIntervalNanos();
            }
            // the topic list has been measured, another listing may be measured again
            policy.exitGate(this);
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
                currentFuture.complete(initialEstimate);
            }
        }

        /**
         * Returns the current size estimate, or 0 when there's no measured estimate yet.
         */
        long getKnownSizeEstimate() {
            CompletableFuture<Long> currentFuture = topicListSizeFuture.get();
            if (currentFuture == null || !currentFuture.isDone() || currentFuture.isCompletedExceptionally()) {
                return 0;
            }
            return currentFuture.getNow(0L);
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
        CacheKey key = new CacheKey(listing, properties);
        ResultHolder holder = topicListSizeCache.getIfPresent(key);
        if (holder != null) {
            return holder;
        }
        // The properties come from the client. A listing with a new set of properties starts from the estimate of
        // the same listing without properties, so that a request can't start below the known size of the listing
        // by sending new property values. With the default topic listing, which ignores the properties, the lists
        // are the same.
        long knownSizeEstimate = 0;
        if (key.propertiesHash() != null) {
            ResultHolder holderWithoutProperties =
                    topicListSizeCache.getIfPresent(new CacheKey(listing, (HashCode) null));
            if (holderWithoutProperties != null) {
                knownSizeEstimate = holderWithoutProperties.getKnownSizeEstimate();
            }
        }
        long initialSizeEstimate = knownSizeEstimate;
        return topicListSizeCache.asMap().computeIfAbsent(key,
                __ -> new ResultHolder(remeasurePolicy, initialSizeEstimate));
    }
}
