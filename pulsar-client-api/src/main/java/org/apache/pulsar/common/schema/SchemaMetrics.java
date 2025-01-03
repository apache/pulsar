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
package org.apache.pulsar.common.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics collector for schema cache performance monitoring.
 */
@UtilityClass
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SchemaCacheMetrics {
    private static final Logger log = LoggerFactory.getLogger(SchemaCacheMetrics.class);

    private static final AtomicLong CACHE_HITS = new AtomicLong();
    private static final AtomicLong CACHE_MISSES = new AtomicLong();
    private static final AtomicLong CACHE_EVICTIONS = new AtomicLong();
    private static final AtomicLong CACHE_SIZE = new AtomicLong();
    private static final AtomicLong CACHE_LOAD_ERRORS = new AtomicLong();
    private static final AtomicLong CACHE_LOAD_TIME_TOTAL = new AtomicLong();

    /**
     * Metric names.
     */
    public static final class Names {
        public static final String HITS = "hits";
        public static final String MISSES = "misses";
        public static final String EVICTIONS = "evictions";
        public static final String SIZE = "size";
        public static final String HIT_RATE = "hitRate";
        public static final String LOAD_ERRORS = "loadErrors";
        public static final String AVG_LOAD_TIME = "avgLoadTime";
        
        private Names() {}
    }

    /**
     * Record a cache hit.
     */
    public static void recordHit() {
        CACHE_HITS.incrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("Schema cache hit recorded");
        }
    }

    /**
     * Record a cache miss.
     */
    public static void recordMiss() {
        CACHE_MISSES.incrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("Schema cache miss recorded");
        }
    }

    /**
     * Record a cache eviction.
     */
    public static void recordEviction() {
        CACHE_EVICTIONS.incrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("Schema cache eviction recorded");
        }
    }

    /**
     * Record a schema load error.
     */
    public static void recordLoadError() {
        CACHE_LOAD_ERRORS.incrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("Schema load error recorded");
        }
    }

    /**
     * Record schema load time in milliseconds.
     *
     * @param loadTimeMs time taken to load schema in milliseconds
     */
    public static void recordLoadTime(long loadTimeMs) {
        CACHE_LOAD_TIME_TOTAL.addAndGet(loadTimeMs);
        if (log.isDebugEnabled()) {
            log.debug("Schema load time recorded: {} ms", loadTimeMs);
        }
    }

    /**
     * Update current cache size.
     *
     * @param size current number of entries in cache
     */
    public static void updateSize(int size) {
        CACHE_SIZE.set(size);
        if (log.isDebugEnabled()) {
            log.debug("Schema cache size updated to: {}", size);
        }
    }

    /**
     * Reset all metrics to zero.
     */
    public static void reset() {
        CACHE_HITS.set(0);
        CACHE_MISSES.set(0);
        CACHE_EVICTIONS.set(0);
        CACHE_SIZE.set(0);
        CACHE_LOAD_ERRORS.set(0);
        CACHE_LOAD_TIME_TOTAL.set(0);
        if (log.isDebugEnabled()) {
            log.debug("Schema cache metrics reset");
        }
    }

    /**
     * Get all current metric values.
     *
     * @return map of metric name to value
     */
    public static Map<String, Number> getMetrics() {
        Map<String, Number> metrics = new HashMap<>();
        metrics.put(Names.HITS, CACHE_HITS.get());
        metrics.put(Names.MISSES, CACHE_MISSES.get());
        metrics.put(Names.EVICTIONS, CACHE_EVICTIONS.get());
        metrics.put(Names.SIZE, CACHE_SIZE.get());
        metrics.put(Names.HIT_RATE, calculateHitRate());
        metrics.put(Names.LOAD_ERRORS, CACHE_LOAD_ERRORS.get());
        metrics.put(Names.AVG_LOAD_TIME, calculateAverageLoadTime());
        return Collections.unmodifiableMap(metrics);
    }

    /**
     * Calculate cache hit rate as percentage.
     *
     * @return hit rate between 0 and 100
     */
    private static double calculateHitRate() {
        long hits = CACHE_HITS.get();
        long total = hits + CACHE_MISSES.get();
        if (total == 0) {
            return 0.0;
        }
        return (double) hits * 100 / total;
    }

    /**
     * Calculate average schema load time in milliseconds.
     *
     * @return average load time in milliseconds
     */
    private static double calculateAverageLoadTime() {
        long totalTime = CACHE_LOAD_TIME_TOTAL.get();
        long misses = CACHE_MISSES.get();
        if (misses == 0) {
            return 0.0;
        }
        return (double) totalTime / misses;
    }

    /**
     * Get current cache size.
     *
     * @return number of entries in cache
     */
    public static long getSize() {
        return CACHE_SIZE.get();
    }

    /**
     * Get total number of cache hits.
     *
     * @return total number of cache hits
     */
    public static long getHits() {
        return CACHE_HITS.get();
    }

    /**
     * Get total number of cache misses.
     *
     * @return total number of cache misses
     */
    public static long getMisses() {
        return CACHE_MISSES.get();
    }

    /**
     * Get total number of cache evictions.
     *
     * @return total number of cache evictions
     */
    public static long getEvictions() {
        return CACHE_EVICTIONS.get();
    }
}