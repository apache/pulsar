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

import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Strategy for cleaning up expired entries from schema cache.
 */
@Slf4j
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SchemaCacheCleanupStrategy {

    private final long expireTimeMillis;
    private final Map<Class<?>, Long> lastAccessTimes;
    
    // Threshold for minimum number of entries to keep in cache
    private static final int MIN_ENTRIES = 10;
    
    // Maximum number of entries to remove in single cleanup
    private static final int MAX_REMOVE_PER_CLEANUP = 100;

    /**
     * Create a new cleanup strategy.
     *
     * @param expireSeconds time in seconds after which entries expire
     */
    public SchemaCacheCleanupStrategy(long expireSeconds) {
        this.expireTimeMillis = expireSeconds * 1000;
        this.lastAccessTimes = new ConcurrentHashMap<>();
        
        if (log.isDebugEnabled()) {
            log.debug("Initialized schema cache cleanup strategy with expire time: {} seconds", expireSeconds);
        }
    }

    /**
     * Record access time for a class.
     *
     * @param clazz the class being accessed
     */
    public void recordAccess(Class<?> clazz) {
        lastAccessTimes.put(clazz, System.currentTimeMillis());
        
        if (log.isTraceEnabled()) {
            log.trace("Recorded access for class: {}", clazz.getName());
        }
    }

    /**
     * Check if an entry should be evicted based on its last access time.
     *
     * @param clazz the class to check
     * @return true if the entry should be evicted
     */
    public boolean shouldEvict(Class<?> clazz) {
        Long lastAccess = lastAccessTimes.get(clazz);
        if (lastAccess == null) {
            if (log.isDebugEnabled()) {
                log.debug("No last access time found for class: {}, marking for eviction", clazz.getName());
            }
            return true;
        }
        
        boolean shouldEvict = System.currentTimeMillis() - lastAccess > expireTimeMillis;
        
        if (log.isDebugEnabled() && shouldEvict) {
            log.debug("Class {} marked for eviction, last access time: {}", clazz.getName(), lastAccess);
        }
        
        return shouldEvict;
    }

    /**
     * Clean up expired entries from the cache.
     *
     * @param cache the schema cache to clean
     */
    public void cleanup(WeakHashMap<Class<?>, Schema<?>> cache) {
        if (cache.size() <= MIN_ENTRIES) {
            if (log.isDebugEnabled()) {
                log.debug("Cache size {} is below minimum threshold {}, skipping cleanup", 
                    cache.size(), MIN_ENTRIES);
            }
            return;
        }

        int removedCount = 0;
        Iterator<Map.Entry<Class<?>, Schema<?>>> iterator = cache.entrySet().iterator();
        
        while (iterator.hasNext() && removedCount < MAX_REMOVE_PER_CLEANUP) {
            Map.Entry<Class<?>, Schema<?>> entry = iterator.next();
            Class<?> clazz = entry.getKey();
            
            if (shouldEvict(clazz)) {
                iterator.remove();
                lastAccessTimes.remove(clazz);
                removedCount++;
                SchemaCacheMetrics.recordEviction();
                
                if (log.isDebugEnabled()) {
                    log.debug("Evicted schema for class: {}", clazz.getName());
                }
            }
        }

        if (log.isInfoEnabled() && removedCount > 0) {
            log.info("Cleaned up {} expired entries from schema cache", removedCount);
        }
    }

    /**
     * Clear all recorded access times.
     */
    public void clear() {
        lastAccessTimes.clear();
        if (log.isDebugEnabled()) {
            log.debug("Cleared all recorded access times");
        }
    }

    /**
     * Get the number of entries being tracked.
     *
     * @return number of entries with recorded access times
     */
    public int getTrackedEntryCount() {
        return lastAccessTimes.size();
    }

    /**
     * Get the expiration time in milliseconds.
     *
     * @return expiration time in milliseconds
     */
    public long getExpireTimeMillis() {
        return expireTimeMillis;
    }

    /**
     * Check if a class has been accessed.
     *
     * @param clazz the class to check
     * @return true if the class has a recorded access time
     */
    public boolean hasAccessRecord(Class<?> clazz) {
        return lastAccessTimes.containsKey(clazz);
    }

    /**
     * Get the last access time for a class.
     *
     * @param clazz the class to check
     * @return last access time in milliseconds, or null if never accessed
     */
    public Long getLastAccessTime(Class<?> clazz) {
        return lastAccessTimes.get(clazz);
    }
}