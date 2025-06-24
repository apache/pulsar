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
package org.apache.pulsar.common.naming;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.common.util.StringInterner;

/**
 * A cache for TopicName and NamespaceName instances that allows deduplication and efficient memory usage.
 * It uses soft references to allow garbage collection of unused instances under heavy memory pressure.
 * This cache uses ConcurrentHashMap for lookups for performance over Guava Cache and Caffeine Cache
 * since there was a concern in https://github.com/apache/pulsar/pull/23052 about high CPU usage for TopicName lookups.
 */
abstract class NameCache<V> {
    // Cache instances using ConcurrentHashMap and SoftReference to allow garbage collection to clear unreferenced
    // entries when heap memory is running low.
    private final ConcurrentMap<String, SoftReferenceValue> cache = new ConcurrentHashMap<>();
    // Reference queue to hold cleared soft references, which will be used to remove entries from the cache.
    private final ReferenceQueue<? super V> referenceQueue = new ReferenceQueue<>();
    // Flag to indicate if the cache size needs to be reduced. This is set when the cache exceeds the maximum size.
    private final AtomicBoolean cacheShrinkNeeded = new AtomicBoolean(false);
    // Next timestamp to run reference queue purging to remove cleared references. Handled when cache is accessed.
    private final AtomicLong nextReferenceQueuePurge = new AtomicLong();
    // Deduplicates instances when the cached entry isn't in the actual cache.
    // Holds weak references to the value so it won't prevent garbage collection.
    private final Interner<V> valueInterner = Interners.newWeakInterner();

    // Values are held as soft references to allow garbage collection when memory is low.
    private final class SoftReferenceValue extends SoftReference<V> {
        private final String key;

        public SoftReferenceValue(String key, V referent, ReferenceQueue<? super V> q) {
            super(referent, q);
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }

    protected abstract V createValue(String key);

    protected abstract int getCacheMaxSize();

    protected abstract int getReduceSizeByPercentage();

    protected abstract long getReferenceQueuePurgeIntervalNanos();

    public void invalidateCache() {
        cache.clear();
    }

    public V getIfPresent(String keyParam) {
        SoftReferenceValue softReferenceValue = cache.get(keyParam);
        return softReferenceValue != null ? softReferenceValue.get() : null;
    }
    public V get(String keyParam) {
        // first do a quick lookup in the cache
        V valueInstance = getIfPresent(keyParam);
        if (valueInstance == null) {
            // intern the topic name to deduplicate topic names used as keys, since this will reduce heap memory usage
            keyParam = StringInterner.intern(keyParam);
            // add new entry or replace the possible stale entry
            valueInstance = cache.compute(keyParam, (key, existingRef) -> {
                if (existingRef == null || existingRef.get() == null) {
                    return createSoftReferenceValue(key);
                }
                return existingRef;
            }).get();
            if (cache.size() > getCacheMaxSize()) {
                cacheShrinkNeeded.compareAndSet(false, true);
            }
        }
        doCacheMaintenance();
        return valueInstance;
    }

    private void doCacheMaintenance() {
        if (cacheShrinkNeeded.compareAndSet(true, false)) {
            shrinkCacheSize();
        }
        long localNextReferenceQueuePurge = nextReferenceQueuePurge.get();
        if (localNextReferenceQueuePurge == 0 || System.nanoTime() > localNextReferenceQueuePurge) {
            if (nextReferenceQueuePurge.compareAndSet(localNextReferenceQueuePurge,
                    System.nanoTime() + getReferenceQueuePurgeIntervalNanos())) {
                purgeReferenceQueue();
            }
        }
    }

    private SoftReferenceValue createSoftReferenceValue(String key) {
        V valueInstance = valueInterner.intern(createValue(key));
        return new SoftReferenceValue(key, valueInstance, referenceQueue);
    }

    private void shrinkCacheSize() {
        int cacheMaxSizeAsInt = getCacheMaxSize();
        if (cache.size() > cacheMaxSizeAsInt) {
            // Reduce the cache size after reaching the maximum size
            int reduceSizeBy =
                    cache.size() - (int) (cacheMaxSizeAsInt * ((100 - getReduceSizeByPercentage()) / 100.0));
            // this doesn't remove the oldest entries, but rather reduces the size by a percentage
            // keeping the order of added entries would add more overhead and Caffeine Cache would be a better fit
            // in that case.
            for (String key : cache.keySet()) {
                if (reduceSizeBy <= 0) {
                    break;
                }
                SoftReferenceValue ref = cache.remove(key);
                if (ref != null) {
                    ref.clear();
                }
                reduceSizeBy--;
            }
        }
    }

    private void purgeReferenceQueue() {
        // Clean up the reference queue to remove any references cleared by the garbage collector.
        while (true) {
            SoftReferenceValue ref = (SoftReferenceValue) referenceQueue.poll();
            if (ref == null) {
                break;
            }
            cache.remove(ref.getKey());
        }
    }

    public int size() {
        return cache.size();
    }
}
