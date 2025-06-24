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
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntSupplier;
import org.apache.pulsar.common.util.StringInterner;

/**
 * A cache for TopicName and NamespaceName instances that allows deduplication and efficient memory usage.
 * It uses soft references to allow garbage collection of unused instances under heavy memory pressure.
 * This cache uses ConcurrentHashMap for lookups for performance over Guava Cache and Caffeine Cache
 * since there was a concern in https://github.com/apache/pulsar/pull/23052 about high CPU usage for TopicName lookups.
 */
abstract class NameCache<V> {
    private final IntSupplier cacheMaxSize;
    private final IntSupplier reduceSizeByPercentage;
    private final Function<String, V> valueFactory;

    // Deduplicates TopicName instances when the cached entry isn't in the actual cache.
    // Holds weak references to TopicName so it won't prevent garbage collection.
    private final Interner<V> valueInterner = Interners.newWeakInterner();
    // Cache for TopicName instances using ConcurrentHashMap and SoftReference to allow
    private final ConcurrentMap<String, SoftReferenceValue> cache = new ConcurrentHashMap<>();
    private final ReferenceQueue<? super V> referenceQueue = new ReferenceQueue<>();
    private final AtomicBoolean cacheShrinkNeeded = new AtomicBoolean(false);
    private final AtomicLong nextReferenceQueuePurge = new AtomicLong();
    private static final long REFERENCE_QUEUE_PURGE_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

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

    NameCache(IntSupplier cacheMaxSize, IntSupplier reduceSizeByPercentage, Function<String, V> valueFactory) {
        this.cacheMaxSize = cacheMaxSize;
        this.reduceSizeByPercentage = reduceSizeByPercentage;
        this.valueFactory = valueFactory;
    }

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
            if (cache.size() > cacheMaxSize.getAsInt()) {
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
                    System.nanoTime() + REFERENCE_QUEUE_PURGE_INTERVAL_NANOS)) {
                purgeReferenceQueue();
            }
        }
    }

    private SoftReferenceValue createSoftReferenceValue(String key) {
        V valueInstance = valueInterner.intern(valueFactory.apply(key));
        return new SoftReferenceValue(key, valueInstance, referenceQueue);
    }

    private void shrinkCacheSize() {
        int cacheMaxSizeAsInt = cacheMaxSize.getAsInt();
        if (cache.size() > cacheMaxSizeAsInt) {
            // Reduce the cache size after reaching the maximum size
            int reduceSizeBy =
                    cache.size() - (int) (cacheMaxSizeAsInt * ((100 - reduceSizeByPercentage.getAsInt()) / 100.0));
            // this doesn't remove the oldest entries, but rather reduces the size by a percentage
            // keeping the order of added entries would add more overhead and Caffeine Cache would be a better fit
            // in that case.
            for (Iterator<String> iterator = cache.keySet().iterator(); iterator.hasNext(); ) {
                if (reduceSizeBy <= 0) {
                    break;
                }
                String oldestKey = iterator.next();
                SoftReferenceValue ref = cache.remove(oldestKey);
                if (ref != null) {
                    ref.clear();
                }
                iterator.remove();
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
}
