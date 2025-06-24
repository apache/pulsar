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
import org.apache.pulsar.common.util.StringInterner;

/**
 * A cache for TopicName instances that allows deduplication and efficient memory usage.
 * It uses soft references to allow garbage collection of unused TopicName instances under heavy memory pressure.
 * This cache uses ConcurrentHashMap for lookups for performance over Guava Cache and Caffeine Cache
 * since there was a concern in https://github.com/apache/pulsar/pull/23052 about high CPU usage for TopicName lookups.
 */
class TopicNameCache {
    static final TopicNameCache INSTANCE = new TopicNameCache();
    private static int cacheMaxSize = 100000;
    private static int reduceSizeByPercentage = 25;

    // Deduplicates TopicName instances when the cached entry isn't in the actual cache.
    // Holds weak references to TopicName so it won't prevent garbage collection.
    private final Interner<TopicName> topicNameInterner = Interners.newWeakInterner();
    // Cache for TopicName instances using ConcurrentHashMap and SoftReference to allow
    private final ConcurrentMap<String, SoftReferenceTopicName> cache = new ConcurrentHashMap<>();
    private final ReferenceQueue<? super TopicName> referenceQueue = new ReferenceQueue<>();
    private final AtomicBoolean cacheShrinkNeeded = new AtomicBoolean(false);
    private final AtomicLong nextReferenceQueuePurge = new AtomicLong();
    private static final long REFERENCE_QUEUE_PURGE_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    // Values are held as soft references to allow garbage collection when memory is low.
    private static final class SoftReferenceTopicName extends SoftReference<TopicName> {
        private final String topic;

        public SoftReferenceTopicName(String topic, TopicName referent, ReferenceQueue<? super TopicName> q) {
            super(referent, q);
            this.topic = topic;
        }

        public String getTopic() {
            return topic;
        }
    }

    public void invalidateCache() {
        cache.clear();
    }

    public TopicName get(String topic) {
        // first do a quick lookup in the cache
        TopicName topicName = cache.get(topic).get();
        if (topicName == null) {
            // intern the topic name to deduplicate topic names used as keys
            topic = StringInterner.intern(topic);
            topicName = cache.computeIfAbsent(topic, key -> {
                return createSoftReferenceTopicName(key);
            }).get();
            if (cache.size() >= cacheMaxSize) {
                cacheShrinkNeeded.compareAndSet(false, true);
            }
        }
        // There has been a garbage collection and the soft reference has been cleared.
        if (topicName == null) {
            // remove the possible stale entry from the cache
            topicName = cache.compute(topic, (key, existingRef) -> {
                if (existingRef == null || existingRef.get() == null) {
                    return createSoftReferenceTopicName(key);
                }
                return existingRef;
            }).get();
            if (cache.size() >= cacheMaxSize) {
                cacheShrinkNeeded.compareAndSet(false, true);
            }
        }
        doCacheMaintenance();
        return topicName;
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

    private SoftReferenceTopicName createSoftReferenceTopicName(String topic) {
        TopicName topicName = topicNameInterner.intern(new TopicName(topic));
        return new SoftReferenceTopicName(topic, topicName, referenceQueue);
    }

    private void shrinkCacheSize() {
        if (cache.size() >= cacheMaxSize) {
            // Reduce the cache size after reaching the maximum size
            int reduceSizeBy =
                    cache.size() - (int) (cacheMaxSize * ((100 - reduceSizeByPercentage) / 100.0));
            // this doesn't remove the oldest entries, but rather reduces the size by a percentage
            // keeping the order of added entries would add more overhead and Caffeine Cache would be a better fit
            // in that case.
            for (Iterator<String> iterator = cache.keySet().iterator(); iterator.hasNext(); ) {
                if (reduceSizeBy <= 0) {
                    break;
                }
                String oldestKey = iterator.next();
                SoftReferenceTopicName ref = cache.remove(oldestKey);
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
            SoftReferenceTopicName ref = (SoftReferenceTopicName) referenceQueue.poll();
            if (ref == null) {
                break;
            }
            String topic = ref.getTopic();
            cache.remove(topic);
        }
    }
}
