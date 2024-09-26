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
package org.apache.pulsar.broker.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.roaringbitmap.RoaringBitmap;

/**
 * This is a consumer selector based fixed hash range.
 *
 * The implementation uses consistent hashing to evenly split, the
 * number of keys assigned to each consumer.
 */
public class ConsistentHashingStickyKeyConsumerSelector implements StickyKeyConsumerSelector {
    // use NUL character as field separator for hash key calculation
    private static final String KEY_SEPARATOR = "\0";
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Consistent-Hash ring
    private final NavigableMap<Integer, ConsumerIdentityWrapper> hashRing;
    private final ConsumerNameIndexTracker consumerNameIndexTracker = new ConsumerNameIndexTracker();


    private final int numberOfPoints;

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints) {
        this.hashRing = new TreeMap<>();
        this.numberOfPoints = numberOfPoints;
    }

    private static class ConsumerIdentityWrapper {
        final Consumer consumer;

        public ConsumerIdentityWrapper(Consumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ConsumerIdentityWrapper) {
                ConsumerIdentityWrapper other = (ConsumerIdentityWrapper) obj;
                return consumer == other.consumer;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return consumer.hashCode();
        }

        @Override
        public String toString() {
            return consumer.toString();
        }
    }

    private static class ConsumerNameIndexTracker {
        private final Map<String, RoaringBitmap> consumerNameCounters = new HashMap<>();
        private final Map<ConsumerIdentityWrapper, ConsumerEntry> consumerEntries = new HashMap<>();
        record ConsumerEntry(String consumerName, int nameIndex, MutableInt refCount) {
        }

        private RoaringBitmap getConsumerNameIndexBitmap(String consumerName) {
            return consumerNameCounters.computeIfAbsent(consumerName,
                    k -> new RoaringBitmap());
        }

        private int allocateConsumerNameIndex(String consumerName) {
            RoaringBitmap bitmap = getConsumerNameIndexBitmap(consumerName);
            // find the first index that is not set, if there is no such index, add a new one
            int index = (int) bitmap.nextAbsentValue(0);
            if (index == -1) {
                index = bitmap.getCardinality();
            }
            bitmap.add(index);
            return index;
        }

        private void deallocateConsumerNameIndex(String consumerName, int index) {
            RoaringBitmap bitmap = getConsumerNameIndexBitmap(consumerName);
            bitmap.remove(index);
            if (bitmap.isEmpty()) {
                consumerNameCounters.remove(consumerName);
            }
        }

        public void removeHashRingReference(ConsumerIdentityWrapper removed) {
            ConsumerEntry consumerEntry = consumerEntries.get(removed);
            int refCount = consumerEntry.refCount.decrementAndGet();
            if (refCount == 0) {
                deallocateConsumerNameIndex(consumerEntry.consumerName, consumerEntry.nameIndex);
                consumerEntries.remove(removed, consumerEntry);
            }
        }

        public int addHashRingReference(ConsumerIdentityWrapper wrapper) {
            String consumerName = wrapper.consumer.consumerName();
            ConsumerEntry entry = consumerEntries.computeIfAbsent(wrapper,
                    k -> new ConsumerEntry(consumerName, allocateConsumerNameIndex(consumerName),
                            new MutableInt(0)));
            entry.refCount.increment();
            return entry.nameIndex;
        }

        public int getTrackedConsumerNameIndex(ConsumerIdentityWrapper wrapper) {
            ConsumerEntry consumerEntry = consumerEntries.get(wrapper);
            return consumerEntry != null ? consumerEntry.nameIndex : -1;
        }
    }

    @Override
    public CompletableFuture<Void> addConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        try {
            ConsumerIdentityWrapper consumerIdentityWrapper = new ConsumerIdentityWrapper(consumer);
            // Insert multiple points on the hash ring for every consumer
            // The points are deterministically added based on the hash of the consumer name
            for (int i = 0; i < numberOfPoints; i++) {
                int consumerNameIndex = consumerNameIndexTracker.addHashRingReference(consumerIdentityWrapper);
                int hash = calculateHashForConsumerAndIndex(consumer, consumerNameIndex, i);
                ConsumerIdentityWrapper removed = hashRing.put(hash, consumerIdentityWrapper);
                if (removed != null) {
                    consumerNameIndexTracker.removeHashRingReference(removed);
                }
            }
            return CompletableFuture.completedFuture(null);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private static int calculateHashForConsumerAndIndex(Consumer consumer, int consumerNameIndex, int index) {
        String key = consumer.consumerName() + KEY_SEPARATOR + consumerNameIndex + KEY_SEPARATOR + index;
        return Murmur3_32Hash.getInstance().makeHash(key.getBytes());
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        try {
            ConsumerIdentityWrapper consumerIdentityWrapper = new ConsumerIdentityWrapper(consumer);
            int consumerNameIndex = consumerNameIndexTracker.getTrackedConsumerNameIndex(consumerIdentityWrapper);
            if (consumerNameIndex > -1) {
                // Remove all the points that were added for this consumer
                for (int i = 0; i < numberOfPoints; i++) {
                    int hash = calculateHashForConsumerAndIndex(consumer, consumerNameIndex, i);
                    if (hashRing.remove(hash, consumerIdentityWrapper)) {
                        consumerNameIndexTracker.removeHashRingReference(consumerIdentityWrapper);
                    }
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public Consumer select(int hash) {
        rwLock.readLock().lock();
        try {
            if (hashRing.isEmpty()) {
                return null;
            }

            Map.Entry<Integer, ConsumerIdentityWrapper> ceilingEntry = hashRing.ceilingEntry(hash);
            if (ceilingEntry != null) {
                return ceilingEntry.getValue().consumer;
            } else {
                return hashRing.firstEntry().getValue().consumer;
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        Map<Consumer, List<Range>> result = new IdentityHashMap<>();
        rwLock.readLock().lock();
        try {
            if (hashRing.isEmpty()) {
                return result;
            }
            int start = 0;
            int lastKey = 0;
            for (Map.Entry<Integer, ConsumerIdentityWrapper> entry: hashRing.entrySet()) {
                Consumer consumer = entry.getValue().consumer;
                result.computeIfAbsent(consumer, key -> new ArrayList<>())
                        .add(Range.of(start, entry.getKey()));
                lastKey = entry.getKey() + 1;
                start = lastKey;
            }
            // Handle wrap-around
            Consumer firstConsumer = hashRing.firstEntry().getValue().consumer;
            List<Range> ranges = result.get(firstConsumer);
            if (lastKey != Integer.MAX_VALUE - 1) {
                ranges.add(Range.of(lastKey + 1, Integer.MAX_VALUE - 1));
            }
        } finally {
            rwLock.readLock().unlock();
        }
        return result;
    }
}
