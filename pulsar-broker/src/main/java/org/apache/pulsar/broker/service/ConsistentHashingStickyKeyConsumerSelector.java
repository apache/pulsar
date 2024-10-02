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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.util.Murmur3_32Hash;

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
    // Tracks the used consumer name indexes for each consumer name
    private final ConsumerNameIndexTracker consumerNameIndexTracker = new ConsumerNameIndexTracker();

    private final int numberOfPoints;

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints) {
        this.hashRing = new TreeMap<>();
        this.numberOfPoints = numberOfPoints;
    }

    @Override
    public CompletableFuture<Void> addConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        try {
            ConsumerIdentityWrapper consumerIdentityWrapper = new ConsumerIdentityWrapper(consumer);
            // Insert multiple points on the hash ring for every consumer
            // The points are deterministically added based on the hash of the consumer name
            for (int i = 0; i < numberOfPoints; i++) {
                int consumerNameIndex =
                        consumerNameIndexTracker.increaseConsumerRefCountAndReturnIndex(consumerIdentityWrapper);
                int hash = calculateHashForConsumerAndIndex(consumer, consumerNameIndex, i);
                // When there's a collision, the new consumer will replace the old one.
                // This is a rare case, and it is acceptable to replace the old consumer since there
                // are multiple points for each consumer. This won't affect the overall distribution significantly.
                ConsumerIdentityWrapper removed = hashRing.put(hash, consumerIdentityWrapper);
                if (removed != null) {
                    consumerNameIndexTracker.decreaseConsumerRefCount(removed);
                }
            }
            return CompletableFuture.completedFuture(null);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Calculate the hash for a consumer and hash ring point.
     * The hash is calculated based on the consumer name, consumer name index, and hash ring point index.
     * The resulting hash is used as the key to insert the consumer into the hash ring.
     *
     * @param consumer the consumer
     * @param consumerNameIndex the index of the consumer name
     * @param hashRingPointIndex the index of the hash ring point
     * @return the hash value
     */
    private static int calculateHashForConsumerAndIndex(Consumer consumer, int consumerNameIndex,
                                                        int hashRingPointIndex) {
        String key = consumer.consumerName() + KEY_SEPARATOR + consumerNameIndex + KEY_SEPARATOR + hashRingPointIndex;
        return Murmur3_32Hash.getInstance().makeHash(key.getBytes());
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        try {
            ConsumerIdentityWrapper consumerIdentityWrapper = new ConsumerIdentityWrapper(consumer);
            int consumerNameIndex = consumerNameIndexTracker.getTrackedIndex(consumerIdentityWrapper);
            if (consumerNameIndex > -1) {
                // Remove all the points that were added for this consumer
                for (int i = 0; i < numberOfPoints; i++) {
                    int hash = calculateHashForConsumerAndIndex(consumer, consumerNameIndex, i);
                    if (hashRing.remove(hash, consumerIdentityWrapper)) {
                        consumerNameIndexTracker.decreaseConsumerRefCount(consumerIdentityWrapper);
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
                // Handle wrap-around in the hash ring, return the first consumer
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
                lastKey = entry.getKey();
                start = lastKey + 1;
            }
            // Handle wrap-around in the hash ring, the first consumer will also contain the range from the last key
            // to the maximum value of the hash range
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
