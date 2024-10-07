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

import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.client.api.Range;

/**
 * This is a consumer selector using consistent hashing to evenly split
 * the number of keys assigned to each consumer.
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
    private final int rangeSize;
    private final Range keyHashRange;
    private ConsumerHashAssignmentsSnapshot consumerHashAssignmentsSnapshot;

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints) {
        this(numberOfPoints, DEFAULT_RANGE_SIZE);
    }

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints, int rangeSize) {
        this.hashRing = new TreeMap<>();
        this.numberOfPoints = numberOfPoints;
        this.rangeSize = rangeSize;
        this.keyHashRange = Range.of(0, rangeSize - 1);
        this.consumerHashAssignmentsSnapshot = ConsumerHashAssignmentsSnapshot.empty();
    }

    @Override
    public CompletableFuture<ImpactedConsumersResult> addConsumer(Consumer consumer) {
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
            ConsumerHashAssignmentsSnapshot assignmentsAfter = internalGetConsumerHashAssignmentsSnapshot();
            ImpactedConsumersResult impactedConsumers =
                    consumerHashAssignmentsSnapshot.resolveImpactedConsumers(assignmentsAfter);
            consumerHashAssignmentsSnapshot = assignmentsAfter;
            return CompletableFuture.completedFuture(impactedConsumers);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public int makeStickyKeyHash(byte[] stickyKey) {
        return StickyKeyConsumerSelectorUtils.makeStickyKeyHash(stickyKey, rangeSize);
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
    private int calculateHashForConsumerAndIndex(Consumer consumer, int consumerNameIndex,
                                                        int hashRingPointIndex) {
        String key = consumer.consumerName() + KEY_SEPARATOR + consumerNameIndex + KEY_SEPARATOR + hashRingPointIndex;
        return makeStickyKeyHash(key.getBytes());
    }

    @Override
    public ImpactedConsumersResult removeConsumer(Consumer consumer) {
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
            ConsumerHashAssignmentsSnapshot assignmentsAfter = internalGetConsumerHashAssignmentsSnapshot();
            ImpactedConsumersResult impactedConsumers =
                    consumerHashAssignmentsSnapshot.resolveImpactedConsumers(assignmentsAfter);
            consumerHashAssignmentsSnapshot = assignmentsAfter;
            return impactedConsumers;
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
    public Range getKeyHashRange() {
        return keyHashRange;
    }

    @Override
    public ConsumerHashAssignmentsSnapshot getConsumerHashAssignmentsSnapshot() {
        rwLock.readLock().lock();
        try {
            return consumerHashAssignmentsSnapshot;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private ConsumerHashAssignmentsSnapshot internalGetConsumerHashAssignmentsSnapshot() {
        if (hashRing.isEmpty()) {
            return ConsumerHashAssignmentsSnapshot.empty();
        }
        SortedMap<Range, Consumer> result = new TreeMap<>();
        int start = 0;
        int lastKey = 0;
        Consumer previousConsumer = null;
        Range previousRange = null;
        for (Map.Entry<Integer, ConsumerIdentityWrapper> entry: hashRing.entrySet()) {
            Consumer consumer = entry.getValue().consumer;
            Range range;
            if (consumer == previousConsumer) {
                // join ranges
                result.remove(previousRange);
                range = Range.of(previousRange.getStart(), entry.getKey());
            } else {
                range = Range.of(start, entry.getKey());
            }
            result.put(range, consumer);
            lastKey = entry.getKey();
            start = lastKey + 1;
            previousConsumer = consumer;
            previousRange = range;
        }
        // Handle wrap-around
        Consumer firstConsumer = hashRing.firstEntry().getValue().consumer;
        if (lastKey != rangeSize - 1) {
            Range range;
            if (firstConsumer == previousConsumer && previousRange.getEnd() == lastKey) {
                // join ranges
                result.remove(previousRange);
                range = Range.of(previousRange.getStart(), rangeSize - 1);
            } else {
                range = Range.of(lastKey + 1, rangeSize - 1);
            }
            result.put(range, firstConsumer);
        }
        return ConsumerHashAssignmentsSnapshot.of(result);
    }
}
