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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.mutable.MutableInt;
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
    private final NavigableMap<Integer, HashRingEntry> hashRing;
    // used for distributing consumer instance selections evenly in the hash ring when there
    // are multiple instances of consumer with the same consumer name or when there are hash collisions
    private final Map<Consumer, MutableInt> consumerSelectionCounters;

    private final int numberOfPoints;

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints) {
        this.hashRing = new TreeMap<>();
        this.consumerSelectionCounters = new WeakHashMap<>();
        this.numberOfPoints = numberOfPoints;
    }

    /**
     * This class is used to store the consumers and the selected consumer for a hash value in the hash ring.
     * This attempts to distribute the consumers evenly in the hash ring for consumers with the same
     * consumer name and priority level. These entries collide in the hash ring.
     * The selected consumer is the consumer that is selected to serve the hash value.
     * It is not changed unless a consumer is removed or a colliding consumer with higher priority or
     * lower selection count is added.
     */
    private static class HashRingEntry {
        // This class is used to store the consumer added to the hash ring
        // sorting will be by priority, consumer name and active "selection" count of the consumer instance
        // so that consumers get evenly distributed
        record ConsumerEntry(Consumer consumer, MutableInt consumerSelectionCounter)
                implements Comparable<ConsumerEntry> {
            private static final Comparator<ConsumerEntry> CONSUMER_ENTRY_COMPARATOR =
                    Comparator.<ConsumerEntry, Integer>
                            // priority level is the primary sorting key
                                    comparing(entry -> entry.consumer().getPriorityLevel()).reversed()
                            // consumer name is the secondary sorting key
                            .thenComparing(entry -> entry.consumer().consumerName())
                            // then prefer the consumer instance with lowest selection count
                            // so that consumers get evenly distributed
                            .thenComparing(ConsumerEntry::consumerSelectionCounter);

            @Override
            public int compareTo(ConsumerEntry o) {
                    return CONSUMER_ENTRY_COMPARATOR.compare(this, o);
            }
        }

        private final List<ConsumerEntry> consumers;
        ConsumerEntry selectedConsumerEntry;

        public HashRingEntry() {
            this.consumers = new ArrayList<>();
        }

        public Consumer getSelectedConsumer() {
            return selectedConsumerEntry != null ? selectedConsumerEntry.consumer() : null;
        }

        public void addConsumer(Consumer consumer, MutableInt selectedCounter) {
            ConsumerEntry consumerEntry = new ConsumerEntry(consumer, selectedCounter);
            consumers.add(consumerEntry);
            if (selectedConsumerEntry == null || consumerEntry.compareTo(selectedConsumerEntry) < 0) {
                // if the new consumer has a higher priority or lower selection count
                // than the currently selected consumer, select the new consumer
                changeSelectedConsumerEntry(consumerEntry);
            }
        }

        public boolean removeConsumer(Consumer consumer) {
            boolean removed = consumers.removeIf(consumerEntry -> consumerEntry.consumer() == consumer);
            if (removed && consumer == getSelectedConsumer()) {
                // if the selected consumer was removed, a new consumer will be selected.
                // The consumers are sorted here to ensure that the consumer with the
                // lowest selection count is selected
                Collections.sort(consumers);
                // select the first consumer in sorting order
                changeSelectedConsumerEntry(consumers.isEmpty() ? null : consumers.get(0));
            }
            return removed;
        }

        private void changeSelectedConsumerEntry(ConsumerEntry newSelectedConsumer) {
            if (newSelectedConsumer == selectedConsumerEntry) {
                return;
            }
            beforeChangingSelectedConsumerEntry();
            selectedConsumerEntry = newSelectedConsumer;
            afterChangingSelectedConsumerEntry();
        }

        private void beforeChangingSelectedConsumerEntry() {
            if (selectedConsumerEntry != null) {
                selectedConsumerEntry.consumerSelectionCounter.decrement();
            }
        }

        private void afterChangingSelectedConsumerEntry() {
            if (selectedConsumerEntry != null) {
                selectedConsumerEntry.consumerSelectionCounter.increment();
            }
        }
    }

    @Override
    public CompletableFuture<Void> addConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        try {
            // Insert multiple points on the hash ring for every consumer
            // The points are deterministically added based on the hash of the consumer name
            for (int i = 0; i < numberOfPoints; i++) {
                int hash = calculateHashForConsumerAndIndex(consumer, i);
                HashRingEntry hashRingEntry = hashRing.computeIfAbsent(hash, k -> new HashRingEntry());
                // Add the consumer to the hash ring entry
                hashRingEntry.addConsumer(consumer, getConsumerSelectedCount(consumer));
            }
            return CompletableFuture.completedFuture(null);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private MutableInt getConsumerSelectedCount(Consumer consumer) {
        return consumerSelectionCounters.computeIfAbsent(consumer, k -> new MutableInt());
    }

    private static int calculateHashForConsumerAndIndex(Consumer consumer, int index) {
        String key = consumer.consumerName() + KEY_SEPARATOR + index;
        return Murmur3_32Hash.getInstance().makeHash(key.getBytes());
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        try {
            // Remove all the points that were added for this consumer
            for (int i = 0; i < numberOfPoints; i++) {
                int hash = calculateHashForConsumerAndIndex(consumer, i);
                hashRing.compute(hash, (k, v) -> {
                    if (v != null) {
                        v.removeConsumer(consumer);
                        if (v.getSelectedConsumer() == null) {
                            v = null;
                        }
                    }
                    return v;
                });
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
            HashRingEntry hashRingEntry;
            Map.Entry<Integer, HashRingEntry> ceilingEntry = hashRing.ceilingEntry(hash);
            if (ceilingEntry != null) {
                hashRingEntry =  ceilingEntry.getValue();
            } else {
                hashRingEntry = hashRing.firstEntry().getValue();
            }
            return hashRingEntry.getSelectedConsumer();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        Map<Consumer, List<Range>> result = new LinkedHashMap<>();
        rwLock.readLock().lock();
        try {
            if (hashRing.isEmpty()) {
                return result;
            }
            int start = 0;
            int lastKey = 0;
            for (Map.Entry<Integer, HashRingEntry> entry: hashRing.entrySet()) {
                Consumer consumer = entry.getValue().getSelectedConsumer();
                result.computeIfAbsent(consumer, key -> new ArrayList<>())
                            .add(Range.of(start, entry.getKey()));
                lastKey = entry.getKey();
                start = lastKey + 1;
            }
            // Handle wrap-around
            HashRingEntry firstHashRingEntry = hashRing.firstEntry().getValue();
            Consumer firstSelectedConsumer = firstHashRingEntry.getSelectedConsumer();
            List<Range> ranges = result.get(firstSelectedConsumer);
            if (lastKey != Integer.MAX_VALUE - 1) {
                ranges.add(Range.of(lastKey + 1, Integer.MAX_VALUE - 1));
            }
        } finally {
            rwLock.readLock().unlock();
        }
        return result;
    }
}
