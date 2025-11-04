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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.lang3.mutable.MutableInt;
import org.roaringbitmap.RoaringBitmap;

/**
 * Tracks the used consumer name indexes for each consumer name.
 * This is used by {@link ConsistentHashingStickyKeyConsumerSelector} to get a unique "consumer name index"
 * for each consumer name. It is useful when there are multiple consumers with the same name, but they are
 * different consumers. The purpose of the index is to prevent collisions in the hash ring.
 *
 * The consumer name index serves as an additional key for the hash ring assignment. The logic keeps track of
 * used "index slots" for each consumer name and assigns the first unused index when a new consumer is added.
 * This approach minimizes hash collisions due to using the same consumer name.
 *
 * An added benefit of this tracking approach is that a consumer that leaves and then rejoins immediately will get the
 * same index and therefore the same assignments in the hash ring. This improves stability since the hash assignment
 * changes are minimized over time, although a better solution would be to avoid reusing the same consumer name
 * in the first place.
 *
 * When a consumer is removed, the index is deallocated. RoaringBitmap is used to keep track of the used indexes.
 * The data structure to track a consumer name is removed when the reference count of the consumer name is zero.
 *
 * This class is not thread-safe and should be used in a synchronized context in the caller.
 */
@NotThreadSafe
class ConsumerNameIndexTracker {
    // tracks the used index slots for each consumer name
    private final Map<String, ConsumerNameIndexSlots> consumerNameIndexSlotsMap = new HashMap<>();
    // tracks the active consumer entries
    private final Map<ConsumerIdentityWrapper, ConsumerEntry> consumerEntries = new HashMap<>();

    // Represents a consumer entry in the tracker, including the consumer name, index, and reference count.
    record ConsumerEntry(String consumerName, int nameIndex, MutableInt refCount) {
    }

    /*
     * Tracks the used indexes for a consumer name using a RoaringBitmap.
     * A specific index slot is used when the bit is set.
     * When all bits are cleared, the customer name can be removed from tracking.
     */
    static class ConsumerNameIndexSlots {
        private RoaringBitmap indexSlots = new RoaringBitmap();

        public int allocateIndexSlot() {
            // find the first index that is not set, if there is no such index, add a new one
            int index = (int) indexSlots.nextAbsentValue(0);
            if (index == -1) {
                index = indexSlots.getCardinality();
            }
            indexSlots.add(index);
            return index;
        }

        public boolean deallocateIndexSlot(int index) {
            indexSlots.remove(index);
            return indexSlots.isEmpty();
        }
    }

    /*
     * Adds a reference to the consumer and returns the index assigned to this consumer.
     */
    public int increaseConsumerRefCountAndReturnIndex(ConsumerIdentityWrapper wrapper) {
        ConsumerEntry entry = consumerEntries.computeIfAbsent(wrapper, k -> {
            String consumerName = wrapper.consumer.consumerName();
            return new ConsumerEntry(consumerName, allocateConsumerNameIndex(consumerName), new MutableInt(0));
        });
        entry.refCount.increment();
        return entry.nameIndex;
    }

    private int allocateConsumerNameIndex(String consumerName) {
        return getConsumerNameIndexBitmap(consumerName).allocateIndexSlot();
    }

    private ConsumerNameIndexSlots getConsumerNameIndexBitmap(String consumerName) {
        return consumerNameIndexSlotsMap.computeIfAbsent(consumerName, k -> new ConsumerNameIndexSlots());
    }

    /*
     * Decreases the reference count of the consumer and removes the consumer name from tracking if the ref count is
     * zero.
     */
    public void decreaseConsumerRefCount(ConsumerIdentityWrapper removed) {
        ConsumerEntry consumerEntry = consumerEntries.get(removed);
        int refCount = consumerEntry.refCount.decrementAndGet();
        if (refCount == 0) {
            deallocateConsumerNameIndex(consumerEntry.consumerName, consumerEntry.nameIndex);
            consumerEntries.remove(removed, consumerEntry);
        }
    }

    private void deallocateConsumerNameIndex(String consumerName, int index) {
        if (getConsumerNameIndexBitmap(consumerName).deallocateIndexSlot(index)) {
            consumerNameIndexSlotsMap.remove(consumerName);
        }
    }

    /*
     * Returns the currently tracked index for the consumer.
     */
    public int getTrackedIndex(ConsumerIdentityWrapper wrapper) {
        ConsumerEntry consumerEntry = consumerEntries.get(wrapper);
        return consumerEntry != null ? consumerEntry.nameIndex : -1;
    }

    int getTrackedConsumerNamesCount() {
        return consumerNameIndexSlotsMap.size();
    }

    int getTrackedConsumersCount() {
        return consumerEntries.size();
    }
}
