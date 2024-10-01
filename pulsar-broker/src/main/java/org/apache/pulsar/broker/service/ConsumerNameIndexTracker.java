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
import org.apache.commons.lang3.mutable.MutableInt;
import org.roaringbitmap.RoaringBitmap;

class ConsumerNameIndexTracker {
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
