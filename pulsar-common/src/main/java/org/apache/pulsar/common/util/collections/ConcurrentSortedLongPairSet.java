/**
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
package org.apache.pulsar.common.util.collections;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet.LongPair;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet.LongPairConsumer;

/**
 * Sorted concurrent {@link LongPairSet} which is not fully accurate in sorting.
 *
 * {@link ConcurrentSortedLongPairSet} creates separate {@link ConcurrentLongPairSet} for unique first-key of
 * inserted item. So, it can iterate over all items by sorting on item's first key. However, item's second key will not
 * be sorted. eg:
 *
 * <pre>
 *  insert: (1,2), (1,4), (2,1), (1,5), (2,6)
 *  while iterating set will first read all the entries for items whose first-key=1 and then first-key=2.
 *  output: (1,4), (1,5), (1,2), (2,6), (2,1)
 * </pre>
 *
 * <p>This map can be expensive and not recommended if set has to store large number of unique item.first's key
 * because set has to create that many {@link ConcurrentLongPairSet} objects.
 */
public class ConcurrentSortedLongPairSet implements LongPairSet {

    protected final NavigableMap<Long, ConcurrentLongPairSet> longPairSets = new ConcurrentSkipListMap<>();
    private final int expectedItems;
    private final int concurrencyLevel;
    /**
     * If {@link #longPairSets} adds and removes the item-set frequently then it allocates and removes
     * {@link ConcurrentLongPairSet} for the same item multiple times which can lead to gc-puases. To avoid such
     * situation, avoid removing empty LogPairSet until it reaches max limit.
     */
    private final int maxAllowedSetOnRemove;
    private final boolean autoShrink;
    private static final int DEFAULT_MAX_ALLOWED_SET_ON_REMOVE = 10;

    public ConcurrentSortedLongPairSet() {
        this(16, 1, DEFAULT_MAX_ALLOWED_SET_ON_REMOVE);
    }

    public ConcurrentSortedLongPairSet(int expectedItems) {
        this(expectedItems, 1, DEFAULT_MAX_ALLOWED_SET_ON_REMOVE);
    }

    public ConcurrentSortedLongPairSet(int expectedItems, int concurrencyLevel) {
        this(expectedItems, concurrencyLevel, DEFAULT_MAX_ALLOWED_SET_ON_REMOVE);
    }

    public ConcurrentSortedLongPairSet(int expectedItems, int concurrencyLevel, boolean autoShrink) {
        this(expectedItems, concurrencyLevel, DEFAULT_MAX_ALLOWED_SET_ON_REMOVE, autoShrink);
    }

    public ConcurrentSortedLongPairSet(int expectedItems, int concurrencyLevel, int maxAllowedSetOnRemove) {
        this(expectedItems, concurrencyLevel, maxAllowedSetOnRemove, false);
    }

    public ConcurrentSortedLongPairSet(int expectedItems, int concurrencyLevel, int maxAllowedSetOnRemove,
                                       boolean autoShrink) {
        this.expectedItems = expectedItems;
        this.concurrencyLevel = concurrencyLevel;
        this.maxAllowedSetOnRemove = maxAllowedSetOnRemove;
        this.autoShrink = autoShrink;
    }

    @Override
    public boolean add(long item1, long item2) {
        ConcurrentLongPairSet messagesToReplay = longPairSets.computeIfAbsent(item1,
                (key) -> ConcurrentLongPairSet.newBuilder()
                        .expectedItems(expectedItems)
                        .concurrencyLevel(concurrencyLevel)
                        .autoShrink(autoShrink)
                        .build());
        return messagesToReplay.add(item1, item2);
    }

    @Override
    public boolean remove(long item1, long item2) {
        ConcurrentLongPairSet messagesToReplay = longPairSets.get(item1);
        if (messagesToReplay != null) {
            boolean removed = messagesToReplay.remove(item1, item2);
            if (messagesToReplay.isEmpty() && longPairSets.size() > maxAllowedSetOnRemove) {
                longPairSets.remove(item1, messagesToReplay);
            }
            return removed;
        }
        return false;
    }

    @Override
    public int removeIf(LongPairPredicate filter) {
        AtomicInteger removedValues = new AtomicInteger(0);
        longPairSets.forEach((item1, longPairSet) -> {
            removedValues.addAndGet(longPairSet.removeIf(filter));
            if (longPairSet.isEmpty() && longPairSets.size() > maxAllowedSetOnRemove) {
                longPairSets.remove(item1, longPairSet);
            }
        });
        return removedValues.get();
    }

    @Override
    public Set<LongPair> items() {
        return items((int) this.size());
    }

    @Override
    public void forEach(LongPairConsumer processor) {
        for (Long item1 : longPairSets.navigableKeySet()) {
            ConcurrentLongPairSet messagesToReplay = longPairSets.get(item1);
            messagesToReplay.forEach((i1, i2) -> {
                processor.accept(i1, i2);
            });
        }
    }

    @Override
    public Set<LongPair> items(int numberOfItems) {
        return items(numberOfItems, (item1, item2) -> new LongPair(item1, item2));
    }

    @Override
    public <T> Set<T> items(int numberOfItems, LongPairFunction<T> longPairConverter) {
        NavigableSet<T> items = new TreeSet<>();
        for (Long item1 : longPairSets.navigableKeySet()) {
            ConcurrentLongPairSet messagesToReplay = longPairSets.get(item1);
            messagesToReplay.forEach((i1, i2) -> {
                items.add(longPairConverter.apply(i1, i2));
                if (items.size() > numberOfItems) {
                    items.pollLast();
                }
            });
        }
        return items;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        final AtomicBoolean first = new AtomicBoolean(true);
        longPairSets.forEach((key, longPairSet) -> {
            longPairSet.forEach((item1, item2) -> {
                if (!first.getAndSet(false)) {
                    sb.append(", ");
                }
                sb.append('[');
                sb.append(item1);
                sb.append(':');
                sb.append(item2);
                sb.append(']');
            });
        });
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean isEmpty() {
        if (longPairSets.isEmpty()) {
            return true;
        }
        for (ConcurrentLongPairSet subSet : longPairSets.values()) {
            if (!subSet.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void clear() {
        longPairSets.clear();
    }

    @Override
    public long size() {
        AtomicLong size = new AtomicLong(0);
        longPairSets.forEach((item1, longPairSet) -> {
            size.getAndAdd(longPairSet.size());
        });
        return size.get();
    }

    @Override
    public long capacity() {
        AtomicLong capacity = new AtomicLong(0);
        longPairSets.forEach((item1, longPairSet) -> {
            capacity.getAndAdd(longPairSet.capacity());
        });
        return capacity.get();
    }

    @Override
    public boolean contains(long item1, long item2) {
        ConcurrentLongPairSet longPairSet = longPairSets.get(item1);
        if (longPairSet != null) {
            return longPairSet.contains(item1, item2);
        }
        return false;
    }

}