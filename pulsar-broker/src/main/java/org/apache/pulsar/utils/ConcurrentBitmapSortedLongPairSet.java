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
package org.apache.pulsar.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.pulsar.common.util.collections.LongPairSet;
import org.roaringbitmap.RoaringBitmap;

public class ConcurrentBitmapSortedLongPairSet {

    private final NavigableMap<Long, RoaringBitmap> map = new TreeMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public void add(long item1, long item2) {
        lock.writeLock().lock();
        try {
            RoaringBitmap bitSet = map.computeIfAbsent(item1, k -> new RoaringBitmap());
            bitSet.add(item2, item2 + 1);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(long item1, long item2) {
        lock.writeLock().lock();
        try {
            RoaringBitmap bitSet = map.get(item1);
            if (bitSet != null) {
                bitSet.remove(item2, item2 + 1);
                if (bitSet.isEmpty()) {
                    map.remove(item1, bitSet);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean contains(long item1, long item2) {
        lock.readLock().lock();
        try {
            RoaringBitmap bitSet = map.get(item1);
            return bitSet != null && bitSet.contains(item2, item2 + 1);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Remove all items up to (and including) the specified item.
     *
     * @param item1 the first part of the item key
     * @param item2 the second part of the item key
     * @return true if any bits were cleared
     */
    public boolean removeUpTo(long item1, long item2) {
        boolean bitsCleared = false;
        lock.writeLock().lock();
        try {
            Map.Entry<Long, RoaringBitmap> firstEntry = map.firstEntry();
            while (firstEntry != null && firstEntry.getKey() <= item1) {
                if (firstEntry.getKey() < item1) {
                    map.remove(firstEntry.getKey(), firstEntry.getValue());
                    bitsCleared = true;
                } else {
                    RoaringBitmap bitSet = firstEntry.getValue();
                    if (bitSet != null) {
                        bitsCleared |= bitSet.contains(0, item2);
                        bitSet.remove(0, item2);
                        if (bitSet.isEmpty()) {
                            map.remove(firstEntry.getKey(), bitSet);
                            bitsCleared = true;
                        }
                    }
                    break;
                }
                firstEntry = map.firstEntry();
            }
        } finally {
            lock.writeLock().unlock();
        }
        return bitsCleared;
    }

    public <T extends Comparable<T>> Optional<T> first(LongPairSet.LongPairFunction<T> longPairConverter) {
        MutableObject<Optional<T>> result = new MutableObject<>(Optional.empty());
        processItems(longPairConverter, item -> {
            result.setValue(Optional.of(item));
            return false;
        });
        return result.getValue();
    }

    public <T extends Comparable<T>> NavigableSet<T> items(int numberOfItems,
                                                           LongPairSet.LongPairFunction<T> longPairConverter) {
        NavigableSet<T> items = new TreeSet<>();
        processItems(longPairConverter, item -> {
            items.add(item);
            return items.size() < numberOfItems;
        });
        return items;
    }

    public interface ItemProcessor<T extends Comparable<T>> {
        /**
         * @param item
         * @return false if there is no further processing required
         */
        boolean process(T item);
    }

    public <T extends Comparable<T>> void processItems(LongPairSet.LongPairFunction<T> longPairConverter,
                                                       ItemProcessor<T> itemProcessor) {
        lock.readLock().lock();
        try {
            for (Map.Entry<Long, RoaringBitmap> entry : map.entrySet()) {
                Iterator<Integer> iterator = entry.getValue().stream().iterator();
                boolean continueProcessing = true;
                while (continueProcessing && iterator.hasNext()) {
                    T item = longPairConverter.apply(entry.getKey(), iterator.next());
                    continueProcessing = itemProcessor.process(item);
                }
                if (!continueProcessing) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return map.isEmpty() || map.values().stream().allMatch(RoaringBitmap::isEmpty);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            map.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return map.isEmpty() ? 0 : map.values().stream().mapToInt(RoaringBitmap::getCardinality).sum();
        } finally {
            lock.readLock().unlock();
        }
    }
}
