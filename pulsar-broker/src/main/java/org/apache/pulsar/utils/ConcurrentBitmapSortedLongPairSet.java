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

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.common.util.collections.LongPairSet;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * A concurrent set of pairs of longs.
 * The right side of the value supports unsigned values up to 2^32.
 */
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

    public void removeUpTo(long item1, long item2) {
        lock.writeLock().lock();
        try {
            Map.Entry<Long, RoaringBitmap> firstEntry = map.firstEntry();
            while (firstEntry != null && firstEntry.getKey() <= item1) {
                if (firstEntry.getKey() < item1) {
                    map.remove(firstEntry.getKey(), firstEntry.getValue());
                } else {
                    RoaringBitmap bitSet = firstEntry.getValue();
                    if (bitSet != null) {
                        bitSet.remove(0, item2);
                        if (bitSet.isEmpty()) {
                            map.remove(firstEntry.getKey(), bitSet);
                        }
                    }
                    break;
                }
                firstEntry = map.firstEntry();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }


    public <T extends Comparable<T>> NavigableSet<T> items(int numberOfItems,
                                                           LongPairSet.LongPairFunction<T> longPairConverter) {
        NavigableSet<T> items = new TreeSet<>();
        lock.readLock().lock();
        try {
            for (Map.Entry<Long, RoaringBitmap> entry : map.entrySet()) {
                PeekableIntIterator intIterator = entry.getValue().getIntIterator();
                while (intIterator.hasNext() && items.size() < numberOfItems) {
                    // RoaringBitmap encodes values as unsigned 32-bit integers internally, it's necessary to use
                    // Integer.toUnsignedLong to convert them to unsigned long values
                    items.add(longPairConverter.apply(entry.getKey(), Integer.toUnsignedLong(intIterator.next())));
                }
                if (items.size() == numberOfItems) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return items;
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
