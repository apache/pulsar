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
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.pulsar.common.util.collections.LongPairSet;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * A concurrent set of pairs of longs.
 * Both sides support the full range of long values.
 */
public class ConcurrentBitmapSortedLongPairSet {

    // Each bitmap stores the low 32 bits of item2. Ordering by item1 and the signed high 32 bits
    // of item2 keeps pairs sorted and allows whole ranges to be removed without visiting each item.
    private final NavigableMap<BitmapKey, RoaringBitmap> map = new TreeMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private record BitmapKey(long item1, int item2HighBits) implements Comparable<BitmapKey> {
        private static BitmapKey of(long item1, long item2) {
            return new BitmapKey(item1, (int) (item2 >> Integer.SIZE));
        }

        @Override
        public int compareTo(BitmapKey other) {
            int result = Long.compare(item1, other.item1);
            return result != 0 ? result : Integer.compare(item2HighBits, other.item2HighBits);
        }
    }

    public void add(long item1, long item2) {
        lock.writeLock().lock();
        try {
            RoaringBitmap bitSet = map.computeIfAbsent(BitmapKey.of(item1, item2), k -> new RoaringBitmap());
            bitSet.add((int) item2);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(long item1, long item2) {
        lock.writeLock().lock();
        try {
            BitmapKey key = BitmapKey.of(item1, item2);
            RoaringBitmap bitSet = map.get(key);
            if (bitSet != null) {
                bitSet.remove((int) item2);
                if (bitSet.isEmpty()) {
                    map.remove(key, bitSet);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean contains(long item1, long item2) {
        lock.readLock().lock();
        try {
            RoaringBitmap bitSet = map.get(BitmapKey.of(item1, item2));
            return bitSet != null && bitSet.contains((int) item2);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Remove all items strictly before the specified item.
     *
     * @param item1 the first part of the item key
     * @param item2 the second part of the item key
     * @return true if any bits were cleared
     */
    public boolean removeUpTo(long item1, long item2) {
        return removeUpTo(item1, item2, false);
    }

    /**
     * Remove all items up to and including the specified item.
     *
     * @param item1 the first part of the item key
     * @param item2 the second part of the item key
     * @return true if any bits were cleared
     */
    public boolean removeUpToInclusive(long item1, long item2) {
        return removeUpTo(item1, item2, true);
    }

    private boolean removeUpTo(long item1, long item2, boolean inclusive) {
        BitmapKey key = BitmapKey.of(item1, item2);
        // Only increment the unsigned low 32 bits, so the endpoint cannot overflow even at Long.MAX_VALUE.
        long end = Integer.toUnsignedLong((int) item2) + (inclusive ? 1 : 0);
        boolean bitsCleared = false;
        lock.writeLock().lock();
        try {
            Map.Entry<BitmapKey, RoaringBitmap> firstEntry = map.firstEntry();
            while (firstEntry != null && firstEntry.getKey().compareTo(key) <= 0) {
                if (firstEntry.getKey().compareTo(key) < 0) {
                    map.remove(firstEntry.getKey(), firstEntry.getValue());
                    bitsCleared = true;
                } else {
                    RoaringBitmap bitSet = firstEntry.getValue();
                    if (bitSet != null) {
                        // A sparse prefix can have gaps before the endpoint.
                        bitsCleared |= bitSet.intersects(0, end);
                        bitSet.remove(0, end);
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
        return result.get();
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
            for (Map.Entry<BitmapKey, RoaringBitmap> entry : map.entrySet()) {
                PeekableIntIterator intIterator = entry.getValue().getIntIterator();
                boolean continueProcessing = true;
                while (continueProcessing && intIterator.hasNext()) {
                    long item2 = ((long) entry.getKey().item2HighBits << Integer.SIZE)
                            | Integer.toUnsignedLong(intIterator.next());
                    T item = longPairConverter.apply(entry.getKey().item1, item2);
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
