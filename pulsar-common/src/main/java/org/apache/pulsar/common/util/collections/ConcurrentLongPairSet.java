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

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.StampedLock;

/**
 * Concurrent hash set where values are composed of pairs of longs.
 *
 * <p>Provides similar methods as a {@code ConcurrentHashSet<V>} but since it's an open hash set with linear probing,
 * no node allocations are required to store the keys and values, and no boxing is required.
 *
 * <p>Values <b>MUST</b> be &gt;= 0.
 */
public class ConcurrentLongPairSet implements LongPairSet {

    private static final long EmptyItem = -1L;
    private static final long DeletedItem = -2L;

    private static final float SetFillFactor = 0.66f;

    private static final int DefaultExpectedItems = 256;
    private static final int DefaultConcurrencyLevel = 16;

    private static final float DefaultMapFillFactor = 0.66f;
    private static final float DefaultMapIdleFactor = 0.15f;

    private static final float DefaultExpandFactor = 2;
    private static final float DefaultShrinkFactor = 2;

    private static final boolean DefaultAutoShrink = false;

    private final Section[] sections;

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder of ConcurrentLongPairSet.
     */
    public static class Builder {
        int expectedItems = DefaultExpectedItems;
        int concurrencyLevel = DefaultConcurrencyLevel;
        float mapFillFactor = DefaultMapFillFactor;
        float mapIdleFactor = DefaultMapIdleFactor;
        float expandFactor = DefaultExpandFactor;
        float shrinkFactor = DefaultShrinkFactor;
        boolean autoShrink = DefaultAutoShrink;

        public Builder expectedItems(int expectedItems) {
            this.expectedItems = expectedItems;
            return this;
        }

        public Builder concurrencyLevel(int concurrencyLevel) {
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }

        public Builder mapFillFactor(float mapFillFactor) {
            this.mapFillFactor = mapFillFactor;
            return this;
        }

        public Builder mapIdleFactor(float mapIdleFactor) {
            this.mapIdleFactor = mapIdleFactor;
            return this;
        }

        public Builder expandFactor(float expandFactor) {
            this.expandFactor = expandFactor;
            return this;
        }

        public Builder shrinkFactor(float shrinkFactor) {
            this.shrinkFactor = shrinkFactor;
            return this;
        }

        public Builder autoShrink(boolean autoShrink) {
            this.autoShrink = autoShrink;
            return this;
        }

        public ConcurrentLongPairSet build() {
            return new ConcurrentLongPairSet(expectedItems, concurrencyLevel,
                    mapFillFactor, mapIdleFactor, autoShrink, expandFactor, shrinkFactor);
        }
    }


    /**
     * Represents a function that accepts an object of the {@code LongPair} type.
     */
    public interface ConsumerLong {
        void accept(LongPair item);
    }

    /**
     * Represents a function that accepts two long arguments.
     */
    public interface LongPairConsumer {
        void accept(long v1, long v2);
    }

    @Deprecated
    public ConcurrentLongPairSet() {
        this(DefaultExpectedItems);
    }

    @Deprecated
    public ConcurrentLongPairSet(int expectedItems) {
        this(expectedItems, DefaultConcurrencyLevel);
    }

    @Deprecated
    public ConcurrentLongPairSet(int expectedItems, int concurrencyLevel) {
        this(expectedItems, concurrencyLevel, DefaultMapFillFactor, DefaultMapIdleFactor,
                DefaultAutoShrink, DefaultExpandFactor, DefaultShrinkFactor);
    }

    public ConcurrentLongPairSet(int expectedItems, int concurrencyLevel,
                                 float mapFillFactor, float mapIdleFactor,
                                 boolean autoShrink, float expandFactor, float shrinkFactor) {
        checkArgument(expectedItems > 0);
        checkArgument(concurrencyLevel > 0);
        checkArgument(expectedItems >= concurrencyLevel);
        checkArgument(mapFillFactor > 0 && mapFillFactor < 1);
        checkArgument(mapIdleFactor > 0 && mapIdleFactor < 1);
        checkArgument(mapFillFactor > mapIdleFactor);
        checkArgument(expandFactor > 1);
        checkArgument(shrinkFactor > 1);

        int numSections = concurrencyLevel;
        int perSectionExpectedItems = expectedItems / numSections;
        int perSectionCapacity = (int) (perSectionExpectedItems / SetFillFactor);
        this.sections = new Section[numSections];

        for (int i = 0; i < numSections; i++) {
            sections[i] = new Section(perSectionCapacity, mapFillFactor, mapIdleFactor,
                    autoShrink, expandFactor, shrinkFactor);
        }
    }

    @Override
    public long size() {
        long size = 0;
        for (int i = 0; i < sections.length; i++) {
            size += sections[i].size;
        }
        return size;
    }

    @Override
    public long capacity() {
        long capacity = 0;
        for (int i = 0; i < sections.length; i++) {
            capacity += sections[i].capacity;
        }
        return capacity;
    }

    public boolean isEmpty() {
        for (int i = 0; i < sections.length; i++) {
            if (sections[i].size != 0) {
                return false;
            }
        }
        return true;
    }

    long getUsedBucketCount() {
        long usedBucketCount = 0;
        for (int i = 0; i < sections.length; i++) {
            usedBucketCount += sections[i].usedBuckets;
        }
        return usedBucketCount;
    }

    public boolean contains(long item1, long item2) {
        checkBiggerEqualZero(item1);
        long h = hash(item1, item2);
        return getSection(h).contains(item1, item2, (int) h);
    }

    public boolean add(long item1, long item2) {
        checkBiggerEqualZero(item1);
        long h = hash(item1, item2);
        return getSection(h).add(item1, item2, (int) h);
    }

    /**
     * Remove an existing entry if found.
     *
     * @param item1
     * @return true if removed or false if item was not present
     */
    public boolean remove(long item1, long item2) {
        checkBiggerEqualZero(item1);
        long h = hash(item1, item2);
        return getSection(h).remove(item1, item2, (int) h);
    }

    private Section getSection(long hash) {
        // Use 32 msb out of long to get the section
        final int sectionIdx = (int) (hash >>> 32) & (sections.length - 1);
        return sections[sectionIdx];
    }

    public void clear() {
        for (Section s : sections) {
            s.clear();
        }
    }

    public void forEach(LongPairConsumer processor) {
        for (int i = 0; i < sections.length; i++) {
            sections[i].forEach(processor);
        }
    }

    /**
     * Removes all of the elements of this collection that satisfy the given predicate.
     *
     * @param filter
     *            a predicate which returns {@code true} for elements to be removed
     *
     * @return number of removed values
     */
    public int removeIf(LongPairPredicate filter) {
        int removedValues = 0;
        for (int i = 0; i < sections.length; i++) {
            removedValues += sections[i].removeIf(filter);
        }
        return removedValues;
    }

    /**
     * @return a new list of all keys (makes a copy)
     */
    public Set<LongPair> items() {
        Set<LongPair> items = new HashSet<>();
        forEach((item1, item2) -> items.add(new LongPair(item1, item2)));
        return items;
    }

    /**
     * @return a new list of keys with max provided numberOfItems (makes a copy)
     */
    public Set<LongPair> items(int numberOfItems) {
        return items(numberOfItems, (item1, item2) -> new LongPair(item1, item2));
    }

    @Override
    public <T> Set<T> items(int numberOfItems, LongPairFunction<T> longPairConverter) {
        Set<T> items = new HashSet<>();
        for (int i = 0; i < sections.length; i++) {
            sections[i].forEach((item1, item2) -> {
                if (items.size() < numberOfItems) {
                    items.add(longPairConverter.apply(item1, item2));
                }
            });
            if (items.size() >= numberOfItems) {
                return items;
            }
        }
        return items;
    }

    // A section is a portion of the hash map that is covered by a single
    @SuppressWarnings("serial")
    private static final class Section extends StampedLock {
        // Keys and values are stored interleaved in the table array
        private volatile long[] table;

        private volatile int capacity;
        private final int initCapacity;
        private static final AtomicIntegerFieldUpdater<Section> SIZE_UPDATER = AtomicIntegerFieldUpdater
                .newUpdater(Section.class, "size");
        private volatile int size;
        private int usedBuckets;
        private int resizeThresholdUp;
        private int resizeThresholdBelow;
        private final float mapFillFactor;
        private final float mapIdleFactor;
        private final float expandFactor;
        private final float shrinkFactor;
        private final boolean autoShrink;

        Section(int capacity, float mapFillFactor, float mapIdleFactor, boolean autoShrink,
                float expandFactor, float shrinkFactor) {
            this.capacity = alignToPowerOfTwo(capacity);
            this.initCapacity = this.capacity;
            this.table = new long[2 * this.capacity];
            this.size = 0;
            this.usedBuckets = 0;
            this.autoShrink = autoShrink;
            this.mapFillFactor = mapFillFactor;
            this.mapIdleFactor = mapIdleFactor;
            this.expandFactor = expandFactor;
            this.shrinkFactor = shrinkFactor;
            this.resizeThresholdUp = (int) (this.capacity * mapFillFactor);
            this.resizeThresholdBelow = (int) (this.capacity * mapIdleFactor);
            Arrays.fill(table, EmptyItem);
        }

        boolean contains(long item1, long item2, int hash) {
            long stamp = tryOptimisticRead();
            boolean acquiredLock = false;
            int bucket = signSafeMod(hash, capacity);

            try {
                while (true) {
                    // First try optimistic locking
                    long storedItem1 = table[bucket];
                    long storedItem2 = table[bucket + 1];

                    if (!acquiredLock && validate(stamp)) {
                        // The values we have read are consistent
                        if (item1 == storedItem1 && item2 == storedItem2) {
                            return true;
                        } else if (storedItem1 == EmptyItem) {
                            // Not found
                            return false;
                        }
                    } else {
                        // Fallback to acquiring read lock
                        if (!acquiredLock) {
                            stamp = readLock();
                            acquiredLock = true;

                            bucket = signSafeMod(hash, capacity);
                            storedItem1 = table[bucket];
                            storedItem2 = table[bucket + 1];
                        }

                        if (item1 == storedItem1 && item2 == storedItem2) {
                            return true;
                        } else if (storedItem1 == EmptyItem) {
                            // Not found
                            return false;
                        }
                    }

                    bucket = (bucket + 2) & (table.length - 1);
                }
            } finally {
                if (acquiredLock) {
                    unlockRead(stamp);
                }
            }
        }

        boolean add(long item1, long item2, long hash) {
            long stamp = writeLock();
            int bucket = signSafeMod(hash, capacity);

            // Remember where we find the first available spot
            int firstDeletedItem = -1;

            try {
                while (true) {
                    long storedItem1 = table[bucket];
                    long storedItem2 = table[bucket + 1];

                    if (item1 == storedItem1 && item2 == storedItem2) {
                        // Item was already in set
                        return false;
                    } else if (storedItem1 == EmptyItem) {
                        // Found an empty bucket. This means the key is not in the set. If we've already seen a deleted
                        // key, we should write at that position
                        if (firstDeletedItem != -1) {
                            bucket = firstDeletedItem;
                        } else {
                            ++usedBuckets;
                        }

                        table[bucket] = item1;
                        table[bucket + 1] = item2;
                        SIZE_UPDATER.incrementAndGet(this);
                        return true;
                    } else if (storedItem1 == DeletedItem) {
                        // The bucket contained a different deleted key
                        if (firstDeletedItem == -1) {
                            firstDeletedItem = bucket;
                        }
                    }

                    bucket = (bucket + 2) & (table.length - 1);
                }
            } finally {
                if (usedBuckets > resizeThresholdUp) {
                    try {
                        // Expand the hashmap
                        int newCapacity = alignToPowerOfTwo((int) (capacity * expandFactor));
                        rehash(newCapacity);
                    } finally {
                        unlockWrite(stamp);
                    }
                } else {
                    unlockWrite(stamp);
                }
            }
        }

        private boolean remove(long item1, long item2, int hash) {
            long stamp = writeLock();
            int bucket = signSafeMod(hash, capacity);

            try {
                while (true) {
                    long storedItem1 = table[bucket];
                    long storedItem2 = table[bucket + 1];
                    if (item1 == storedItem1 && item2 == storedItem2) {
                        SIZE_UPDATER.decrementAndGet(this);

                        cleanBucket(bucket);
                        return true;

                    } else if (storedItem1 == EmptyItem) {
                        return false;
                    }

                    bucket = (bucket + 2) & (table.length - 1);
                }
            } finally {
                tryShrinkThenUnlock(stamp);
            }
        }

        private int removeIf(LongPairPredicate filter) {
            Objects.requireNonNull(filter);
            int removedItems = 0;

            // Go through all the buckets for this section
            long stamp = writeLock();
            try {
                for (int bucket = 0; bucket < table.length; bucket += 2) {
                    long storedItem1 = table[bucket];
                    long storedItem2 = table[bucket + 1];
                    if (storedItem1 != DeletedItem && storedItem1 != EmptyItem) {
                        if (filter.test(storedItem1, storedItem2)) {
                            SIZE_UPDATER.decrementAndGet(this);
                            cleanBucket(bucket);
                            removedItems++;
                        }
                    }
                }
            } finally {
                tryShrinkThenUnlock(stamp);
            }
            return removedItems;
        }

        private void tryShrinkThenUnlock(long stamp) {
            if (autoShrink && size < resizeThresholdBelow) {
                try {
                    int newCapacity = alignToPowerOfTwo((int) (capacity / shrinkFactor));
                    int newResizeThresholdUp = (int) (newCapacity * mapFillFactor);
                    if (newCapacity < capacity && newResizeThresholdUp > size) {
                        // shrink the hashmap
                        rehash(newCapacity);
                    }
                } finally {
                    unlockWrite(stamp);
                }
            } else {
                unlockWrite(stamp);
            }
        }

        private void cleanBucket(int bucket) {
            int nextInArray = (bucket + 2) & (table.length - 1);
            if (table[nextInArray] == EmptyItem) {
                table[bucket] = EmptyItem;
                table[bucket + 1] = EmptyItem;
                --usedBuckets;

                // Cleanup all the buckets that were in `DeletedItem` state,
                // so that we can reduce unnecessary expansions
                int lastBucket = (bucket - 2) & (table.length - 1);
                while (table[lastBucket] == DeletedItem) {
                    table[lastBucket] = EmptyItem;
                    table[lastBucket + 1] = EmptyItem;
                    --usedBuckets;

                    lastBucket = (lastBucket - 2) & (table.length - 1);
                }
            } else {
                table[bucket] = DeletedItem;
                table[bucket + 1] = DeletedItem;
            }
        }

        void clear() {
            long stamp = writeLock();

            try {
                Arrays.fill(table, EmptyItem);
                this.size = 0;
                this.usedBuckets = 0;
                if (autoShrink) {
                    rehash(initCapacity);
                }
            } finally {
                unlockWrite(stamp);
            }
        }

        public void forEach(LongPairConsumer processor) {
            long[] table = this.table;

            // Go through all the buckets for this section. We try to renew the stamp only after a validation
            // error, otherwise we keep going with the same.
            long stamp = 0;
            for (int bucket = 0; bucket < table.length; bucket += 2) {
                if (stamp == 0) {
                    stamp = tryOptimisticRead();
                }

                long storedItem1 = table[bucket];
                long storedItem2 = table[bucket + 1];

                if (!validate(stamp)) {
                    // Fallback to acquiring read lock
                    stamp = readLock();

                    try {
                        storedItem1 = table[bucket];
                        storedItem2 = table[bucket + 1];
                    } finally {
                        unlockRead(stamp);
                    }

                    stamp = 0;
                }

                if (storedItem1 != DeletedItem && storedItem1 != EmptyItem) {
                    processor.accept(storedItem1, storedItem2);
                }
            }
        }

        private void rehash(int newCapacity) {
            // Expand the hashmap
            long[] newTable = new long[2 * newCapacity];
            Arrays.fill(newTable, EmptyItem);

            // Re-hash table
            for (int i = 0; i < table.length; i += 2) {
                long storedItem1 = table[i];
                long storedItem2 = table[i + 1];
                if (storedItem1 != EmptyItem && storedItem1 != DeletedItem) {
                    insertKeyValueNoLock(newTable, newCapacity, storedItem1, storedItem2);
                }
            }

            table = newTable;
            usedBuckets = size;
            // Capacity needs to be updated after the values, so that we won't see
            // a capacity value bigger than the actual array size
            capacity = newCapacity;
            resizeThresholdUp = (int) (capacity * mapFillFactor);
            resizeThresholdBelow = (int) (capacity * mapIdleFactor);
        }

        private static void insertKeyValueNoLock(long[] table, int capacity, long item1, long item2) {
            int bucket = signSafeMod(hash(item1, item2), capacity);

            while (true) {
                long storedKey = table[bucket];

                if (storedKey == EmptyItem) {
                    // The bucket is empty, so we can use it
                    table[bucket] = item1;
                    table[bucket + 1] = item2;
                    return;
                }

                bucket = (bucket + 2) & (table.length - 1);
            }
        }
    }

    private static final long HashMixer = 0xc6a4a7935bd1e995L;
    private static final int R = 47;

    static final long hash(long key1, long key2) {
        long hash = key1 * HashMixer;
        hash ^= hash >>> R;
        hash *= HashMixer;
        hash += 31 + (key2 * HashMixer);
        hash ^= hash >>> R;
        hash *= HashMixer;
        return hash;
    }

    static final int signSafeMod(long n, int max) {
        return (int) (n & (max - 1)) << 1;
    }

    private static int alignToPowerOfTwo(int n) {
        return (int) Math.pow(2, 32 - Integer.numberOfLeadingZeros(n - 1));
    }

    private static void checkBiggerEqualZero(long n) {
        if (n < 0L) {
            throw new IllegalArgumentException("Keys and values must be >= 0");
        }
    }

    /**
     * Class representing two long values.
     */
    public static class LongPair implements Comparable<LongPair> {
        public final long first;
        public final long second;

        public LongPair(long first, long second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof LongPair) {
                LongPair other = (LongPair) obj;
                return first == other.first && second == other.second;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return (int) hash(first, second);
        }

        @Override
        public int compareTo(LongPair o) {
            if (first != o.first) {
                return Long.compare(first, o.first);
            } else {
                return Long.compare(second, o.second);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        final AtomicBoolean first = new AtomicBoolean(true);
        forEach((item1, item2) -> {
            if (!first.getAndSet(false)) {
                sb.append(", ");
            }
            sb.append('[');
            sb.append(item1);
            sb.append(':');
            sb.append(item2);
            sb.append(']');
        });
        sb.append('}');
        return sb.toString();
    }

}
