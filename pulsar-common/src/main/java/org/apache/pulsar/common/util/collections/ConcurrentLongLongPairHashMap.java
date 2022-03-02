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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.StampedLock;

/**
 * Concurrent hash map where both keys and values are composed of pairs of longs.
 *
 * <p>(long,long) --&gt; (long,long)
 *
 * <p>Provides similar methods as a {@code ConcurrentMap<K,V>} but since it's an open hash map with linear probing,
 * no node allocations are required to store the keys and values, and no boxing is required.
 *
 * <p>Keys <strong>MUST</strong> be &gt;= 0.
 */
public class ConcurrentLongLongPairHashMap {

    private static final long EmptyKey = -1L;
    private static final long DeletedKey = -2L;

    private static final long ValueNotFound = -1L;


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
     * Builder of ConcurrentLongLongPairHashMap.
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

        public ConcurrentLongLongPairHashMap build() {
            return new ConcurrentLongLongPairHashMap(expectedItems, concurrencyLevel,
                    mapFillFactor, mapIdleFactor, autoShrink, expandFactor, shrinkFactor);
        }
    }

    /**
     * A BiConsumer Long pair.
     */
    public interface BiConsumerLongPair {
        void accept(long key1, long key2, long value1, long value2);
    }

    /**
     * A Long pair function.
     */
    public interface LongLongPairFunction {
        long apply(long key1, long key2);
    }

    /**
     * A Long pair predicate.
     */
    public interface LongLongPairPredicate {
        boolean test(long key1, long key2, long value1, long value2);
    }

    private ConcurrentLongLongPairHashMap(int expectedItems, int concurrencyLevel,
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
        int perSectionCapacity = (int) (perSectionExpectedItems / mapFillFactor);
        this.sections = new Section[numSections];

        for (int i = 0; i < numSections; i++) {
            sections[i] = new Section(perSectionCapacity, mapFillFactor, mapIdleFactor,
                    autoShrink, expandFactor, shrinkFactor);
        }
    }

    public long size() {
        long size = 0;
        for (Section s : sections) {
            size += s.size;
        }
        return size;
    }

    public long capacity() {
        long capacity = 0;
        for (Section s : sections) {
            capacity += s.capacity;
        }
        return capacity;
    }

    public boolean isEmpty() {
        for (Section s : sections) {
            if (s.size != 0) {
                return false;
            }
        }

        return true;
    }

    long getUsedBucketCount() {
        long usedBucketCount = 0;
        for (Section s : sections) {
            usedBucketCount += s.usedBuckets;
        }
        return usedBucketCount;
    }

    /**
     * @param key1
     * @param key2
     * @return the value or -1 if the key was not present.
     */
    public LongPair get(long key1, long key2) {
        checkBiggerEqualZero(key1);
        long h = hash(key1, key2);
        return getSection(h).get(key1, key2, (int) h);
    }

    public boolean containsKey(long key1, long key2) {
        return get(key1, key2) != null;
    }

    public boolean put(long key1, long key2, long value1, long value2) {
        checkBiggerEqualZero(key1);
        checkBiggerEqualZero(value1);
        long h = hash(key1, key2);
        return getSection(h).put(key1, key2, value1, value2, (int) h, false);
    }

    public boolean putIfAbsent(long key1, long key2, long value1, long value2) {
        checkBiggerEqualZero(key1);
        checkBiggerEqualZero(value1);
        long h = hash(key1, key2);
        return getSection(h).put(key1, key2, value1, value2, (int) h, true);
    }

    /**
     * Remove an existing entry if found.
     *
     * @param key1
     * @param key2
     * @return the value associated with the key or -1 if key was not present.
     */
    public boolean remove(long key1, long key2) {
        checkBiggerEqualZero(key1);
        long h = hash(key1, key2);
        return getSection(h).remove(key1, key2, ValueNotFound, ValueNotFound, (int) h);
    }

    public boolean remove(long key1, long key2, long value1, long value2) {
        checkBiggerEqualZero(key1);
        checkBiggerEqualZero(value1);
        long h = hash(key1, key2);
        return getSection(h).remove(key1, key2, value1, value2, (int) h);
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

    public void forEach(BiConsumerLongPair processor) {
        for (Section s : sections) {
            s.forEach(processor);
        }
    }

    /**
     * @return a new list of all keys (makes a copy).
     */
    public List<LongPair> keys() {
        List<LongPair> keys = Lists.newArrayListWithExpectedSize((int) size());
        forEach((key1, key2, value1, value2) -> keys.add(new LongPair(key1, key2)));
        return keys;
    }

    public List<LongPair> values() {
        List<LongPair> values = Lists.newArrayListWithExpectedSize((int) size());
        forEach((key1, key2, value1, value2) -> values.add(new LongPair(value1, value2)));
        return values;
    }

    public Map<LongPair, LongPair> asMap() {
        Map<LongPair, LongPair> map = Maps.newHashMapWithExpectedSize((int) size());
        forEach((key1, key2, value1, value2) -> map.put(new LongPair(key1, key2), new LongPair(value1, value2)));
        return map;
    }

    // A section is a portion of the hash map that is covered by a single
    @SuppressWarnings("serial")
    private static final class Section extends StampedLock {
        // Keys and values are stored interleaved in the table array
        private volatile long[] table;

        private volatile int capacity;
        private final int initCapacity;
        private static final AtomicIntegerFieldUpdater<Section> SIZE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(Section.class, "size");

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
            this.table = new long[4 * this.capacity];
            this.size = 0;
            this.usedBuckets = 0;
            this.autoShrink = autoShrink;
            this.mapFillFactor = mapFillFactor;
            this.mapIdleFactor = mapIdleFactor;
            this.expandFactor = expandFactor;
            this.shrinkFactor = shrinkFactor;
            this.resizeThresholdUp = (int) (this.capacity * mapFillFactor);
            this.resizeThresholdBelow = (int) (this.capacity * mapIdleFactor);
            Arrays.fill(table, EmptyKey);
        }

        LongPair get(long key1, long key2, int keyHash) {
            long stamp = tryOptimisticRead();
            boolean acquiredLock = false;
            int bucket = signSafeMod(keyHash, capacity);

            try {
                while (true) {
                    // First try optimistic locking
                    long storedKey1 = table[bucket];
                    long storedKey2 = table[bucket + 1];
                    long storedValue1 = table[bucket + 2];
                    long storedValue2 = table[bucket + 3];

                    if (!acquiredLock && validate(stamp)) {
                        // The values we have read are consistent
                        if (key1 == storedKey1 && key2 == storedKey2) {
                            return new LongPair(storedValue1, storedValue2);
                        } else if (storedKey1 == EmptyKey) {
                            // Not found
                            return null;
                        }
                    } else {
                        // Fallback to acquiring read lock
                        if (!acquiredLock) {
                            stamp = readLock();
                            acquiredLock = true;

                            bucket = signSafeMod(keyHash, capacity);
                            storedKey1 = table[bucket];
                            storedKey2 = table[bucket + 1];
                            storedValue1 = table[bucket + 2];
                            storedValue2 = table[bucket + 3];
                        }

                        if (key1 == storedKey1 && key2 == storedKey2) {
                            return new LongPair(storedValue1, storedValue2);
                        } else if (storedKey1 == EmptyKey) {
                            // Not found
                            return null;
                        }
                    }

                    bucket = (bucket + 4) & (table.length - 1);
                }
            } finally {
                if (acquiredLock) {
                    unlockRead(stamp);
                }
            }
        }

        boolean put(long key1, long key2, long value1, long value2, int keyHash, boolean onlyIfAbsent) {
            long stamp = writeLock();
            int bucket = signSafeMod(keyHash, capacity);

            // Remember where we find the first available spot
            int firstDeletedKey = -1;

            try {
                while (true) {
                    long storedKey1 = table[bucket];
                    long storedKey2 = table[bucket + 1];

                    if (key1 == storedKey1 && key2 == storedKey2) {
                        if (!onlyIfAbsent) {
                            // Over written an old value for same key
                            table[bucket + 2] = value1;
                            table[bucket + 3] = value2;
                            return true;
                        } else {
                            return false;
                        }
                    } else if (storedKey1 == EmptyKey) {
                        // Found an empty bucket. This means the key is not in the map. If we've already seen a deleted
                        // key, we should write at that position
                        if (firstDeletedKey != -1) {
                            bucket = firstDeletedKey;
                        } else {
                            ++usedBuckets;
                        }

                        table[bucket] = key1;
                        table[bucket + 1] = key2;
                        table[bucket + 2] = value1;
                        table[bucket + 3] = value2;
                        SIZE_UPDATER.incrementAndGet(this);
                        return true;
                    } else if (storedKey1 == DeletedKey) {
                        // The bucket contained a different deleted key
                        if (firstDeletedKey == -1) {
                            firstDeletedKey = bucket;
                        }
                    }

                    bucket = (bucket + 4) & (table.length - 1);
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

        private boolean remove(long key1, long key2, long value1, long value2, int keyHash) {
            long stamp = writeLock();
            int bucket = signSafeMod(keyHash, capacity);

            try {
                while (true) {
                    long storedKey1 = table[bucket];
                    long storedKey2 = table[bucket + 1];
                    long storedValue1 = table[bucket + 2];
                    long storedValue2 = table[bucket + 3];
                    if (key1 == storedKey1 && key2 == storedKey2) {
                        if (value1 == ValueNotFound || (value1 == storedValue1 && value2 == storedValue2)) {
                            SIZE_UPDATER.decrementAndGet(this);

                            cleanBucket(bucket);
                            return true;
                        } else {
                            return false;
                        }
                    } else if (storedKey1 == EmptyKey) {
                        // Key wasn't found
                        return false;
                    }

                    bucket = (bucket + 4) & (table.length - 1);
                }

            } finally {
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
        }

        private void cleanBucket(int bucket) {
            int nextInArray = (bucket + 4) & (table.length - 1);
            if (table[nextInArray] == EmptyKey) {
                table[bucket] = EmptyKey;
                table[bucket + 1] = EmptyKey;
                table[bucket + 2] = ValueNotFound;
                table[bucket + 3] = ValueNotFound;
                --usedBuckets;

                // Cleanup all the buckets that were in `DeletedKey` state, so that we can reduce unnecessary expansions
                bucket = (bucket - 4) & (table.length - 1);
                while (table[bucket] == DeletedKey) {
                    table[bucket] = EmptyKey;
                    table[bucket + 1] = EmptyKey;
                    table[bucket + 2] = ValueNotFound;
                    table[bucket + 3] = ValueNotFound;
                    --usedBuckets;

                    bucket = (bucket - 4) & (table.length - 1);
                }
            } else {
                table[bucket] = DeletedKey;
                table[bucket + 1] = DeletedKey;
                table[bucket + 2] = ValueNotFound;
                table[bucket + 3] = ValueNotFound;
            }
        }

        void clear() {
            long stamp = writeLock();

            try {
                Arrays.fill(table, EmptyKey);
                this.size = 0;
                this.usedBuckets = 0;
                if (autoShrink) {
                    rehash(initCapacity);
                }
            } finally {
                unlockWrite(stamp);
            }
        }

        public void forEach(BiConsumerLongPair processor) {
            long stamp = tryOptimisticRead();

            long[] table = this.table;
            boolean acquiredReadLock = false;

            try {

                // Validate no rehashing
                if (!validate(stamp)) {
                    // Fallback to read lock
                    stamp = readLock();
                    acquiredReadLock = true;
                    table = this.table;
                }

                // Go through all the buckets for this section
                for (int bucket = 0; bucket < table.length; bucket += 4) {
                    long storedKey1 = table[bucket];
                    long storedKey2 = table[bucket + 1];
                    long storedValue1 = table[bucket + 2];
                    long storedValue2 = table[bucket + 3];

                    if (!acquiredReadLock && !validate(stamp)) {
                        // Fallback to acquiring read lock
                        stamp = readLock();
                        acquiredReadLock = true;

                        storedKey1 = table[bucket];
                        storedKey2 = table[bucket + 1];
                        storedValue1 = table[bucket + 2];
                        storedValue2 = table[bucket + 3];
                    }

                    if (storedKey1 != DeletedKey && storedKey1 != EmptyKey) {
                        processor.accept(storedKey1, storedKey2, storedValue1, storedValue2);
                    }
                }
            } finally {
                if (acquiredReadLock) {
                    unlockRead(stamp);
                }
            }
        }

        private void rehash(int newCapacity) {
            long[] newTable = new long[4 * newCapacity];
            Arrays.fill(newTable, EmptyKey);

            // Re-hash table
            for (int i = 0; i < table.length; i += 4) {
                long storedKey1 = table[i];
                long storedKey2 = table[i + 1];
                long storedValue1 = table[i + 2];
                long storedValue2 = table[i + 3];
                if (storedKey1 != EmptyKey && storedKey1 != DeletedKey) {
                    insertKeyValueNoLock(newTable, newCapacity, storedKey1, storedKey2, storedValue1, storedValue2);
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

        private static void insertKeyValueNoLock(long[] table, int capacity, long key1, long key2, long value1,
                long value2) {
            int bucket = signSafeMod(hash(key1, key2), capacity);

            while (true) {
                long storedKey1 = table[bucket];

                if (storedKey1 == EmptyKey) {
                    // The bucket is empty, so we can use it
                    table[bucket] = key1;
                    table[bucket + 1] = key2;
                    table[bucket + 2] = value1;
                    table[bucket + 3] = value2;
                    return;
                }

                bucket = (bucket + 4) & (table.length - 1);
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
        return (int) (n & (max - 1)) << 2;
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
     * A pair of long values.
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
}
