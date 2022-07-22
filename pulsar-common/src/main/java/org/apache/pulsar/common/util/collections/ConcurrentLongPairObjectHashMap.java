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
import static java.util.Objects.requireNonNull;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.StampedLock;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Map from long to an Object.
 *
 * <p>Provides similar methods as a {@code ConcurrentMap<Pair<Long,Long>>,Object>} with 2 differences:
 * <ol>
 * <li>No boxing/unboxing from (long,long) -> Object
 * <li>Open hash map with linear probing, no node allocations to store the values
 * </ol>
 *
 * @param <V>
 */
@SuppressWarnings("unchecked")
public class ConcurrentLongPairObjectHashMap<V> {

    private static final Object EmptyValue = null;
    private static final Object DeletedValue = new Object();

    private static final int DefaultExpectedItems = 256;
    private static final int DefaultConcurrencyLevel = 16;

    private static final float DefaultMapFillFactor = 0.66f;
    private static final float DefaultMapIdleFactor = 0.15f;

    private static final float DefaultExpandFactor = 2;
    private static final float DefaultShrinkFactor = 2;

    private static final boolean DefaultAutoShrink = false;

    public interface LongLongFunction<R> {
        R apply(long key1, long key2);
    }

    public static <V> Builder<V> newBuilder() {
        return new Builder<>();
    }

    /**
     * Builder of ConcurrentLongHashMap.
     */
    public static class Builder<T> {
        int expectedItems = DefaultExpectedItems;
        int concurrencyLevel = DefaultConcurrencyLevel;
        float mapFillFactor = DefaultMapFillFactor;
        float mapIdleFactor = DefaultMapIdleFactor;
        float expandFactor = DefaultExpandFactor;
        float shrinkFactor = DefaultShrinkFactor;
        boolean autoShrink = DefaultAutoShrink;

        public Builder<T> expectedItems(int expectedItems) {
            this.expectedItems = expectedItems;
            return this;
        }

        public Builder<T> concurrencyLevel(int concurrencyLevel) {
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }

        public Builder<T> mapFillFactor(float mapFillFactor) {
            this.mapFillFactor = mapFillFactor;
            return this;
        }

        public Builder<T> mapIdleFactor(float mapIdleFactor) {
            this.mapIdleFactor = mapIdleFactor;
            return this;
        }

        public Builder<T> expandFactor(float expandFactor) {
            this.expandFactor = expandFactor;
            return this;
        }

        public Builder<T> shrinkFactor(float shrinkFactor) {
            this.shrinkFactor = shrinkFactor;
            return this;
        }

        public Builder<T> autoShrink(boolean autoShrink) {
            this.autoShrink = autoShrink;
            return this;
        }

        public ConcurrentLongPairObjectHashMap<T> build() {
            return new ConcurrentLongPairObjectHashMap<>(expectedItems, concurrencyLevel,
                    mapFillFactor, mapIdleFactor, autoShrink, expandFactor, shrinkFactor);
        }
    }

    private final Section<V>[] sections;

    @Deprecated
    public ConcurrentLongPairObjectHashMap() {
        this(DefaultExpectedItems);
    }

    @Deprecated
    public ConcurrentLongPairObjectHashMap(int expectedItems) {
        this(expectedItems, DefaultConcurrencyLevel);
    }

    @Deprecated
    public ConcurrentLongPairObjectHashMap(int expectedItems, int concurrencyLevel) {
        this(expectedItems, concurrencyLevel, DefaultMapFillFactor, DefaultMapIdleFactor,
                DefaultAutoShrink, DefaultExpandFactor, DefaultShrinkFactor);
    }

    public ConcurrentLongPairObjectHashMap(int expectedItems, int concurrencyLevel,
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
        this.sections = (Section<V>[]) new Section[numSections];

        for (int i = 0; i < numSections; i++) {
            sections[i] = new Section<>(perSectionCapacity, mapFillFactor, mapIdleFactor,
                    autoShrink, expandFactor, shrinkFactor);
        }
    }

    public long size() {
        long size = 0;
        for (Section<V> s : sections) {
            size += s.size;
        }
        return size;
    }

    long getUsedBucketCount() {
        long usedBucketCount = 0;
        for (Section<V> s : sections) {
            usedBucketCount += s.usedBuckets;
        }
        return usedBucketCount;
    }

    public long capacity() {
        long capacity = 0;
        for (Section<V> s : sections) {
            capacity += s.capacity;
        }
        return capacity;
    }

    public boolean isEmpty() {
        for (Section<V> s : sections) {
            if (s.size != 0) {
                return false;
            }
        }

        return true;
    }

    public V get(long key1, long key2) {
        long h = hash(key1, key2);
        return getSection(h).get(key1, key2, (int) h);
    }

    public boolean containsKey(long key1, long key2) {
        return get(key1, key2) != null;
    }

    public V put(long key1, long key2, V value) {
        requireNonNull(value);
        long h = hash(key1, key2);
        return getSection(h).put(key1, key2, value, (int) h, false, null);
    }

    public V putIfAbsent(long key1, long key2, V value) {
        requireNonNull(value);
        long h = hash(key1, key2);
        return getSection(h).put(key1, key2, value, (int) h, true, null);
    }

    public V computeIfAbsent(long key1, long key2, LongLongFunction<V> provider) {
        requireNonNull(provider);
        long h = hash(key1, key2);
        return getSection(h).put(key1, key2, null, (int) h, true, provider);
    }

    public V remove(long key1, long key2) {
        long h = hash(key1, key2);
        return getSection(h).remove(key1, key2, null, (int) h);
    }

    public boolean remove(long key1, long key2, Object value) {
        requireNonNull(value);
        long h = hash(key1, key2);
        return getSection(h).remove(key1, key2, value, (int) h) != null;
    }

    private Section<V> getSection(long hash) {
        // Use 32 msb out of long to get the section
        final int sectionIdx = (int) (hash >>> 32) & (sections.length - 1);
        return sections[sectionIdx];
    }

    public void clear() {
        for (int i = 0; i < sections.length; i++) {
            sections[i].clear();
        }
    }

    public void forEach(EntryProcessor<V> processor) {
        for (int i = 0; i < sections.length; i++) {
            sections[i].forEach(processor);
        }
    }

    /**
     * @return a new list of all keys (makes a copy)
     */
    public List<Pair<Long, Long>> keys() {
        List<Pair<Long, Long>> keys = Lists.newArrayListWithExpectedSize((int) size());
        forEach((key1, key2, value) -> keys.add(Pair.of(key1, key2)));
        return keys;
    }

    public List<V> values() {
        List<V> values = Lists.newArrayListWithExpectedSize((int) size());
        forEach((key1, key2, value) -> values.add(value));
        return values;
    }

    /**
     * Processor for one key-value entry, where the key is {@code long}.
     *
     * @param <V> type of the value.
     */
    public interface EntryProcessor<V> {
        void accept(long key1, long key2, V value);
    }

    // A section is a portion of the hash map that is covered by a single
    @SuppressWarnings("serial")
    private static final class Section<V> extends StampedLock {
        private volatile long[] keys1;
        private volatile long[] keys2;
        private volatile V[] values;

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
            this.keys1 = new long[this.capacity];
            this.keys2 = new long[this.capacity];
            this.values = (V[]) new Object[this.capacity];
            this.size = 0;
            this.usedBuckets = 0;
            this.autoShrink = autoShrink;
            this.mapFillFactor = mapFillFactor;
            this.mapIdleFactor = mapIdleFactor;
            this.expandFactor = expandFactor;
            this.shrinkFactor = shrinkFactor;
            this.resizeThresholdUp = (int) (this.capacity * mapFillFactor);
            this.resizeThresholdBelow = (int) (this.capacity * mapIdleFactor);
        }

        V get(long key1, long key2, int keyHash) {
            int bucket = keyHash;

            long stamp = tryOptimisticRead();
            boolean acquiredLock = false;

            try {
                while (true) {
                    int capacity = this.capacity;
                    bucket = signSafeMod(bucket, capacity);

                    // First try optimistic locking
                    long storedKey1 = keys1[bucket];
                    long storedKey2 = keys2[bucket];
                    V storedValue = values[bucket];

                    if (!acquiredLock && validate(stamp)) {
                        // The values we have read are consistent
                        if (storedKey1 == key1 && storedKey2 == key2) {
                            return storedValue != DeletedValue ? storedValue : null;
                        } else if (storedValue == EmptyValue) {
                            // Not found
                            return null;
                        }
                    } else {
                        // Fallback to acquiring read lock
                        if (!acquiredLock) {
                            stamp = readLock();
                            acquiredLock = true;
                            storedKey1 = keys1[bucket];
                            storedKey2 = keys2[bucket];
                            storedValue = values[bucket];
                        }

                        if (capacity != this.capacity) {
                            // There has been a rehashing. We need to restart the search
                            bucket = keyHash;
                            continue;
                        }

                        if (storedKey1 == key1 && storedKey2 == key2) {
                            return storedValue != DeletedValue ? storedValue : null;
                        } else if (storedValue == EmptyValue) {
                            // Not found
                            return null;
                        }
                    }

                    ++bucket;
                }
            } finally {
                if (acquiredLock) {
                    unlockRead(stamp);
                }
            }
        }

        V put(long key1, long key2, V value, int keyHash, boolean onlyIfAbsent, LongLongFunction<V> valueProvider) {
            int bucket = keyHash;

            long stamp = writeLock();
            int capacity = this.capacity;

            // Remember where we find the first available spot
            int firstDeletedKey = -1;

            try {
                while (true) {
                    bucket = signSafeMod(bucket, capacity);

                    long storedKey1 = keys1[bucket];
                    long storedKey2 = keys2[bucket];
                    V storedValue = values[bucket];

                    if (storedKey1 == key1 && storedKey2 == key2) {
                        if (storedValue == EmptyValue) {
                            values[bucket] = value != null ? value : valueProvider.apply(key1, key2);
                            SIZE_UPDATER.incrementAndGet(this);
                            ++usedBuckets;
                            return valueProvider != null ? values[bucket] : null;
                        } else if (storedValue == DeletedValue) {
                            values[bucket] = value != null ? value : valueProvider.apply(key1, key2);
                            SIZE_UPDATER.incrementAndGet(this);
                            return valueProvider != null ? values[bucket] : null;
                        } else if (!onlyIfAbsent) {
                            // Over written an old value for same key
                            values[bucket] = value;
                            return storedValue;
                        } else {
                            return storedValue;
                        }
                    } else if (storedValue == EmptyValue) {
                        // Found an empty bucket. This means the key is not in the map. If we've already seen a deleted
                        // key, we should write at that position
                        if (firstDeletedKey != -1) {
                            bucket = firstDeletedKey;
                        } else {
                            ++usedBuckets;
                        }

                        keys1[bucket] = key1;
                        keys2[bucket] = key2;
                        values[bucket] = value != null ? value : valueProvider.apply(key1, key2);
                        SIZE_UPDATER.incrementAndGet(this);
                        return valueProvider != null ? values[bucket] : null;
                    } else if (storedValue == DeletedValue) {
                        // The bucket contained a different deleted key
                        if (firstDeletedKey == -1) {
                            firstDeletedKey = bucket;
                        }
                    }

                    ++bucket;
                }
            } finally {
                if (usedBuckets > resizeThresholdUp) {
                    try {
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

        private V remove(long key1, long key2, Object value, int keyHash) {
            int bucket = keyHash;
            long stamp = writeLock();

            try {
                while (true) {
                    int capacity = this.capacity;
                    bucket = signSafeMod(bucket, capacity);

                    long storedKey1 = keys1[bucket];
                    long storedKey2 = keys2[bucket];
                    V storedValue = values[bucket];
                    if (storedKey1 == key1 && storedKey2 == key2) {
                        if (value == null || value.equals(storedValue)) {
                            if (storedValue == EmptyValue || storedValue == DeletedValue) {
                                return null;
                            }

                            SIZE_UPDATER.decrementAndGet(this);
                            V nextValueInArray = values[signSafeMod(bucket + 1, capacity)];
                            if (nextValueInArray == EmptyValue) {
                                values[bucket] = (V) EmptyValue;
                                --usedBuckets;

                                // Cleanup all the buckets that were in `DeletedValue` state,
                                // so that we can reduce unnecessary expansions
                                int lastBucket = signSafeMod(bucket - 1, capacity);
                                while (values[lastBucket] == DeletedValue) {
                                    values[lastBucket] = (V) EmptyValue;
                                    --usedBuckets;

                                    lastBucket = signSafeMod(lastBucket - 1, capacity);
                                }
                            } else {
                                values[bucket] = (V) DeletedValue;
                            }

                            return storedValue;
                        } else {
                            return null;
                        }
                    } else if (storedValue == EmptyValue) {
                        // Key wasn't found
                        return null;
                    }

                    ++bucket;
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

        void clear() {
            long stamp = writeLock();

            try {
                Arrays.fill(keys1, 0);
                Arrays.fill(keys2, 0);
                Arrays.fill(values, EmptyValue);
                this.size = 0;
                this.usedBuckets = 0;
                if (autoShrink) {
                    rehash(initCapacity);
                }
            } finally {
                unlockWrite(stamp);
            }
        }

        public void forEach(EntryProcessor<V> processor) {
            long stamp = tryOptimisticRead();

            // We need to make sure that we read these 3 variables in a consistent way
            int capacity = this.capacity;
            long[] keys1 = this.keys1;
            long[] keys2 = this.keys2;
            V[] values = this.values;

            // Validate no rehashing
            if (!validate(stamp)) {
                // Fallback to read lock
                stamp = readLock();

                capacity = this.capacity;
                keys1 = this.keys1;
                keys2 = this.keys2;
                values = this.values;
                unlockRead(stamp);
            }

            // Go through all the buckets for this section. We try to renew the stamp only after a validation
            // error, otherwise we keep going with the same.
            for (int bucket = 0; bucket < capacity; bucket++) {
                if (stamp == 0) {
                    stamp = tryOptimisticRead();
                }

                long storedKey1 = keys1[bucket];
                long storedKey2 = keys2[bucket];
                V storedValue = values[bucket];

                if (!validate(stamp)) {
                    // Fallback to acquiring read lock
                    stamp = readLock();

                    try {
                        storedKey1 = keys1[bucket];
                        storedKey2 = keys2[bucket];
                        storedValue = values[bucket];
                    } finally {
                        unlockRead(stamp);
                    }

                    stamp = 0;
                }

                if (storedValue != DeletedValue && storedValue != EmptyValue) {
                    processor.accept(storedKey1, storedKey2, storedValue);
                }
            }
        }

        private void rehash(int newCapacity) {
            // Expand the hashmap
            long[] newKeys1 = new long[newCapacity];
            long[] newKeys2 = new long[newCapacity];
            V[] newValues = (V[]) new Object[newCapacity];

            // Re-hash table
            for (int i = 0; i < keys1.length; i++) {
                long storedKey1 = keys1[i];
                long storedKey2 = keys2[i];
                V storedValue = values[i];
                if (storedValue != EmptyValue && storedValue != DeletedValue) {
                    insertKeyValueNoLock(newKeys1, newKeys2, newValues, storedKey1, storedKey2, storedValue);
                }
            }

            keys1 = newKeys1;
            keys2 = newKeys2;
            values = newValues;
            capacity = newCapacity;
            usedBuckets = size;
            resizeThresholdUp = (int) (capacity * mapFillFactor);
            resizeThresholdBelow = (int) (capacity * mapIdleFactor);
        }

        private static <V> void insertKeyValueNoLock(long[] keys1, long[] keys2, V[] values, long key1, long key2,
                                                     V value) {
            int bucket = (int) hash(key1, key2);

            while (true) {
                bucket = signSafeMod(bucket, keys1.length);

                V storedValue = values[bucket];

                if (storedValue == EmptyValue) {
                    // The bucket is empty, so we can use it
                    keys1[bucket] = key1;
                    keys2[bucket] = key2;
                    values[bucket] = value;
                    return;
                }

                ++bucket;
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
        return (int) n & (max - 1);
    }

    private static int alignToPowerOfTwo(int n) {
        return (int) Math.pow(2, 32 - Integer.numberOfLeadingZeros(n - 1));
    }
}
