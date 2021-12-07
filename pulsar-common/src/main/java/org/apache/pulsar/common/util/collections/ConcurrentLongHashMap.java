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
import java.util.function.LongFunction;

/**
 * Map from long to an Object.
 *
 * <p>Provides similar methods as a {@code ConcurrentMap<long,Object>} with 2 differences:
 * <ol>
 * <li>No boxing/unboxing from long -> Long
 * <li>Open hash map with linear probing, no node allocations to store the values
 * </ol>
 *
 * @param <V>
 */
@SuppressWarnings("unchecked")
public class ConcurrentLongHashMap<V> {

    private static final Object EmptyValue = null;
    private static final Object DeletedValue = new Object();

    private static final float MapFillFactor = 0.66f;

    private static final int DefaultExpectedItems = 256;
    private static final int DefaultConcurrencyLevel = 16;

    private final Section<V>[] sections;

    public ConcurrentLongHashMap() {
        this(DefaultExpectedItems);
    }

    public ConcurrentLongHashMap(int expectedItems) {
        this(expectedItems, DefaultConcurrencyLevel);
    }

    public ConcurrentLongHashMap(int expectedItems, int concurrencyLevel) {
        checkArgument(expectedItems > 0);
        checkArgument(concurrencyLevel > 0);
        checkArgument(expectedItems >= concurrencyLevel);

        int numSections = concurrencyLevel;
        int perSectionExpectedItems = expectedItems / numSections;
        int perSectionCapacity = (int) (perSectionExpectedItems / MapFillFactor);
        this.sections = (Section<V>[]) new Section[numSections];

        for (int i = 0; i < numSections; i++) {
            sections[i] = new Section<>(perSectionCapacity);
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

    public V get(long key) {
        long h = hash(key);
        return getSection(h).get(key, (int) h);
    }

    public boolean containsKey(long key) {
        return get(key) != null;
    }

    public V put(long key, V value) {
        requireNonNull(value);
        long h = hash(key);
        return getSection(h).put(key, value, (int) h, false, null);
    }

    public V putIfAbsent(long key, V value) {
        requireNonNull(value);
        long h = hash(key);
        return getSection(h).put(key, value, (int) h, true, null);
    }

    public V computeIfAbsent(long key, LongFunction<V> provider) {
        requireNonNull(provider);
        long h = hash(key);
        return getSection(h).put(key, null, (int) h, true, provider);
    }

    public V remove(long key) {
        long h = hash(key);
        return getSection(h).remove(key, null, (int) h);
    }

    public boolean remove(long key, Object value) {
        requireNonNull(value);
        long h = hash(key);
        return getSection(h).remove(key, value, (int) h) != null;
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
    public List<Long> keys() {
        List<Long> keys = Lists.newArrayListWithExpectedSize((int) size());
        forEach((key, value) -> keys.add(key));
        return keys;
    }

    public List<V> values() {
        List<V> values = Lists.newArrayListWithExpectedSize((int) size());
        forEach((key, value) -> values.add(value));
        return values;
    }

    /**
     * Processor for one key-value entry, where the key is {@code long}.
     *
     * @param <V> type of the value.
     */
    public interface EntryProcessor<V> {
        void accept(long key, V value);
    }

    // A section is a portion of the hash map that is covered by a single
    @SuppressWarnings("serial")
    private static final class Section<V> extends StampedLock {
        private volatile long[] keys;
        private volatile V[] values;

        private volatile int capacity;
        private static final AtomicIntegerFieldUpdater<Section> SIZE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(Section.class, "size");

        private volatile int size;
        private int usedBuckets;
        private int resizeThreshold;

        Section(int capacity) {
            this.capacity = alignToPowerOfTwo(capacity);
            this.keys = new long[this.capacity];
            this.values = (V[]) new Object[this.capacity];
            this.size = 0;
            this.usedBuckets = 0;
            this.resizeThreshold = (int) (this.capacity * MapFillFactor);
        }

        V get(long key, int keyHash) {
            int bucket = keyHash;

            long stamp = tryOptimisticRead();
            boolean acquiredLock = false;

            try {
                while (true) {
                    int capacity = this.capacity;
                    bucket = signSafeMod(bucket, capacity);

                    // First try optimistic locking
                    long storedKey = keys[bucket];
                    V storedValue = values[bucket];

                    if (!acquiredLock && validate(stamp)) {
                        // The values we have read are consistent
                        if (storedKey == key) {
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
                            storedKey = keys[bucket];
                            storedValue = values[bucket];
                        }

                        if (capacity != this.capacity) {
                            // There has been a rehashing. We need to restart the search
                            bucket = keyHash;
                            continue;
                        }

                        if (storedKey == key) {
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

        V put(long key, V value, int keyHash, boolean onlyIfAbsent, LongFunction<V> valueProvider) {
            int bucket = keyHash;

            long stamp = writeLock();
            int capacity = this.capacity;

            // Remember where we find the first available spot
            int firstDeletedKey = -1;

            try {
                while (true) {
                    bucket = signSafeMod(bucket, capacity);

                    long storedKey = keys[bucket];
                    V storedValue = values[bucket];

                    if (storedKey == key) {
                        if (storedValue == EmptyValue) {
                            values[bucket] = value != null ? value : valueProvider.apply(key);
                            SIZE_UPDATER.incrementAndGet(this);
                            ++usedBuckets;
                            return valueProvider != null ? values[bucket] : null;
                        } else if (storedValue == DeletedValue) {
                            values[bucket] = value != null ? value : valueProvider.apply(key);
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

                        keys[bucket] = key;
                        values[bucket] = value != null ? value : valueProvider.apply(key);
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
                if (usedBuckets >= resizeThreshold) {
                    try {
                        rehash();
                    } finally {
                        unlockWrite(stamp);
                    }
                } else {
                    unlockWrite(stamp);
                }
            }
        }

        private V remove(long key, Object value, int keyHash) {
            int bucket = keyHash;
            long stamp = writeLock();

            try {
                while (true) {
                    int capacity = this.capacity;
                    bucket = signSafeMod(bucket, capacity);

                    long storedKey = keys[bucket];
                    V storedValue = values[bucket];
                    if (storedKey == key) {
                        if (value == null || value.equals(storedValue)) {
                            if (storedValue == EmptyValue || storedValue == DeletedValue) {
                                return null;
                            }

                            SIZE_UPDATER.decrementAndGet(this);
                            V nextValueInArray = values[signSafeMod(bucket + 1, capacity)];
                            if (nextValueInArray == EmptyValue) {
                                values[bucket] = (V) EmptyValue;
                                --usedBuckets;
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
                unlockWrite(stamp);
            }
        }

        void clear() {
            long stamp = writeLock();

            try {
                Arrays.fill(keys, 0);
                Arrays.fill(values, EmptyValue);
                this.size = 0;
                this.usedBuckets = 0;
            } finally {
                unlockWrite(stamp);
            }
        }

        public void forEach(EntryProcessor<V> processor) {
            long stamp = tryOptimisticRead();

            // We need to make sure that we read these 3 variables in a consistent way
            int capacity = this.capacity;
            long[] keys = this.keys;
            V[] values = this.values;

            // Validate no rehashing
            if (!validate(stamp)) {
                // Fallback to read lock
                stamp = readLock();

                capacity = this.capacity;
                keys = this.keys;
                values = this.values;
                unlockRead(stamp);
            }

            // Go through all the buckets for this section. We try to renew the stamp only after a validation
            // error, otherwise we keep going with the same.
            for (int bucket = 0; bucket < capacity; bucket++) {
                if (stamp == 0) {
                    stamp = tryOptimisticRead();
                }

                long storedKey = keys[bucket];
                V storedValue = values[bucket];

                if (!validate(stamp)) {
                    // Fallback to acquiring read lock
                    stamp = readLock();

                    try {
                        storedKey = keys[bucket];
                        storedValue = values[bucket];
                    } finally {
                        unlockRead(stamp);
                    }

                    stamp = 0;
                }

                if (storedValue != DeletedValue && storedValue != EmptyValue) {
                    processor.accept(storedKey, storedValue);
                }
            }
        }

        private void rehash() {
            // Expand the hashmap
            int newCapacity = capacity * 2;
            long[] newKeys = new long[newCapacity];
            V[] newValues = (V[]) new Object[newCapacity];

            // Re-hash table
            for (int i = 0; i < keys.length; i++) {
                long storedKey = keys[i];
                V storedValue = values[i];
                if (storedValue != EmptyValue && storedValue != DeletedValue) {
                    insertKeyValueNoLock(newKeys, newValues, storedKey, storedValue);
                }
            }

            keys = newKeys;
            values = newValues;
            capacity = newCapacity;
            usedBuckets = size;
            resizeThreshold = (int) (capacity * MapFillFactor);
        }

        private static <V> void insertKeyValueNoLock(long[] keys, V[] values, long key, V value) {
            int bucket = (int) hash(key);

            while (true) {
                bucket = signSafeMod(bucket, keys.length);

                V storedValue = values[bucket];

                if (storedValue == EmptyValue) {
                    // The bucket is empty, so we can use it
                    keys[bucket] = key;
                    values[bucket] = value;
                    return;
                }

                ++bucket;
            }
        }
    }

    private static final long HashMixer = 0xc6a4a7935bd1e995L;
    private static final int R = 47;

    static final long hash(long key) {
        long hash = key * HashMixer;
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
