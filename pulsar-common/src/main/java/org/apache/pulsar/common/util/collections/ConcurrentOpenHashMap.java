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
import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Concurrent hash map.
 *
 * <p>Provides similar methods as a {@code ConcurrentMap<K,V>} but since it's an open hash map with linear probing,
 * no node allocations are required to store the values.
 *
 * @param <V>
 */
@SuppressWarnings("unchecked")
public class ConcurrentOpenHashMap<K, V> {

    private static final Object EmptyKey = null;
    private static final Object DeletedKey = new Object();

    private static final float MapFillFactor = 0.66f;

    private static final int DefaultExpectedItems = 256;
    private static final int DefaultConcurrencyLevel = 16;

    private final Section<K, V>[] sections;

    public ConcurrentOpenHashMap() {
        this(DefaultExpectedItems);
    }

    public ConcurrentOpenHashMap(int expectedItems) {
        this(expectedItems, DefaultConcurrencyLevel);
    }

    public ConcurrentOpenHashMap(int expectedItems, int concurrencyLevel) {
        checkArgument(expectedItems > 0);
        checkArgument(concurrencyLevel > 0);
        checkArgument(expectedItems >= concurrencyLevel);

        int numSections = concurrencyLevel;
        int perSectionExpectedItems = expectedItems / numSections;
        int perSectionCapacity = (int) (perSectionExpectedItems / MapFillFactor);
        this.sections = (Section<K, V>[]) new Section[numSections];

        for (int i = 0; i < numSections; i++) {
            sections[i] = new Section<>(perSectionCapacity);
        }
    }

    public long size() {
        long size = 0;
        for (Section<K, V> s : sections) {
            size += s.size;
        }
        return size;
    }

    public long capacity() {
        long capacity = 0;
        for (Section<K, V> s : sections) {
            capacity += s.capacity;
        }
        return capacity;
    }

    public boolean isEmpty() {
        for (Section<K, V> s : sections) {
            if (s.size != 0) {
                return false;
            }
        }

        return true;
    }

    public V get(K key) {
        checkNotNull(key);
        long h = hash(key);
        return getSection(h).get(key, (int) h);
    }

    public boolean containsKey(K key) {
        return get(key) != null;
    }

    public V put(K key, V value) {
        checkNotNull(key);
        checkNotNull(value);
        long h = hash(key);
        return getSection(h).put(key, value, (int) h, false, null);
    }

    public V putIfAbsent(K key, V value) {
        checkNotNull(key);
        checkNotNull(value);
        long h = hash(key);
        return getSection(h).put(key, value, (int) h, true, null);
    }

    public V computeIfAbsent(K key, Function<K, V> provider) {
        checkNotNull(key);
        checkNotNull(provider);
        long h = hash(key);
        return getSection(h).put(key, null, (int) h, true, provider);
    }

    public V remove(K key) {
        checkNotNull(key);
        long h = hash(key);
        return getSection(h).remove(key, null, (int) h);
    }

    public boolean remove(K key, Object value) {
        checkNotNull(key);
        checkNotNull(value);
        long h = hash(key);
        return getSection(h).remove(key, value, (int) h) != null;
    }

    private Section<K, V> getSection(long hash) {
        // Use 32 msb out of long to get the section
        final int sectionIdx = (int) (hash >>> 32) & (sections.length - 1);
        return sections[sectionIdx];
    }

    public void clear() {
        for (int i = 0; i < sections.length; i++) {
            sections[i].clear();
        }
    }

    public void forEach(BiConsumer<? super K, ? super V> processor) {
        for (int i = 0; i < sections.length; i++) {
            sections[i].forEach(processor);
        }
    }

    /**
     * @return a new list of all keys (makes a copy)
     */
    public List<K> keys() {
        List<K> keys = new ArrayList<>((int) size());
        forEach((key, value) -> keys.add(key));
        return keys;
    }

    public List<V> values() {
        List<V> values = new ArrayList<>((int) size());
        forEach((key, value) -> values.add(value));
        return values;
    }

    // A section is a portion of the hash map that is covered by a single
    @SuppressWarnings("serial")
    private static final class Section<K, V> extends StampedLock {
        // Keys and values are stored interleaved in the table array
        private volatile Object[] table;

        private volatile int capacity;
        private static final AtomicIntegerFieldUpdater<Section> SIZE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(Section.class, "size");
        private volatile int size;
        private int usedBuckets;
        private int resizeThreshold;

        Section(int capacity) {
            this.capacity = alignToPowerOfTwo(capacity);
            this.table = new Object[2 * this.capacity];
            this.size = 0;
            this.usedBuckets = 0;
            this.resizeThreshold = (int) (this.capacity * MapFillFactor);
        }

        V get(K key, int keyHash) {
            long stamp = tryOptimisticRead();
            boolean acquiredLock = false;
            int bucket = signSafeMod(keyHash, capacity);

            try {
                while (true) {
                    // First try optimistic locking
                    K storedKey = (K) table[bucket];
                    V storedValue = (V) table[bucket + 1];

                    if (!acquiredLock && validate(stamp)) {
                        // The values we have read are consistent
                        if (key.equals(storedKey)) {
                            return storedValue;
                        } else if (storedKey == EmptyKey) {
                            // Not found
                            return null;
                        }
                    } else {
                        // Fallback to acquiring read lock
                        if (!acquiredLock) {
                            stamp = readLock();
                            acquiredLock = true;

                            bucket = signSafeMod(keyHash, capacity);
                            storedKey = (K) table[bucket];
                            storedValue = (V) table[bucket + 1];
                        }

                        if (key.equals(storedKey)) {
                            return storedValue;
                        } else if (storedKey == EmptyKey) {
                            // Not found
                            return null;
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

        V put(K key, V value, int keyHash, boolean onlyIfAbsent, Function<K, V> valueProvider) {
            long stamp = writeLock();
            int bucket = signSafeMod(keyHash, capacity);

            // Remember where we find the first available spot
            int firstDeletedKey = -1;

            try {
                while (true) {
                    K storedKey = (K) table[bucket];
                    V storedValue = (V) table[bucket + 1];

                    if (key.equals(storedKey)) {
                        if (!onlyIfAbsent) {
                            // Over written an old value for same key
                            table[bucket + 1] = value;
                            return storedValue;
                        } else {
                            return storedValue;
                        }
                    } else if (storedKey == EmptyKey) {
                        // Found an empty bucket. This means the key is not in the map. If we've already seen a deleted
                        // key, we should write at that position
                        if (firstDeletedKey != -1) {
                            bucket = firstDeletedKey;
                        } else {
                            ++usedBuckets;
                        }

                        if (value == null) {
                            value = valueProvider.apply(key);
                        }

                        table[bucket] = key;
                        table[bucket + 1] = value;
                        SIZE_UPDATER.incrementAndGet(this);
                        return valueProvider != null ? value : null;
                    } else if (storedKey == DeletedKey) {
                        // The bucket contained a different deleted key
                        if (firstDeletedKey == -1) {
                            firstDeletedKey = bucket;
                        }
                    }

                    bucket = (bucket + 2) & (table.length - 1);
                }
            } finally {
                if (usedBuckets > resizeThreshold) {
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

        private V remove(K key, Object value, int keyHash) {
            long stamp = writeLock();
            int bucket = signSafeMod(keyHash, capacity);

            try {
                while (true) {
                    K storedKey = (K) table[bucket];
                    V storedValue = (V) table[bucket + 1];
                    if (key.equals(storedKey)) {
                        if (value == null || value.equals(storedValue)) {
                            SIZE_UPDATER.decrementAndGet(this);

                            int nextInArray = (bucket + 2) & (table.length - 1);
                            if (table[nextInArray] == EmptyKey) {
                                table[bucket] = EmptyKey;
                                table[bucket + 1] = null;
                                --usedBuckets;
                            } else {
                                table[bucket] = DeletedKey;
                                table[bucket + 1] = null;
                            }

                            return storedValue;
                        } else {
                            return null;
                        }
                    } else if (storedKey == EmptyKey) {
                        // Key wasn't found
                        return null;
                    }

                    bucket = (bucket + 2) & (table.length - 1);
                }

            } finally {
                unlockWrite(stamp);
            }
        }

        void clear() {
            long stamp = writeLock();

            try {
                Arrays.fill(table, EmptyKey);
                this.size = 0;
                this.usedBuckets = 0;
            } finally {
                unlockWrite(stamp);
            }
        }

        public void forEach(BiConsumer<? super K, ? super V> processor) {
            // Take a reference to the data table, if there is a rehashing event, we'll be
            // simply iterating over a snapshot of the data.
            Object[] table = this.table;

            // Go through all the buckets for this section. We try to renew the stamp only after a validation
            // error, otherwise we keep going with the same.
            long stamp = 0;
            for (int bucket = 0; bucket < table.length; bucket += 2) {
                if (stamp == 0) {
                    stamp = tryOptimisticRead();
                }

                K storedKey = (K) table[bucket];
                V storedValue = (V) table[bucket + 1];

                if (!validate(stamp)) {
                    // Fallback to acquiring read lock
                    stamp = readLock();

                    try {
                        storedKey = (K) table[bucket];
                        storedValue = (V) table[bucket + 1];
                    } finally {
                        unlockRead(stamp);
                    }

                    stamp = 0;
                }

                if (storedKey != DeletedKey && storedKey != EmptyKey) {
                    processor.accept(storedKey, storedValue);
                }
            }
        }

        private void rehash() {
            // Expand the hashmap
            int newCapacity = capacity * 2;
            Object[] newTable = new Object[2 * newCapacity];

            // Re-hash table
            for (int i = 0; i < table.length; i += 2) {
                K storedKey = (K) table[i];
                V storedValue = (V) table[i + 1];
                if (storedKey != EmptyKey && storedKey != DeletedKey) {
                    insertKeyValueNoLock(newTable, newCapacity, storedKey, storedValue);
                }
            }

            table = newTable;
            capacity = newCapacity;
            usedBuckets = size;
            resizeThreshold = (int) (capacity * MapFillFactor);
        }

        private static <K, V> void insertKeyValueNoLock(Object[] table, int capacity, K key, V value) {
            int bucket = signSafeMod(hash(key), capacity);

            while (true) {
                K storedKey = (K) table[bucket];

                if (storedKey == EmptyKey) {
                    // The bucket is empty, so we can use it
                    table[bucket] = key;
                    table[bucket + 1] = value;
                    return;
                }

                bucket = (bucket + 2) & (table.length - 1);
            }
        }
    }

    private static final long HashMixer = 0xc6a4a7935bd1e995L;
    private static final int R = 47;

    static final <K> long hash(K key) {
        long hash = key.hashCode() * HashMixer;
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
}
