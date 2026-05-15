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
package org.apache.pulsar.common.util.collections;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.LongFunction;

/**
 * Open-addressing hash map with primitive long keys and object values.
 * Uses linear probing and fibonacci hashing for good distribution.
 * Not thread-safe.
 */
@SuppressWarnings("unchecked")
public class Long2ObjectOpenHashMap<V> implements Long2ObjectMap<V> {

    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;

    private long[] keys;
    private Object[] values;
    private boolean[] used;
    private int size;
    private int capacity;
    private int threshold;

    /**
     * Creates a new map with default capacity.
     */
    public Long2ObjectOpenHashMap() {
        this(MIN_CAPACITY);
    }

    /**
     * Creates a new map sized to hold the expected number of items without rehashing.
     *
     * @param expectedItems the expected number of items
     */
    public Long2ObjectOpenHashMap(int expectedItems) {
        int cap = tableSizeFor(Math.max(MIN_CAPACITY, (int) (expectedItems / LOAD_FACTOR) + 1));
        keys = new long[cap];
        values = new Object[cap];
        used = new boolean[cap];
        capacity = cap;
        threshold = (int) (cap * LOAD_FACTOR);
    }

    @Override
    public V get(long key) {
        int idx = indexOf(key);
        if (idx >= 0) {
            return (V) values[idx];
        }
        return null;
    }

    @Override
    public V put(long key, V value) {
        int idx = indexOf(key);
        if (idx >= 0) {
            V old = (V) values[idx];
            values[idx] = value;
            return old;
        }
        if (size >= threshold) {
            rehash(capacity * 2);
        }
        insertNew(key, value);
        return null;
    }

    @Override
    public V remove(long key) {
        int idx = indexOf(key);
        if (idx < 0) {
            return null;
        }
        V old = (V) values[idx];
        removeAt(idx);
        return old;
    }

    @Override
    public V computeIfAbsent(long key, LongFunction<? extends V> mappingFunction) {
        int idx = indexOf(key);
        if (idx >= 0) {
            return (V) values[idx];
        }
        V value = mappingFunction.apply(key);
        if (value != null) {
            if (size >= threshold) {
                rehash(capacity * 2);
            }
            insertNew(key, value);
        }
        return value;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void clear() {
        if (size > 0) {
            java.util.Arrays.fill(used, false);
            java.util.Arrays.fill(values, null);
            size = 0;
        }
    }

    @Override
    public void forEach(LongObjConsumer<? super V> consumer) {
        for (int i = 0; i < capacity; i++) {
            if (used[i]) {
                consumer.accept(keys[i], (V) values[i]);
            }
        }
    }

    @Override
    public Collection<V> values() {
        return new ValuesCollection();
    }

    /**
     * Find the index of the given key, or -1 if not present.
     */
    private int indexOf(long key) {
        int mask = capacity - 1;
        int idx = hash(key) & mask;
        while (true) {
            if (!used[idx]) {
                return -1;
            }
            if (keys[idx] == key) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }

    private void insertNew(long key, V value) {
        int mask = capacity - 1;
        int idx = hash(key) & mask;
        while (used[idx]) {
            idx = (idx + 1) & mask;
        }
        keys[idx] = key;
        values[idx] = value;
        used[idx] = true;
        size++;
    }

    /**
     * Remove the entry at the given index using backward-shift deletion
     * (no tombstones needed).
     */
    private void removeAt(int idx) {
        int mask = capacity - 1;
        values[idx] = null;
        size--;
        // Shift back entries that may have been displaced by the removed entry
        int next = (idx + 1) & mask;
        while (used[next]) {
            int naturalSlot = hash(keys[next]) & mask;
            // Check if 'next' is displaced past 'idx' (wrapping around)
            if ((next > idx && (naturalSlot <= idx || naturalSlot > next))
                    || (next < idx && (naturalSlot <= idx && naturalSlot > next))) {
                keys[idx] = keys[next];
                values[idx] = values[next];
                values[next] = null;
                idx = next;
            }
            next = (next + 1) & mask;
        }
        used[idx] = false;
    }

    private void rehash(int newCapacity) {
        long[] oldKeys = keys;
        Object[] oldValues = values;
        boolean[] oldUsed = used;
        int oldCapacity = capacity;

        capacity = newCapacity;
        keys = new long[newCapacity];
        values = new Object[newCapacity];
        used = new boolean[newCapacity];
        threshold = (int) (newCapacity * LOAD_FACTOR);
        size = 0;

        for (int i = 0; i < oldCapacity; i++) {
            if (oldUsed[i]) {
                insertNew(oldKeys[i], (V) oldValues[i]);
            }
        }
    }

    /**
     * Fibonacci hashing for long keys.
     */
    static int hash(long key) {
        long h = key * 0x9E3779B97F4A7C15L;
        return (int) (h ^ (h >>> 32));
    }

    private static int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < MIN_CAPACITY) ? MIN_CAPACITY : n + 1;
    }

    private class ValuesCollection extends AbstractCollection<V> {
        @Override
        public Iterator<V> iterator() {
            return new Iterator<>() {
                private int idx = findNext(0);

                @Override
                public boolean hasNext() {
                    return idx < capacity;
                }

                @Override
                public V next() {
                    if (idx >= capacity) {
                        throw new NoSuchElementException();
                    }
                    V val = (V) values[idx];
                    idx = findNext(idx + 1);
                    return val;
                }

                private int findNext(int from) {
                    while (from < capacity && !used[from]) {
                        from++;
                    }
                    return from;
                }
            };
        }

        @Override
        public int size() {
            return Long2ObjectOpenHashMap.this.size;
        }
    }
}
