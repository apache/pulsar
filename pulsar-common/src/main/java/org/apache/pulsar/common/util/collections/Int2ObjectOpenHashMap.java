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

/**
 * Open-addressing hash map with primitive int keys and object values.
 * Uses linear probing and fibonacci hashing.
 * Not thread-safe.
 */
@SuppressWarnings("unchecked")
public class Int2ObjectOpenHashMap<V> {

    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;

    private int[] keys;
    private Object[] values;
    private boolean[] used;
    private int size;
    private int capacity;
    private int threshold;

    public Int2ObjectOpenHashMap() {
        this(MIN_CAPACITY);
    }

    public Int2ObjectOpenHashMap(int expectedItems) {
        int cap = tableSizeFor(Math.max(MIN_CAPACITY, (int) (expectedItems / LOAD_FACTOR) + 1));
        keys = new int[cap];
        values = new Object[cap];
        used = new boolean[cap];
        capacity = cap;
        threshold = (int) (cap * LOAD_FACTOR);
    }

    public V get(int key) {
        int idx = indexOf(key);
        return idx >= 0 ? (V) values[idx] : null;
    }

    public V put(int key, V value) {
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

    public V remove(int key) {
        int idx = indexOf(key);
        if (idx < 0) {
            return null;
        }
        V old = (V) values[idx];
        removeAt(idx);
        return old;
    }

    /**
     * Remove the entry only if it maps to the given value (by reference equality).
     *
     * @return true if the entry was removed
     */
    public boolean remove(int key, Object value) {
        int idx = indexOf(key);
        if (idx < 0 || values[idx] != value) {
            return false;
        }
        removeAt(idx);
        return true;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    public void clear() {
        if (size > 0) {
            java.util.Arrays.fill(used, false);
            java.util.Arrays.fill(values, null);
            size = 0;
        }
    }

    private int indexOf(int key) {
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

    private void insertNew(int key, V value) {
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

    private void removeAt(int idx) {
        int mask = capacity - 1;
        values[idx] = null;
        size--;
        int next = (idx + 1) & mask;
        while (used[next]) {
            int naturalSlot = hash(keys[next]) & mask;
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
        int[] oldKeys = keys;
        Object[] oldValues = values;
        boolean[] oldUsed = used;
        int oldCapacity = capacity;

        capacity = newCapacity;
        keys = new int[newCapacity];
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
     * Fibonacci hashing for int keys.
     */
    private static int hash(int key) {
        int h = key * 0x9E3779B9;
        return h ^ (h >>> 16);
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
}
