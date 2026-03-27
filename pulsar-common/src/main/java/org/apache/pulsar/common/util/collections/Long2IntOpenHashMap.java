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

import java.util.function.LongToIntFunction;

/**
 * Open-addressing hash map with primitive long keys and primitive int values.
 * Uses linear probing and fibonacci hashing. Returns 0 for missing keys.
 * Not thread-safe.
 */
public class Long2IntOpenHashMap implements Long2IntMap {

    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;

    private long[] keys;
    private int[] values;
    private boolean[] used;
    private int size;
    private int capacity;
    private int threshold;

    public Long2IntOpenHashMap() {
        this(MIN_CAPACITY);
    }

    public Long2IntOpenHashMap(int expectedItems) {
        int cap = tableSizeFor(Math.max(MIN_CAPACITY, (int) (expectedItems / LOAD_FACTOR) + 1));
        keys = new long[cap];
        values = new int[cap];
        used = new boolean[cap];
        capacity = cap;
        threshold = (int) (cap * LOAD_FACTOR);
    }

    @Override
    public int get(long key) {
        int idx = indexOf(key);
        return idx >= 0 ? values[idx] : 0;
    }

    @Override
    public int put(long key, int value) {
        int idx = indexOf(key);
        if (idx >= 0) {
            int old = values[idx];
            values[idx] = value;
            return old;
        }
        if (size >= threshold) {
            rehash(capacity * 2);
        }
        insertNew(key, value);
        return 0;
    }

    @Override
    public int remove(long key) {
        int idx = indexOf(key);
        if (idx < 0) {
            return 0;
        }
        int old = values[idx];
        removeAt(idx);
        return old;
    }

    @Override
    public int getOrDefault(long key, int defaultValue) {
        int idx = indexOf(key);
        return idx >= 0 ? values[idx] : defaultValue;
    }

    @Override
    public int computeIfAbsent(long key, LongToIntFunction mappingFunction) {
        int idx = indexOf(key);
        if (idx >= 0) {
            return values[idx];
        }
        int value = mappingFunction.applyAsInt(key);
        if (size >= threshold) {
            rehash(capacity * 2);
        }
        insertNew(key, value);
        return value;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public void clear() {
        if (size > 0) {
            java.util.Arrays.fill(used, false);
            size = 0;
        }
    }

    private int indexOf(long key) {
        int mask = capacity - 1;
        int idx = Long2ObjectOpenHashMap.hash(key) & mask;
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

    private void insertNew(long key, int value) {
        int mask = capacity - 1;
        int idx = Long2ObjectOpenHashMap.hash(key) & mask;
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
        size--;
        int next = (idx + 1) & mask;
        while (used[next]) {
            int naturalSlot = Long2ObjectOpenHashMap.hash(keys[next]) & mask;
            if ((next > idx && (naturalSlot <= idx || naturalSlot > next))
                    || (next < idx && (naturalSlot <= idx && naturalSlot > next))) {
                keys[idx] = keys[next];
                values[idx] = values[next];
                idx = next;
            }
            next = (next + 1) & mask;
        }
        used[idx] = false;
    }

    private void rehash(int newCapacity) {
        long[] oldKeys = keys;
        int[] oldValues = values;
        boolean[] oldUsed = used;
        int oldCapacity = capacity;

        capacity = newCapacity;
        keys = new long[newCapacity];
        values = new int[newCapacity];
        used = new boolean[newCapacity];
        threshold = (int) (newCapacity * LOAD_FACTOR);
        size = 0;

        for (int i = 0; i < oldCapacity; i++) {
            if (oldUsed[i]) {
                insertNew(oldKeys[i], oldValues[i]);
            }
        }
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
