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
 * Open-addressing hash set for primitive int values.
 * Not thread-safe.
 */
public class IntOpenHashSet {

    private static final float LOAD_FACTOR = 0.75f;
    private static final int MIN_CAPACITY = 16;

    private int[] keys;
    private boolean[] used;
    private int size;
    private int capacity;
    private int threshold;

    public IntOpenHashSet() {
        this(MIN_CAPACITY);
    }

    public IntOpenHashSet(int expectedItems) {
        int cap = tableSizeFor(Math.max(MIN_CAPACITY, (int) (expectedItems / LOAD_FACTOR) + 1));
        keys = new int[cap];
        used = new boolean[cap];
        capacity = cap;
        threshold = (int) (cap * LOAD_FACTOR);
    }

    public boolean add(int key) {
        int mask = capacity - 1;
        int idx = hash(key) & mask;
        while (true) {
            if (!used[idx]) {
                if (size >= threshold) {
                    rehash(capacity * 2);
                    return add(key);
                }
                keys[idx] = key;
                used[idx] = true;
                size++;
                return true;
            }
            if (keys[idx] == key) {
                return false;
            }
            idx = (idx + 1) & mask;
        }
    }

    public boolean contains(int key) {
        int mask = capacity - 1;
        int idx = hash(key) & mask;
        while (true) {
            if (!used[idx]) {
                return false;
            }
            if (keys[idx] == key) {
                return true;
            }
            idx = (idx + 1) & mask;
        }
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    private void rehash(int newCapacity) {
        int[] oldKeys = keys;
        boolean[] oldUsed = used;
        int oldCapacity = capacity;

        capacity = newCapacity;
        keys = new int[newCapacity];
        used = new boolean[newCapacity];
        threshold = (int) (newCapacity * LOAD_FACTOR);
        size = 0;

        for (int i = 0; i < oldCapacity; i++) {
            if (oldUsed[i]) {
                add(oldKeys[i]);
            }
        }
    }

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
