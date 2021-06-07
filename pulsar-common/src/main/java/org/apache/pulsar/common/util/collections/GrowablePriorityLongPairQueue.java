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
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * An unbounded priority queue based on a min heap where values are composed of pairs of longs.
 *
 * <p>When the capacity is reached, data will be moved to a bigger array.
 *
 * <b>It also act as a set and doesn't store duplicate values if {@link #allowedDuplicate} flag is passed false</b>
 *
 * <p>(long,long)
 */
public class GrowablePriorityLongPairQueue {

    private long[] data;
    private int capacity;
    private static final AtomicIntegerFieldUpdater<GrowablePriorityLongPairQueue> SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(GrowablePriorityLongPairQueue.class, "size");
    private volatile int size = 0;
    private static final long EmptyItem = -1L;

    public GrowablePriorityLongPairQueue() {
        this(64);
    }

    public GrowablePriorityLongPairQueue(int initialCapacity) {
        checkArgument(initialCapacity > 0);
        this.capacity = io.netty.util.internal.MathUtil.findNextPositivePowerOfTwo(initialCapacity);
        data = new long[2 * capacity];
        Arrays.fill(data, 0, data.length, EmptyItem);
    }

    /**
     * Predicate to checks for a key-value pair where both of them have long types.
     */
    public interface LongPairPredicate {
        boolean test(long v1, long v2);
    }

    /**
     * Represents a function that accepts two long arguments.
     */
    public interface LongPairConsumer {
        void accept(long v1, long v2);
    }

    public synchronized void add(long item1, long item2) {

        if (this.size >= this.capacity) {
            expandArray();
        }

        int lastIndex = this.size << 1;
        data[lastIndex] = item1;
        data[lastIndex + 1] = item2;

        int loc = lastIndex;

        // Swap with parent until parent not larger
        while (loc > 0 && compare(loc, parent(loc)) < 0) {
            swap(loc, parent(loc));
            loc = parent(loc);
        }

        SIZE_UPDATER.incrementAndGet(this);

    }

    public synchronized void forEach(LongPairConsumer processor) {
        int index = 0;
        for (int i = 0; i < this.size; i++) {
            processor.accept(data[index], data[index + 1]);
            index = index + 2;
        }
    }

    /**
     * @return a new list of all keys (makes a copy)
     */
    public Set<LongPair> items() {
        Set<LongPair> items = new HashSet<>(this.size);
        forEach((item1, item2) -> items.add(new LongPair(item1, item2)));
        return items;
    }

    /**
     * @return a new list of keys with max provided numberOfItems (makes a copy)
     */
    public Set<LongPair> items(int numberOfItems) {
        Set<LongPair> items = new HashSet<>(this.size);
        forEach((item1, item2) -> {
            if (items.size() < numberOfItems) {
                items.add(new LongPair(item1, item2));
            }
        });

        return items;
    }

    /**
     * Removes all of the elements of this collection that satisfy the given predicate.
     *
     * @param filter
     *            a predicate which returns {@code true} for elements to be removed
     *
     * @return number of removed values
     */
    public synchronized int removeIf(LongPairPredicate filter) {
        int removedValues = 0;
        int index = 0;
        long[] deletedItems = new long[size * 2];
        int deleteItemsIndex = 0;
        // collect eligible items for deletion
        for (int i = 0; i < this.size; i++) {
            if (filter.test(data[index], data[index + 1])) {
                deletedItems[deleteItemsIndex++] = data[index];
                deletedItems[deleteItemsIndex++] = data[index + 1];
                removedValues++;
            }
            index = index + 2;
        }

        // delete collected items
        deleteItemsIndex = 0;
        for (int deleteItem = 0; deleteItem < removedValues; deleteItem++) {
            // delete item from the heap
            index = 0;
            for (int i = 0; i < this.size; i++) {
                if (data[index] == deletedItems[deleteItemsIndex]
                        && data[index + 1] == deletedItems[deleteItemsIndex + 1]) {
                    removeAtWithoutLock(index);
                }
                index = index + 2;
            }
            deleteItemsIndex = deleteItemsIndex + 2;
        }
        return removedValues;
    }

    /**
     * It removes all occurrence of given pair from the queue.
     *
     * @param item1
     * @param item2
     * @return
     */
    public synchronized boolean remove(long item1, long item2) {
        boolean removed = false;

        int index = 0;
        for (int i = 0; i < this.size; i++) {
            if (data[index] == item1 && data[index + 1] == item2) {
                removeAtWithoutLock(index);
                removed = true;
            }
            index = index + 2;
        }

        return removed;
    }

    /**
     * Removes min element from the heap.
     *
     * @return
     */
    public LongPair remove() {
        return removeAt(0);
    }

    private synchronized LongPair removeAt(int index) {
        return removeAtWithoutLock(index);
    }

    /**
     * it is not a thread-safe method and it should be called before acquiring a lock by a caller.
     *
     * @param index
     * @return
     */
    private LongPair removeAtWithoutLock(int index) {
        if (this.size > 0) {
            LongPair item = new LongPair(data[index], data[index + 1]);
            data[index] = EmptyItem;
            data[index + 1] = EmptyItem;
            SIZE_UPDATER.decrementAndGet(this);
            int lastIndex = this.size << 1;
            swap(index, lastIndex);
            minHeapify(index, lastIndex - 2);
            return item;
        } else {
            return null;
        }
    }

    public synchronized LongPair peek() {
        if (this.size > 0) {
            return new LongPair(data[0], data[1]);
        } else {
            return null;
        }
    }

    public boolean isEmpty() {
        return this.size == 0;
    }

    public int capacity() {
        return this.capacity;
    }

    public synchronized void clear() {
        int index = 0;
        for (int i = 0; i < this.size; i++) {
            data[index] = -1;
            data[index + 1] = -1;
            index = index + 2;
        }

        this.size = 0;

    }

    public int size() {
        return this.size;
    }

    public synchronized boolean exists(long item1, long item2) {

        int index = 0;
        for (int i = 0; i < this.size; i++) {
            if (data[index] == item1 && data[index + 1] == item2) {
                return true;
            }
            index = index + 2;
        }
        return false;
    }

    private int compare(int index1, int index2) {
        if (data[index1] != data[index2]) {
            return Long.compare(data[index1], data[index2]);
        } else {
            return Long.compare(data[index1 + 1], data[index2 + 1]);
        }
    }

    private void expandArray() {
        this.capacity = capacity * 2;
        long[] newData = new long[2 * this.capacity];

        int index = 0;
        for (int i = 0; i < this.size; i++) {
            newData[index] = data[index];
            newData[index + 1] = data[index + 1];
            index = index + 2;
        }
        Arrays.fill(newData, index, newData.length, EmptyItem);
        data = newData;
    }

    private void swap(int i, int j) {
        long t = data[i];
        data[i] = data[j];
        data[j] = t;
        t = data[i + 1];
        data[i + 1] = data[j + 1];
        data[j + 1] = t;
    }

    private static int leftChild(int i) {
        return (i << 1) + 2;
    }

    private static int rightChild(int i) {
        return (i << 1) + 4;
    }

    private static int parent(int i) {
        return ((i - 2) >> 1) & ~1;
    }

    private void minHeapify(int index, int lastIndex) {
        int left = leftChild(index);
        int right = rightChild(index);
        int smallest;

        if (left <= lastIndex && compare(left, index) < 0) {
            smallest = left;
        } else {
            smallest = index;
        }
        if (right <= lastIndex && compare(right, smallest) < 0) {
            smallest = right;
        }
        if (smallest != index) {
            swap(index, smallest);
            minHeapify(smallest, lastIndex);
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

        @Override
        public String toString() {
            return "LongPair [first=" + first + ", second=" + second + "]";
        }

    }

}
