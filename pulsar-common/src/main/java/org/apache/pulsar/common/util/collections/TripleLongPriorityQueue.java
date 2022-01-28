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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Provides a priority-queue implementation specialized on items composed by 3 longs.
 *
 * <p>This class is not thread safe and the items are stored in direct memory.
 */
public class TripleLongPriorityQueue implements AutoCloseable {

    private static final int SIZE_OF_LONG = 8;
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    // Each item is composed of 3 longs
    private static final int ITEMS_COUNT = 3;

    private static final int TUPLE_SIZE = ITEMS_COUNT * SIZE_OF_LONG;

    private final ByteBuf buffer;

    private int capacity;
    private int size;

    /**
     * Create a new priority queue with default initial capacity.
     */
    public TripleLongPriorityQueue() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    /**
     * Create a new priority queue with a given initial capacity.
     * @param initialCapacity
     */
    public TripleLongPriorityQueue(int initialCapacity) {
        capacity = initialCapacity;
        buffer = PooledByteBufAllocator.DEFAULT.directBuffer(initialCapacity * ITEMS_COUNT * SIZE_OF_LONG);
        size = 0;
    }

    /**
     * Close the priority queue and free the memory associated.
     */
    @Override
    public void close() {
        buffer.release();
    }

    /**
     * Add a tuple of 3 long items to the priority queue.
     *
     * @param n1
     * @param n2
     * @param n3
     */
    public void add(long n1, long n2, long n3) {
        if (size == capacity) {
            increaseCapacity();
        }

        put(size, n1, n2, n3);
        siftUp(size);
        ++size;
    }

    /**
     * Read the 1st long item in the top tuple in the priority queue.
     *
     * <p>The tuple will not be extracted
     */
    public long peekN1() {
        checkArgument(size != 0);
        return buffer.getLong(0);
    }

    /**
     * Read the 2nd long item in the top tuple in the priority queue.
     *
     * <p>The tuple will not be extracted
     */
    public long peekN2() {
        checkArgument(size != 0);
        return buffer.getLong(0 + 1 * SIZE_OF_LONG);
    }

    /**
     * Read the 3rd long item in the top tuple in the priority queue.
     *
     * <p>The tuple will not be extracted
     */
    public long peekN3() {
        checkArgument(size != 0);
        return buffer.getLong(0 + 2 * SIZE_OF_LONG);
    }

    /**
     * Removes the first item from the queue.
     */
    public void pop() {
        checkArgument(size != 0);
        swap(0, size - 1);
        size--;
        siftDown(0);
    }

    /**
     * Returns whether the priority queue is empty.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns the number of tuples in the priority queue.
     */
    public int size() {
        return size;
    }

    /**
     * Clear all items.
     */
    public void clear() {
        this.buffer.clear();
        this.size = 0;
    }

    private void increaseCapacity() {
        // For bigger sizes, increase by 50%
        this.capacity += (capacity <= 256 ? capacity : capacity / 2);
        buffer.capacity(this.capacity * TUPLE_SIZE);
    }

    private void siftUp(int idx) {
        while (idx > 0) {
            int parentIdx = (idx - 1) / 2;
            if (compare(idx, parentIdx) >= 0) {
                break;
            }

            swap(idx, parentIdx);
            idx = parentIdx;
        }
    }

    private void siftDown(int idx) {
        int half = size / 2;
        while (idx < half) {
            int left = 2 * idx + 1;
            int right = 2 * idx + 2;

            int swapIdx = idx;

            if (compare(idx, left) > 0) {
                swapIdx = left;
            }

            if (right < size && compare(swapIdx, right) > 0) {
                swapIdx = right;
            }

            if (swapIdx == idx) {
                return;
            }

            swap(idx, swapIdx);
            idx = swapIdx;
        }
    }

    private void put(int idx, long n1, long n2, long n3) {
        int i = idx * TUPLE_SIZE;
        buffer.setLong(i, n1);
        buffer.setLong(i + 1 * SIZE_OF_LONG, n2);
        buffer.setLong(i + 2 * SIZE_OF_LONG, n3);
    }

    private int compare(int idx1, int idx2) {
        int i1 = idx1 * TUPLE_SIZE;
        int i2 = idx2 * TUPLE_SIZE;

        int c1 = Long.compare(buffer.getLong(i1), buffer.getLong(i2));
        if (c1 != 0) {
            return c1;
        }

        int c2 = Long.compare(buffer.getLong(i1 + SIZE_OF_LONG), buffer.getLong(i2 + SIZE_OF_LONG));
        if (c2 != 0) {
            return c2;
        }

        return Long.compare(buffer.getLong(i1 + 2 * SIZE_OF_LONG), buffer.getLong(i2 + 2 * SIZE_OF_LONG));
    }

    private void swap(int idx1, int idx2) {
        int i1 = idx1 * TUPLE_SIZE;
        int i2 = idx2 * TUPLE_SIZE;

        long tmp1 = buffer.getLong(i1);
        long tmp2 = buffer.getLong(i1 + 1 * SIZE_OF_LONG);
        long tmp3 = buffer.getLong(i1 + 2 * SIZE_OF_LONG);

        buffer.setLong(i1, buffer.getLong(i2));
        buffer.setLong(i1 + 1 * SIZE_OF_LONG, buffer.getLong(i2 + 1 * SIZE_OF_LONG));
        buffer.setLong(i1 + 2 * SIZE_OF_LONG, buffer.getLong(i2 + 2 * SIZE_OF_LONG));

        buffer.setLong(i2, tmp1);
        buffer.setLong(i2 + 1 * SIZE_OF_LONG, tmp2);
        buffer.setLong(i2 + 2 * SIZE_OF_LONG, tmp3);
    }
}
