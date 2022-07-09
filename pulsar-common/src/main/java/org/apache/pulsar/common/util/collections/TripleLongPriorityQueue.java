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

/**
 * Provides a priority-queue implementation specialized on items composed by 3 longs.
 *
 * <p>This class is not thread safe and the items are stored in direct memory.
 */
public class TripleLongPriorityQueue implements AutoCloseable {
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final float DEFAULT_SHRINK_FACTOR = 0.5f;

    // Each item is composed of 3 longs
    private static final int ITEMS_COUNT = 3;

    /**
     * Reserve 10% of the capacity when shrinking to avoid frequent expansion and shrinkage.
     */
    private static final float RESERVATION_FACTOR = 0.9f;

    private final SegmentedLongArray array;

    // Count of how many (long,long,long) tuples are currently inserted
    private long tuplesCount;

    /**
     * When size < capacity * shrinkFactor, may trigger shrinking.
     */
    private final float shrinkFactor;

    private long shrinkThreshold;

    /**
     * Create a new priority queue with default initial capacity.
     */
    public TripleLongPriorityQueue() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public TripleLongPriorityQueue(long initialCapacity, float shrinkFactor) {
        checkArgument(initialCapacity > 0);
        checkArgument(shrinkFactor > 0);
        this.array = new SegmentedLongArray(initialCapacity * ITEMS_COUNT);
        this.tuplesCount = 0;
        this.shrinkThreshold = (long) (initialCapacity * shrinkFactor);
        this.shrinkFactor = shrinkFactor;
    }

    /**
     * Create a new priority queue with a given initial capacity.
     * @param initialCapacity
     */
    public TripleLongPriorityQueue(int initialCapacity) {
        this(initialCapacity, DEFAULT_SHRINK_FACTOR);
    }

    /**
     * Close the priority queue and free the memory associated.
     */
    @Override
    public void close() {
        array.close();
    }

    /**
     * Add a tuple of 3 long items to the priority queue.
     *
     * @param n1
     * @param n2
     * @param n3
     */
    public void add(long n1, long n2, long n3) {
        long arrayIdx = tuplesCount * ITEMS_COUNT;
        if ((arrayIdx + 2) >= array.getCapacity()) {
            array.increaseCapacity();
        }

        put(tuplesCount, n1, n2, n3);
        siftUp(tuplesCount);
        ++tuplesCount;
    }

    /**
     * Read the 1st long item in the top tuple in the priority queue.
     *
     * <p>The tuple will not be extracted
     */
    public long peekN1() {
        checkArgument(tuplesCount != 0);
        return array.readLong(0);
    }

    /**
     * Read the 2nd long item in the top tuple in the priority queue.
     *
     * <p>The tuple will not be extracted
     */
    public long peekN2() {
        checkArgument(tuplesCount != 0);
        return array.readLong(1);
    }

    /**
     * Read the 3rd long item in the top tuple in the priority queue.
     *
     * <p>The tuple will not be extracted
     */
    public long peekN3() {
        checkArgument(tuplesCount != 0);
        return array.readLong(2);
    }

    /**
     * Removes the first item from the queue.
     */
    public void pop() {
        checkArgument(tuplesCount != 0);
        swap(0, tuplesCount - 1);
        tuplesCount--;
        siftDown(0);
        shrinkCapacity();
    }

    /**
     * Returns whether the priority queue is empty.
     */
    public boolean isEmpty() {
        return tuplesCount == 0;
    }

    /**
     * Returns the number of tuples in the priority queue.
     */
    public long size() {
        return tuplesCount;
    }

    /**
     * The amount of memory used to back the priority queue.
     */
    public long bytesCapacity() {
        return array.bytesCapacity();
    }

    /**
     * Clear all items.
     */
    public void clear() {
        this.tuplesCount = 0;
        shrinkCapacity();
    }

    private void shrinkCapacity() {
        if (tuplesCount <= shrinkThreshold && array.getCapacity() > array.getInitialCapacity()) {
            long sizeToShrink = (long) (array.getCapacity() * shrinkFactor * RESERVATION_FACTOR);
            if (sizeToShrink == 0) {
                return;
            }

            long newCapacity;
            if (array.getCapacity() - sizeToShrink <= array.getInitialCapacity()) {
                newCapacity = array.getInitialCapacity();
            } else {
                newCapacity = array.getCapacity() - sizeToShrink;
            }

            array.shrink(newCapacity);
            this.shrinkThreshold = (long) (array.getCapacity() / (double) ITEMS_COUNT * shrinkFactor);
        }
    }

    private void siftUp(long tupleIdx) {
        while (tupleIdx > 0) {
            long parentIdx = (tupleIdx - 1) / 2;
            if (compare(tupleIdx, parentIdx) >= 0) {
                break;
            }

            swap(tupleIdx, parentIdx);
            tupleIdx = parentIdx;
        }
    }

    private void siftDown(long tupleIdx) {
        long half = tuplesCount / 2;
        while (tupleIdx < half) {
            long left = 2 * tupleIdx + 1;
            long right = 2 * tupleIdx + 2;

            long swapIdx = tupleIdx;

            if (compare(tupleIdx, left) > 0) {
                swapIdx = left;
            }

            if (right < tuplesCount && compare(swapIdx, right) > 0) {
                swapIdx = right;
            }

            if (swapIdx == tupleIdx) {
                return;
            }

            swap(tupleIdx, swapIdx);
            tupleIdx = swapIdx;
        }
    }

    private void put(long tupleIdx, long n1, long n2, long n3) {
        long idx = tupleIdx * ITEMS_COUNT;
        array.writeLong(idx, n1);
        array.writeLong(idx + 1, n2);
        array.writeLong(idx + 2, n3);
    }

    private int compare(long tupleIdx1, long tupleIdx2) {
        long idx1 = tupleIdx1 * ITEMS_COUNT;
        long idx2 = tupleIdx2 * ITEMS_COUNT;

        int c1 = Long.compare(array.readLong(idx1), array.readLong(idx2));
        if (c1 != 0) {
            return c1;
        }

        int c2 = Long.compare(array.readLong(idx1 + 1), array.readLong(idx2 + 1));
        if (c2 != 0) {
            return c2;
        }

        return Long.compare(array.readLong(idx1 + 2), array.readLong(idx2 + 2));
    }

    private void swap(long tupleIdx1, long tupleIdx2) {
        long idx1 = tupleIdx1 * ITEMS_COUNT;
        long idx2 = tupleIdx2 * ITEMS_COUNT;

        long tmp1 = array.readLong(idx1);
        long tmp2 = array.readLong(idx1 + 1);
        long tmp3 = array.readLong(idx1 + 2);

        array.writeLong(idx1, array.readLong(idx2));
        array.writeLong(idx1 + 1, array.readLong(idx2 + 1));
        array.writeLong(idx1 + 2, array.readLong(idx2 + 2));

        array.writeLong(idx2, tmp1);
        array.writeLong(idx2 + 1, tmp2);
        array.writeLong(idx2 + 2, tmp3);
    }
}
