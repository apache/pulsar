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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Provides a priority-queue implementation specialized on items composed by 3 longs.
 *
 * <p>This class is not thread safe and the items are stored in direct memory.
 *
 * <h3>Algorithm</h3>
 *
 * <p>This is a <b>binary min-heap</b> stored in a flat array, where each heap node occupies
 * 3 consecutive longs (the tuple). The children of the node at index {@code i} are at
 * {@code 2i + 1} and {@code 2i + 2}; the parent of node {@code i} is at {@code (i - 1) / 2}.
 *
 * <p>Both {@code siftUp} (on insert) and {@code siftDown} (on remove) use the
 * <b>hole-based</b> (also called "bottom-up" or "Floyd's") optimization: instead of swapping
 * the displaced element with its parent/child at each level, the displaced values are held in
 * local variables (registers) and written only once at the final position. This reduces the
 * number of array writes per sift layer from 6 (swap: 3 reads + 3 writes on each side) to 3
 * (one directional write), and avoids re-reading the displaced element from the array on every
 * comparison.
 *
 * <p>Comparison is lexicographic on (n1, n2, n3), using {@code Long.compare} at each level.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Heapsort#Bottom-up_heapsort">Bottom-up heapsort
 *      (Wikipedia)</a>
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

        siftUp(tuplesCount, n1, n2, n3);
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

        if (--tuplesCount == 0) {
            return;
        }

        long lastBase = tuplesCount * ITEMS_COUNT;
        long n1 = array.readLong(lastBase);
        long n2 = array.readLong(lastBase + 1);
        long n3 = array.readLong(lastBase + 2);

        siftDown(0, n1, n2, n3);
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

    private void siftUp(long tupleIdx, long n1, long n2, long n3) {
        long idx = tupleIdx * ITEMS_COUNT;

        while (tupleIdx > 0) {
            long parentIdx = (tupleIdx - 1) >>> 1;
            long parentBase = parentIdx * ITEMS_COUNT;

            long p0 = array.readLong(parentBase);
            long p1 = array.readLong(parentBase + 1);
            long p2 = array.readLong(parentBase + 2);

            if (compareTuple(n1, n2, n3, p0, p1, p2) >= 0) {
                break;
            }

            array.writeLong(idx, p0);
            array.writeLong(idx + 1, p1);
            array.writeLong(idx + 2, p2);

            tupleIdx = parentIdx;
            idx = parentBase;
        }

        array.writeLong(idx, n1);
        array.writeLong(idx + 1, n2);
        array.writeLong(idx + 2, n3);
    }

    private void siftDown(long tupleIdx, long val0, long val1, long val2) {
        long half = tuplesCount >>> 1;

        long idx = tupleIdx * ITEMS_COUNT;

        while (tupleIdx < half) {
            long left = (tupleIdx << 1) + 1;
            long right = left + 1;

            long child = left;
            long childBase = left * ITEMS_COUNT;

            long child0 = array.readLong(childBase);
            long child1 = array.readLong(childBase + 1);
            long child2 = array.readLong(childBase + 2);

            if (right < tuplesCount) {
                long rightBase = right * ITEMS_COUNT;

                long right0 = array.readLong(rightBase);
                long right1 = array.readLong(rightBase + 1);
                long right2 = array.readLong(rightBase + 2);

                if (compareTuple(right0, right1, right2, child0, child1, child2) < 0) {

                    child = right;
                    childBase = rightBase;

                    child0 = right0;
                    child1 = right1;
                    child2 = right2;
                }
            }

            if (compareTuple(val0, val1, val2, child0, child1, child2) <= 0) {
                break;
            }

            array.writeLong(idx, child0);
            array.writeLong(idx + 1, child1);
            array.writeLong(idx + 2, child2);

            tupleIdx = child;
            idx = childBase;
        }

        array.writeLong(idx, val0);
        array.writeLong(idx + 1, val1);
        array.writeLong(idx + 2, val2);
    }

    private static int compareTuple(
            long a0, long a1, long a2,
            long b0, long b1, long b2) {

        int c = Long.compare(a0, b0);
        if (c != 0) {
            return c;
        }

        c = Long.compare(a1, b1);
        if (c != 0) {
            return c;
        }

        return Long.compare(a2, b2);
    }
}
