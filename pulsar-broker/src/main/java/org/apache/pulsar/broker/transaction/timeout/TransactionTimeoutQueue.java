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
package org.apache.pulsar.broker.transaction.timeout;

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import java.util.Arrays;
import java.util.NoSuchElementException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A primitive min-heap indexed by transaction sequence ID, allowing completed transactions to be removed.
 * Entries are ordered by deadline, then sequence ID. The owning tracker provides synchronization.
 */
@NotThreadSafe
final class TransactionTimeoutQueue {

    private static final int INITIAL_CAPACITY = 16;

    private long[] sequenceIds = new long[INITIAL_CAPACITY];
    private long[] timeouts = new long[INITIAL_CAPACITY];
    private Long2IntMap indexes = newIndexMap();
    private int size;

    void add(long sequenceId, long timeout) {
        int index = indexes.get(sequenceId);
        if (index >= 0) {
            long previousTimeout = timeouts[index];
            if (timeout < previousTimeout) {
                siftUp(index, sequenceId, timeout);
            } else if (timeout > previousTimeout) {
                siftDown(index, sequenceId, timeout);
            }
            return;
        }

        if (size == sequenceIds.length) {
            resize(Math.toIntExact((long) size * 2));
        }
        siftUp(size, sequenceId, timeout);
        size++;
    }

    boolean remove(long sequenceId) {
        int index = indexes.remove(sequenceId);
        if (index < 0) {
            return false;
        }

        int lastIndex = --size;
        if (index != lastIndex) {
            long lastSequenceId = sequenceIds[lastIndex];
            long lastTimeout = timeouts[lastIndex];
            int parentIndex = (index - 1) >>> 1;
            if (index > 0 && compare(lastTimeout, lastSequenceId,
                    timeouts[parentIndex], sequenceIds[parentIndex]) < 0) {
                siftUp(index, lastSequenceId, lastTimeout);
            } else {
                siftDown(index, lastSequenceId, lastTimeout);
            }
        }

        if (sequenceIds.length > INITIAL_CAPACITY && size <= sequenceIds.length / 4) {
            resize(sequenceIds.length / 2);
        }
        return true;
    }

    long peekSequenceId() {
        checkNotEmpty();
        return sequenceIds[0];
    }

    long peekTimeout() {
        checkNotEmpty();
        return timeouts[0];
    }

    void pop() {
        remove(peekSequenceId());
    }

    int size() {
        return size;
    }

    boolean isEmpty() {
        return size == 0;
    }

    void clear() {
        sequenceIds = new long[INITIAL_CAPACITY];
        timeouts = new long[INITIAL_CAPACITY];
        indexes = newIndexMap();
        size = 0;
    }

    @VisibleForTesting
    int capacity() {
        return sequenceIds.length;
    }

    private static Long2IntMap newIndexMap() {
        Long2IntMap map = new Long2IntOpenHashMap(INITIAL_CAPACITY);
        map.defaultReturnValue(-1);
        return map;
    }

    private void resize(int capacity) {
        sequenceIds = Arrays.copyOf(sequenceIds, capacity);
        timeouts = Arrays.copyOf(timeouts, capacity);
    }

    private void siftUp(int index, long sequenceId, long timeout) {
        while (index > 0) {
            int parentIndex = (index - 1) >>> 1;
            if (compare(timeout, sequenceId, timeouts[parentIndex], sequenceIds[parentIndex]) >= 0) {
                break;
            }
            set(index, sequenceIds[parentIndex], timeouts[parentIndex]);
            index = parentIndex;
        }
        set(index, sequenceId, timeout);
    }

    private void siftDown(int index, long sequenceId, long timeout) {
        int half = size >>> 1;
        while (index < half) {
            int childIndex = (index << 1) + 1;
            int rightIndex = childIndex + 1;
            if (rightIndex < size && compare(timeouts[rightIndex], sequenceIds[rightIndex],
                    timeouts[childIndex], sequenceIds[childIndex]) < 0) {
                childIndex = rightIndex;
            }
            if (compare(timeout, sequenceId, timeouts[childIndex], sequenceIds[childIndex]) <= 0) {
                break;
            }
            set(index, sequenceIds[childIndex], timeouts[childIndex]);
            index = childIndex;
        }
        set(index, sequenceId, timeout);
    }

    private void set(int index, long sequenceId, long timeout) {
        sequenceIds[index] = sequenceId;
        timeouts[index] = timeout;
        indexes.put(sequenceId, index);
    }

    private void checkNotEmpty() {
        if (isEmpty()) {
            throw new NoSuchElementException("The transaction timeout queue is empty");
        }
    }

    private static int compare(long timeout, long sequenceId, long otherTimeout, long otherSequenceId) {
        int result = Long.compare(timeout, otherTimeout);
        return result != 0 ? result : Long.compare(sequenceId, otherSequenceId);
    }
}
