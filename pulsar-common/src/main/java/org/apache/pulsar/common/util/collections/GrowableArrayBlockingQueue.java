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

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * This implements a {@link BlockingQueue} backed by an array with no fixed capacity.
 *
 * <p>When the capacity is reached, data will be moved to a bigger array.
 */
public class GrowableArrayBlockingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {

    private final ReentrantLock headLock = new ReentrantLock();
    private final PaddedInt headIndex = new PaddedInt();
    private final PaddedInt tailIndex = new PaddedInt();
    private final ReentrantLock tailLock = new ReentrantLock();
    private final Condition isNotEmpty = headLock.newCondition();

    private T[] data;

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<GrowableArrayBlockingQueue> SIZE_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(GrowableArrayBlockingQueue.class, "size");
    private volatile int size = 0;

    private volatile boolean terminated = false;

    private volatile Consumer<T> itemAfterTerminatedHandler;

    public GrowableArrayBlockingQueue() {
        this(64);
    }

    @SuppressWarnings("unchecked")
    public GrowableArrayBlockingQueue(int initialCapacity) {
        headIndex.value = 0;
        tailIndex.value = 0;

        int capacity = io.netty.util.internal.MathUtil.findNextPositivePowerOfTwo(initialCapacity);
        data = (T[]) new Object[capacity];
    }

    @Override
    public T remove() {
        T item = poll();
        if (item == null) {
            throw new NoSuchElementException();
        }

        return item;
    }

    @Override
    public T poll() {
        headLock.lock();
        try {
            if (SIZE_UPDATER.get(this) > 0) {
                T item = data[headIndex.value];
                data[headIndex.value] = null;
                headIndex.value = (headIndex.value + 1) & (data.length - 1);
                SIZE_UPDATER.decrementAndGet(this);
                return item;
            } else {
                return null;
            }
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public T element() {
        T item = peek();
        if (item == null) {
            throw new NoSuchElementException();
        }

        return item;
    }

    @Override
    public T peek() {
        headLock.lock();
        try {
            if (SIZE_UPDATER.get(this) > 0) {
                return data[headIndex.value];
            } else {
                return null;
            }
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public boolean offer(T e) {
        // Queue is unbounded and it will never reject new items
        put(e);
        return true;
    }

    @Override
    public void put(T e) {
        tailLock.lock();

        boolean wasEmpty = false;

        try {
            if (terminated){
                if (itemAfterTerminatedHandler != null) {
                    itemAfterTerminatedHandler.accept(e);
                }
                return;
            }

            if (SIZE_UPDATER.get(this) == data.length) {
                expandArray();
            }

            data[tailIndex.value] = e;
            tailIndex.value = (tailIndex.value + 1) & (data.length - 1);
            if (SIZE_UPDATER.getAndIncrement(this) == 0) {
                wasEmpty = true;
            }
        } finally {
            tailLock.unlock();
        }

        if (wasEmpty) {
            headLock.lock();
            try {
                isNotEmpty.signal();
            } finally {
                headLock.unlock();
            }
        }
    }

    @Override
    public boolean add(T e) {
        put(e);
        return true;
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) {
        // Queue is unbounded and it will never reject new items
        put(e);
        return true;
    }

    @Override
    public T take() throws InterruptedException {
        headLock.lockInterruptibly();

        try {
            while (SIZE_UPDATER.get(this) == 0) {
                isNotEmpty.await();
            }

            T item = data[headIndex.value];
            data[headIndex.value] = null;
            headIndex.value = (headIndex.value + 1) & (data.length - 1);
            if (SIZE_UPDATER.decrementAndGet(this) > 0) {
                // There are still entries to consume
                isNotEmpty.signal();
            }
            return item;
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        headLock.lockInterruptibly();

        try {
            long timeoutNanos = unit.toNanos(timeout);
            while (SIZE_UPDATER.get(this) == 0) {
                if (timeoutNanos <= 0) {
                    return null;
                }

                timeoutNanos = isNotEmpty.awaitNanos(timeoutNanos);
            }

            T item = data[headIndex.value];
            data[headIndex.value] = null;
            headIndex.value = (headIndex.value + 1) & (data.length - 1);
            if (SIZE_UPDATER.decrementAndGet(this) > 0) {
                // There are still entries to consume
                isNotEmpty.signal();
            }
            return item;
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        headLock.lock();

        try {
            int drainedItems = 0;
            int size = SIZE_UPDATER.get(this);

            while (size > 0 && drainedItems < maxElements) {
                T item = data[headIndex.value];
                data[headIndex.value] = null;
                c.add(item);

                headIndex.value = (headIndex.value + 1) & (data.length - 1);
                --size;
                ++drainedItems;
            }

            if (SIZE_UPDATER.addAndGet(this, -drainedItems) > 0) {
                // There are still entries to consume
                isNotEmpty.signal();
            }

            return drainedItems;
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public void clear() {
        headLock.lock();

        try {
            int size = SIZE_UPDATER.get(this);

            for (int i = 0; i < size; i++) {
                data[headIndex.value] = null;
                headIndex.value = (headIndex.value + 1) & (data.length - 1);
            }

            if (SIZE_UPDATER.addAndGet(this, -size) > 0) {
                // There are still entries to consume
                isNotEmpty.signal();
            }
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public boolean remove(Object o) {
        tailLock.lock();
        headLock.lock();

        try {
            int index = this.headIndex.value;
            int size = this.size;

            for (int i = 0; i < size; i++) {
                T item = data[index];

                if (Objects.equals(item, o)) {
                    remove(index);
                    return true;
                }

                index = (index + 1) & (data.length - 1);
            }
        } finally {
            headLock.unlock();
            tailLock.unlock();
        }

        return false;
    }

    private void remove(int index) {
        int tailIndex = this.tailIndex.value;

        if (index < tailIndex) {
            System.arraycopy(data, index + 1, data, index, tailIndex - index - 1);
            this.tailIndex.value--;
        } else {
            System.arraycopy(data, index + 1, data, index, data.length - index - 1);
            data[data.length - 1] = data[0];
            if (tailIndex > 0) {
                System.arraycopy(data, 1, data, 0, tailIndex);
                this.tailIndex.value--;
            } else {
                this.tailIndex.value = data.length - 1;
            }
        }

        if (tailIndex > 0) {
            data[tailIndex - 1] = null;
        } else {
            data[data.length - 1] = null;
        }

        SIZE_UPDATER.decrementAndGet(this);
    }

    @Override
    public int size() {
        return SIZE_UPDATER.get(this);
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    public List<T> toList() {
        List<T> list = new ArrayList<>(size());
        forEach(list::add);
        return list;
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        tailLock.lock();
        headLock.lock();

        try {
            int headIndex = this.headIndex.value;
            int size = this.size;

            for (int i = 0; i < size; i++) {
                T item = data[headIndex];

                action.accept(item);

                headIndex = (headIndex + 1) & (data.length - 1);
            }

        } finally {
            headLock.unlock();
            tailLock.unlock();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        tailLock.lock();
        headLock.lock();

        try {
            int headIndex = this.headIndex.value;
            int size = SIZE_UPDATER.get(this);

            sb.append('[');

            for (int i = 0; i < size; i++) {
                T item = data[headIndex];
                if (i > 0) {
                    sb.append(", ");
                }

                sb.append(item);

                headIndex = (headIndex + 1) & (data.length - 1);
            }

            sb.append(']');
        } finally {
            headLock.unlock();
            tailLock.unlock();
        }
        return sb.toString();
    }

    /**
     * Make the queue not accept new items. if there are still new data trying to enter the queue, it will be handed
     * by {@param itemAfterTerminatedHandler}.
     */
    public void terminate(@Nullable Consumer<T> itemAfterTerminatedHandler) {
        // After wait for the in-flight item enqueue, it means the operation of terminate is finished.
        tailLock.lock();
        try {
            terminated = true;
            if (itemAfterTerminatedHandler != null) {
                this.itemAfterTerminatedHandler = itemAfterTerminatedHandler;
            }
        } finally {
            tailLock.unlock();
        }
    }

    public boolean isTerminated() {
        return terminated;
    }

    @SuppressWarnings("unchecked")
    private void expandArray() {
        // We already hold the tailLock
        headLock.lock();

        try {
            int size = SIZE_UPDATER.get(this);
            int newCapacity = data.length * 2;
            T[] newData = (T[]) new Object[newCapacity];

            int oldHeadIndex = headIndex.value;
            int newTailIndex = 0;

            for (int i = 0; i < size; i++) {
                newData[newTailIndex++] = data[oldHeadIndex];
                oldHeadIndex = (oldHeadIndex + 1) & (data.length - 1);
            }

            data = newData;
            headIndex.value = 0;
            tailIndex.value = size;
        } finally {
            headLock.unlock();
        }
    }

    static final class PaddedInt {
        private int value;

        // Padding to avoid false sharing
        public volatile int pi1 = 1;
        public volatile long p1 = 1L, p2 = 2L, p3 = 3L, p4 = 4L, p5 = 5L, p6 = 6L;

        public long exposeToAvoidOptimization() {
            return pi1 + p1 + p2 + p3 + p4 + p5 + p6;
        }
    }
}
