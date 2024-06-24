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

import java.util.BitSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;
import org.roaringbitmap.RoaringBitSet;

public class ConcurrentRoaringBitSet extends RoaringBitSet {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock rlock = lock.readLock();
    private final Lock wlock = lock.writeLock();

    public ConcurrentRoaringBitSet() {
        super();
    }

    @Override
    public boolean get(int bitIndex) {
        rlock.lock();
        try {
            return super.get(bitIndex);
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public void set(int bitIndex) {
        wlock.lock();
        try {
            super.set(bitIndex);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void clear(int bitIndex) {
        wlock.lock();
        try {
            super.clear(bitIndex);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void set(int fromIndex, int toIndex) {
        wlock.lock();
        try {
            super.set(fromIndex, toIndex);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void clear(int fromIndex, int toIndex) {
        wlock.lock();
        try {
            super.clear(fromIndex, toIndex);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void clear() {
        wlock.lock();
        try {
            super.clear();
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public int nextSetBit(int fromIndex) {
        rlock.lock();
        try {
            return super.nextSetBit(fromIndex);
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public int nextClearBit(int fromIndex) {
        rlock.lock();
        try {
            return super.nextClearBit(fromIndex);
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public int previousSetBit(int fromIndex) {
        rlock.lock();
        try {
            return super.previousSetBit(fromIndex);
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public int previousClearBit(int fromIndex) {
        rlock.lock();
        try {
            return super.previousClearBit(fromIndex);
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public int length() {
        rlock.lock();
        try {
            return super.length();
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        rlock.lock();
        try {
            return super.isEmpty();
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public int cardinality() {
        rlock.lock();
        try {
            return super.cardinality();
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public int size() {
        rlock.lock();
        try {
            return super.size();
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public byte[] toByteArray() {
        rlock.lock();
        try {
            return super.toByteArray();
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public long[] toLongArray() {
        rlock.lock();
        try {
            return super.toLongArray();
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public void flip(int bitIndex) {
        wlock.lock();
        try {
            super.flip(bitIndex);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void flip(int fromIndex, int toIndex) {
        wlock.lock();
        try {
            super.flip(fromIndex, toIndex);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void set(int bitIndex, boolean value) {
        wlock.lock();
        try {
            super.set(bitIndex, value);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void set(int fromIndex, int toIndex, boolean value) {
        wlock.lock();
        try {
            super.set(fromIndex, toIndex, value);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public BitSet get(int fromIndex, int toIndex) {
        rlock.lock();
        try {
            return super.get(fromIndex, toIndex);
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public boolean intersects(BitSet set) {
        rlock.lock();
        try {
            return super.intersects(set);
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public void and(BitSet set) {
        wlock.lock();
        try {
            super.and(set);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void or(BitSet set) {
        wlock.lock();
        try {
            super.or(set);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void xor(BitSet set) {
        wlock.lock();
        try {
            super.xor(set);
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void andNot(BitSet set) {
        wlock.lock();
        try {
            super.andNot(set);
        } finally {
            wlock.unlock();
        }
    }

    /**
     * Returns the clone of the internal wrapped {@code BitSet}.
     * This won't be a clone of the {@code ConcurrentBitSet} object.
     *
     * @return a clone of the internal wrapped {@code BitSet}
     */
    @Override
    public Object clone() {
        rlock.lock();
        try {
            return super.clone();
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public String toString() {
        rlock.lock();
        try {
            return super.toString();
        } finally {
            rlock.unlock();
        }
    }

    /**
     * This operation is not supported on {@code ConcurrentBitSet}.
     */
    @Override
    public IntStream stream() {
        throw new UnsupportedOperationException("stream is not supported");
    }

    public boolean equals(final Object o) {
        rlock.lock();
        try {
            return super.equals(o);
        } finally {
            rlock.unlock();
        }
    }

    public int hashCode() {
        rlock.lock();
        try {
            return super.hashCode();
        } finally {
            rlock.unlock();
        }
    }
}
