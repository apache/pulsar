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

import java.util.concurrent.locks.StampedLock;
import java.util.stream.IntStream;

public class ConcurrentRoaringBitSet extends RoaringBitSet {
    private final StampedLock rwLock = new StampedLock();

    public ConcurrentRoaringBitSet() {
        super();
    }

    @Override
    public boolean get(int bitIndex) {
        long stamp = rwLock.tryOptimisticRead();
        boolean isSet = super.get(bitIndex);
        if (!rwLock.validate(stamp)) {
            stamp = rwLock.readLock();
            try {
                isSet = super.get(bitIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return isSet;
    }

    @Override
    public void set(int bitIndex) {
        long stamp = rwLock.writeLock();
        try {
            super.set(bitIndex);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void clear(int bitIndex) {
        long stamp = rwLock.writeLock();
        try {
            super.clear(bitIndex);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void set(int fromIndex, int toIndex) {
        long stamp = rwLock.writeLock();
        try {
            super.set(fromIndex, toIndex);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void clear(int fromIndex, int toIndex) {
        long stamp = rwLock.writeLock();
        try {
            super.clear(fromIndex, toIndex);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void clear() {
        long stamp = rwLock.writeLock();
        try {
            super.clear();
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public int nextSetBit(int fromIndex) {
        long stamp = rwLock.tryOptimisticRead();
        int nextSetBit = super.nextSetBit(fromIndex);
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                nextSetBit = super.nextSetBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return nextSetBit;
    }

    @Override
    public int nextClearBit(int fromIndex) {
        long stamp = rwLock.tryOptimisticRead();
        int nextClearBit = super.nextClearBit(fromIndex);
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                nextClearBit = super.nextClearBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return nextClearBit;
    }

    @Override
    public int previousSetBit(int fromIndex) {
        long stamp = rwLock.tryOptimisticRead();
        int previousSetBit = super.previousSetBit(fromIndex);
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                previousSetBit = super.previousSetBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return previousSetBit;
    }

    @Override
    public int previousClearBit(int fromIndex) {
        long stamp = rwLock.tryOptimisticRead();
        int previousClearBit = super.previousClearBit(fromIndex);
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                previousClearBit = super.previousClearBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return previousClearBit;
    }

    @Override
    public int length() {
        long stamp = rwLock.tryOptimisticRead();
        int length = super.length();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                length = super.length();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return length;
    }

    @Override
    public boolean isEmpty() {
        long stamp = rwLock.tryOptimisticRead();
        boolean isEmpty = super.isEmpty();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                isEmpty = super.isEmpty();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return isEmpty;
    }

    @Override
    public int cardinality() {
        long stamp = rwLock.tryOptimisticRead();
        int cardinality = super.cardinality();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                cardinality = super.cardinality();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return cardinality;
    }

    @Override
    public int size() {
        long stamp = rwLock.tryOptimisticRead();
        int size = super.size();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                size = super.size();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return size;
    }

    @Override
    public byte[] toByteArray() {
        long stamp = rwLock.tryOptimisticRead();
        byte[] byteArray = super.toByteArray();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                byteArray = super.toByteArray();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return byteArray;
    }

    @Override
    public long[] toLongArray() {
        long stamp = rwLock.tryOptimisticRead();
        long[] longArray = super.toLongArray();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                longArray = super.toLongArray();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return longArray;
    }

    @Override
    public void flip(int bitIndex) {
        long stamp = rwLock.writeLock();
        try {
            super.flip(bitIndex);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void flip(int fromIndex, int toIndex) {
        long stamp = rwLock.writeLock();
        try {
            super.flip(fromIndex, toIndex);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void set(int bitIndex, boolean value) {
        long stamp = rwLock.writeLock();
        try {
            super.set(bitIndex, value);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void set(int fromIndex, int toIndex, boolean value) {
        long stamp = rwLock.writeLock();
        try {
            super.set(fromIndex, toIndex, value);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public RoaringBitSet get(int fromIndex, int toIndex) {
        long stamp = rwLock.tryOptimisticRead();
        RoaringBitSet bitSet = super.get(fromIndex, toIndex);
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                bitSet = super.get(fromIndex, toIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return bitSet;
    }

    /**
     * Thread-safe version of {@code length()}.
     * StampedLock is not reentrant and that's why the length() method is not overridden. Overriding length() method
     * would require to use a reentrant lock which would be less performant.
     *
     * @return length of the bit set
     */
    public int safeLength() {
        long stamp = rwLock.tryOptimisticRead();
        int length = super.length();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                length = super.length();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return length;
    }

    @Override
    public boolean intersects(RoaringBitSet set) {
        long stamp = rwLock.writeLock();
        try {
            return super.intersects(set);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void and(RoaringBitSet set) {
        long stamp = rwLock.writeLock();
        try {
            super.and(set);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void or(RoaringBitSet set) {
        long stamp = rwLock.writeLock();
        try {
            super.or(set);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void xor(RoaringBitSet set) {
        long stamp = rwLock.writeLock();
        try {
            super.xor(set);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void andNot(RoaringBitSet set) {
        long stamp = rwLock.writeLock();
        try {
            super.andNot(set);
        } finally {
            rwLock.unlockWrite(stamp);
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
        long stamp = rwLock.tryOptimisticRead();
        RoaringBitSet clone = (RoaringBitSet) super.clone();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                clone = (RoaringBitSet) super.clone();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return clone;
    }

    @Override
    public String toString() {
        long stamp = rwLock.tryOptimisticRead();
        String str = super.toString();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                str = super.toString();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return str;
    }

    /**
     * This operation is not supported on {@code ConcurrentBitSet}.
     */
    @Override
    public IntStream stream() {
        throw new UnsupportedOperationException("stream is not supported");
    }

    public boolean equals(final Object o) {
        long stamp = rwLock.tryOptimisticRead();
        boolean isEqual = super.equals(o);
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                isEqual = super.equals(o);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return isEqual;
    }

    public int hashCode() {
        long stamp = rwLock.tryOptimisticRead();
        int hashCode = super.hashCode();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                hashCode = super.hashCode();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return hashCode;
    }
}
