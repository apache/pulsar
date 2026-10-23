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
import java.util.concurrent.locks.StampedLock;
import java.util.stream.IntStream;

/**
 * A {@code BitSet} that is protected by a {@code StampedLock} to provide thread-safe access.
 * The {@link #length()} method is not thread safe and is not overridden because StampedLock is not reentrant.
 * Use the {@link #safeLength()} method to get the length of the bit set in a thread-safe manner.
 */
public class ConcurrentBitSet extends BitSet {

    private static final long serialVersionUID = 1L;
    private final StampedLock rwLock = new StampedLock();

    public ConcurrentBitSet() {
        super();
    }

    /**
     * Creates a bit set whose initial size is large enough to explicitly represent bits with indices in the range
     * {@code 0} through {@code nbits-1}. All bits are initially {@code false}.
     *
     * @param nbits the initial size of the bit set
     * @throws NegativeArraySizeException if the specified initial size is negative
     */
    public ConcurrentBitSet(int nbits) {
        super(nbits);
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
    public BitSet get(int fromIndex, int toIndex) {
        long stamp = rwLock.tryOptimisticRead();
        BitSet bitSet = super.get(fromIndex, toIndex);
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
    public boolean intersects(BitSet set) {
        long stamp = rwLock.writeLock();
        try {
            return super.intersects(set);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void and(BitSet set) {
        long stamp = rwLock.writeLock();
        try {
            super.and(set);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void or(BitSet set) {
        long stamp = rwLock.writeLock();
        try {
            super.or(set);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void xor(BitSet set) {
        long stamp = rwLock.writeLock();
        try {
            super.xor(set);
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void andNot(BitSet set) {
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
        BitSet clonedBitSet = (BitSet) super.clone();
        if (!rwLock.validate(stamp)) {
            // Fallback to read lock
            stamp = rwLock.readLock();
            try {
                clonedBitSet = (BitSet) super.clone();
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return clonedBitSet;
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
        if (o == this) {
            return true;
        }
        if (!(o instanceof ConcurrentBitSet)) {
            return false;
        }
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
