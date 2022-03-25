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

import java.util.BitSet;
import java.util.concurrent.locks.StampedLock;
import lombok.EqualsAndHashCode;

/**
 * Safe multithreaded version of {@code BitSet}.
 */
@EqualsAndHashCode(callSuper = true)
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
     * @param nbits
     *            the initial size of the bit set
     * @throws NegativeArraySizeException
     *             if the specified initial size is negative
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
        long stamp = rwLock.tryOptimisticRead();
        super.set(bitIndex);
        if (!rwLock.validate(stamp)) {
            stamp = rwLock.readLock();
            try {
                super.set(bitIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
    }

    @Override
    public void set(int fromIndex, int toIndex) {
        long stamp = rwLock.tryOptimisticRead();
        super.set(fromIndex, toIndex);
        if (!rwLock.validate(stamp)) {
            stamp = rwLock.readLock();
            try {
                super.set(fromIndex, toIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
    }

    @Override
    public int nextSetBit(int fromIndex) {
        long stamp = rwLock.tryOptimisticRead();
        int bit = super.nextSetBit(fromIndex);
        if (!rwLock.validate(stamp)) {
            stamp = rwLock.readLock();
            try {
                bit = super.nextSetBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return bit;
    }

    @Override
    public int nextClearBit(int fromIndex) {
        long stamp = rwLock.tryOptimisticRead();
        int bit = super.nextClearBit(fromIndex);
        if (!rwLock.validate(stamp)) {
            stamp = rwLock.readLock();
            try {
                bit = super.nextClearBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return bit;
    }

    @Override
    public int previousSetBit(int fromIndex) {
        long stamp = rwLock.tryOptimisticRead();
        int bit = super.previousSetBit(fromIndex);
        if (!rwLock.validate(stamp)) {
            stamp = rwLock.readLock();
            try {
                bit = super.previousSetBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return bit;
    }

    @Override
    public int previousClearBit(int fromIndex) {
        long stamp = rwLock.tryOptimisticRead();
        int bit = super.previousClearBit(fromIndex);
        if (!rwLock.validate(stamp)) {
            stamp = rwLock.readLock();
            try {
                bit = super.previousClearBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return bit;
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
}
