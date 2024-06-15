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

import org.roaringbitmap.Container;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RelativeRangeConsumer;
import org.roaringbitmap.RoaringBitmap;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.StampedLock;

public class ConcurrentRoaringBitmap extends RoaringBitmap {

    private final StampedLock lock = new StampedLock();

    @Override
    public void add(int x) {
        long stamp = lock.writeLock();
        try {
            super.add(x);
        } finally {
            lock.unlockWrite(stamp);
        }
    }


    @Override
    public void addN(int[] dat, int offset, int n) {
        long stamp = lock.writeLock();
        try {
            super.addN(dat, offset, n);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void add(long rangeStart, long rangeEnd) {
        long stamp = lock.writeLock();
        try {
            super.add(rangeStart, rangeEnd);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void add(int rangeStart, int rangeEnd) {
        long stamp = lock.writeLock();
        try {
            super.add(rangeStart, rangeEnd);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean intersects(long minimum, long supremum) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            boolean intersects = super.intersects(minimum, supremum);
            if (lock.validate(stamp)) {
                return intersects;
            }
        }
        stamp = lock.readLock();
        try {
            return super.intersects(minimum, supremum);
        } finally {
            lock.unlockRead(stamp);
        }
    }


    public void and(final RoaringBitmap x2) {
        long stamp = lock.writeLock();
        try {
            super.and(x2);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void andNot(RoaringBitmap x2) {
        long stamp = lock.writeLock();
        try {
            super.andNot(x2);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void orNot(RoaringBitmap other, long rangeEnd) {
        long stamp = lock.writeLock();
        try {
            super.orNot(other, rangeEnd);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean checkedAdd(int x) {
        long stamp = lock.writeLock();
        try {
            return super.checkedAdd(x);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean checkedRemove(int x) {
        long stamp = lock.writeLock();
        try {
            return super.checkedRemove(x);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void clear() {
        long stamp = lock.writeLock();
        try {
            super.clear();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public RoaringBitmap clone() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            RoaringBitmap clone = super.clone();
            if (lock.validate(stamp)) {
                return clone;
            }
        }
        stamp = lock.readLock();
        try {
            return super.clone();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public boolean contains(int x) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            boolean contains = super.contains(x);
            if (lock.validate(stamp)) {
                return contains;
            }
        }
        stamp = lock.readLock();
        try {
            return super.contains(x);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public boolean contains(long minimum, long supremum) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            boolean contains = super.contains(minimum, supremum);
            if (lock.validate(stamp)) {
                return contains;
            }
        }
        stamp = lock.readLock();
        try {
            return super.contains(minimum, supremum);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void deserialize(DataInput in, byte[] buffer) throws IOException {
        long stamp = lock.writeLock();
        try {
            super.deserialize(in, buffer);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void deserialize(DataInput in) throws IOException {
        long stamp = lock.writeLock();
        try {
            super.deserialize(in);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void deserialize(ByteBuffer bbf) throws IOException {
        long stamp = lock.writeLock();
        try {
            super.deserialize(bbf);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean equals(Object o) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            boolean equals = super.equals(o);
            if (lock.validate(stamp)) {
                return equals;
            }
        }
        stamp = lock.readLock();
        try {
            return super.equals(o);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public boolean isHammingSimilar(RoaringBitmap other, int tolerance) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            boolean isHammingSimilar = super.isHammingSimilar(other, tolerance);
            if (lock.validate(stamp)) {
                return isHammingSimilar;
            }
        }
        stamp = lock.readLock();
        try {
            return super.isHammingSimilar(other, tolerance);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void flip(int x) {
        long stamp = lock.writeLock();
        try {
            super.flip(x);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void flip(long rangeStart, long rangeEnd) {
        long stamp = lock.writeLock();
        try {
            super.flip(rangeStart, rangeEnd);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void flip(int rangeStart, int rangeEnd) {
        long stamp = lock.writeLock();
        try {
            super.flip(rangeStart, rangeEnd);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public long getLongCardinality() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            long longCardinality = super.getLongCardinality();
            if (lock.validate(stamp)) {
                return longCardinality;
            }
        }
        stamp = lock.readLock();
        try {
            return super.getLongCardinality();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public boolean cardinalityExceeds(long threshold) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            boolean cardinalityExceeds = super.cardinalityExceeds(threshold);
            if (lock.validate(stamp)) {
                return cardinalityExceeds;
            }
        }
        stamp = lock.readLock();
        try {
            return super.cardinalityExceeds(threshold);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void forEach(IntConsumer ic) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            super.forEach(ic);
            if (lock.validate(stamp)) {
                return;
            }
        }
        stamp = lock.readLock();
        try {
            super.forEach(ic);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void forAllInRange(int uStart, int length, RelativeRangeConsumer rrc) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            super.forAllInRange(uStart, length, rrc);
            if (lock.validate(stamp)) {
                return;
            }
        }
        stamp = lock.readLock();
        try {
            super.forAllInRange(uStart, length, rrc);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long getLongSizeInBytes() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            long longSizeInBytes = super.getLongSizeInBytes();
            if (lock.validate(stamp)) {
                return longSizeInBytes;
            }
        }
        stamp = lock.readLock();
        try {
            return super.getLongSizeInBytes();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public int hashCode() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            int hashCode = super.hashCode();
            if (lock.validate(stamp)) {
                return hashCode;
            }
        }
        stamp = lock.readLock();
        try {
            return super.hashCode();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public boolean hasRunCompression() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            boolean hasRunCompression = super.hasRunCompression();
            if (lock.validate(stamp)) {
                return hasRunCompression;
            }
        }
        stamp = lock.readLock();
        try {
            return super.hasRunCompression();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public boolean isEmpty() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            boolean isEmpty = super.isEmpty();
            if (lock.validate(stamp)) {
                return isEmpty;
            }
        }
        stamp = lock.readLock();
        try {
            return super.isEmpty();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public RoaringBitmap limit(int maxcardinality) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            RoaringBitmap limit = super.limit(maxcardinality);
            if (lock.validate(stamp)) {
                return limit;
            }
        }
        stamp = lock.readLock();
        try {
            return super.limit(maxcardinality);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void or(RoaringBitmap x2) {
        long stamp = lock.writeLock();
        try {
            super.or(x2);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public long rankLong(int x) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            long rankLong = super.rankLong(x);
            if (lock.validate(stamp)) {
                return rankLong;
            }
        }
        stamp = lock.readLock();
        try {
            return super.rankLong(x);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long rangeCardinality(long start, long end) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            long rangeCardinality = super.rangeCardinality(start, end);
            if (lock.validate(stamp)) {
                return rangeCardinality;
            }
        }
        stamp = lock.readLock();
        try {
            return super.rangeCardinality(start, end);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        long stamp = lock.writeLock();
        try {
            super.readExternal(in);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void remove(int x) {
        long stamp = lock.writeLock();
        try {
            super.remove(x);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void remove(long rangeStart, long rangeEnd) {
        long stamp = lock.writeLock();
        try {
            super.remove(rangeStart, rangeEnd);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean removeRunCompression() {
        long stamp = lock.writeLock();
        try {
            return super.removeRunCompression();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean runOptimize() {
        long stamp = lock.writeLock();
        try {
            return super.runOptimize();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean contains(RoaringBitmap subset) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            boolean contains = super.contains(subset);
            if (lock.validate(stamp)) {
                return contains;
            }
        }
        stamp = lock.readLock();
        try {
            return super.contains(subset);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public int select(int j) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            int select = super.select(j);
            if (lock.validate(stamp)) {
                return select;
            }
        }
        stamp = lock.readLock();
        try {
            return super.select(j);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long nextValue(int fromValue) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            long nextValue = super.nextValue(fromValue);
            if (lock.validate(stamp)) {
                return nextValue;
            }
        }
        stamp = lock.readLock();
        try {
            return super.nextValue(fromValue);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long previousValue(int fromValue) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            long previousValue = super.previousValue(fromValue);
            if (lock.validate(stamp)) {
                return previousValue;
            }
        }
        stamp = lock.readLock();
        try {
            return super.previousValue(fromValue);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long nextAbsentValue(int fromValue) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            long nextAbsentValue = super.nextAbsentValue(fromValue);
            if (lock.validate(stamp)) {
                return nextAbsentValue;
            }
        }
        stamp = lock.readLock();
        try {
            return super.nextAbsentValue(fromValue);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long previousAbsentValue(int fromValue) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            long previousAbsentValue = super.previousAbsentValue(fromValue);
            if (lock.validate(stamp)) {
                return previousAbsentValue;
            }
        }
        stamp = lock.readLock();
        try {
            return super.previousAbsentValue(fromValue);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public int first() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            int first = super.first();
            if (lock.validate(stamp)) {
                return first;
            }
        }
        stamp = lock.readLock();
        try {
            return super.first();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public int last() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            int last = super.last();
            if (lock.validate(stamp)) {
                return last;
            }
        }
        stamp = lock.readLock();
        try {
            return super.last();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void serialize(DataOutput out) throws IOException {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            super.serialize(out);
            if (lock.validate(stamp)) {
                return;
            }
        }
        stamp = lock.readLock();
        try {
            super.serialize(out);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            super.serialize(buffer);
            if (lock.validate(stamp)) {
                return;
            }
        }
        stamp = lock.readLock();
        try {
            super.serialize(buffer);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public int[] toArray() {
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0) {
            int[] toArray = super.toArray();
            if (lock.validate(stamp)) {
                return toArray;
            }
        }
        stamp = lock.readLock();
        try {
            return super.toArray();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void append(char key, Container container) {
        long stamp = lock.writeLock();
        try {
            super.append(key, container);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void trim() {
        long stamp = lock.writeLock();
        try {
            super.trim();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        long stamp = lock.writeLock();
        try {
            super.writeExternal(out);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void xor(RoaringBitmap x2) {
        long stamp = lock.writeLock();
        try {
            super.xor(x2);
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}
