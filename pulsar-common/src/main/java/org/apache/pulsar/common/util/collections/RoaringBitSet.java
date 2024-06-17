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
import java.util.stream.IntStream;
import org.roaringbitmap.BitSetUtil;
import org.roaringbitmap.RoaringBitmap;

/**
 *  A RoaringBitmap-backed BitSet implementation.
 */
@SuppressWarnings("all")
public class RoaringBitSet {
    private static final long serialVersionUID = 1L;

    private RoaringBitmap roaringBitmap;

    public RoaringBitSet() {
        roaringBitmap = new RoaringBitmap();
    }

    private RoaringBitSet(RoaringBitmap roaringBitmap) {
        this.roaringBitmap = roaringBitmap;
    }

    public RoaringBitmap getRoaringBitmap() {
        return roaringBitmap;
    }

    public void set(int bitIndex) {
        roaringBitmap.add(bitIndex);
    }

    public void set(int bitIndex, boolean value) {
        if (value) {
            roaringBitmap.add(bitIndex);
        } else {
            roaringBitmap.remove(bitIndex);
        }
    }

    public void set(int fromIndex, int toIndex) {
        roaringBitmap.add(fromIndex, toIndex);
    }

    public void set(int fromIndex, int toIndex, boolean value) {
        if (value) {
            roaringBitmap.add(fromIndex, toIndex);
        } else {
            roaringBitmap.remove(fromIndex, toIndex);
        }
    }

    public void clear(int bitIndex) {
        roaringBitmap.remove(bitIndex);
    }

    public void clear(int fromIndex, int toIndex) {
        roaringBitmap.remove(fromIndex, toIndex);
    }

    public void clear() {
        roaringBitmap.clear();
    }

    public boolean get(int bitIndex) {
        return roaringBitmap.contains(bitIndex);
    }

    public RoaringBitSet get(int fromIndex, int toIndex) {
        BitSet bitSet = BitSetUtil.bitsetOf(roaringBitmap);
        bitSet = bitSet.get(fromIndex, toIndex);
        return new RoaringBitSet(fromBitSet(bitSet));
    }

    public int nextSetBit(int fromIndex) {
        return (int) roaringBitmap.nextValue(fromIndex);
    }

    public int nextClearBit(int fromIndex) {
        return (int) roaringBitmap.nextAbsentValue(fromIndex);
    }

    public int previousSetBit(int fromIndex) {
        return (int) roaringBitmap.previousValue(fromIndex);
    }

    public int previousClearBit(int fromIndex) {
        return (int) roaringBitmap.previousAbsentValue(fromIndex);
    }

    public int length() {
        if (roaringBitmap.isEmpty()) {
            return 0;
        }
        return roaringBitmap.last() + 1;
    }

    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    public boolean intersects(RoaringBitSet set) {
        return RoaringBitmap.intersects(roaringBitmap, ((RoaringBitSet) set).roaringBitmap);
    }

    public int cardinality() {
        return roaringBitmap.getCardinality();
    }

    public void and(RoaringBitSet set) {
        roaringBitmap.and(((RoaringBitSet) set).roaringBitmap);
    }

    public void or(RoaringBitSet set) {
        roaringBitmap.or(((RoaringBitSet) set).roaringBitmap);
    }

    public void xor(RoaringBitSet set) {
        roaringBitmap.xor(((RoaringBitSet) set).roaringBitmap);
    }

    public void andNot(RoaringBitSet set) {
        roaringBitmap.andNot(((RoaringBitSet) set).roaringBitmap);
    }

    @Override
    public int hashCode() {
        return roaringBitmap.hashCode();
    }

    public int size() {
        if (roaringBitmap.isEmpty()) {
            return 0;
        }
        int lastBit = Math.max(length(), 64);
        int remainder = lastBit % 64;
        return remainder == 0 ? lastBit : lastBit + 64 - remainder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof RoaringBitSet) {
            return roaringBitmap.equals(((RoaringBitSet) obj).roaringBitmap);
        }
        return false;
    }

    @Override
    public Object clone() {
        return new RoaringBitSet(roaringBitmap.clone());
    }

    public IntStream stream() {
        return roaringBitmap.stream();
    }

    @Override
    public String toString() {
        return roaringBitmap.toString();
    }

    public void flip(int bitIndex) {
        roaringBitmap.flip(bitIndex, bitIndex + 1);
    }

    public void flip(int fromIndex, int toIndex) {
        roaringBitmap.flip(fromIndex, toIndex);
    }

    public long[] toLongArray() {
        return BitSetUtil.toLongArray(roaringBitmap);
    }

    public byte[] toByteArray() {
        return BitSetUtil.toByteArray(roaringBitmap);
    }

    private static RoaringBitmap fromBitSet(BitSet bitSet) {
        return BitSetUtil.bitmapOf(bitSet);
    }
}
