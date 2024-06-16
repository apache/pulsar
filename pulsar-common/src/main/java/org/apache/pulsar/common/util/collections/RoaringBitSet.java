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

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.stream.IntStream;
import org.roaringbitmap.RoaringBitmap;

@SuppressWarnings("all")
public class RoaringBitSet extends BitSet {
    private static final long serialVersionUID = 1L;

    private RoaringBitmap roaringBitmap;

    public RoaringBitSet() {
        super(0);
        roaringBitmap = new RoaringBitmap();
    }

    private RoaringBitSet(RoaringBitmap roaringBitmap) {
        super(0);
        this.roaringBitmap = roaringBitmap;
    }

    public RoaringBitmap getRoaringBitmap() {
        return roaringBitmap;
    }

    @Override
    public void set(int bitIndex) {
        roaringBitmap.add(bitIndex);
    }

    @Override
    public void set(int bitIndex, boolean value) {
        if (value) {
            roaringBitmap.add(bitIndex);
        } else {
            roaringBitmap.remove(bitIndex);
        }
    }

    @Override
    public void set(int fromIndex, int toIndex) {
        roaringBitmap.add(fromIndex, toIndex);
    }

    @Override
    public void set(int fromIndex, int toIndex, boolean value) {
        if (value) {
            roaringBitmap.add(fromIndex, toIndex);
        } else {
            roaringBitmap.remove(fromIndex, toIndex);
        }
    }

    @Override
    public void clear(int bitIndex) {
        roaringBitmap.remove(bitIndex);
    }

    @Override
    public void clear(int fromIndex, int toIndex) {
        roaringBitmap.remove(fromIndex, toIndex);
    }

    @Override
    public void clear() {
        roaringBitmap.clear();
    }

    @Override
    public boolean get(int bitIndex) {
        return roaringBitmap.contains(bitIndex);
    }

    @Override
    public BitSet get(int fromIndex, int toIndex) {
        RoaringBitmap rb = roaringBitmap.clone();
        rb.flip(0, fromIndex);
        rb.flip(toIndex, rb.last());
        return new RoaringBitSet(rb);
    }

    @Override
    public int nextSetBit(int fromIndex) {
        return (int) roaringBitmap.nextValue(fromIndex);
    }

    @Override
    public int nextClearBit(int fromIndex) {
        return (int) roaringBitmap.nextAbsentValue(fromIndex);
    }

    @Override
    public int previousSetBit(int fromIndex) {
        return (int) roaringBitmap.previousValue(fromIndex);
    }

    @Override
    public int previousClearBit(int fromIndex) {
        return (int) roaringBitmap.previousAbsentValue(fromIndex);
    }

    @Override
    public int length() {
        if (roaringBitmap.isEmpty()) {
            return 0;
        }
        return roaringBitmap.last() + 1;
    }

    @Override
    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    @Override
    public boolean intersects(BitSet set) {
        RoaringBitmap bitmap = set instanceof RoaringBitSet ? ((RoaringBitSet) set).roaringBitmap : fromBitSet(set);
        if (bitmap.isEmpty() || roaringBitmap.isEmpty()) {
            return false;
        }
        long rangeStart = bitmap.first();
        long rangeEnd = bitmap.last();
        return roaringBitmap.intersects(rangeStart, rangeEnd);
    }

    @Override
    public int cardinality() {
        return roaringBitmap.getCardinality();
    }

    @Override
    public void and(BitSet set) {
        if (set instanceof RoaringBitSet) {
            roaringBitmap.and(((RoaringBitSet) set).roaringBitmap);
        } else {
            roaringBitmap.and(fromBitSet(set));
        }
    }

    @Override
    public void or(BitSet set) {
        if (set instanceof RoaringBitSet) {
            roaringBitmap.or(((RoaringBitSet) set).roaringBitmap);
        } else {
            roaringBitmap.or(fromBitSet(set));
        }
    }

    @Override
    public void xor(BitSet set) {
        if (set instanceof RoaringBitSet) {
            roaringBitmap.xor(((RoaringBitSet) set).roaringBitmap);
        } else {
            roaringBitmap.xor(fromBitSet(set));
        }
    }

    @Override
    public void andNot(BitSet set) {
        if (set instanceof RoaringBitSet) {
            roaringBitmap.andNot(((RoaringBitSet) set).roaringBitmap);
        } else {
            roaringBitmap.andNot(fromBitSet(set));
        }
    }

    @Override
    public int hashCode() {
        return roaringBitmap.hashCode();
    }

    @Override
    public int size() {
        return roaringBitmap.getCardinality();
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
        Object ignore = super.clone();
        return new RoaringBitSet(roaringBitmap.clone());
    }


    @Override
    public IntStream stream() {
        return roaringBitmap.stream();
    }

    @Override
    public String toString() {
        return roaringBitmap.toString();
    }

    @Override
    public void flip(int bitIndex) {
        roaringBitmap.flip(bitIndex, bitIndex + 1);
    }

    @Override
    public void flip(int fromIndex, int toIndex) {
        roaringBitmap.flip(fromIndex, toIndex);
    }

    @Override
    public long[] toLongArray() {
        int[] arr = roaringBitmap.toArray();
        ByteBuffer buffer = ByteBuffer.allocate(arr.length * 4);
        buffer.asIntBuffer().put(arr);
        return buffer.asLongBuffer().array();
    }

    @Override
    public byte[] toByteArray() {
        int[] arr = roaringBitmap.toArray();
        ByteBuffer buffer = ByteBuffer.allocate(arr.length * 4);
        buffer.asIntBuffer().put(arr);
        return buffer.array();
    }

    private static RoaringBitmap fromBitSet(BitSet bitSet) {
        long[] arr = bitSet.toLongArray();
        ByteBuffer buffer = ByteBuffer.allocate(arr.length * 8);
        buffer.asLongBuffer().put(arr);
        return RoaringBitmap.bitmapOf(buffer.asIntBuffer().array());
    }
}
