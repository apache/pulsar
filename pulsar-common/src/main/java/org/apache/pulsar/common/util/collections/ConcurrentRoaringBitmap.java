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

import io.netty.buffer.ByteBuf;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.StampedLock;
import java.util.function.LongConsumer;
import org.roaringbitmap.BitSetUtil;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * {@link LongBitmap} implementation backed by {@link MutableRoaringBitmap} and guarded
 * by a {@link StampedLock}.
 *
 * <p><b>Thread-safety basis.</b> RoaringBitmap is not thread-safe by default
 * (see <a href="https://github.com/apache/pulsar/issues/25991">pulsar#25991</a>). This
 * wrapper relies on the documented contract that {@link MutableRoaringBitmap}'s read
 * methods — the {@code ImmutableBitmapDataProvider} surface inherited from
 * {@code ImmutableRoaringBitmap} — do not mutate internal state, while methods added by
 * {@code BitmapDataProvider} and other {@code MutableRoaringBitmap} mutators
 * ({@code andNot}, {@code or}, {@code checkedRemove}, {@code runOptimize}, {@code clone},
 * ...) do. Read methods run under the read lock; mutators under the write lock.
 * {@code clone()} is used under the read lock in {@link #forEachLong} and {@link #serialize};
 * its source has been audited to be read-only on the live bitmap. <b>Before upgrading
 * the RoaringBitmap dependency or changing the lock split</b>, re-audit these methods
 * and run the concurrency regression tests ({@code testConcurrentForEachLongAndMutate},
 * {@code testOrDoesNotMutateInput}).
 *
 * <p><b>Critical sections.</b> Single-value reads take the read lock; mutations take the
 * write lock. Bulk mutations that touch two bitmaps ({@link #or}) acquire this bitmap's
 * write lock and the other's read lock in {@code identityHashCode} order, so concurrent
 * {@code A.or(B)} and {@code B.or(A)} cannot deadlock. Long non-mutating work
 * ({@link #serialize}, {@link #forEachLong}) clones under a brief read lock and finishes
 * without holding it, so optimize/iterate/runOptimize don't block writers.
 *
 * <p><b>Memory.</b> {@link MutableRoaringBitmap#trim()} fires when removals since the
 * last trim reach {@link #TRIM_AFTER_REMOVES}, or whenever the bitmap becomes empty.
 * {@link #serialize} runs {@code runOptimize()} on the clone so persisted bytes are compact.
 */
class ConcurrentRoaringBitmap implements LongBitmap {

    private static final long TRIM_AFTER_REMOVES = 10000;
    private static final long UINT32_SIZE = 1L << 32;
    private static final long MAX_UINT32 = UINT32_SIZE - 1;

    private final MutableRoaringBitmap bitmap;
    private final StampedLock lock;
    private long removesSinceTrim;

    ConcurrentRoaringBitmap() {
        this.bitmap = new MutableRoaringBitmap();
        this.lock = new StampedLock();
    }

    private ConcurrentRoaringBitmap(MutableRoaringBitmap bitmap) {
        this.bitmap = bitmap;
        this.lock = new StampedLock();
    }

    @Override
    public void add(long value) {
        validateRange(value);
        long stamp = lock.writeLock();
        try {
            bitmap.add((int) value);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean checkedAdd(long value) {
        validateRange(value);
        long stamp = lock.writeLock();
        try {
            return bitmap.checkedAdd((int) value);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void add(long from, long to) {
        if (to <= from) {
            return;
        }
        validateRange(from);
        validateRange(to - 1);
        long stamp = lock.writeLock();
        try {
            bitmap.add(from, to);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void remove(long value) {
        validateRange(value);
        long stamp = lock.writeLock();
        try {
            if (bitmap.checkedRemove((int) value)) {
                removesSinceTrim++;
                maybeTrim();
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void remove(long from, long to) {
        if (to <= from) {
            return;
        }
        validateRange(from);
        validateRange(to - 1);
        long stamp = lock.writeLock();
        try {
            bitmap.remove(from, to);
            // Range size upper-bounds removals; clamp so a huge range can't overflow the counter.
            removesSinceTrim = Math.min(removesSinceTrim + (to - from), TRIM_AFTER_REMOVES);
            maybeTrim();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean contains(long value) {
        if (value < 0 || value > MAX_UINT32) {
            return false;
        }
        long stamp = lock.readLock();
        try {
            return bitmap.contains((int) value);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public boolean contains(long from, long to) {
        if (from < 0 || from > MAX_UINT32 || to <= from || to > UINT32_SIZE) {
            return false;
        }
        long stamp = lock.readLock();
        try {
            return bitmap.contains(from, to);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long cardinality() {
        long stamp = lock.readLock();
        try {
            return bitmap.getLongCardinality();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public boolean isEmpty() {
        long stamp = lock.readLock();
        try {
            return bitmap.isEmpty();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void clear() {
        long stamp = lock.writeLock();
        try {
            bitmap.clear();
            removesSinceTrim = 0;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public long nextAbsentValue(long from) {
        if (from < 0 || from > MAX_UINT32) {
            return -1;
        }
        long stamp = lock.readLock();
        try {
            return bitmap.nextAbsentValue((int) from);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long nextPresentValue(long from) {
        if (from < 0 || from > MAX_UINT32) {
            return -1;
        }
        long stamp = lock.readLock();
        try {
            return bitmap.nextValue((int) from);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long previousAbsentValue(long from) {
        if (from < 0) {
            return -1;
        }
        // Clamp to MAX_UINT32 instead of returning -1 for out-of-range input.
        // This allows safe usage in PositionRangeSet.lastRange() where
        // previousAbsentValue(lastPresentValue()) is called and lastPresentValue
        // may be MAX_UINT32. Clamping avoids the need for Math.min() guards at call sites.
        if (from > MAX_UINT32) {
            from = MAX_UINT32;
        }
        long stamp = lock.readLock();
        try {
            return bitmap.previousAbsentValue((int) from);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long lastPresentValue() {
        long stamp = lock.readLock();
        try {
            if (bitmap.isEmpty()) {
                return -1;
            }
            return bitmap.previousValue(-1);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long rank(long value) {
        if (value <= 0) {
            return 0;
        }
        // Clamp to UINT32_SIZE instead of MAX_UINT32 to allow rank(UINT32_SIZE) to return
        // the total cardinality. This is needed in PositionRangeSet.cardinality() where
        // rank(upperValue + 1) is called and upperValue may be MAX_UINT32. The clamping
        // ensures rank(0x100000000L) counts all values in the uint32 range.
        if (value > UINT32_SIZE) {
            value = UINT32_SIZE;
        }
        long stamp = lock.readLock();
        try {
            return bitmap.rank((int) (value - 1));
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void or(LongBitmap other) {
        if (other == this) {
            return;
        }
        if (!(other instanceof ConcurrentRoaringBitmap)) {
            throw new IllegalArgumentException("Unsupported LongBitmap type: " + other.getClass());
        }
        ConcurrentRoaringBitmap that = (ConcurrentRoaringBitmap) other;

        // Acquire this.writeLock + that.readLock in identityHashCode order so concurrent
        // A.or(B) and B.or(A) don't deadlock. Fall back to inner bitmap identity on collision.
        boolean thisFirst;
        int outerCmp = Integer.compare(
                System.identityHashCode(this), System.identityHashCode(that));
        if (outerCmp != 0) {
            thisFirst = outerCmp < 0;
        } else {
            thisFirst = System.identityHashCode(this.bitmap) < System.identityHashCode(that.bitmap);
        }

        if (thisFirst) {
            long thisStamp = this.lock.writeLock();
            try {
                long thatStamp = that.lock.readLock();
                try {
                    this.bitmap.or(that.bitmap);
                } finally {
                    that.lock.unlockRead(thatStamp);
                }
            } finally {
                this.lock.unlockWrite(thisStamp);
            }
        } else {
            long thatStamp = that.lock.readLock();
            try {
                long thisStamp = this.lock.writeLock();
                try {
                    this.bitmap.or(that.bitmap);
                } finally {
                    this.lock.unlockWrite(thisStamp);
                }
            } finally {
                that.lock.unlockRead(thatStamp);
            }
        }
    }

    @Override
    public void forEachLong(LongConsumer action) {
        MutableRoaringBitmap snapshot;
        long stamp = lock.readLock();
        try {
            snapshot = bitmap.clone();
        } finally {
            lock.unlockRead(stamp);
        }
        snapshot.forEach((org.roaringbitmap.IntConsumer) v ->
                action.accept(Integer.toUnsignedLong(v)));
    }

    @Override
    public long drainTo(long limit, LongConsumer action) {
        if (limit <= 0) {
            return 0;
        }
        MutableRoaringBitmap toRemove = new MutableRoaringBitmap();
        long collected;
        long writeStamp = lock.writeLock();
        try {
            PeekableIntIterator it = bitmap.getIntIterator();
            collected = 0;
            while (collected < limit && it.hasNext()) {
                toRemove.add(it.next());
                collected++;
            }
            if (collected == 0) {
                return 0;
            }
            bitmap.andNot(toRemove);
            removesSinceTrim = Math.min(removesSinceTrim + collected, TRIM_AFTER_REMOVES);
            maybeTrim();
        } finally {
            lock.unlockWrite(writeStamp);
        }

        toRemove.forEach((org.roaringbitmap.IntConsumer) v ->
                action.accept(Integer.toUnsignedLong(v)));
        return collected;
    }

    @Override
    public long serializedSize() {
        long stamp = lock.readLock();
        try {
            return bitmap.serializedSizeInBytes();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public byte[] serialize() {
        MutableRoaringBitmap copy;
        long stamp = lock.readLock();
        try {
            copy = bitmap.clone();
        } finally {
            lock.unlockRead(stamp);
        }
        copy.runOptimize();
        byte[] bytes = new byte[copy.serializedSizeInBytes()];
        copy.serialize(ByteBuffer.wrap(bytes));
        return bytes;
    }

    @Override
    public long[] serializeToLongArray() {
        long stamp = lock.readLock();
        try {
            RoaringBitmap immutable = bitmap.toRoaringBitmap();
            return BitSetUtil.toLongArray(immutable);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void deserializeFromLongArray(long[] data) {
        long stamp = lock.writeLock();
        try {
            bitmap.clear();
            RoaringBitmap rb = BitSetUtil.bitmapOf(data);
            bitmap.or(rb.toMutableRoaringBitmap());
            removesSinceTrim = 0;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    static ConcurrentRoaringBitmap deserialize(ByteBuf buf) {
        try {
            ByteBuffer nioBuffer = buf.nioBuffer(buf.readerIndex(), buf.readableBytes());
            int startPosition = nioBuffer.position();
            MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
            bitmap.deserialize(new ByteBufferDataInput(nioBuffer));
            buf.skipBytes(nioBuffer.position() - startPosition);
            return new ConcurrentRoaringBitmap(bitmap);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize LongBitmap", e);
        }
    }

    /**
     * Trims the underlying bitmap if enough removals have accumulated or it's empty.
     * Caller must hold the write lock and have already updated {@link #removesSinceTrim}.
     */
    private void maybeTrim() {
        if (removesSinceTrim >= TRIM_AFTER_REMOVES || bitmap.isEmpty()) {
            bitmap.trim();
            removesSinceTrim = 0;
        }
    }

    private static void validateRange(long value) {
        if (value < 0 || value > MAX_UINT32) {
            throw new IllegalArgumentException(
                    "Value out of range [0, " + MAX_UINT32 + "]: " + value);
        }
    }

    /** Minimal {@link DataInput} over a {@link ByteBuffer} for RoaringBitmap deserialization. */
    private static final class ByteBufferDataInput implements DataInput {
        private final ByteBuffer buffer;

        ByteBufferDataInput(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void readFully(byte[] b) {
            buffer.get(b);
        }

        @Override
        public void readFully(byte[] b, int off, int len) {
            buffer.get(b, off, len);
        }

        @Override
        public int skipBytes(int n) {
            int skip = Math.min(n, buffer.remaining());
            buffer.position(buffer.position() + skip);
            return skip;
        }

        @Override
        public boolean readBoolean() {
            return buffer.get() != 0;
        }

        @Override
        public byte readByte() {
            return buffer.get();
        }

        @Override
        public int readUnsignedByte() {
            return Byte.toUnsignedInt(buffer.get());
        }

        @Override
        public short readShort() {
            return buffer.getShort();
        }

        @Override
        public int readUnsignedShort() {
            return Short.toUnsignedInt(buffer.getShort());
        }

        @Override
        public char readChar() {
            return buffer.getChar();
        }

        @Override
        public int readInt() {
            return buffer.getInt();
        }

        @Override
        public long readLong() {
            return buffer.getLong();
        }

        @Override
        public float readFloat() {
            return buffer.getFloat();
        }

        @Override
        public double readDouble() {
            return buffer.getDouble();
        }

        @Override
        public String readLine() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String readUTF() {
            throw new UnsupportedOperationException();
        }
    }
}
