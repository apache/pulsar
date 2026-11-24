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

/**
 * {@link LongBitmap} implementation backed by {@link RoaringBitmap} and guarded by a
 * {@link StampedLock}.
 *
 * <p><b>Why the on-heap variant.</b> {@link RoaringBitmap} stores each container in a plain
 * {@code char[]}/{@code long[]}. Its {@code org.roaringbitmap.buffer.MutableRoaringBitmap}
 * counterpart stores them in NIO buffers, which exists so that a bitmap can be read straight out
 * of a memory-mapped {@link ByteBuffer}. Pulsar never memory-maps a bitmap and never hands one out
 * as a zero-copy read-only view, so it paid the indirection for nothing: the buffer variant's
 * {@code MappeableArrayContainer.iadd} reallocates its container to exactly the new cardinality
 * with no growth headroom, so appending one value at a time through {@link #add(long, long)} — the
 * shape an individual message acknowledgement produces — reallocated on every single call, while
 * {@code ArrayContainer.iadd} grows geometrically. Both variants share the portable serialization
 * format, so persisted cursor state and delayed-delivery snapshots round-trip across this choice in
 * both directions.
 *
 * <p><b>Thread-safety basis.</b> RoaringBitmap is not thread-safe by default
 * (see <a href="https://github.com/apache/pulsar/issues/25991">pulsar#25991</a>).
 * {@link RoaringBitmap} is a single class implementing both {@code ImmutableBitmapDataProvider} and
 * {@code BitmapDataProvider}, so the type system does not separate readers from mutators here — the
 * split below is maintained by audit rather than by the compiler. Under the READ lock this class
 * calls only {@code contains(int)}, {@code contains(long, long)}, {@code getLongCardinality()},
 * {@code isEmpty()}, {@code rank(int)}, {@code nextValue(int)}, {@code previousValue(int)},
 * {@code nextAbsentValue(int)}, {@code previousAbsentValue(int)}, {@code serializedSizeInBytes()},
 * {@code clone()} and {@link BitSetUtil#toLongArray(RoaringBitmap)}; each was verified to read
 * {@code highLowContainer} and its containers without writing to either. Everything that mutates —
 * {@code add}, {@code checkedAdd}, {@code checkedRemove}, {@code remove}, {@code clear},
 * {@code or}, {@code andNot}, {@code runOptimize}, {@code trim}, {@code getIntIterator} (used by
 * {@link #drainTo} alongside the removal it feeds) and {@code deserialize} — runs under the WRITE
 * lock. {@code clone()} is used under the read lock in {@link #forEachLong} and {@link #serialize};
 * {@code runOptimize()} and {@code serialize(ByteBuffer)} then run on that clone with no lock held.
 * <b>Before upgrading the RoaringBitmap dependency or changing the lock split</b>, re-audit that
 * list and run the concurrency regression tests ({@code testConcurrentForEachLongAndMutate},
 * {@code testOrDoesNotMutateInput}, {@code testConcurrentSerializeToLongArray}).
 *
 * <p>The instance must be a plain {@link RoaringBitmap}, never {@code FastRankRoaringBitmap}: the
 * latter memoizes cardinalities inside {@code rank}/{@code select}, which would turn a read-lock
 * call into a data race.
 *
 * <p><b>Critical sections.</b> Single-value reads take the read lock; mutations take the
 * write lock. Bulk mutations that touch two bitmaps ({@link #or}) acquire this bitmap's
 * write lock and the other's read lock in {@code identityHashCode} order, so concurrent
 * {@code A.or(B)} and {@code B.or(A)} cannot deadlock. Long non-mutating work
 * ({@link #serialize}, {@link #forEachLong}) clones under a brief read lock and finishes
 * without holding it, so optimize/iterate/runOptimize don't block writers.
 *
 * <p><b>Memory.</b> {@link RoaringBitmap#trim()} fires when removals since the last trim reach
 * {@link #TRIM_AFTER_REMOVES}, or whenever the bitmap becomes empty. {@link #serialize} runs
 * {@code runOptimize()} on the clone so persisted bytes are compact.
 */
class ConcurrentRoaringBitmap implements LongBitmap {

    private static final long TRIM_AFTER_REMOVES = 10000;
    private static final long UINT32_SIZE = 1L << 32;
    private static final long MAX_UINT32 = UINT32_SIZE - 1;

    private final RoaringBitmap bitmap;
    private final StampedLock lock;
    private long removesSinceTrim;

    ConcurrentRoaringBitmap() {
        this(new RoaringBitmap());
    }

    private ConcurrentRoaringBitmap(RoaringBitmap bitmap) {
        this.bitmap = bitmap;
        this.lock = new StampedLock();
    }

    /**
     * Creates a bitmap holding the values encoded in {@code data}, in the
     * {@link java.util.BitSet#toLongArray()} format produced by {@link #serializeToLongArray()}.
     *
     * <p>{@link BitSetUtil#bitmapOf(long[])} already builds every container at its exact block
     * cardinality, so adopting its result avoids the empty-bitmap plus {@code clear()} plus
     * {@code or()} round trip that the instance method {@link #deserializeFromLongArray(long[])}
     * cannot avoid — {@link #bitmap} is {@code final}, because {@link #or} reads its identity
     * without holding a lock to order the two locks it acquires.
     *
     * @param data long array in BitSet format
     * @return a new bitmap containing exactly the values encoded in {@code data}
     */
    static ConcurrentRoaringBitmap fromLongArray(long[] data) {
        return new ConcurrentRoaringBitmap(BitSetUtil.bitmapOf(data));
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
            if (to - from == 1) {
                // Single-value ranges dominate the acknowledgement path: PositionRangeSet
                // .addOpenClosed issues add(entryId, entryId + 1) for every individually acked
                // message. add(int) seeds a brand-new container at ArrayContainer's default
                // capacity, while add(long, long) seeds it through Container.rangeOfOnes at
                // exactly 1 and then has to grow on the next value. The two are equivalent for a
                // one-wide range: rangeOfOnes returns an ArrayContainer for cardinality <= 2, the
                // same class add(int) creates, and both convert to a bitmap container at
                // cardinality 4096. Must stay inside the lock and after both validateRange calls.
                bitmap.add((int) from);
            } else {
                bitmap.add(from, to);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean remove(long value) {
        validateRange(value);
        long stamp = lock.writeLock();
        try {
            boolean removed = bitmap.checkedRemove((int) value);
            if (removed) {
                removesSinceTrim++;
                maybeTrim();
            }
            return removed;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean remove(long from, long to) {
        if (to <= from) {
            return false;
        }
        validateRange(from);
        validateRange(to - 1);
        long stamp = lock.writeLock();
        try {
            long cardinalityBefore = bitmap.getLongCardinality();
            bitmap.remove(from, to);
            long removedCount = cardinalityBefore - bitmap.getLongCardinality();
            if (removedCount > 0) {
                removesSinceTrim += removedCount;
                maybeTrim();
            }
            return removedCount > 0;
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
        RoaringBitmap snapshot;
        long stamp = lock.readLock();
        try {
            if (bitmap.isEmpty()) {
                return;
            }
            snapshot = bitmap.clone();
        } finally {
            lock.unlockRead(stamp);
        }
        // The cast is load-bearing: RoaringBitmap also implements Iterable<Integer>, so an
        // uncast lambda binds to Iterable.forEach(Consumer<? super Integer>) and boxes every value.
        snapshot.forEach((org.roaringbitmap.IntConsumer) v ->
                action.accept(Integer.toUnsignedLong(v)));
    }

    @Override
    public long drainTo(long limit, LongConsumer action) {
        if (limit <= 0) {
            return 0;
        }
        RoaringBitmap toRemove = new RoaringBitmap();
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
        RoaringBitmap copy;
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
            // BitSetUtil.toLongArray only reads the bitmap (isEmpty / last / getContainerPointer /
            // Container.copyBitmapTo, all of which write solely into the returned array), so it is
            // correct against the live instance under the read lock. Do not reintroduce a
            // defensive copy: MutableRoaringBitmap.toRoaringBitmap(), which this replaced,
            // advanced the live containers' buffer positions and so mutated under the read lock.
            return BitSetUtil.toLongArray(bitmap);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void deserializeFromLongArray(long[] data) {
        // Built outside the lock: BitSetUtil.bitmapOf reads only the caller-owned long[] and the
        // result is thread-confined until it is merged in below.
        RoaringBitmap replacement = BitSetUtil.bitmapOf(data);
        long stamp = lock.writeLock();
        try {
            bitmap.clear();
            bitmap.or(replacement);
            removesSinceTrim = 0;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    static ConcurrentRoaringBitmap deserialize(ByteBuf buf) {
        try {
            ByteBuffer nioBuffer = buf.nioBuffer(buf.readerIndex(), buf.readableBytes());
            int startPosition = nioBuffer.position();
            RoaringBitmap bitmap = new RoaringBitmap();
            // Deliberately the DataInput overload: RoaringBitmap.deserialize(ByteBuffer) slices its
            // argument and leaves the caller's position untouched, which would break the
            // skipBytes accounting below.
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
