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
package io.netty.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import io.netty.util.Recycler;

public class RecyclableSlicedByteBuf extends AbstractRecyclableDerivedByteBuf {
    private int adjustment;
    private int length;

    private static final Recycler<RecyclableSlicedByteBuf> RECYCLER = new Recycler<RecyclableSlicedByteBuf>() {
        @Override
        protected RecyclableSlicedByteBuf newObject(Handle handle) {
            return new RecyclableSlicedByteBuf(handle);
        }
    };

    public static RecyclableSlicedByteBuf create(ByteBuf buffer, int index, int length) {
        RecyclableSlicedByteBuf buf = RECYCLER.get();
        buf.init(buffer, index, length);
        return buf;
    }

    private final Recycler.Handle recyclerHandle;

    private RecyclableSlicedByteBuf(Recycler.Handle recyclerHandle) {
        super(0);
        this.recyclerHandle = recyclerHandle;
    }

    @Override
    protected void recycle() {
        RECYCLER.recycle(this, recyclerHandle);
    }

    final void init(ByteBuf buffer, int index, int length) {
        super.init(buffer);
        if (index < 0 || index > buffer.capacity() - length) {
            throw new IndexOutOfBoundsException(buffer + ".slice(" + index + ", " + length + ')');
        }

        if (buffer instanceof RecyclableSlicedByteBuf) {
            this.origBuffer = ((RecyclableSlicedByteBuf) buffer).origBuffer;
            adjustment = ((RecyclableSlicedByteBuf) buffer).adjustment + index;
        } else if (buffer instanceof DuplicatedByteBuf) {
            this.origBuffer = buffer.unwrap();
            adjustment = index;
        } else {
            this.origBuffer = buffer;
            adjustment = index;
        }
        this.length = length;
        maxCapacity(length);
        setIndex(0, length);
        discardMarks();
    }

    @Override
    public ByteBuf unwrap() {
        return origBuffer;
    }

    @Override
    public ByteBufAllocator alloc() {
        return origBuffer.alloc();
    }

    @Override
    public ByteOrder order() {
        return origBuffer.order();
    }

    @Override
    public boolean isDirect() {
        return origBuffer.isDirect();
    }

    @Override
    public int capacity() {
        return length;
    }

    @Override
    public ByteBuf capacity(int newCapacity) {
        throw new UnsupportedOperationException("sliced buffer");
    }

    @Override
    public boolean hasArray() {
        return origBuffer.hasArray();
    }

    @Override
    public byte[] array() {
        return origBuffer.array();
    }

    @Override
    public int arrayOffset() {
        return origBuffer.arrayOffset() + adjustment;
    }

    @Override
    public boolean hasMemoryAddress() {
        return origBuffer.hasMemoryAddress();
    }

    @Override
    public long memoryAddress() {
        return origBuffer.memoryAddress() + adjustment;
    }

    @Override
    protected byte _getByte(int index) {
        return origBuffer.getByte(index + adjustment);
    }

    @Override
    protected short _getShort(int index) {
        return origBuffer.getShort(index + adjustment);
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        return origBuffer.getUnsignedMedium(index + adjustment);
    }

    @Override
    protected int _getInt(int index) {
        return origBuffer.getInt(index + adjustment);
    }

    @Override
    protected long _getLong(int index) {
        return origBuffer.getLong(index + adjustment);
    }

    @Override
    public ByteBuf copy(int index, int length) {
        checkIndex(index, length);
        return origBuffer.copy(index + adjustment, length);
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
        checkIndex(index, length);
        origBuffer.getBytes(index + adjustment, dst, dstIndex, length);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        checkIndex(index, length);
        origBuffer.getBytes(index + adjustment, dst, dstIndex, length);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuffer dst) {
        checkIndex(index, dst.remaining());
        origBuffer.getBytes(index + adjustment, dst);
        return this;
    }

    @Override
    protected void _setByte(int index, int value) {
        origBuffer.setByte(index + adjustment, value);
    }

    @Override
    protected void _setShort(int index, int value) {
        origBuffer.setShort(index + adjustment, value);
    }

    @Override
    protected void _setMedium(int index, int value) {
        origBuffer.setMedium(index + adjustment, value);
    }

    @Override
    protected void _setInt(int index, int value) {
        origBuffer.setInt(index + adjustment, value);
    }

    @Override
    protected void _setLong(int index, long value) {
        origBuffer.setLong(index + adjustment, value);
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        checkIndex(index, length);
        origBuffer.setBytes(index + adjustment, src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        checkIndex(index, length);
        origBuffer.setBytes(index + adjustment, src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src) {
        checkIndex(index, src.remaining());
        origBuffer.setBytes(index + adjustment, src);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
        checkIndex(index, length);
        origBuffer.getBytes(index + adjustment, out, length);
        return this;
    }

    @Override
    public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        checkIndex(index, length);
        return origBuffer.getBytes(index + adjustment, out, length);
    }

    @Override
    public int setBytes(int index, InputStream in, int length) throws IOException {
        checkIndex(index, length);
        return origBuffer.setBytes(index + adjustment, in, length);
    }

    @Override
    public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        checkIndex(index, length);
        return origBuffer.setBytes(index + adjustment, in, length);
    }

    @Override
    public int nioBufferCount() {
        return origBuffer.nioBufferCount();
    }

    @Override
    public ByteBuffer nioBuffer(int index, int length) {
        checkIndex(index, length);
        return origBuffer.nioBuffer(index + adjustment, length);
    }

    @Override
    public ByteBuffer[] nioBuffers(int index, int length) {
        checkIndex(index, length);
        return origBuffer.nioBuffers(index + adjustment, length);
    }

    @Override
    public ByteBuffer internalNioBuffer(int index, int length) {
        checkIndex(index, length);
        return nioBuffer(index, length);
    }

    @Override
    public int forEachByte(int index, int length, ByteBufProcessor processor) {
        int ret = origBuffer.forEachByte(index + adjustment, length, processor);
        if (ret >= adjustment) {
            return ret - adjustment;
        } else {
            return -1;
        }
    }

    @Override
    public int forEachByteDesc(int index, int length, ByteBufProcessor processor) {
        int ret = origBuffer.forEachByteDesc(index + adjustment, length, processor);
        if (ret >= adjustment) {
            return ret - adjustment;
        } else {
            return -1;
        }
    }
}
