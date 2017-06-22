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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufProcessor;
import io.netty.util.Recycler;

public class RecyclableDuplicateByteBuf extends AbstractRecyclableDerivedByteBuf {

    private static final Recycler<RecyclableDuplicateByteBuf> RECYCLER = new Recycler<RecyclableDuplicateByteBuf>() {
        @Override
        protected RecyclableDuplicateByteBuf newObject(Handle handle) {
            return new RecyclableDuplicateByteBuf(handle);
        }
    };

    public static RecyclableDuplicateByteBuf create(ByteBuf buffer) {
        RecyclableDuplicateByteBuf buf = RECYCLER.get();
        buf.init(buffer);
        return buf;
    }

    private final Recycler.Handle recyclerHandle;

    private RecyclableDuplicateByteBuf(Recycler.Handle recyclerHandle) {
        super(0);
        this.recyclerHandle = recyclerHandle;
    }

    @Override
    protected void recycle() {
        RECYCLER.recycle(this, recyclerHandle);
    }

    protected final void init(ByteBuf buffer) {
        super.init(buffer);
        maxCapacity(buffer.maxCapacity());

        setIndex(buffer.readerIndex(), buffer.writerIndex());
        markReaderIndex();
        markWriterIndex();
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
        return origBuffer.capacity();
    }

    @Override
    public ByteBuf capacity(int newCapacity) {
        origBuffer.capacity(newCapacity);
        return this;
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
        return origBuffer.arrayOffset();
    }

    @Override
    public boolean hasMemoryAddress() {
        return origBuffer.hasMemoryAddress();
    }

    @Override
    public long memoryAddress() {
        return origBuffer.memoryAddress();
    }

    @Override
    public byte getByte(int index) {
        return _getByte(index);
    }

    @Override
    protected byte _getByte(int index) {
        return origBuffer.getByte(index);
    }

    @Override
    public short getShort(int index) {
        return _getShort(index);
    }

    @Override
    protected short _getShort(int index) {
        return origBuffer.getShort(index);
    }

    @Override
    public int getUnsignedMedium(int index) {
        return _getUnsignedMedium(index);
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        return origBuffer.getUnsignedMedium(index);
    }

    @Override
    public int getInt(int index) {
        return _getInt(index);
    }

    @Override
    protected int _getInt(int index) {
        return origBuffer.getInt(index);
    }

    @Override
    public long getLong(int index) {
        return _getLong(index);
    }

    @Override
    protected long _getLong(int index) {
        return origBuffer.getLong(index);
    }

    @Override
    public ByteBuf copy(int index, int length) {
        return origBuffer.copy(index, length);
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
        origBuffer.getBytes(index, dst, dstIndex, length);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        origBuffer.getBytes(index, dst, dstIndex, length);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuffer dst) {
        origBuffer.getBytes(index, dst);
        return this;
    }

    @Override
    public ByteBuf setByte(int index, int value) {
        _setByte(index, value);
        return this;
    }

    @Override
    protected void _setByte(int index, int value) {
        origBuffer.setByte(index, value);
    }

    @Override
    public ByteBuf setShort(int index, int value) {
        _setShort(index, value);
        return this;
    }

    @Override
    protected void _setShort(int index, int value) {
        origBuffer.setShort(index, value);
    }

    @Override
    public ByteBuf setMedium(int index, int value) {
        _setMedium(index, value);
        return this;
    }

    @Override
    protected void _setMedium(int index, int value) {
        origBuffer.setMedium(index, value);
    }

    @Override
    public ByteBuf setInt(int index, int value) {
        _setInt(index, value);
        return this;
    }

    @Override
    protected void _setInt(int index, int value) {
        origBuffer.setInt(index, value);
    }

    @Override
    public ByteBuf setLong(int index, long value) {
        _setLong(index, value);
        return this;
    }

    @Override
    protected void _setLong(int index, long value) {
        origBuffer.setLong(index, value);
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        origBuffer.setBytes(index, src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        origBuffer.setBytes(index, src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src) {
        origBuffer.setBytes(index, src);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
        origBuffer.getBytes(index, out, length);
        return this;
    }

    @Override
    public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        return origBuffer.getBytes(index, out, length);
    }

    @Override
    public int setBytes(int index, InputStream in, int length) throws IOException {
        return origBuffer.setBytes(index, in, length);
    }

    @Override
    public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        return origBuffer.setBytes(index, in, length);
    }

    @Override
    public int nioBufferCount() {
        return origBuffer.nioBufferCount();
    }

    @Override
    public ByteBuffer[] nioBuffers(int index, int length) {
        return origBuffer.nioBuffers(index, length);
    }

    @Override
    public ByteBuffer internalNioBuffer(int index, int length) {
        return nioBuffer(index, length);
    }

    @Override
    public ByteBuffer nioBuffer(int index, int length) {
        return unwrap().nioBuffer(index, length);
    }

    @Override
    public int forEachByte(int index, int length, ByteBufProcessor processor) {
        return origBuffer.forEachByte(index, length, processor);
    }

    @Override
    public int forEachByteDesc(int index, int length, ByteBufProcessor processor) {
        return origBuffer.forEachByteDesc(index, length, processor);
    }
}
