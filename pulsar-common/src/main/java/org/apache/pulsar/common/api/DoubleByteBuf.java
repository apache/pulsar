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
/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.pulsar.common.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.AbstractReferenceCountedByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.ResourceLeakTracker;

/**
 * ByteBuf that holds 2 buffers. Similar to {@see CompositeByteBuf} but doesn't allocate list to hold them.
 */
@SuppressWarnings("unchecked")
public final class DoubleByteBuf extends AbstractReferenceCountedByteBuf {

    private ByteBuf b1;
    private ByteBuf b2;
    private final Handle recyclerHandle;

    private static final Recycler<DoubleByteBuf> RECYCLER = new Recycler<DoubleByteBuf>() {
        @Override
        protected DoubleByteBuf newObject(Recycler.Handle handle) {
            return new DoubleByteBuf(handle);
        }
    };

    private DoubleByteBuf(Handle recyclerHandle) {
        super(Integer.MAX_VALUE);
        this.recyclerHandle = recyclerHandle;
    }

    public static ByteBuf get(ByteBuf b1, ByteBuf b2) {
        DoubleByteBuf buf = RECYCLER.get();
        buf.setRefCnt(1);

        // Make sure the buffers are not deallocated as long as we hold them. Also, buffers can get retained/releases
        // outside of DoubleByteBuf scope
        buf.b1 = b1.retain();
        buf.b2 = b2.retain();
        buf.setIndex(0, b1.readableBytes() + b2.readableBytes());
        return toLeakAwareBuffer(buf);
    }

    public ByteBuf getFirst() {
        return b1;
    }

    public ByteBuf getSecond() {
        return b2;
    }

    @Override
    public boolean isDirect() {
        return b1.isDirect() && b2.isDirect();
    }

    @Override
    public boolean hasArray() {
        // There's no single array available
        return false;
    }

    @Override
    public byte[] array() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int arrayOffset() {

        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasMemoryAddress() {
        return false;
    }

    @Override
    public long memoryAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int capacity() {
        return b1.capacity() + b2.capacity();
    }

    @Override
    public int writableBytes() {
        return 0;
    }

    @Override
    public DoubleByteBuf capacity(int newCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBufAllocator alloc() {
        return PooledByteBufAllocator.DEFAULT;
    }

    @Override
    public ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public byte getByte(int index) {
        if (index < b1.writerIndex()) {
            return b1.getByte(index);
        } else {
            return b2.getByte(index - b1.writerIndex());
        }
    }

    @Override
    protected byte _getByte(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected short _getShort(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int _getInt(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected long _getLong(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        return getBytes(index, Unpooled.wrappedBuffer(dst), dstIndex, length);
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuffer dst) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
        checkDstIndex(index, length, dstIndex, dst.capacity());
        if (length == 0) {
            return this;
        }

        int b1Length = Math.min(length, b1.readableBytes() - index);
        if (b1Length > 0) {
            b1.getBytes(b1.readerIndex() + index, dst, dstIndex, b1Length);
            dstIndex += b1Length;
            length -= b1Length;
            index = 0;
        } else {
            index -= b1.readableBytes();
        }

        if (length > 0) {
            int b2Length = Math.min(length, b2.readableBytes() - index);
            b2.getBytes(b2.readerIndex() + index, dst, dstIndex, b2Length);
        }
        return this;
    }

    @Override
    public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf setByte(int index, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setByte(int index, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf setShort(int index, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setShort(int index, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf setMedium(int index, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setMedium(int index, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf setInt(int index, int value) {
        return (DoubleByteBuf) super.setInt(index, value);
    }

    @Override
    protected void _setInt(int index, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf setLong(int index, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _setLong(int index, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf setBytes(int index, ByteBuffer src) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DoubleByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int setBytes(int index, InputStream in, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf copy(int index, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int nioBufferCount() {
        return b1.nioBufferCount() + b2.nioBufferCount();
    }

    @Override
    public ByteBuffer internalNioBuffer(int index, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer nioBuffer(int index, int length) {
        ByteBuffer dst = ByteBuffer.allocate(length);
        ByteBuf b = Unpooled.wrappedBuffer(dst);
        b.writerIndex(0);
        getBytes(index, b, length);
        return dst;
    }

    @Override
    public ByteBuffer[] nioBuffers(int index, int length) {
        return new ByteBuffer[] { nioBuffer(index, length) };
    }

    @Override
    public DoubleByteBuf discardReadBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        String result = super.toString();
        result = result.substring(0, result.length() - 1);
        return result + ", components=2)";
    }

    @Override
    public ByteBuffer[] nioBuffers() {
        return nioBuffers(readerIndex(), readableBytes());
    }

    @Override
    protected void deallocate() {
        // Double release of buffer for the initial ref-count and the internal retain() when the DoubleByteBuf was
        // created
        b1.release(2);
        b2.release(2);
        b1 = b2 = null;
        RECYCLER.recycle(this, recyclerHandle);
    }

    @Override
    public ByteBuf unwrap() {
        return null;
    }

    private static final Logger log = LoggerFactory.getLogger(DoubleByteBuf.class);

    private static final ResourceLeakDetector<DoubleByteBuf> leakDetector = ResourceLeakDetectorFactory.instance()
            .newResourceLeakDetector(DoubleByteBuf.class);
    private static final Constructor<ByteBuf> simpleLeakAwareByteBufConstructor;
    private static final Constructor<ByteBuf> advancedLeakAwareByteBufConstructor;

    static {
        Constructor<ByteBuf> _simpleLeakAwareByteBufConstructor = null;
        Constructor<ByteBuf> _advancedLeakAwareByteBufConstructor = null;
        try {
            Class<?> simpleLeakAwareByteBufClass = Class.forName("io.netty.buffer.SimpleLeakAwareByteBuf");
            _simpleLeakAwareByteBufConstructor = (Constructor<ByteBuf>) simpleLeakAwareByteBufClass
                    .getDeclaredConstructor(ByteBuf.class, ResourceLeakTracker.class);
            _simpleLeakAwareByteBufConstructor.setAccessible(true);

            Class<?> advancedLeakAwareByteBufClass = Class.forName("io.netty.buffer.AdvancedLeakAwareByteBuf");
            _advancedLeakAwareByteBufConstructor = (Constructor<ByteBuf>) advancedLeakAwareByteBufClass
                    .getDeclaredConstructor(ByteBuf.class, ResourceLeakTracker.class);
            _advancedLeakAwareByteBufConstructor.setAccessible(true);
        } catch (Exception e) {
            log.error("Failed to use reflection to enable leak detection");
        } finally {
            simpleLeakAwareByteBufConstructor = _simpleLeakAwareByteBufConstructor;
            advancedLeakAwareByteBufConstructor = _advancedLeakAwareByteBufConstructor;
        }
    }

    private static ByteBuf toLeakAwareBuffer(DoubleByteBuf buf) {
        try {
            ResourceLeakTracker<DoubleByteBuf> leak;
            switch (ResourceLeakDetector.getLevel()) {
            case DISABLED:
                break;

            case SIMPLE:
                leak = leakDetector.track(buf);
                if (leak != null) {
                    return simpleLeakAwareByteBufConstructor.newInstance(buf, leak);
                }
                break;
            case ADVANCED:
            case PARANOID:
                leak = leakDetector.track(buf);
                if (leak != null) {
                    return advancedLeakAwareByteBufConstructor.newInstance(buf, leak);
                }
                break;
            }
            return buf;
        } catch (Throwable t) {
            // Catch reflection exception
            throw new RuntimeException(t);
        }
    }
}