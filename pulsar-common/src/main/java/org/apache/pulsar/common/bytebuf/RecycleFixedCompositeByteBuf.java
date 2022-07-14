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
package org.apache.pulsar.common.bytebuf;

import io.netty.buffer.AbstractReferenceCountedByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.internal.EmptyArrays;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.RecyclableArrayList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

@Slf4j
public class RecycleFixedCompositeByteBuf extends AbstractReferenceCountedByteBuf {

    private static final ByteBuf[] EMPTY = { Unpooled.EMPTY_BUFFER };

    private static final Recycler<RecycleFixedCompositeByteBuf> RECYCLER =
            new Recycler<RecycleFixedCompositeByteBuf>() {
                @Override
                protected RecycleFixedCompositeByteBuf newObject(Handle<RecycleFixedCompositeByteBuf> handle) {
                    return new RecycleFixedCompositeByteBuf(handle);
                }
            };

    private int nioBufferCount;
    private int capacity;
    private ByteBufAllocator allocator;
    private ByteOrder order;
    private ByteBuf[] buffers;
    private boolean direct;

    private final ObjectPool.Handle<RecycleFixedCompositeByteBuf> recyclerHandle;

    public static RecycleFixedCompositeByteBuf newInstance(ByteBuf...buffers){
        RecycleFixedCompositeByteBuf byteBuf = RECYCLER.get();
        // reset attributes for reuse.
        byteBuf.maxCapacity(Integer.MAX_VALUE);
        byteBuf.resetRefCnt();
        byteBuf.readerIndex(0);
        byteBuf.writerIndex(0);
        byteBuf.markReaderIndex();
        byteBuf.markWriterIndex();
        // fill buffer.
        byteBuf.init(buffers);
        return byteBuf;
    }

    private RecycleFixedCompositeByteBuf(Recycler.Handle<RecycleFixedCompositeByteBuf> handle) {
        super(Integer.MAX_VALUE);
        this.recyclerHandle = handle;
    }

    private void init(ByteBuf[] buffers){
        if (buffers.length == 0) {
            throw new IllegalArgumentException("Could not create RecycleFixedCompositeByteBuf with empty buffers");
        } else {
            ByteBuf b = buffers[0];
            this.buffers = buffers;
            boolean direct = true;
            int nioBufferCount = b.nioBufferCount();
            int capacity = b.readableBytes();
            order = b.order();
            for (int i = 1; i < buffers.length; i++) {
                b = buffers[i];
                if (buffers[i].order() != order) {
                    throw new IllegalArgumentException("All ByteBufs need to have same ByteOrder");
                }
                nioBufferCount += b.nioBufferCount();
                capacity += b.readableBytes();
                if (!b.isDirect()) {
                    direct = false;
                }
            }
            this.nioBufferCount = nioBufferCount;
            this.capacity = capacity;
            this.direct = direct;
        }
        setIndex(0, capacity());
        this.allocator = PulsarByteBufAllocator.DEFAULT;
    }

    @Override
    public boolean isWritable() {
        return false;
    }

    @Override
    public boolean isWritable(int size) {
        return false;
    }

    @Override
    public ByteBuf discardReadBytes() {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setByte(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setByte(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setShort(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setShort(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setShortLE(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setMedium(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setMedium(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setMediumLE(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setInt(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setInt(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setIntLE(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setLong(int index, long value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setLong(int index, long value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setLongLE(int index, long value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public int setBytes(int index, InputStream in, int length) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public int setBytes(int index, ScatteringByteChannel in, int length) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public int setBytes(int index, FileChannel in, long position, int length) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public int maxCapacity() {
        return capacity;
    }

    @Override
    public ByteBuf capacity(int newCapacity) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBufAllocator alloc() {
        return allocator;
    }

    @Override
    public ByteOrder order() {
        return order;
    }

    @Override
    public ByteBuf unwrap() {
        return null;
    }

    @Override
    public boolean isDirect() {
        return direct;
    }

    private Component findComponent(int index) {
        int readable = 0;
        for (int i = 0; i < buffers.length; i++) {
            Component comp = null;
            ByteBuf b = buffers[i];
            if (b instanceof Component) {
                comp = (Component) b;
                b = comp.buf;
            }
            readable += b.readableBytes();
            if (index < readable) {
                if (comp == null) {
                    // Create a new component and store it in the array so it not create a new object
                    // on the next access.
                    comp = Component.newInstance(i, readable - b.readableBytes(), b);
                    buffers[i] = comp;
                }
                return comp;
            }
        }
        throw new IllegalStateException();
    }

    /**
     * Return the {@link ByteBuf} stored at the given index of the array.
     */
    public ByteBuf buffer(int i) {
        ByteBuf b = buffers[i];
        return b instanceof Component ? ((Component) b).buf : b;
    }

    @Override
    public byte getByte(int index) {
        return _getByte(index);
    }

    @Override
    protected byte _getByte(int index) {
        Component c = findComponent(index);
        return c.buf.getByte(index - c.offset);
    }

    @Override
    protected short _getShort(int index) {
        Component c = findComponent(index);
        if (index + 2 <= c.endOffset) {
            return c.buf.getShort(index - c.offset);
        } else if (order() == ByteOrder.BIG_ENDIAN) {
            return (short) ((_getByte(index) & 0xff) << 8 | _getByte(index + 1) & 0xff);
        } else {
            return (short) (_getByte(index) & 0xff | (_getByte(index + 1) & 0xff) << 8);
        }
    }

    @Override
    protected short _getShortLE(int index) {
        Component c = findComponent(index);
        if (index + 2 <= c.endOffset) {
            return c.buf.getShortLE(index - c.offset);
        } else if (order() == ByteOrder.BIG_ENDIAN) {
            return (short) (_getByte(index) & 0xff | (_getByte(index + 1) & 0xff) << 8);
        } else {
            return (short) ((_getByte(index) & 0xff) << 8 | _getByte(index + 1) & 0xff);
        }
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        Component c = findComponent(index);
        if (index + 3 <= c.endOffset) {
            return c.buf.getUnsignedMedium(index - c.offset);
        } else if (order() == ByteOrder.BIG_ENDIAN) {
            return (_getShort(index) & 0xffff) << 8 | _getByte(index + 2) & 0xff;
        } else {
            return _getShort(index) & 0xFFFF | (_getByte(index + 2) & 0xFF) << 16;
        }
    }

    @Override
    protected int _getUnsignedMediumLE(int index) {
        Component c = findComponent(index);
        if (index + 3 <= c.endOffset) {
            return c.buf.getUnsignedMediumLE(index - c.offset);
        } else if (order() == ByteOrder.BIG_ENDIAN) {
            return _getShortLE(index) & 0xffff | (_getByte(index + 2) & 0xff) << 16;
        } else {
            return (_getShortLE(index) & 0xffff) << 8 | _getByte(index + 2) & 0xff;
        }
    }

    @Override
    protected int _getInt(int index) {
        Component c = findComponent(index);
        if (index + 4 <= c.endOffset) {
            return c.buf.getInt(index - c.offset);
        } else if (order() == ByteOrder.BIG_ENDIAN) {
            return (_getShort(index) & 0xffff) << 16 | _getShort(index + 2) & 0xffff;
        } else {
            return _getShort(index) & 0xFFFF | (_getShort(index + 2) & 0xFFFF) << 16;
        }
    }

    @Override
    protected int _getIntLE(int index) {
        Component c = findComponent(index);
        if (index + 4 <= c.endOffset) {
            return c.buf.getIntLE(index - c.offset);
        } else if (order() == ByteOrder.BIG_ENDIAN) {
            return _getShortLE(index) & 0xFFFF | (_getShortLE(index + 2) & 0xFFFF) << 16;
        } else {
            return (_getShortLE(index) & 0xffff) << 16 | _getShortLE(index + 2) & 0xffff;
        }
    }

    @Override
    protected long _getLong(int index) {
        Component c = findComponent(index);
        if (index + 8 <= c.endOffset) {
            return c.buf.getLong(index - c.offset);
        } else if (order() == ByteOrder.BIG_ENDIAN) {
            return (_getInt(index) & 0xffffffffL) << 32 | _getInt(index + 4) & 0xffffffffL;
        } else {
            return _getInt(index) & 0xFFFFFFFFL | (_getInt(index + 4) & 0xFFFFFFFFL) << 32;
        }
    }

    @Override
    protected long _getLongLE(int index) {
        Component c = findComponent(index);
        if (index + 8 <= c.endOffset) {
            return c.buf.getLongLE(index - c.offset);
        } else if (order() == ByteOrder.BIG_ENDIAN) {
            return _getIntLE(index) & 0xffffffffL | (_getIntLE(index + 4) & 0xffffffffL) << 32;
        } else {
            return (_getIntLE(index) & 0xffffffffL) << 32 | _getIntLE(index + 4) & 0xffffffffL;
        }
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        checkDstIndex(index, length, dstIndex, dst.length);
        if (length == 0) {
            return this;
        }

        Component c = findComponent(index);
        int i = c.index;
        int adjustment = c.offset;
        ByteBuf s = c.buf;
        for (;;) {
            int localLength = Math.min(length, s.readableBytes() - (index - adjustment));
            s.getBytes(index - adjustment, dst, dstIndex, localLength);
            index += localLength;
            dstIndex += localLength;
            length -= localLength;
            adjustment += s.readableBytes();
            if (length <= 0) {
                break;
            }
            s = buffer(++i);
        }
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuffer dst) {
        int limit = dst.limit();
        int length = dst.remaining();

        checkIndex(index, length);
        if (length == 0) {
            return this;
        }

        try {
            Component c = findComponent(index);
            int i = c.index;
            int adjustment = c.offset;
            ByteBuf s = c.buf;
            for (;;) {
                int localLength = Math.min(length, s.readableBytes() - (index - adjustment));
                dst.limit(dst.position() + localLength);
                s.getBytes(index - adjustment, dst);
                index += localLength;
                length -= localLength;
                adjustment += s.readableBytes();
                if (length <= 0) {
                    break;
                }
                s = buffer(++i);
            }
        } finally {
            dst.limit(limit);
        }
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
        checkDstIndex(index, length, dstIndex, dst.capacity());
        if (length == 0) {
            return this;
        }

        Component c = findComponent(index);
        int i = c.index;
        int adjustment = c.offset;
        ByteBuf s = c.buf;
        for (;;) {
            int localLength = Math.min(length, s.readableBytes() - (index - adjustment));
            s.getBytes(index - adjustment, dst, dstIndex, localLength);
            index += localLength;
            dstIndex += localLength;
            length -= localLength;
            adjustment += s.readableBytes();
            if (length <= 0) {
                break;
            }
            s = buffer(++i);
        }
        return this;
    }

    @Override
    public int getBytes(int index, GatheringByteChannel out, int length)
            throws IOException {
        int count = nioBufferCount();
        if (count == 1) {
            return out.write(internalNioBuffer(index, length));
        } else {
            long writtenBytes = out.write(nioBuffers(index, length));
            if (writtenBytes > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            } else {
                return (int) writtenBytes;
            }
        }
    }

    @Override
    public int getBytes(int index, FileChannel out, long position, int length)
            throws IOException {
        int count = nioBufferCount();
        if (count == 1) {
            return out.write(internalNioBuffer(index, length), position);
        } else {
            long writtenBytes = 0;
            for (ByteBuffer buf : nioBuffers(index, length)) {
                writtenBytes += out.write(buf, position + writtenBytes);
            }
            if (writtenBytes > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            } else {
                return (int) writtenBytes;
            }
        }
    }

    @Override
    public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
        checkIndex(index, length);
        if (length == 0) {
            return this;
        }

        Component c = findComponent(index);
        int i = c.index;
        int adjustment = c.offset;
        ByteBuf s = c.buf;
        for (;;) {
            int localLength = Math.min(length, s.readableBytes() - (index - adjustment));
            s.getBytes(index - adjustment, out, localLength);
            index += localLength;
            length -= localLength;
            adjustment += s.readableBytes();
            if (length <= 0) {
                break;
            }
            s = buffer(++i);
        }
        return this;
    }

    @Override
    public ByteBuf copy(int index, int length) {
        checkIndex(index, length);
        boolean release = true;
        ByteBuf buf = alloc().buffer(length);
        try {
            buf.writeBytes(this, index, length);
            release = false;
            return buf;
        } finally {
            if (release) {
                buf.release();
            }
        }
    }

    @Override
    public int nioBufferCount() {
        return nioBufferCount;
    }

    @Override
    public ByteBuffer nioBuffer(int index, int length) {
        checkIndex(index, length);
        if (buffers.length == 1) {
            ByteBuf buf = buffer(0);
            if (buf.nioBufferCount() == 1) {
                return buf.nioBuffer(index, length);
            }
        }
        ByteBuffer merged = ByteBuffer.allocate(length).order(order());
        ByteBuffer[] buffers = nioBuffers(index, length);

        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < buffers.length; i++) {
            merged.put(buffers[i]);
        }

        merged.flip();
        return merged;
    }

    @Override
    public ByteBuffer internalNioBuffer(int index, int length) {
        if (buffers.length == 1) {
            return buffer(0).internalNioBuffer(index, length);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer[] nioBuffers(int index, int length) {
        checkIndex(index, length);
        if (length == 0) {
            return EmptyArrays.EMPTY_BYTE_BUFFERS;
        }

        RecyclableArrayList array = RecyclableArrayList.newInstance(buffers.length);
        try {
            Component c = findComponent(index);
            int i = c.index;
            int adjustment = c.offset;
            ByteBuf s = c.buf;
            for (;;) {
                int localLength = Math.min(length, s.readableBytes() - (index - adjustment));
                switch (s.nioBufferCount()) {
                    case 0:
                        throw new UnsupportedOperationException();
                    case 1:
                        array.add(s.nioBuffer(index - adjustment, localLength));
                        break;
                    default:
                        Collections.addAll(array, s.nioBuffers(index - adjustment, localLength));
                }

                index += localLength;
                length -= localLength;
                adjustment += s.readableBytes();
                if (length <= 0) {
                    break;
                }
                s = buffer(++i);
            }

            return array.toArray(new ByteBuffer[0]);
        } finally {
            array.recycle();
        }
    }

    @Override
    public boolean hasArray() {
        switch (buffers.length) {
            case 0:
                return true;
            case 1:
                return buffer(0).hasArray();
            default:
                return false;
        }
    }

    @Override
    public byte[] array() {
        switch (buffers.length) {
            case 0:
                return EmptyArrays.EMPTY_BYTES;
            case 1:
                return buffer(0).array();
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public int arrayOffset() {
        switch (buffers.length) {
            case 0:
                return 0;
            case 1:
                return buffer(0).arrayOffset();
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean hasMemoryAddress() {
        switch (buffers.length) {
            case 0:
                return Unpooled.EMPTY_BUFFER.hasMemoryAddress();
            case 1:
                return buffer(0).hasMemoryAddress();
            default:
                return false;
        }
    }

    @Override
    public long memoryAddress() {
        switch (buffers.length) {
            case 0:
                return Unpooled.EMPTY_BUFFER.memoryAddress();
            case 1:
                return buffer(0).memoryAddress();
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    protected void deallocate() {
        for (int i = 0; i < buffers.length; i++) {
            buffer(i).release();
        }
        for (ByteBuf byteBuf : buffers){
            if (byteBuf instanceof Component){
                Component component = (Component) byteBuf;
                component.recycle();
            }
        }
        this.buffers = EMPTY;
        this.order = ByteOrder.BIG_ENDIAN;
        this.nioBufferCount = 1;
        this.allocator = PulsarByteBufAllocator.DEFAULT;
        this.direct = Unpooled.EMPTY_BUFFER.isDirect();
        readerIndex(0);
        writerIndex(0);
        this.capacity = 0;
        recyclerHandle.recycle(this);
    }

    @Override
    public String toString() {
        String result = super.toString();
        result = result.substring(0, result.length() - 1);
        return result + ", components=" + buffers.length + ')';
    }

    private static final class Component extends PulsarWrappedByteBuf {

        private int index;
        private int offset;
        private int endOffset;
        private final Recycler.Handle<Component> handle;

        private static final Recycler<Component> COMPONENT_RECYCLER = new Recycler<Component>() {
            @Override
            protected Component newObject(Handle<Component> handle) {
                return new Component(handle);
            }
        };

        public static Component newInstance(int index, int offset, ByteBuf buf){
            Component component = COMPONENT_RECYCLER.get();
            component.init(index, offset, buf);
            return component;
        }

        public void recycle() {
            if (super.buf == null){
                log.warn("RecycleFixedCompositeByteBuf Component: has already recycled, skip current recycle");
                return;
            }
            super.buf = null;
            this.index = 0;
            this.offset = 0;
            this.endOffset = 0;
            this.handle.recycle(this);
        }

        private Component(Recycler.Handle<Component> handle){
            this.handle = handle;
        }

        private void init(int index, int offset, ByteBuf buf){
            super.buf = buf;
            this.index = index;
            this.offset = offset;
            this.endOffset = offset + buf.readableBytes();
        }
    }
}
