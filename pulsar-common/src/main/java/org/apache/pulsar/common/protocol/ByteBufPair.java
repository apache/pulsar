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
package org.apache.pulsar.common.protocol;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

/**
 * ByteBuf holder that contains 2 buffers.
 */
public final class ByteBufPair extends AbstractReferenceCounted {

    private ByteBuf b1;
    private ByteBuf b2;
    private final Handle<ByteBufPair> recyclerHandle;

    private static final Recycler<ByteBufPair> RECYCLER = new Recycler<ByteBufPair>() {
        @Override
        protected ByteBufPair newObject(Recycler.Handle<ByteBufPair> handle) {
            return new ByteBufPair(handle);
        }
    };

    private ByteBufPair(Handle<ByteBufPair> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    /**
     * Get a new {@link ByteBufPair} from the pool and assign 2 buffers to it.
     *
     * <p>The buffers b1 and b2 lifecycles are now managed by the ByteBufPair:
     * when the {@link ByteBufPair} is deallocated, b1 and b2 will be released as well.
     *
     * @param b1
     * @param b2
     * @return
     */
    public static ByteBufPair get(ByteBuf b1, ByteBuf b2) {
        ByteBufPair buf = RECYCLER.get();
        buf.setRefCnt(1);
        buf.b1 = b1;
        buf.b2 = b2;
        return buf;
    }

    public ByteBuf getFirst() {
        return b1;
    }

    public ByteBuf getSecond() {
        return b2;
    }

    public int readableBytes() {
        return b1.readableBytes() + b2.readableBytes();
    }

    /**
     * @return a single buffer with the content of both individual buffers
     */
    @VisibleForTesting
    public static ByteBuf coalesce(ByteBufPair pair) {
        ByteBuf b = Unpooled.buffer(pair.readableBytes());
        b.writeBytes(pair.b1, pair.b1.readerIndex(), pair.b1.readableBytes());
        b.writeBytes(pair.b2, pair.b2.readerIndex(), pair.b2.readableBytes());
        return b;
    }

    @Override
    protected void deallocate() {
        b1.release();
        b2.release();
        b1 = b2 = null;
        recyclerHandle.recycle(this);
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        b1.touch(hint);
        b2.touch(hint);
        return this;
    }

    public static final Encoder ENCODER = new Encoder();
    public static final CopyingEncoder COPYING_ENCODER = new CopyingEncoder();

    @Sharable
    @SuppressWarnings("checkstyle:JavadocType")
    public static class Encoder extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof ByteBufPair) {
                ByteBufPair b = (ByteBufPair) msg;

                // Write each buffer individually on the socket. The retain() here is needed to preserve the fact that
                // ByteBuf are automatically released after a write. If the ByteBufPair ref count is increased and it
                // gets written multiple times, the individual buffers refcount should be reflected as well.
                try {
                    ctx.write(b.getFirst().retainedDuplicate(), ctx.voidPromise());
                    ctx.write(b.getSecond().retainedDuplicate(), promise);
                } finally {
                    ReferenceCountUtil.safeRelease(b);
                }
            } else {
                ctx.write(msg, promise);
            }
        }
    }

    @Sharable
    @SuppressWarnings("checkstyle:JavadocType")
    public static class CopyingEncoder extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof ByteBufPair) {
                ByteBufPair b = (ByteBufPair) msg;

                // Some handlers in the pipeline will modify the bytebufs passed in to them (i.e. SslHandler).
                // For these handlers, we need to pass a copy of the buffers as the source buffers may be cached
                // for multiple requests.
                try {
                    ctx.write(b.getFirst().copy(), ctx.voidPromise());
                    ctx.write(b.getSecond().copy(), promise);
                } finally {
                    ReferenceCountUtil.safeRelease(b);
                }
            } else {
                ctx.write(msg, promise);
            }
        }
    }

}
