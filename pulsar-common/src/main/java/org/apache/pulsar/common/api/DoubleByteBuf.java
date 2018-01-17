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
package org.apache.pulsar.common.api;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCounted;

/**
 * ByteBuf that holds 2 buffers. Similar to {@see CompositeByteBuf} but doesn't allocate list to hold them.
 */
public final class DoubleByteBuf extends AbstractReferenceCounted {

    private ByteBuf b1;
    private ByteBuf b2;
    private final Handle<DoubleByteBuf> recyclerHandle;

    private static final Recycler<DoubleByteBuf> RECYCLER = new Recycler<DoubleByteBuf>() {
        @Override
        protected DoubleByteBuf newObject(Recycler.Handle<DoubleByteBuf> handle) {
            return new DoubleByteBuf(handle);
        }
    };

    private DoubleByteBuf(Handle<DoubleByteBuf> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public static DoubleByteBuf get(ByteBuf b1, ByteBuf b2) {
        DoubleByteBuf buf = RECYCLER.get();
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
    public ByteBuf coalesce() {
        ByteBuf b = Unpooled.buffer(readableBytes());
        b1.getBytes(0, b);
        b2.getBytes(0, b);
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

    @Sharable
    public static class Encoder extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof DoubleByteBuf) {
                DoubleByteBuf b = (DoubleByteBuf) msg;

                ctx.write(b.getFirst(), ctx.voidPromise());
                ctx.write(b.getSecond(), promise);
            } else {
                ctx.write(msg, promise);
            }
        }
    }

}