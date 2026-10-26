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
package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.WrappedByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Tracks the capacity of cumulation buffers while frames derived from them are retained downstream.
 *
 * <p>This implementation depends on Netty's {@link ByteToMessageDecoder} cumulation, frame extraction, and
 * reference-counting behavior. When upgrading Netty, review this class against the new implementation and rerun
 * the root-buffer lifecycle tests to verify that all derived views retain and release their tracked root correctly.
 */
final class InProgressRequestFrameTrackingDecoder extends LengthFieldBasedFrameDecoder {
    private final InProgressRequestFrameMemoryLimiter limiter;
    private final Map<ByteBuf, RootBuffer> roots = new IdentityHashMap<>();
    private ChannelHandlerContext context;

    InProgressRequestFrameTrackingDecoder(int maxMessageSize, InProgressRequestFrameMemoryLimiter limiter) {
        super(maxMessageSize + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4);
        this.limiter = limiter;
        setCumulator(this::cumulate);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        context = ctx;
        limiter.registerThrottleStateListener(ctx.channel().id(),
                throttled -> executeSafely(ctx, () -> setThrottled(throttled)));
        super.handlerAdded(ctx);
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        limiter.unregisterThrottleStateListener(ctx.channel().id());
        setThrottled(false);
        context = null;
        super.handlerRemoved0(ctx);
    }

    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        // Normal decoder input is a TrackedByteBuf, whose retainedSlice implementation propagates the root. Keep
        // this defensive wrapper in case a future cumulation path supplies an unwrapped buffer; losing the root
        // there would make the frame's direct-memory allocation invisible until it is released.
        RootBuffer root = rootFor(buffer);
        ByteBuf frame = super.extractFrame(ctx, buffer, index, length);
        return frame instanceof TrackedByteBuf ? frame : new TrackedByteBuf(frame, root);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf buffer) {
            RootBuffer root = rootFor(buffer);
            limiter.updateAutomaticLimitEnabledState();
            super.channelRead(ctx, new TrackedByteBuf(buffer, root));
        } else {
            super.channelRead(ctx, msg);
        }
    }

    private ByteBuf cumulate(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in) {
        // Keep the branch conditions and release behavior aligned with Netty's MERGE_CUMULATOR. Unlike the Netty
        // implementation, expandCumulation wraps the newly allocated buffer so an incomplete frame is accounted for.
        // Update this method when upgrading Netty's ByteToMessageDecoder cumulation behavior.
        if (cumulation == in) {
            in.release();
            return cumulation;
        }
        if (!cumulation.isReadable() && in.isContiguous()) {
            cumulation.release();
            return in;
        }
        try {
            int required = in.readableBytes();
            if (required > cumulation.maxWritableBytes()
                    || required > cumulation.maxFastWritableBytes() && cumulation.refCnt() > 1
                    || cumulation.isReadOnly()) {
                return expandCumulation(alloc, cumulation, in);
            }
            cumulation.writeBytes(in, in.readerIndex(), required);
            in.readerIndex(in.writerIndex());
            return cumulation;
        } finally {
            in.release();
        }
    }

    private ByteBuf expandCumulation(ByteBufAllocator alloc, ByteBuf oldCumulation, ByteBuf in) {
        int oldBytes = oldCumulation.readableBytes();
        int newBytes = in.readableBytes();
        int totalBytes = oldBytes + newBytes;
        ByteBuf expanded = alloc.buffer(alloc.calculateNewCapacity(totalBytes, Integer.MAX_VALUE));
        ByteBuf toRelease = expanded;
        try {
            expanded.setBytes(0, oldCumulation, oldCumulation.readerIndex(), oldBytes)
                    .setBytes(oldBytes, in, in.readerIndex(), newBytes)
                    .writerIndex(totalBytes);
            in.readerIndex(in.writerIndex());
            toRelease = oldCumulation;
            return new TrackedByteBuf(expanded, rootFor(expanded));
        } finally {
            toRelease.release();
        }
    }

    private RootBuffer rootFor(ByteBuf buffer) {
        if (buffer instanceof TrackedByteBuf trackedByteBuf) {
            return trackedByteBuf.root;
        }
        synchronized (roots) {
            RootBuffer root = roots.get(buffer);
            if (root == null) {
                root = new RootBuffer(buffer, buffer.capacity());
                roots.put(buffer, root);
            }
            return root;
        }
    }

    private static void executeSafely(ChannelHandlerContext ctx, Runnable task) {
        try {
            ctx.executor().execute(task);
        } catch (RejectedExecutionException ignored) {
            // The channel is closing and no longer needs a throttle state update.
        }
    }

    private void setThrottled(boolean throttled) {
        if (context == null || !context.channel().isActive()) {
            return;
        }
        ServerCnx serverCnx = (ServerCnx) context.pipeline().get(ServerCnx.SERVER_CNX_HANDLER_NAME);
        if (serverCnx == null) {
            return;
        }
        if (throttled) {
            serverCnx.getThrottleTracker().markThrottled(
                    ServerCnxThrottleTracker.ThrottleType.InProgressRequestFrameMemoryExceeded);
        } else {
            serverCnx.getThrottleTracker().unmarkThrottled(
                    ServerCnxThrottleTracker.ThrottleType.InProgressRequestFrameMemoryExceeded);
        }
    }

    private final class RootBuffer {
        private final ByteBuf buffer;
        private final long capacity;

        RootBuffer(ByteBuf buffer, long capacity) {
            this.buffer = buffer;
            this.capacity = buffer.isDirect() ? capacity : 0;
            limiter.acquire(this.capacity);
        }

        void beforeRelease() {
            synchronized (roots) {
                roots.remove(buffer, this);
            }
        }

        void released() {
            limiter.release(capacity);
        }
    }

    private static final class TrackedByteBuf extends WrappedByteBuf {
        private final RootBuffer root;

        TrackedByteBuf(ByteBuf buffer, RootBuffer root) {
            super(buffer);
            this.root = root;
        }

        @Override
        public ByteBuf retain() {
            buf.retain();
            return this;
        }

        @Override
        public ByteBuf retain(int increment) {
            buf.retain(increment);
            return this;
        }

        @Override
        public ByteBuf duplicate() {
            return new TrackedByteBuf(buf.duplicate(), root);
        }

        @Override
        public ByteBuf retainedDuplicate() {
            return new TrackedByteBuf(buf.retainedDuplicate(), root);
        }

        @Override
        public ByteBuf slice() {
            return new TrackedByteBuf(buf.slice(), root);
        }

        @Override
        public ByteBuf slice(int index, int length) {
            return new TrackedByteBuf(buf.slice(index, length), root);
        }

        @Override
        public ByteBuf retainedSlice() {
            return new TrackedByteBuf(buf.retainedSlice(), root);
        }

        @Override
        public ByteBuf retainedSlice(int index, int length) {
            return new TrackedByteBuf(buf.retainedSlice(index, length), root);
        }

        @Override
        public ByteBuf readSlice(int length) {
            return new TrackedByteBuf(buf.readSlice(length), root);
        }

        @Override
        public ByteBuf readRetainedSlice(int length) {
            return new TrackedByteBuf(buf.readRetainedSlice(length), root);
        }

        @Override
        public ByteBuf asReadOnly() {
            return new TrackedByteBuf(buf.asReadOnly(), root);
        }

        @Override
        public boolean release() {
            if (buf.refCnt() == 1) {
                root.beforeRelease();
            }
            boolean released = buf.release();
            if (released) {
                root.released();
            }
            return released;
        }

        @Override
        public boolean release(int decrement) {
            if (buf.refCnt() == decrement) {
                root.beforeRelease();
            }
            boolean released = buf.release(decrement);
            if (released) {
                root.released();
            }
            return released;
        }
    }
}
