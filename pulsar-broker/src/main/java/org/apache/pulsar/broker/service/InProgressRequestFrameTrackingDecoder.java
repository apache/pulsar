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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.OptionalProxyProtocolDecoder;

/**
 * Tracks the capacity of cumulation buffers while frames derived from them are retained downstream.
 *
 * <p>Each untracked input must transfer a distinct allocation's ownership exactly once. The built-in transport,
 * TLS and PROXY protocol handlers satisfy this contract; downstream views carry their root directly. Validate
 * upstream handlers before reading and after controlled pipeline changes. Arbitrary upstream insertion after
 * activation is unsupported: Netty provides no general pipeline-change notification, so every such mutation
 * must invoke {@link #validateUpstreamHandlers()} before forwarding input. Do not add per-request pipeline scans.
 *
 * <p>This implementation depends on Netty's {@link ByteToMessageDecoder} cumulation, frame extraction, and
 * reference-counting and partial-frame continuation behavior. When upgrading Netty, review this class against
 * the new implementation (including the allowed upstream handlers) and rerun the root-buffer lifecycle and
 * flow-control tests to verify that derived views retain and release their tracked root correctly and incomplete
 * frames can still make progress.
 */
final class InProgressRequestFrameTrackingDecoder extends LengthFieldBasedFrameDecoder {
    private final InProgressRequestFrameMemoryLimiter limiter;
    private final InProgressRequestFrameMemoryController controller;
    private ChannelHandlerContext context;
    private InProgressRequestFrameMemoryNotifier notifier;
    private boolean upstreamValidated;
    private boolean receivedBufferInReadCycle;
    private boolean decodedFrameInReadCycle;
    private boolean continuationReadPending;

    InProgressRequestFrameTrackingDecoder(int maxMessageSize, InProgressRequestFrameMemoryLimiter limiter) {
        super(maxMessageSize + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4);
        this.limiter = limiter;
        this.controller = limiter.getController();
        setCumulator(this::cumulate);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        context = ctx;
        // TRACKING lasts for the broker's lifetime. Keep root accounting for metrics, but avoid allocating
        // callbacks and registering with the shared notifier when this decoder can never throttle a connection.
        if (!controller.isTrackingOnly()) {
            notifier = controller.getEventLoopNotifier(ctx.executor());
            notifier.add(this, () -> setThrottled(limiter.isThrottled()));
        }
        super.handlerAdded(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        validateUpstreamHandlers();
        super.channelActive(ctx);
        // Registration may precede activation and ServerCnx's initial auto-read setup.
        if (notifier != null) {
            setThrottled(limiter.isThrottled());
        }
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        if (notifier != null) {
            notifier.remove(this);
            notifier = null;
            setThrottled(false);
        }
        context = null;
        super.handlerRemoved0(ctx);
    }

    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        // Netty must extract frames through the tracked cumulation's retainedSlice so every view carries its
        // root. Creating a new root here would count the same cumulation once for each extracted frame.
        if (!(buffer instanceof TrackedByteBuf)) {
            throw new IllegalStateException("Cannot extract a frame from an untracked cumulation");
        }
        ByteBuf frame = super.extractFrame(ctx, buffer, index, length);
        decodedFrameInReadCycle = true;
        return frame;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Also covers adding this decoder to an already-active channel. Once validated, normal reads do not
        // inspect the pipeline; OptionalProxyProtocolDecoder explicitly validates its one-time transition.
        if (!upstreamValidated) {
            try {
                validateUpstreamHandlers();
            } catch (RuntimeException | Error error) {
                ReferenceCountUtil.release(msg);
                throw error;
            }
        }
        if (msg instanceof ByteBuf buffer) {
            receivedBufferInReadCycle = true;
            continuationReadPending = false;
            ByteBuf tracked = track(buffer);
            if (!controller.isTrackingOnly()) {
                controller.onInboundRead();
            }
            super.channelRead(ctx, tracked);
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        boolean decodedFrame = decodedFrameInReadCycle;
        // ByteToMessageDecoder requests a continuation when input produced no frame and auto-read is off.
        // Remember that demand so a later throttle transition does not request the same continuation again.
        if (receivedBufferInReadCycle && !decodedFrame && !ctx.channel().config().isAutoRead()) {
            continuationReadPending = true;
        }
        receivedBufferInReadCycle = false;
        decodedFrameInReadCycle = false;
        super.channelReadComplete(ctx);
        if (decodedFrame) {
            // Netty does not continue a read that emitted a complete frame followed by a partial one. Under
            // memory-only throttling, leaving that partial frame pinned would prevent the limit from clearing.
            readIncompleteFrame();
        }
    }

    void readIncompleteFrame() {
        ChannelHandlerContext ctx = context;
        if (ctx == null || ctx.isRemoved() || !ctx.channel().isActive() || ctx.channel().config().isAutoRead()
                || continuationReadPending || actualReadableBytes() == 0) {
            return;
        }
        ServerCnx cnx = (ServerCnx) ctx.pipeline().get(ServerCnx.SERVER_CNX_HANDLER_NAME);
        if (cnx != null && cnx.getThrottleTracker().isReadPaused()
                && !cnx.getThrottleTracker().isMessageDeliveryPaused()) {
            // Bypass the downstream flow-control queue only to complete an already-started frame. Auto-read
            // stays off, and LengthFieldBasedFrameDecoder still enforces the maximum frame length.
            continuationReadPending = true;
            ctx.read();
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
            if (cumulation instanceof TrackedByteBuf tracked) {
                tracked.root.updateCapacity();
            }
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
            return track(expanded);
        } finally {
            toRelease.release();
        }
    }

    // Called on the channel event loop at activation and by the PROXY protocol pipeline-change validator.
    void validateUpstreamHandlers() {
        ChannelHandlerContext ctx = context;
        if (ctx == null) {
            throw new IllegalStateException("Request-frame tracking decoder is not installed");
        }
        upstreamValidated = false;
        int previousStage = -1;
        for (var entry : ctx.pipeline()) {
            ChannelHandler handler = entry.getValue();
            if (handler == this) {
                upstreamValidated = true;
                return;
            }
            if (!(handler instanceof ChannelInboundHandler)) {
                continue;
            }
            // Exact classes deliberately exclude subclasses that could split/replay a shared allocation.
            // OptionalProxyProtocolDecoder is replaced by HAProxyMessageDecoder before validation is invoked.
            Class<?> type = handler.getClass();
            int stage;
            if (type == FlushConsolidationHandler.class) {
                stage = 0;
            } else if (type == SslHandler.class) {
                stage = 1;
            } else if (type == OptionalProxyProtocolDecoder.class || type == HAProxyMessageDecoder.class) {
                stage = 2;
            } else {
                stage = -1;
            }
            if (stage < 0 || stage <= previousStage) {
                ctx.close();
                throw new IllegalStateException("Unsupported upstream handler or order for request-frame tracking: "
                        + entry.getKey() + " (" + type.getName() + ")");
            }
            previousStage = stage;
        }
        ctx.close();
        throw new IllegalStateException("Request-frame tracking decoder is missing from the pipeline");
    }

    private ByteBuf track(ByteBuf buffer) {
        if (buffer instanceof TrackedByteBuf) {
            return buffer;
        }
        ByteBuf allocation = buffer;
        while (allocation.unwrap() != null) {
            allocation = allocation.unwrap();
        }
        return new TrackedByteBuf(buffer, new RootBuffer(allocation, controller));
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

    private static final class RootBuffer {
        private static final AtomicIntegerFieldUpdater<RootBuffer> REFERENCES_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(RootBuffer.class, "references");
        private final ByteBuf buffer;
        private final InProgressRequestFrameMemoryController controller;
        // Pooled retained views have independent Netty refCnts. Count the explicit owners of this root across
        // those views, while leaving their actual reference counting and deallocation entirely to Netty.
        private volatile int references = 1;
        // Only the decoder's event loop changes capacity, while it still owns a reference. Publishing that
        // reference's decrement orders prior capacity/accounting updates before the last downstream release.
        private long capacity;

        RootBuffer(ByteBuf buffer, InProgressRequestFrameMemoryController controller) {
            this.buffer = buffer;
            this.controller = controller;
            this.capacity = buffer.isDirect() ? buffer.capacity() : 0;
            controller.acquire(this.capacity);
        }

        void retainReference(int increment) {
            REFERENCES_UPDATER.addAndGet(this, increment);
        }

        void updateCapacity() {
            long newCapacity = buffer.isDirect() ? buffer.capacity() : 0;
            long delta = newCapacity - capacity;
            capacity = newCapacity;
            if (delta > 0) {
                controller.acquire(delta);
            } else if (delta < 0) {
                controller.release(-delta);
            }
        }

        boolean release(ByteBuf view, int decrement) {
            // Let Netty release/recycle the actual buffer first. Independent pooled views have their own
            // Netty refCnts, so the aggregate owner count decides which release returns the tracked capacity.
            boolean deallocated = view.release(decrement);
            if (REFERENCES_UPDATER.addAndGet(this, -decrement) == 0) {
                controller.release(capacity);
            }
            return deallocated;
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
            root.retainReference(1);
            return this;
        }

        @Override
        public ByteBuf retain(int increment) {
            buf.retain(increment);
            root.retainReference(increment);
            return this;
        }

        @Override
        public ByteBuf duplicate() {
            return new TrackedByteBuf(buf.duplicate(), root);
        }

        @Override
        public ByteBuf retainedDuplicate() {
            ByteBuf view = buf.retainedDuplicate();
            root.retainReference(1);
            return new TrackedByteBuf(view, root);
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
            ByteBuf view = buf.retainedSlice();
            root.retainReference(1);
            return new TrackedByteBuf(view, root);
        }

        @Override
        public ByteBuf retainedSlice(int index, int length) {
            ByteBuf view = buf.retainedSlice(index, length);
            root.retainReference(1);
            return new TrackedByteBuf(view, root);
        }

        @Override
        public ByteBuf readSlice(int length) {
            return new TrackedByteBuf(buf.readSlice(length), root);
        }

        @Override
        public ByteBuf readRetainedSlice(int length) {
            ByteBuf view = buf.readRetainedSlice(length);
            root.retainReference(1);
            return new TrackedByteBuf(view, root);
        }

        @Override
        public ByteBuf asReadOnly() {
            return new TrackedByteBuf(buf.asReadOnly(), root);
        }

        @Override
        public ByteBuf order(ByteOrder endianness) {
            return endianness == order() ? this : new TrackedByteBuf(buf.order(endianness), root);
        }

        @Override
        public boolean release() {
            return root.release(buf, 1);
        }

        @Override
        public boolean release(int decrement) {
            return root.release(buf, decrement);
        }
    }
}
