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
package org.apache.pulsar.common.util.netty;

import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.ObjectPool.Handle;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * The {@link PulsarFlowControlHandler} ensures that only one message per {@code read()} is sent downstream.
 *
 * <p>Classes such as {@link ByteToMessageDecoder} are free to emit as many events as they like for any given input.
 * A channel's auto reading configuration doesn't usually apply in these scenarios. This is causing problems in
 * downstream {@link ChannelHandler}s that would like to hold subsequent events while they're processing one event.
 *
 * <p>This class is derived from {@code io.netty.handler.flow.FlowControlHandler} of Netty 4.2.14.Final
 * (Copyright 2016 The Netty Project, licensed under the Apache License, version 2.0), which is behaviorally
 * identical to the implementation in Netty 4.1.135.Final. Up to Netty
 * 4.1.135/4.2.14, the dequeue loop of Netty's handler re-checked {@link ChannelConfig#isAutoRead()} before
 * releasing each queued message, so a downstream handler calling {@code setAutoRead(false)} from inside
 * {@code channelRead} stopped the delivery of queued messages immediately. Netty 4.2.15 (netty/netty#16837,
 * backport of netty/netty#15053; the equivalent 4.1 change is netty/netty#16912) rewrote the handler to decide
 * once up front: with auto-read enabled it dequeues the entire queue and ignores auto-read changes made mid-drain
 * by downstream handlers. Pulsar's reactive request throttling (see {@code ServerCnxThrottleTracker} in the broker)
 * pauses a connection by disabling auto-read while processing a message and relies on the delivery of queued
 * messages stopping immediately; with Netty's rewritten handler, the whole queued backlog would be delivered
 * regardless, defeating throttling (and rate limiting) of already-buffered traffic. This copy preserves the
 * pre-4.2.15 per-message auto-read behavior.
 *
 * @see ChannelConfig#setAutoRead(boolean)
 */
public class PulsarFlowControlHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PulsarFlowControlHandler.class);

    private final boolean releaseMessages;

    private RecyclableArrayDeque queue;

    private ChannelConfig config;

    private boolean shouldConsume;

    public PulsarFlowControlHandler() {
        this(true);
    }

    public PulsarFlowControlHandler(boolean releaseMessages) {
        this.releaseMessages = releaseMessages;
    }

    /**
     * Determine if the underlying {@link Queue} is empty. This method exists for
     * testing, debugging and inspection purposes and it is not Thread safe!
     */
    boolean isQueueEmpty() {
        return queue == null || queue.isEmpty();
    }

    /**
     * Releases all messages and destroys the {@link Queue}.
     */
    private void destroy() {
        if (queue != null) {

            if (!queue.isEmpty()) {
                logger.trace("Non-empty queue: {}", queue);

                if (releaseMessages) {
                    Object msg;
                    while ((msg = queue.poll()) != null) {
                        ReferenceCountUtil.safeRelease(msg);
                    }
                }
            }

            queue.recycle();
            queue = null;
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        config = ctx.channel().config();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        if (!isQueueEmpty()) {
            dequeue(ctx, queue.size());
        }
        destroy();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        destroy();
        ctx.fireChannelInactive();
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        if (dequeue(ctx, 1) == 0) {
            // It seems no messages were consumed. We need to read() some
            // messages from upstream and once one arrives it need to be
            // relayed to downstream to keep the flow going.
            shouldConsume = true;
            ctx.read();
        } else if (config.isAutoRead()) {
            ctx.read();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (queue == null) {
            queue = RecyclableArrayDeque.newInstance();
        }

        queue.offer(msg);

        // We just received one message. Do we need to relay it regardless
        // of the auto reading configuration? The answer is yes if this
        // method was called as a result of a prior read() call.
        int minConsume = shouldConsume ? 1 : 0;
        shouldConsume = false;

        dequeue(ctx, minConsume);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (isQueueEmpty()) {
            ctx.fireChannelReadComplete();
        } else {
            // Don't relay completion events from upstream as they
            // make no sense in this context. See dequeue() where
            // a new set of completion events is being produced.
        }
    }

    /**
     * Dequeues one or many (or none) messages depending on the channel's auto
     * reading state and returns the number of messages that were consumed from
     * the internal queue.
     *
     * <p>The {@code minConsume} argument is used to force {@code dequeue()} into
     * consuming that number of messages regardless of the channel's auto
     * reading configuration.
     *
     * @see #read(ChannelHandlerContext)
     * @see #channelRead(ChannelHandlerContext, Object)
     */
    private int dequeue(ChannelHandlerContext ctx, int minConsume) {
        int consumed = 0;

        // fireChannelRead(...) may call ctx.read() and so this method may reentrance. Because of this we need to
        // check if queue was set to null in the meantime and if so break the loop.
        while (queue != null && (consumed < minConsume || config.isAutoRead())) {
            Object msg = queue.poll();
            if (msg == null) {
                break;
            }

            ++consumed;
            ctx.fireChannelRead(msg);
        }

        // We're firing a completion event every time one (or more)
        // messages were consumed and the queue ended up being drained
        // to an empty state.
        if (queue != null && queue.isEmpty()) {
            queue.recycle();
            queue = null;

            if (consumed > 0) {
                ctx.fireChannelReadComplete();
            }
        }

        return consumed;
    }

    /**
     * A recyclable {@link ArrayDeque}.
     */
    private static final class RecyclableArrayDeque extends ArrayDeque<Object> {

        private static final long serialVersionUID = 0L;

        /**
         * A value of {@code 2} should be a good choice for most scenarios.
         */
        private static final int DEFAULT_NUM_ELEMENTS = 2;

        private static final Recycler<RecyclableArrayDeque> RECYCLER =
                new Recycler<RecyclableArrayDeque>() {
                    @Override
                    protected RecyclableArrayDeque newObject(Handle<RecyclableArrayDeque> handle) {
                        return new RecyclableArrayDeque(DEFAULT_NUM_ELEMENTS, handle);
                    }
                };

        public static RecyclableArrayDeque newInstance() {
            return RECYCLER.get();
        }

        private final Handle<RecyclableArrayDeque> handle;

        private RecyclableArrayDeque(int numElements, Handle<RecyclableArrayDeque> handle) {
            super(numElements);
            this.handle = handle;
        }

        public void recycle() {
            clear();
            handle.recycle(this);
        }
    }
}
