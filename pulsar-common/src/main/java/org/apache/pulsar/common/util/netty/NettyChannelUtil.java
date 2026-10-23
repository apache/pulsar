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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.VoidChannelPromise;

/**
 * Contains utility methods for working with Netty Channels.
 */
public final class NettyChannelUtil {

    private NettyChannelUtil() {
    }

    /**
     * Write and flush the message to the channel.
     *
     * The promise is an instance of {@link VoidChannelPromise} that properly propagates exceptions up to the pipeline.
     * Netty has many ad-hoc optimization if the promise is an instance of {@link VoidChannelPromise}.
     * Lastly, it reduces pollution of useless {@link io.netty.channel.ChannelPromise} objects created
     * by the default write and flush method {@link ChannelOutboundInvoker#writeAndFlush(Object)}.
     * See https://stackoverflow.com/q/54169262 and https://stackoverflow.com/a/9030420 for more details.
     *
     * @param ctx channel's context
     * @param msg buffer to write in the channel
     */
    public static void writeAndFlushWithVoidPromise(ChannelOutboundInvoker ctx, ByteBuf msg) {
        ctx.writeAndFlush(msg, ctx.voidPromise());
    }

    /**
     * Write and flush the message to the channel and the close the channel.
     *
     * This method is particularly helpful when the connection is in an invalid state
     * and therefore a new connection must be created to continue.
     *
     * @param ctx channel's context
     * @param msg buffer to write in the channel
     */
    public static void writeAndFlushWithClosePromise(ChannelOutboundInvoker ctx, ByteBuf msg) {
        ctx.writeAndFlush(msg).addListener(ChannelFutureListener.CLOSE);
    }

}
