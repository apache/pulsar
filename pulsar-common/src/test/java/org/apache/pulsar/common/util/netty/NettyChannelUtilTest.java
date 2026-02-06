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

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPromise;
import io.netty.channel.VoidChannelPromise;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

public class NettyChannelUtilTest {

    @Test
    public void testWriteAndFlushWithVoidPromise() {
        final ChannelOutboundInvoker ctx = mock(ChannelOutboundInvoker.class);
        final VoidChannelPromise voidChannelPromise = mock(VoidChannelPromise.class);
        when(ctx.voidPromise()).thenReturn(voidChannelPromise);
        final byte[] data = "test".getBytes(StandardCharsets.UTF_8);
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, data.length);
        try {
            NettyChannelUtil.writeAndFlushWithVoidPromise(ctx, byteBuf);
            verify(ctx).writeAndFlush(same(byteBuf), same(voidChannelPromise));
            verify(ctx).voidPromise();
        } finally {
            byteBuf.release();
        }
    }

    @Test
    public void testWriteAndFlushWithClosePromise() {
        final ChannelOutboundInvoker ctx = mock(ChannelOutboundInvoker.class);
        final ChannelPromise promise = mock(ChannelPromise.class);

        final byte[] data = "test".getBytes(StandardCharsets.UTF_8);
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, data.length);
        when(ctx.writeAndFlush(same(byteBuf))).thenReturn(promise);
        try {
            NettyChannelUtil.writeAndFlushWithClosePromise(ctx, byteBuf);
            verify(ctx).writeAndFlush(same(byteBuf));
            verify(promise).addListener(same(ChannelFutureListener.CLOSE));
        } finally {
            byteBuf.release();
        }
    }
}