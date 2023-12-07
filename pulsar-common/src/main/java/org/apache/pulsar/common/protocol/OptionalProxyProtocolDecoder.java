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
package org.apache.pulsar.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.ProtocolDetectionResult;
import io.netty.handler.codec.ProtocolDetectionState;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import lombok.extern.slf4j.Slf4j;

/**
 * Decoder that added whether a new connection is prefixed with the ProxyProtocol.
 * More about the ProxyProtocol see: http://www.haproxy.org/download/1.8/doc/proxy-protocol.txt.
 */
@Slf4j
public class OptionalProxyProtocolDecoder extends ChannelInboundHandlerAdapter {

    public static final String NAME = "optional-proxy-protocol-decoder";

    ByteBuf cumulatedByteBuf;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        doChannelRead(ctx, msg);
    }

    /**
     * @return the msg has been handled correctly, so not need to keep it.
     */
    public void doChannelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            // Combine cumulated buffers.
            ByteBuf buf = (ByteBuf) msg;
            if (cumulatedByteBuf != null) {
                buf = new CompositeByteBuf(ByteBufAllocator.DEFAULT, false, 2, cumulatedByteBuf, buf);
            }

            ProtocolDetectionResult<HAProxyProtocolVersion> result = HAProxyMessageDecoder.detectProtocol(buf);
            if (result.state() == ProtocolDetectionState.NEEDS_MORE_DATA) {
                // Accumulate data if need more data to detect the protocol.
                cumulatedByteBuf = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(ctx.alloc(),
                        cumulatedByteBuf == null ? Unpooled.EMPTY_BUFFER : cumulatedByteBuf, (ByteBuf) msg);
                return;
            }
            if (result.state() == ProtocolDetectionState.DETECTED) {
                ctx.pipeline().addAfter(NAME, null, new HAProxyMessageDecoder());
                ctx.pipeline().remove(this);
            }
            try {
                super.channelRead(ctx, buf);
            } finally {
                // After the cumulated buffer has been handle correctly, release it.
                if (cumulatedByteBuf != null && !cumulatedByteBuf.isReadable()) {
                    cumulatedByteBuf.release();
                    cumulatedByteBuf = null;
                }
            }
            return;
        }
        super.channelRead(ctx, msg);
    }
}
