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
package org.apache.pulsar.io.netty.http;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;

/**
 * Netty Channel Initializer to register HTTP decoder and handler.
 */
public class NettyHttpChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;
    private ChannelInboundHandlerAdapter handler;

    public NettyHttpChannelInitializer(ChannelInboundHandlerAdapter handler, SslContext sslCtx) {
        this.handler = handler;
        this.sslCtx = sslCtx;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        if (sslCtx != null) {
            socketChannel.pipeline().addLast(sslCtx.newHandler(socketChannel.alloc()));
        }
        socketChannel.pipeline().addLast(new HttpServerCodec());
        socketChannel.pipeline().addLast(this.handler);
    }
}
