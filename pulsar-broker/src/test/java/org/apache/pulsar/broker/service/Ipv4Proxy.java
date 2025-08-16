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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;

public class Ipv4Proxy {
    @Getter
    private final int localPort;
    private final String backendServerHost;
    private final int backendServerPort;
    private final EventLoopGroup serverGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private ChannelFuture localServerChannel;
    private ServerBootstrap serverBootstrap = new ServerBootstrap();
    private List<Channel> frontChannels = Collections.synchronizedList(new ArrayList<>());
    private AtomicBoolean rejectAllConnections = new AtomicBoolean();

    public Ipv4Proxy(int localPort, String backendServerHost, int backendServerPort) {
        this.localPort = localPort;
        this.backendServerHost = backendServerHost;
        this.backendServerPort = backendServerPort;
    }

    public synchronized void startup() throws InterruptedException {
        localServerChannel = serverBootstrap.group(serverGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new FrontendHandler());
                }
            }).childOption(ChannelOption.AUTO_READ, false)
            .bind(localPort).sync();
    }

    public synchronized void stop() throws InterruptedException{
        localServerChannel.channel().close().sync();
        serverGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    private static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

    public void disconnectFrontChannels() throws InterruptedException {
        for (Channel channel : new ArrayList<>(frontChannels)) {
            channel.close();
        }
    }

    public void rejectAllConnections() throws InterruptedException {
        rejectAllConnections.set(true);
    }

    public void unRejectAllConnections() throws InterruptedException {
        rejectAllConnections.set(false);
    }

    private class FrontendHandler extends ChannelInboundHandlerAdapter {

        private Channel backendChannel;

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            if (rejectAllConnections.get()) {
                ctx.close();
                return;
            }
            final Channel frontendChannel = ctx.channel();
            frontChannels.add(frontendChannel);
            Bootstrap backendBootstrap = new Bootstrap();
            backendBootstrap.group(frontendChannel.eventLoop())
                    .channel(ctx.channel().getClass())
                    .handler(new BackendHandler(frontendChannel))
                    .option(ChannelOption.AUTO_READ, false);
            ChannelFuture backendChannelFuture =
                    backendBootstrap.connect(Ipv4Proxy.this.backendServerHost, Ipv4Proxy.this.backendServerPort);
            backendChannel = backendChannelFuture.channel();
            backendChannelFuture.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    frontendChannel.read();
                } else {
                    frontChannels.remove(frontendChannel);
                    frontendChannel.close();
                }
            });
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            if (backendChannel.isActive()) {
                backendChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        ctx.channel().read();
                    } else {
                        future.channel().close();
                    }
                });
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            frontChannels.remove(ctx.channel());
            if (backendChannel != null) {
                closeOnFlush(backendChannel);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            closeOnFlush(ctx.channel());
        }
    }

    private class BackendHandler extends ChannelInboundHandlerAdapter {

        private final Channel frontendChannel;

        public BackendHandler(Channel inboundChannel) {
            this.frontendChannel = inboundChannel;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            if (!frontendChannel.isActive()) {
                closeOnFlush(ctx.channel());
            } else {
                ctx.read();
            }
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            frontendChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    ctx.channel().read();
                } else {
                    future.channel().close();
                }
            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            closeOnFlush(frontendChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            closeOnFlush(ctx.channel());
        }
    }
}
