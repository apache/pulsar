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
package org.apache.pulsar.socks5;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.socksx.v5.Socks5CommandRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5InitialRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthRequestDecoder;
import io.netty.handler.codec.socksx.v5.Socks5ServerEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.socks5.config.Socks5Config;
import org.apache.pulsar.socks5.handler.CommandRequestHandler;
import org.apache.pulsar.socks5.handler.IdleHandler;
import org.apache.pulsar.socks5.handler.InitialRequestHandler;
import org.apache.pulsar.socks5.handler.PasswordAuthRequestHandler;

@Slf4j
public class Socks5Server {

    @Getter
    private EventLoopGroup boss = new NioEventLoopGroup();
    private EventLoopGroup worker = new NioEventLoopGroup();

    private final Socks5Config socks5Config;

    public Socks5Server(Socks5Config socks5Config) {
        this.socks5Config = socks5Config;
    }

    public void start() throws Exception {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new IdleStateHandler(90, 90, 0));
                        ch.pipeline().addLast(new IdleHandler());
                        ch.pipeline().addLast(Socks5ServerEncoder.DEFAULT);
                        ch.pipeline().addLast(new Socks5InitialRequestDecoder());
                        ch.pipeline().addLast(new InitialRequestHandler(socks5Config));
                        if (socks5Config.isEnableAuth()) {
                            ch.pipeline().addLast(new Socks5PasswordAuthRequestDecoder());
                            ch.pipeline().addLast(new PasswordAuthRequestHandler());
                        }
                        ch.pipeline().addLast(new Socks5CommandRequestDecoder());
                        ch.pipeline().addLast(new CommandRequestHandler(Socks5Server.this));
                    }
                });
        ChannelFuture future = bootstrap.bind(socks5Config.getPort()).sync();
        if (log.isInfoEnabled()) {
            log.info("bind port : {}", socks5Config.getPort());
        }
        future.channel().closeFuture().sync();
    }

    public void shutdown() {
        boss.shutdownGracefully();
        worker.shutdownGracefully();
    }

}
