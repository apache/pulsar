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
package org.apache.pulsar.io.netty.server;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.netty.NettySource;
import org.apache.pulsar.io.netty.http.NettyHttpChannelInitializer;
import org.apache.pulsar.io.netty.http.NettyHttpServerHandler;
import org.apache.pulsar.io.netty.tcp.NettyTCPChannelInitializer;
import org.apache.pulsar.io.netty.tcp.NettyTCPServerHandler;
import org.apache.pulsar.io.netty.udp.NettyUDPChannelInitializer;
import org.apache.pulsar.io.netty.udp.NettyUDPServerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty Server to accept incoming data via the configured type.
 */
public class NettyServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private Type type;
    private String host;
    private int port;
    private NettySource nettySource;
    private int numberOfThreads;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private NettyServer(Builder builder) {
        this.type = builder.type;
        this.host = builder.host;
        this.port = builder.port;
        this.nettySource = builder.nettySource;
        this.numberOfThreads = builder.numberOfThreads;
    }

    public void run() {
        try {
            switch (type) {
                case UDP:
                    runUdp();
                    break;
                case HTTP:
                    runHttp();
                    break;
                case TCP:
                default:
                    runTcp();
                    break;
            }
        } catch (Exception ex) {
            logger.error("Error occurred when Netty Server is running", ex);
        } finally {
            shutdownGracefully();
        }
    }

    public void shutdownGracefully() {
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }

    private void runUdp() throws InterruptedException {
        workerGroup = new NioEventLoopGroup(this.numberOfThreads);
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioDatagramChannel.class);
        bootstrap.handler(new NettyUDPChannelInitializer(new NettyUDPServerHandler(this.nettySource)))
                .option(ChannelOption.SO_BACKLOG, 1024);

        ChannelFuture channelFuture = bootstrap.bind(this.host, this.port).sync();
        channelFuture.channel().closeFuture().sync();
    }

    private void runTcp() throws InterruptedException {
        ServerBootstrap serverBootstrap = getServerBootstrap(
                new NettyTCPChannelInitializer(new NettyTCPServerHandler(this.nettySource)));

        ChannelFuture channelFuture = serverBootstrap.bind(this.host, this.port).sync();
        channelFuture.channel().closeFuture().sync();
    }

    private void runHttp() throws InterruptedException {
        ServerBootstrap serverBootstrap = getServerBootstrap(
                new NettyHttpChannelInitializer(new NettyHttpServerHandler(this.nettySource), null));

        ChannelFuture channelFuture = serverBootstrap.bind(this.host, this.port).sync();
        channelFuture.channel().closeFuture().sync();
    }

    private ServerBootstrap getServerBootstrap(ChannelHandler childHandler) {
        bossGroup = new NioEventLoopGroup(this.numberOfThreads);
        workerGroup = new NioEventLoopGroup(this.numberOfThreads);
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup);
        serverBootstrap.channel(NioServerSocketChannel.class);
        serverBootstrap.childHandler(childHandler)
        .option(ChannelOption.SO_BACKLOG, 1024)
        .childOption(ChannelOption.SO_KEEPALIVE, true);

        return serverBootstrap;
    }

    /**
     * Pulsar Netty Server Builder.
     */
    public static class Builder {

        private Type type;
        private String host;
        private int port;
        private NettySource nettySource;
        private int numberOfThreads;

        public Builder setType(Type type) {
            this.type = type;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setNettySource(NettySource nettySource) {
            this.nettySource = nettySource;
            return this;
        }

        public Builder setNumberOfThreads(int numberOfThreads) {
            this.numberOfThreads = numberOfThreads;
            return this;
        }

        public NettyServer build() {
            checkNotNull(this.type, "type cannot be blank/null");
            checkArgument(StringUtils.isNotBlank(host), "host cannot be blank/null");
            checkArgument(this.port >= 1024, "port must be set equal or bigger than 1024");
            checkNotNull(this.nettySource, "nettySource must be set");
            checkArgument(this.numberOfThreads > 0,
                    "numberOfThreads must be set as positive");

            return new NettyServer(this);
        }
    }

    /**
     * Network protocol.
     */
    public enum Type {
        TCP,
        UDP,
        HTTP
    }
}
