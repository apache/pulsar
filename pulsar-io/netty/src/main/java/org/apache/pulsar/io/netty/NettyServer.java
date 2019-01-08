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
package org.apache.pulsar.io.netty;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty Tcp Server to accept any incoming data.
 */
public class NettyServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private String host;
    private int port;
    private NettySource nettySource;
    private int numberOfThreads;
    private ChannelInitializer channelInitializer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private NettyServer(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.nettySource = builder.nettySource;
        this.numberOfThreads = builder.numberOfThreads;
        this.channelInitializer = builder.channelInitializer;
    }

    public void run() {
        try {
            bossGroup = new NioEventLoopGroup(this.numberOfThreads);
            workerGroup = new NioEventLoopGroup(this.numberOfThreads);

            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(channelInitializer)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture channelFuture = serverBootstrap.bind(this.host, this.port).sync();
            channelFuture.channel().closeFuture().sync();
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

    /**
     * Pulsar Server Builder.
     */
    public static class Builder {

        private String host;
        private int port;
        private NettySource nettySource;
        private int numberOfThreads;
        private ChannelInitializer channelInitializer;

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setNettyTcpSource(NettySource nettySource) {
            this.nettySource = nettySource;
            return this;
        }

        public Builder setNumberOfThreads(int numberOfThreads) {
            this.numberOfThreads = numberOfThreads;
            return this;
        }

        public Builder setChannelInitializer(ChannelInitializer channelInitializer) {
            this.channelInitializer = channelInitializer;
            return this;
        }

        public NettyServer build() {
            checkArgument(StringUtils.isNotBlank(host), "host cannot be blank/null");
            checkArgument(this.port >= 1024, "port must be set equal or bigger than 1024");
            checkNotNull(this.nettySource, "nettySource must be set");
            checkNotNull(this.channelInitializer, "Protocol must be set");
            checkArgument(this.numberOfThreads > 0,
                    "numberOfThreads must be set as positive");

            return new NettyServer(this);
        }
    }

}