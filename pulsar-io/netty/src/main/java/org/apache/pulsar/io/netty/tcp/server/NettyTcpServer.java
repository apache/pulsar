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
package org.apache.pulsar.io.netty.tcp.server;

import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.netty.NettyTcpSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty Tcp Server to accept any incoming data through Tcp.
 */
public class NettyTcpServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyTcpServer.class);

    private String host;
    private int port;
    private NettyTcpSource nettyTcpSource;
    private int numberOfThreads;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private NettyTcpServer(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.nettyTcpSource = builder.nettyTcpSource;
        this.numberOfThreads = builder.numberOfThreads;
    }

    public void run() {
        try {
            bossGroup = new NioEventLoopGroup(this.numberOfThreads);
            workerGroup = new NioEventLoopGroup(this.numberOfThreads);

            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new NettyChannelInitializer(new NettyTcpServerHandler(this.nettyTcpSource)))
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture channelFuture = serverBootstrap.bind(this.host, this.port).sync();
            channelFuture.channel().closeFuture().sync();
        } catch(Exception ex) {
            logger.error("Error occurred when Netty Tcp Server is running", ex);
        } finally {
            shutdownGracefully();
        }
    }

    public void shutdownGracefully() {
        if (workerGroup != null)
            workerGroup.shutdownGracefully();
        if (bossGroup != null)
            bossGroup.shutdownGracefully();
    }

    /**
     * Pulsar Tcp Server Builder.
     */
    public static class Builder {

        private String host;
        private int port;
        private NettyTcpSource nettyTcpSource;
        private int numberOfThreads;

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setNettyTcpSource(NettyTcpSource nettyTcpSource) {
            this.nettyTcpSource = nettyTcpSource;
            return this;
        }

        public Builder setNumberOfThreads(int numberOfThreads) {
            this.numberOfThreads = numberOfThreads;
            return this;
        }

        public NettyTcpServer build() {
            Preconditions.checkArgument(StringUtils.isNotBlank(host), "host cannot be blank/null");
            Preconditions.checkArgument(this.port >= 1024, "port must be set equal or bigger than 1024");
            Preconditions.checkNotNull(this.nettyTcpSource, "nettyTcpSource must be set");
            Preconditions.checkArgument(this.numberOfThreads > 0,
                    "numberOfThreads must be set as positive");

            return new NettyTcpServer(this);
        }
    }

}