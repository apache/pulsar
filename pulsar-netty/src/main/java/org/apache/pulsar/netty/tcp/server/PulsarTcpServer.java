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
package org.apache.pulsar.netty.tcp.server;

import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.netty.serde.PulsarSerializer;

import java.util.Optional;

/**
 * Pulsar Tcp Server to accept any incoming data through Tcp.
 */
public class PulsarTcpServer<T> {

    private String host;
    private int port;
    private String serviceUrl;
    private String topicName;
    private PulsarSerializer<T> pulsarSerializer;
    private Optional<ChannelInboundHandlerAdapter> decoder;
    private int numberOfThreads;

    private PulsarTcpServer(Builder<T> builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.serviceUrl = builder.serviceUrl;
        this.topicName = builder.topicName;
        this.pulsarSerializer = builder.pulsarSerializer;
        this.decoder = Optional.ofNullable(builder.decoder);
        this.numberOfThreads = builder.numberOfThreads;
    }

    public void run() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup(this.numberOfThreads);
        EventLoopGroup workerGroup = new NioEventLoopGroup(this.numberOfThreads);
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new PulsarChannelInitializer(this.decoder,
                            new PulsarTcpServerHandler(this.serviceUrl, this.topicName, this.pulsarSerializer)))
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture channelFuture = serverBootstrap.bind(this.host, this.port).sync();
            channelFuture.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    /**
     * Pulsar Tcp Server Builder.
     */
    public static class Builder<T> {

        private String host;
        private int port;
        private String serviceUrl;
        private String topicName;
        private PulsarSerializer pulsarSerializer;
        private ChannelInboundHandlerAdapter decoder;
        private int numberOfThreads;

        public Builder<T> setHost(String host) {
            Preconditions.checkArgument(StringUtils.isNotBlank(host), "host cannot be blank");
            this.host = host;
            return this;
        }

        public Builder<T> setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> setServiceUrl(String serviceUrl) {
            Preconditions.checkArgument(StringUtils.isNotBlank(serviceUrl), "serviceUrl cannot be blank");
            this.serviceUrl = serviceUrl;
            return this;
        }

        public Builder<T> setTopicName(String topicName) {
            Preconditions.checkArgument(StringUtils.isNotBlank(topicName), "topicName cannot be blank");
            this.topicName = topicName;
            return this;
        }

        public Builder<T> setPulsarSerializer(PulsarSerializer<T> pulsarSerializer) {
            this.pulsarSerializer = Preconditions.checkNotNull(pulsarSerializer, "pulsarSerializer cannot be null");
            return this;
        }

        public Builder<T> setDecoder(ChannelInboundHandlerAdapter decoder) {
            this.decoder = decoder;
            return this;
        }

        public Builder<T> setNumberOfThreads(int numberOfThreads) {
            Preconditions.checkArgument(numberOfThreads > 0, "numberOfThreads must be positive");
            this.numberOfThreads = numberOfThreads;
            return this;
        }

        public PulsarTcpServer<T> build() {
            Preconditions.checkNotNull(this.host, "host must be set");
            Preconditions.checkArgument(this.port >= 1024, "port must be set equal or bigger than 1024");
            Preconditions.checkNotNull(this.serviceUrl, "serviceUrl must be set");
            Preconditions.checkNotNull(this.topicName, "topicName must be set");
            Preconditions.checkNotNull(this.pulsarSerializer, "pulsarSerializer must be set");
            Preconditions.checkArgument(this.numberOfThreads > 0,
                    "numberOfThreads must be set as positive");

            return new PulsarTcpServer<>(this);
        }
    }

}