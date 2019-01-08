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

import io.netty.channel.ChannelInitializer;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.netty.http.NettyHttpChannelInitializer;
import org.apache.pulsar.io.netty.http.NettyHttpServerHandler;
import org.apache.pulsar.io.netty.tcp.NettyTcpChannelInitializer;
import org.apache.pulsar.io.netty.tcp.NettyTcpServerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple Netty Source connector that can listen for incoming TCP or HTTP messages
 * and write to user-defined Pulsar topic.
 */
@Connector(
        name = "netty",
        type = IOType.SOURCE,
        help = "A simple connector that can listen for incoming TCP or HTTP messages"
                + " and write to user-defined Pulsar topic",
        configClass = NettySourceConfig.class)
public class NettySource extends PushSource<byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(NettySource.class);

    private NettyServer nettyServer;
    private Thread thread;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        NettySourceConfig nettSourceConfig = NettySourceConfig.load(config);

        thread = new Thread(new PulsarTcpServerRunnable(nettSourceConfig, this));
        thread.start();
    }

    @Override
    public void close() throws Exception {
        nettyServer.shutdownGracefully();
    }

    private class PulsarTcpServerRunnable implements Runnable {

        private NettySourceConfig nettySourceConfig;
        private NettySource nettySource;

        public PulsarTcpServerRunnable(NettySourceConfig nettySourceConfig, NettySource nettySource) {
            this.nettySourceConfig = nettySourceConfig;
            this.nettySource = nettySource;
        }

        @Override
        public void run() {
            nettyServer = new NettyServer.Builder()
                .setHost(nettySourceConfig.getHost())
                .setPort(nettySourceConfig.getPort())
                .setNumberOfThreads(nettySourceConfig.getNumberOfThreads())
                .setNettyTcpSource(nettySource)
                .setChannelInitializer(getInitializer(nettySourceConfig))
                .build();

            nettyServer.run();
        }

        private ChannelInitializer getInitializer(NettySourceConfig config) {
            if (config == null || StringUtils.isBlank(config.getProtocol())) {
                throw new IllegalArgumentException("Invalid configuration provided.");
            }

            if ("TCP".equals(config.getProtocol().toUpperCase())) {
                logger.info("Using TCP protocol");
                return new NettyTcpChannelInitializer(new NettyTcpServerHandler(this.nettySource), null);
            } else if ("HTTP".equals(config.getProtocol().toUpperCase())) {
                logger.info("Using HTTP protocol");
                return new NettyHttpChannelInitializer(new NettyHttpServerHandler(this.nettySource), null);
            } else {
                throw new IllegalArgumentException("Unsupported protocol specified.");
            }
        }
    }

}
