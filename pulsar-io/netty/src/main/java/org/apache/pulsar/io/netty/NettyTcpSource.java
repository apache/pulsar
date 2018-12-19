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

import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.netty.tcp.server.NettyTcpServer;
import java.util.Map;

/**
 * A simple Netty Tcp Source connector to listen Tcp messages and write to user-defined Pulsar topic
 */
@Connector(
        name = "tcp",
        type = IOType.SOURCE,
        help = "A simple Netty Tcp Source connector to listen Tcp messages and write to user-defined Pulsar topic",
        configClass = NettyTcpSourceConfig.class)
public class NettyTcpSource extends PushSource<byte[]> {

    private NettyTcpServer nettyTcpServer;
    private Thread thread;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        NettyTcpSourceConfig nettyTcpSourceConfig = NettyTcpSourceConfig.load(config);

        thread = new Thread(new PulsarTcpServerRunnable(nettyTcpSourceConfig, this));
        thread.start();
    }

    @Override
    public void close() throws Exception {
        nettyTcpServer.shutdownGracefully();
    }

    private class PulsarTcpServerRunnable implements Runnable {

        private NettyTcpSourceConfig nettyTcpSourceConfig;
        private NettyTcpSource nettyTcpSource;

        public PulsarTcpServerRunnable(NettyTcpSourceConfig nettyTcpSourceConfig, NettyTcpSource nettyTcpSource) {
            this.nettyTcpSourceConfig = nettyTcpSourceConfig;
            this.nettyTcpSource = nettyTcpSource;
        }

        @Override
        public void run() {
            nettyTcpServer = new NettyTcpServer.Builder()
                .setHost(nettyTcpSourceConfig.getHost())
                .setPort(nettyTcpSourceConfig.getPort())
                .setNumberOfThreads(nettyTcpSourceConfig.getNumberOfThreads())
                .setNettyTcpSource(nettyTcpSource)
                .build();

            nettyTcpServer.run();
        }
    }

}
