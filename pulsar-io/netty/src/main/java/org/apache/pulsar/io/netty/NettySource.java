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

import java.util.Map;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.netty.server.NettyServer;

/**
 * A simple Netty Source connector to listen for incoming messages and write to user-defined Pulsar topic.
 */
@Connector(
    name = "netty",
    type = IOType.SOURCE,
    help = "A simple Netty Source connector to listen for incoming messages and write to user-defined Pulsar topic",
    configClass = NettySourceConfig.class)
public class NettySource extends PushSource<byte[]> {

    private NettyServer nettyServer;
    private Thread thread;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        NettySourceConfig nettySourceConfig = NettySourceConfig.load(config);
        if (nettySourceConfig.getType() == null
                || nettySourceConfig.getHost() == null
                || nettySourceConfig.getPort() <= 0) {
            throw new IllegalArgumentException("Required property not set.");
        }

        thread = new Thread(new PulsarServerRunnable(nettySourceConfig, this));
        thread.start();
    }

    @Override
    public void close() throws Exception {
        nettyServer.shutdownGracefully();
    }

    private class PulsarServerRunnable implements Runnable {

        private NettySourceConfig nettySourceConfig;
        private NettySource nettySource;

        public PulsarServerRunnable(NettySourceConfig nettySourceConfig, NettySource nettySource) {
            this.nettySourceConfig = nettySourceConfig;
            this.nettySource = nettySource;
        }

        @Override
        public void run() {
            nettyServer = new NettyServer.Builder()
                    .setType(NettyServer.Type.valueOf(nettySourceConfig.getType().toUpperCase()))
                    .setHost(nettySourceConfig.getHost())
                    .setPort(nettySourceConfig.getPort())
                    .setNumberOfThreads(nettySourceConfig.getNumberOfThreads())
                    .setNettySource(nettySource)
                    .build();

            nettyServer.run();
        }
    }

}
