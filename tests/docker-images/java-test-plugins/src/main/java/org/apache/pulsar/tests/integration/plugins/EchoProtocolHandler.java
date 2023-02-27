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
package org.apache.pulsar.tests.integration.plugins;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;


public class EchoProtocolHandler implements ProtocolHandler {

    private ServiceConfiguration conf;

    private BrokerService brokerService;

    @Override
    public String protocolName() {
        return "echo";
    }

    @Override
    public boolean accept(String protocol) {
        return protocolName().equals(protocol.toLowerCase());
    }

    @Override
    public void initialize(ServiceConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public String getProtocolDataToAdvertise() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName()
                    + ":" + conf.getProperties().getProperty("echoServerPort");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start(BrokerService service) {
        Objects.requireNonNull(conf, "initialize(ServiceConfiguration) has not been called before start");
        this.brokerService = service;
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        // Although this protocol handler does not need the BrokerService instance,
        // we should verify that `start` was called in the right order
        Objects.requireNonNull(brokerService,
                "start(BrokerService) has not been called before newChannelInitializers");
        InetSocketAddress address = getEchoAddress();
        ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<>() {
            @Override
            protected void initChannel(SocketChannel socketChannel) {
                socketChannel.pipeline().addLast(new EchoChannelHandler());
            }
        };
        return Collections.singletonMap(address, initializer);
    }

    private InetSocketAddress getEchoAddress() {
        String hostAndPort = getProtocolDataToAdvertise();
        String[] parsed = hostAndPort.split(":");
        return new InetSocketAddress(parsed[0], Integer.parseInt(parsed[1]));
    }

    @Override
    public void close() {

    }
}
