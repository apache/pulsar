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
package org.apache.pulsar.broker.protocol;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.nar.NarClassLoader;

/**
 * A protocol handler with its classloader.
 */
@Slf4j
@Data
@RequiredArgsConstructor
class ProtocolHandlerWithClassLoader implements ProtocolHandler {

    private final ProtocolHandler handler;
    private final NarClassLoader classLoader;

    @Override
    public String protocolName() {
        return handler.protocolName();
    }

    @Override
    public boolean accept(String protocol) {
        return handler.accept(protocol);
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        handler.initialize(conf);
    }

    @Override
    public String getProtocolDataToAdvertise() {
        return handler.getProtocolDataToAdvertise();
    }

    @Override
    public void start(BrokerService service) {
        handler.start(service);
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        return handler.newChannelInitializers();
    }

    @Override
    public void close() {
        handler.close();
        try {
            classLoader.close();
        } catch (IOException e) {
            log.warn("Failed to close the protocol handler class loader", e);
        }
    }
}
