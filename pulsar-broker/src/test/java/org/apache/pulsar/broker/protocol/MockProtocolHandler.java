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
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;

class MockProtocolHandler implements ProtocolHandler {

    public static final String NAME = "mock";

    @Override
    public String protocolName() {
        return NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return NAME.equals(protocol);
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        // no-op
    }

    @Override
    public String getProtocolDataToAdvertise() {
        return "mock-data";
    }

    @Override
    public void start(BrokerService service) {
        // no-op
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        return Collections.emptyMap();
    }

    @Override
    public void close() {
        // no-op
    }
}
