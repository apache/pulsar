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
package org.apache.pulsar.proxy.protocol;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * The protocol handler interface for support additional protocols on Pulsar brokers.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public interface ProtocolHandler extends AutoCloseable {

    /**
     * Returns the unique protocol name. For example, `kafka-v2` for protocol handler for Kafka v2 protocol.
     */
    String protocolName();

    /**
     * Verify if the protocol can speak the given <tt>protocol</tt>.
     *
     * @param protocol the protocol to verify
     * @return true if the protocol handler can handle the given protocol, otherwise false.
     */
    boolean accept(String protocol);

    /**
     * Initialize the protocol handler when the protocol is constructed from reflection.
     *
     * <p>The initialize should initialize all the resources required for serving the protocol
     * handler but don't start those resources until {@link #start(ProxyService)} is called.
     *
     * @param conf broker service configuration
     * @throws Exception when fail to initialize the protocol handler.
     */
    void initialize(ProxyConfiguration conf) throws Exception;

    /**
     * Start the protocol handler with the provided broker service.
     *
     * <p>The broker service provides the accesses to the Pulsar components such as load
     * manager, namespace service, managed ledger and etc.
     *
     * @param service the broker service to start with.
     */
    void start(ProxyService service);

    /**
     * Create the list of channel initializers for the ports that this protocol handler
     * will listen on.
     *
     * <p>NOTE: this method is called after {@link #start(ProxyService)}.
     *
     * @return the list of channel initializers for the ports that this protocol handler listens on.
     */
    Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers();

    @Override
    void close();
}

