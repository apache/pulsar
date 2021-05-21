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
import java.util.Map;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

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
     * handler but don't start those resources until {@link #start(BrokerService)} is called.
     *
     * @param conf broker service configuration
     * @throws Exception when fail to initialize the protocol handler.
     */
    void initialize(ServiceConfiguration conf) throws Exception;

    /**
     * Retrieve the protocol related data to advertise as part of
     * {@link org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData}.
     *
     * <p>For example, when implementing a Kafka protocol handler, you need to advertise
     * corresponding Kafka listeners so that Pulsar brokers understand how to give back
     * the listener information when handling metadata requests.
     *
     * <p>NOTE: this method is called after {@link #initialize(ServiceConfiguration)}
     * and before {@link #start(BrokerService)}.
     *
     * @return the protocol related data to be advertised as part of LocalBrokerData.
     */
    String getProtocolDataToAdvertise();

    /**
     * Start the protocol handler with the provided broker service.
     *
     * <p>The broker service provides the accesses to the Pulsar components such as load
     * manager, namespace service, managed ledger and etc.
     *
     * @param service the broker service to start with.
     */
    void start(BrokerService service);

    /**
     * Create the list of channel initializers for the ports that this protocol handler
     * will listen on.
     *
     * <p>NOTE: this method is called after {@link #start(BrokerService)}.
     *
     * @return the list of channel initializers for the ports that this protocol handler listens on.
     */
    Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers();

    @Override
    void close();
}

