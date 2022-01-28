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
package org.apache.pulsar.proxy.extensions;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.Map;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

/**
 * The extension interface for support additional extensions on Pulsar Proxy.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public interface ProxyExtension extends AutoCloseable {

    /**
     * Returns the unique extension name. For example, `kafka-v2` for extension for Kafka v2 protocol.
     */
    String extensionName();

    /**
     * Verify if the extension can handle the given <tt>extension name</tt>.
     *
     * @param extension the extension to verify
     * @return true if the extension can handle the given extension name, otherwise false.
     */
    boolean accept(String extension);

    /**
     * Initialize the extension when the extension is constructed from reflection.
     *
     * <p>The initialize should initialize all the resources required for serving the extension
     * but don't start those resources until {@link #start(ProxyService)} is called.
     *
     * @param conf proxy service configuration
     * @throws Exception when fail to initialize the extension.
     */
    void initialize(ProxyConfiguration conf) throws Exception;

    /**
     * Start the extension with the provided proxy service.
     *
     * <p>The proxy service provides the accesses to the Pulsar Proxy components.
     *
     * @param service the broker service to start with.
     */
    void start(ProxyService service);

    /**
     * Create the list of channel initializers for the ports that this extension
     * will listen on.
     *
     * <p>NOTE: this method is called after {@link #start(ProxyService)}.
     *
     * @return the list of channel initializers for the ports that this extension listens on.
     */
    Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers();

    @Override
    void close();
}

