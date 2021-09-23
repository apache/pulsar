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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

/**
 * A collection of loaded handlers.
 */
@Slf4j
public class ProtocolHandlers implements AutoCloseable {

    /**
     * Load the protocol handlers for the given <tt>protocol</tt> list.
     *
     * @param conf the pulsar broker service configuration
     * @return the collection of protocol handlers
     */
    public static ProtocolHandlers load(ProxyConfiguration conf) throws IOException {
        ProtocolHandlerDefinitions definitions =
                ProtocolHandlerUtils.searchForHandlers(
                        conf.getProxyProtocolHandlerDirectory(), conf.getNarExtractionDirectory());

        ImmutableMap.Builder<String, ProxyExtensionWithClassLoader> handlersBuilder = ImmutableMap.builder();

        conf.getProxyMessagingProtocols().forEach(protocol -> {

            ProtocolHandlerMetadata definition = definitions.handlers().get(protocol);
            if (null == definition) {
                throw new RuntimeException("No protocol handler is found for protocol `" + protocol
                    + "`. Available protocols are : " + definitions.handlers());
            }

            ProxyExtensionWithClassLoader handler;
            try {
                handler = ProtocolHandlerUtils.load(definition, conf.getNarExtractionDirectory());
            } catch (IOException e) {
                log.error("Failed to load the protocol handler for protocol `" + protocol + "`", e);
                throw new RuntimeException("Failed to load the protocol handler for protocol `" + protocol + "`");
            }

            if (!handler.accept(protocol)) {
                handler.close();
                log.error("Malformed protocol handler found for protocol `" + protocol + "`");
                throw new RuntimeException("Malformed protocol handler found for protocol `" + protocol + "`");
            }

            handlersBuilder.put(protocol, handler);
            log.info("Successfully loaded protocol handler for protocol `{}`", protocol);
        });

        return new ProtocolHandlers(handlersBuilder.build());
    }

    private final Map<String, ProxyExtensionWithClassLoader> handlers;

    ProtocolHandlers(Map<String, ProxyExtensionWithClassLoader> handlers) {
        this.handlers = handlers;
    }

    /**
     * Return the handler for the provided <tt>protocol</tt>.
     *
     * @param protocol the protocol to use
     * @return the protocol handler to handle the provided protocol
     */
    public ProxyExtension protocol(String protocol) {
        ProxyExtensionWithClassLoader h = handlers.get(protocol);
        if (null == h) {
            return null;
        } else {
            return h.getHandler();
        }
    }

    public void initialize(ProxyConfiguration conf) throws Exception {
        for (ProxyExtension handler : handlers.values()) {
            handler.initialize(conf);
        }
    }

    public Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> newChannelInitializers() {
        Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> channelInitializers = Maps.newHashMap();
        Set<InetSocketAddress> addresses = Sets.newHashSet();

        for (Map.Entry<String, ProxyExtensionWithClassLoader> handler : handlers.entrySet()) {
            Map<InetSocketAddress, ChannelInitializer<SocketChannel>> initializers =
                handler.getValue().newChannelInitializers();
            initializers.forEach((address, initializer) -> {
                if (!addresses.add(address)) {
                    log.error("Protocol handler for `{}` attempts to use {} for its listening port."
                        + " But it is already occupied by other message protocols.",
                        handler.getKey(), address);
                    throw new RuntimeException("Protocol handler for `" + handler.getKey()
                        + "` attempts to use " + address + " for its listening port. But it is"
                        + " already occupied by other messaging protocols");
                }
                channelInitializers.put(handler.getKey(), initializers);
            });
        }

        return channelInitializers;
    }

    public void start(ProxyService service) {
        handlers.values().forEach(handler -> handler.start(service));
    }

    @Override
    public void close() {
        handlers.values().forEach(ProxyExtension::close);
    }
}
