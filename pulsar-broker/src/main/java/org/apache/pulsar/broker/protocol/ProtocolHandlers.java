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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;

/**
 * A collection of loaded handlers.
 */
@Slf4j
public class ProtocolHandlers implements AutoCloseable {

    @Getter
    private final Map<SocketAddress, String> endpoints = new ConcurrentHashMap<>();

    /**
     * Load the protocol handlers for the given <tt>protocol</tt> list.
     *
     * @param conf the pulsar broker service configuration
     * @return the collection of protocol handlers
     */
    public static ProtocolHandlers load(ServiceConfiguration conf) throws IOException {
        ProtocolHandlerDefinitions definitions =
                ProtocolHandlerUtils.searchForHandlers(
                        conf.getProtocolHandlerDirectory(), conf.getNarExtractionDirectory());

        ImmutableMap.Builder<String, ProtocolHandlerWithClassLoader> handlersBuilder = ImmutableMap.builder();

        conf.getMessagingProtocols().forEach(protocol -> {

            ProtocolHandlerMetadata definition = definitions.handlers().get(protocol);
            if (null == definition) {
                throw new RuntimeException("No protocol handler is found for protocol `" + protocol
                    + "`. Available protocols are : " + definitions.handlers());
            }

            ProtocolHandlerWithClassLoader handler;
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

    private final Map<String, ProtocolHandlerWithClassLoader> handlers;

    ProtocolHandlers(Map<String, ProtocolHandlerWithClassLoader> handlers) {
        this.handlers = handlers;
    }

    /**
     * Return the handler for the provided <tt>protocol</tt>.
     *
     * @param protocol the protocol to use
     * @return the protocol handler to handle the provided protocol
     */
    public ProtocolHandler protocol(String protocol) {
        ProtocolHandlerWithClassLoader h = handlers.get(protocol);
        if (null == h) {
            return null;
        } else {
            return h.getHandler();
        }
    }

    public void initialize(ServiceConfiguration conf) throws Exception {
        for (ProtocolHandler handler : handlers.values()) {
            handler.initialize(conf);
        }
    }

    public Map<String, String> getProtocolDataToAdvertise() {
        return handlers.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().getProtocolDataToAdvertise()
            ));
    }

    public Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> newChannelInitializers() {
        Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> channelInitializers = Maps.newHashMap();
        Set<InetSocketAddress> addresses = Sets.newHashSet();

        for (Map.Entry<String, ProtocolHandlerWithClassLoader> handler : handlers.entrySet()) {
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
                endpoints.put(address, handler.getKey());
            });
        }

        return channelInitializers;
    }

    public void start(BrokerService service) {
        handlers.values().forEach(handler -> handler.start(service));
    }

    @Override
    public void close() {
        handlers.values().forEach(ProtocolHandler::close);
    }
}
