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
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            return handler.protocolName();
        }
    }

    @Override
    public boolean accept(String protocol) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            return handler.accept(protocol);
        }
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            handler.initialize(conf);
        }
    }

    @Override
    public String getProtocolDataToAdvertise() {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            return handler.getProtocolDataToAdvertise();
        }
    }

    @Override
    public void start(BrokerService service) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            handler.start(service);
        }
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            return handler.newChannelInitializers();
        }
    }

    @Override
    public void close() {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            handler.close();
        }

        try {
            classLoader.close();
        } catch (IOException e) {
            log.warn("Failed to close the protocol handler class loader", e);
        }
    }

    /**
     * Help to switch the class loader of current thread to the NarClassLoader, and change it back when it's done.
     * With the help of try-with-resources statement, the code would be cleaner than using try finally every time.
     */
    private static class ClassLoaderSwitcher implements AutoCloseable {
        private final ClassLoader prevClassLoader;

        ClassLoaderSwitcher(ClassLoader classLoader) {
            prevClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(classLoader);
        }

        @Override
        public void close() {
            Thread.currentThread().setContextClassLoader(prevClassLoader);
        }
    }
}
