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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

/**
 * A extension with its classloader.
 */
@Slf4j
@Data
@RequiredArgsConstructor
class ProxyExtensionWithClassLoader implements ProxyExtension {

    private final ProxyExtension extension;
    private final NarClassLoader classLoader;

    @Override
    public String extensionName() {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            return extension.extensionName();
        }
    }

    @Override
    public boolean accept(String extensionName) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            return extension.accept(extensionName);
        }
    }

    @Override
    public void initialize(ProxyConfiguration conf) throws Exception {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            extension.initialize(conf);
        }
    }

    @Override
    public void start(ProxyService service) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            extension.start(service);
        }
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            return extension.newChannelInitializers();
        }
    }

    @Override
    public void close() {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            extension.close();
        }

        try {
            classLoader.close();
        } catch (IOException e) {
            log.warn("Failed to close the extension class loader", e);
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
