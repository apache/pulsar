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

import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A collection of loaded extensions.
 */
@Slf4j
public class ProxyExtensions implements AutoCloseable {

    @Getter
    private final Map<SocketAddress, String> endpoints = new ConcurrentHashMap<>();
    /**
     * Load the extensions for the given <tt>extensions</tt> list.
     *
     * @param conf the pulsar broker service configuration
     * @return the collection of extensions
     */
    public static ProxyExtensions load(ProxyConfiguration conf) throws IOException {
        ExtensionsDefinitions definitions =
                ProxyExtensionsUtils.searchForExtensions(
                        conf.getProxyExtensionsDirectory(), conf.getNarExtractionDirectory());

        ImmutableMap.Builder<String, ProxyExtensionWithClassLoader> extensionsBuilder = ImmutableMap.builder();

        conf.getProxyExtensions().forEach(extensionName -> {

            ProxyExtensionMetadata definition = definitions.extensions().get(extensionName);
            if (null == definition) {
                throw new RuntimeException("No extension is found for extension name `" + extensionName
                    + "`. Available extensions are : " + definitions.extensions());
            }

            ProxyExtensionWithClassLoader extension;
            try {
                extension = ProxyExtensionsUtils.load(definition, conf.getNarExtractionDirectory());
            } catch (IOException e) {
                log.error("Failed to load the extension for extension `" + extensionName + "`", e);
                throw new RuntimeException("Failed to load the extension for extension name `" + extensionName + "`");
            }

            if (!extension.accept(extensionName)) {
                extension.close();
                log.error("Malformed extension found for extensionName `" + extensionName + "`");
                throw new RuntimeException("Malformed extension found for extension name `" + extensionName + "`");
            }

            extensionsBuilder.put(extensionName, extension);
            log.info("Successfully loaded extension for extension name `{}`", extensionName);
        });

        return new ProxyExtensions(extensionsBuilder.build());
    }

    private final Map<String, ProxyExtensionWithClassLoader> extensions;

    ProxyExtensions(Map<String, ProxyExtensionWithClassLoader> extensions) {
        this.extensions = extensions;
    }

    /**
     * Return the handler for the provided <tt>extension</tt>.
     *
     * @param extension the extension to use
     * @return the extension to handle the provided extension
     */
    public ProxyExtension extension(String extension) {
        ProxyExtensionWithClassLoader h = extensions.get(extension);
        if (null == h) {
            return null;
        } else {
            return h.getExtension();
        }
    }

    public void initialize(ProxyConfiguration conf) throws Exception {
        for (ProxyExtension extension : extensions.values()) {
            extension.initialize(conf);
        }
    }

    public Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> newChannelInitializers() {
        Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> channelInitializers = new HashMap<>();
        Set<InetSocketAddress> addresses = new HashSet<>();

        for (Map.Entry<String, ProxyExtensionWithClassLoader> extension : extensions.entrySet()) {
            Map<InetSocketAddress, ChannelInitializer<SocketChannel>> initializers =
                extension.getValue().newChannelInitializers();
            initializers.forEach((address, initializer) -> {
                if (!addresses.add(address)) {
                    log.error("extension for `{}` attempts to use {} for its listening port."
                        + " But it is already occupied by other extensions.",
                        extension.getKey(), address);
                    throw new RuntimeException("extension for `" + extension.getKey()
                        + "` attempts to use " + address + " for its listening port. But it is"
                        + " already occupied by other messaging extensions");
                }
                endpoints.put(address, extension.getKey());
                channelInitializers.put(extension.getKey(), initializers);
            });
        }

        return channelInitializers;
    }

    public void start(ProxyService service) {
        extensions.values().forEach(extension -> extension.start(service));
    }

    @Override
    public void close() {
        extensions.values().forEach(ProxyExtension::close);
    }
}
