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

/**
 * Pulsar broker interceptor.
 */
package org.apache.pulsar.proxy.server.protocol;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.proxy.server.ProxyConfiguration;

/**
 * A collection of loaded proxy handlers.
 */
@Slf4j
public class ProxyProtocols implements AutoCloseable {
    private final Map<String, ProxyProtocolWithClassLoader> protocols;

    public ProxyProtocols(Map<String, ProxyProtocolWithClassLoader> interceptors) {
        this.protocols = interceptors;
    }

    /**
     * Load the proxy event protocol for the given <tt>protocol</tt> list.
     *
     * @param conf the pulsar proxy service configuration
     * @return the collection of proxy event protocol
     */
    public static ProxyProtocols load(ProxyConfiguration conf) throws IOException {
        ProxyProtocolDefinitions definitions =
                ProxyProtocolUtils.searchForInterceptors(conf.getProxyProtocolsDirectory(), conf.getNarExtractionDirectory());

        ImmutableMap.Builder<String, ProxyProtocolWithClassLoader> builder = ImmutableMap.builder();

        conf.getProxyProtocols().forEach(protocolName -> {

            ProxyProtocolMetadata definition = definitions.protocols().get(protocolName);
            if (null == definition) {
                throw new RuntimeException("No proxy protocol is found for name `" + protocolName
                        + "`. Available proxy protocols are : " + definitions.protocols());
            }

            ProxyProtocolWithClassLoader protocol;
            try {
                protocol = ProxyProtocolUtils.load(definition, conf.getNarExtractionDirectory());
                if (protocol != null) {
                    builder.put(protocolName, protocol);
                }
                log.info("Successfully loaded proxy protocol for name `{}`", protocolName);
            } catch (IOException e) {
                log.error("Failed to load the proxy protocol for name `" + protocolName + "`", e);
                throw new RuntimeException("Failed to load the proxy protocol for name `" + protocolName + "`");
            }
        });

        Map<String, ProxyProtocolWithClassLoader> protocols = builder.build();
        if (protocols != null && !protocols.isEmpty()) {
            return new ProxyProtocols(protocols);
        }

        return null;
    }

    @Override
    public void close() {
        protocols.values().forEach(ProxyProtocolWithClassLoader::close);
    }
}
