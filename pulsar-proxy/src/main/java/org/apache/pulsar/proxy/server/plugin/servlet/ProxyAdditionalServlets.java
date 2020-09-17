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
package org.apache.pulsar.proxy.server.plugin.servlet;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.proxy.server.ProxyConfiguration;

/**
 * A collection of loaded proxy handlers.
 */
@Slf4j
public class ProxyAdditionalServlets implements AutoCloseable {

    @Getter
    private final Map<String, ProxyAdditionalServletWithClassLoader> servlets;

    public ProxyAdditionalServlets(Map<String, ProxyAdditionalServletWithClassLoader> servlets) {
        this.servlets = servlets;
    }

    /**
     * Load the proxy additional servlet for the given <tt>servlet name</tt> list.
     *
     * @param conf the pulsar proxy service configuration
     * @return the collection of proxy additional servlet
     */
    public static ProxyAdditionalServlets load(ProxyConfiguration conf) throws IOException {
        ProxyAdditionalServletDefinitions definitions =
                ProxyAdditionalServletUtils.searchForServlets(
                        conf.getProxyAdditionalServletDirectory(), conf.getNarExtractionDirectory());

        ImmutableMap.Builder<String, ProxyAdditionalServletWithClassLoader> builder = ImmutableMap.builder();

        conf.getProxyAdditionalServlets().forEach(servletName -> {

            ProxyAdditionalServletMetadata definition = definitions.servlets().get(servletName);
            if (null == definition) {
                throw new RuntimeException("No proxy additional servlet is found for name `" + servletName
                        + "`. Available proxy additional servlet are : " + definitions.servlets());
            }

            ProxyAdditionalServletWithClassLoader servletWithClassLoader;
            try {
                servletWithClassLoader = ProxyAdditionalServletUtils.load(definition, conf.getNarExtractionDirectory());
                if (servletWithClassLoader != null) {
                    builder.put(servletName, servletWithClassLoader);
                }
                log.info("Successfully loaded proxy additional servlet for name `{}`", servletName);
            } catch (IOException e) {
                log.error("Failed to load the proxy additional servlet for name `" + servletName + "`", e);
                throw new RuntimeException("Failed to load the proxy additional servlet for name `" + servletName + "`");
            }
        });

        Map<String, ProxyAdditionalServletWithClassLoader> servlets = builder.build();
        if (servlets != null && !servlets.isEmpty()) {
            return new ProxyAdditionalServlets(servlets);
        }

        return null;
    }

    @Override
    public void close() {
        servlets.values().forEach(ProxyAdditionalServletWithClassLoader::close);
    }
}
