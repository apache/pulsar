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
package org.apache.pulsar.broker.web.plugin.servlet;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

/**
 * A collection of loaded additional servlets.
 */
@Slf4j
public class AdditionalServlets implements AutoCloseable {

    private static final String ADDITIONAL_SERVLET_DIRECTORY = "additionalServletDirectory";

    private static final String ADDITIONAL_SERVLETS = "additionalServlets";

    private static final String NAR_EXTRACTION_DIRECTORY = "narExtractionDirectory";

    @Deprecated
    private static final String PROXY_ADDITIONAL_SERVLET_DIRECTORY = "proxyAdditionalServletDirectory";

    @Deprecated
    private static final String PROXY_ADDITIONAL_SERVLETS = "proxyAdditionalServlets";

    @Getter
    private final Map<String, AdditionalServletWithClassLoader> servlets;

    public AdditionalServlets(Map<String, AdditionalServletWithClassLoader> servlets) {
        this.servlets = servlets;
    }

    /**
     * Load the additional servlet for the given <tt>servlet name</tt> list.
     *
     * @param conf the pulsar service configuration
     * @return the collection of additional servlet
     */
    public static AdditionalServlets load(PulsarConfiguration conf) throws IOException {
        String additionalServletDirectory = conf.getProperties().getProperty(ADDITIONAL_SERVLET_DIRECTORY);
        if (additionalServletDirectory == null) {
            // Compatible with the current proxy configuration
            additionalServletDirectory = conf.getProperties().getProperty(PROXY_ADDITIONAL_SERVLET_DIRECTORY);
        }

        String additionalServlets = conf.getProperties().getProperty(ADDITIONAL_SERVLETS);
        if (additionalServlets == null) {
            additionalServlets = conf.getProperties().getProperty(PROXY_ADDITIONAL_SERVLETS);
        }
        if (additionalServletDirectory == null || additionalServlets == null) {
            return null;
        }

        AdditionalServletDefinitions definitions =
                AdditionalServletUtils.searchForServlets(additionalServletDirectory
                        , null);
        ImmutableMap.Builder<String, AdditionalServletWithClassLoader> builder = ImmutableMap.builder();

        List<String> additionalServletsList = Arrays.asList(additionalServlets.split(","));
        additionalServletsList.forEach(servletName -> {

            AdditionalServletMetadata definition = definitions.servlets().get(servletName);
            if (null == definition) {
                throw new RuntimeException("No additional servlet is found for name `" + servletName
                        + "`. Available additional servlet are : " + definitions.servlets());
            }

            AdditionalServletWithClassLoader servletWithClassLoader;
            try {
                servletWithClassLoader = AdditionalServletUtils.load(definition,
                        conf.getProperties().getProperty(NAR_EXTRACTION_DIRECTORY));
                if (servletWithClassLoader != null) {
                    builder.put(servletName, servletWithClassLoader);
                }
                log.info("Successfully loaded additional servlet for name `{}`", servletName);
            } catch (IOException e) {
                log.error("Failed to load the additional servlet for name `" + servletName + "`", e);
                throw new RuntimeException("Failed to load the additional servlet for name `" + servletName + "`");
            }
        });

        Map<String, AdditionalServletWithClassLoader> servlets = builder.build();
        if (servlets != null && !servlets.isEmpty()) {
            return new AdditionalServlets(servlets);
        }

        return null;
    }

    @Override
    public void close() {
        servlets.values().forEach(AdditionalServletWithClassLoader::close);
    }
}
