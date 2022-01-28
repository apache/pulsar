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

import java.io.IOException;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ClassLoaderSwitcher;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * An additional servlet with it's classloader.
 */
@Slf4j
@Data
@RequiredArgsConstructor
public class AdditionalServletWithClassLoader implements AdditionalServlet {

    private final AdditionalServlet servlet;
    private final NarClassLoader classLoader;

    @Override
    public void loadConfig(PulsarConfiguration pulsarConfiguration) {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            servlet.loadConfig(pulsarConfiguration);
        }
    }

    @Override
    public String getBasePath() {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            return servlet.getBasePath();
        }
    }

    @Override
    public ServletHolder getServletHolder() {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            return servlet.getServletHolder();
        }
    }

    @Override
    public void close() {
        try (ClassLoaderSwitcher ignored = new ClassLoaderSwitcher(classLoader)) {
            servlet.close();
        }
        try {
            classLoader.close();
        } catch (IOException e) {
            log.warn("Failed to close the broker additional servlet class loader", e);
        }
    }
}
