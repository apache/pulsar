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

import com.google.common.annotations.Beta;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * The additional servlet interface for support additional servlet.
 */
@Beta
public interface AdditionalServlet extends AutoCloseable {

    /**
     * load plugin config.
     *
     * @param pulsarConfiguration
     */
    void loadConfig(PulsarConfiguration pulsarConfiguration);

    /**
     * Get the base path of prometheus metrics.
     *
     * @return the base path of prometheus metrics
     */
    String getBasePath();

    /**
     * Get the servlet holder.
     *
     * @return the servlet holder
     */
    ServletHolder getServletHolder();

    @Override
    void close();
}
