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
package org.apache.pulsar.client.api;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * {@link ControlledClusterFailoverBuilder} is used to configure and create instance of {@link ServiceUrlProvider}.
 *
 * @since 2.10.0
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ControlledClusterFailoverBuilder {
    /**
     * Set default service url.
     *
     * @param serviceUrl
     * @return
     */
    ControlledClusterFailoverBuilder defaultServiceUrl(String serviceUrl);

    /**
     * Set the service url provider. ServiceUrlProvider will fetch serviceUrl from urlProvider periodically.
     *
     * @param urlProvider
     * @return
     */
    ControlledClusterFailoverBuilder urlProvider(String urlProvider);

    /**
     * Set the service url provider header to authenticate provider service.
     * @param header
     * @return
     */
    ControlledClusterFailoverBuilder urlProviderHeader(Map<String, String> header);

    /**
     * Set the probe check interval.
     * @param interval
     * @param timeUnit
     * @return
     */
    ControlledClusterFailoverBuilder checkInterval(long interval, TimeUnit timeUnit);

    /**
     * Build the ServiceUrlProvider instance.
     *
     * @return
     * @throws IOException
     */
    ServiceUrlProvider build() throws IOException;
}
