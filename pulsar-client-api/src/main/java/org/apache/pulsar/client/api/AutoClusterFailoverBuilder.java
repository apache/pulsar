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

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * {@link AutoClusterFailoverBuilder} is used to configure and create instance of {@link ServiceUrlProvider}
 *
 * @since 2.10.0
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AutoClusterFailoverBuilder {
    /**
     * Set the primary service url.
     *
     * @param primary
     * @return
     */
    AutoClusterFailoverBuilder primary(String primary);

    /**
     * Set the secondary service url.
     *
     * @param secondary
     * @return
     */
    AutoClusterFailoverBuilder secondary(List<String> secondary);

    /**
     * Set primary authentication.
     *
     * @param authentication
     * @return
     */
    AutoClusterFailoverBuilder primaryAuthentication(Authentication authentication);

    /**
     * Set secondary authentication.
     *
     * @param authentication
     * @return
     */
    AutoClusterFailoverBuilder secondaryAuthentication(List<Authentication> authentication);

    /**
     * Set primary tlsTrustCertsFilePath.
     *
     * @param tlsTrustCertsFilePath
     * @return
     */
    AutoClusterFailoverBuilder primaryTlsTrustCertsFilePath(String tlsTrustCertsFilePath);

    /**
     * Set secondary tlsTrustCertsFilePath.
     *
     * @param tlsTrustCertsFilePath
     * @return
     */
    AutoClusterFailoverBuilder secondaryTlsTrustCertsFilePath(List<String> tlsTrustCertsFilePath);

    /**
     * Set primary tlsTrustStorePath.
     *
     * @param tlsTrustStorePath
     * @return
     */
    AutoClusterFailoverBuilder primaryTlsTrustStorePath(String tlsTrustStorePath);

    /**
     * Set secondary tlsTrustStorePath.
     *
     * @param tlsTrustStorePath
     * @return
     */
    AutoClusterFailoverBuilder secondaryTlsTrustStorePath(List<String> tlsTrustStorePath);

    /**
     * Set primary tlsTrustStorePassword.
     *
     * @param tlsTrustStorePassword
     * @return
     */
    AutoClusterFailoverBuilder primaryTlsTrustStorePassword(String tlsTrustStorePassword);

    /**
     * Set secondary tlsTrustStorePassword.
     *
     * @param tlsTrustStorePassword
     * @return
     */
    AutoClusterFailoverBuilder secondaryTlsTrustStorePassword(List<String> tlsTrustStorePassword);
    /**
     * Set the switch failoverDelay. When one cluster failed longer than failoverDelay, it will trigger cluster switch.
     *
     * @param failoverDelay
     * @param timeUnit
     * @return
     */
    AutoClusterFailoverBuilder failoverDelay(long failoverDelay, TimeUnit timeUnit);

    /**
     * Set the switchBackDelay. When switched to the secondary cluster, and after the primary cluster comes back,
     * it will wait for switchBackDelay to switch back to the primary cluster.
     *
     * @param switchBackDelay
     * @param timeUnit
     * @return
     */
    AutoClusterFailoverBuilder switchBackDelay(long switchBackDelay, TimeUnit timeUnit);

    /**
     * Set the checkInterval for probe.
     *
     * @param interval
     * @param timeUnit
     * @return
     */
    AutoClusterFailoverBuilder checkInterval(long interval, TimeUnit timeUnit);

    /**
     * Build the ServiceUrlProvider instance.
     *
     * @return
     */
    ServiceUrlProvider build();
}
