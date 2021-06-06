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
package org.apache.pulsar.common.policies.data;

import java.util.LinkedHashSet;
import org.apache.pulsar.client.admin.utils.ReflectionUtils;
import org.apache.pulsar.client.api.ProxyProtocol;

public interface ClusterData {
    String getServiceUrl();

    String getServiceUrlTls();

    String getBrokerServiceUrl();

    String getBrokerServiceUrlTls();

    String getProxyServiceUrl();

    ProxyProtocol getProxyProtocol();

    LinkedHashSet<String> getPeerClusterNames();

    String getAuthenticationPlugin();

    String getAuthenticationParameters();

    boolean isBrokerClientTlsEnabled();

    boolean isTlsAllowInsecureConnection();

    boolean isBrokerClientTlsEnabledWithKeyStore();

    String getBrokerClientTlsTrustStoreType();

    String getBrokerClientTlsTrustStore();

    String getBrokerClientTlsTrustStorePassword();

    String getBrokerClientTrustCertsFilePath();

    String getListenerName();

    interface Builder {
        Builder serviceUrl(String serviceUrl);

        Builder serviceUrlTls(String serviceUrlTls);

        Builder brokerServiceUrl(String brokerServiceUrl);

        Builder brokerServiceUrlTls(String brokerServiceUrlTls);

        Builder proxyServiceUrl(String proxyServiceUrl);

        Builder proxyProtocol(ProxyProtocol proxyProtocol);

        Builder authenticationPlugin(String authenticationPlugin);

        Builder authenticationParameters(String authenticationParameters);

        Builder peerClusterNames(LinkedHashSet<String> peerClusterNames);

        Builder brokerClientTlsEnabled(boolean enabled);

        Builder tlsAllowInsecureConnection(boolean enabled);

        Builder brokerClientTlsEnabledWithKeyStore(boolean enabled);

        Builder brokerClientTlsTrustStoreType(String trustStoreType);

        Builder brokerClientTlsTrustStore(String tlsTrustStore);

        Builder brokerClientTlsTrustStorePassword(String trustStorePassword);

        Builder brokerClientTrustCertsFilePath(String trustCertsFilePath);

        Builder listenerName(String listenerName);

        ClusterData build();
    }

    Builder clone();

    static Builder builder() {
        return ReflectionUtils.newBuilder("org.apache.pulsar.common.policies.data.ClusterDataImpl");
    }
}
