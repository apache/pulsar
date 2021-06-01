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
import org.apache.pulsar.client.api.ProxyProtocol;

public interface ClusterData {
    String getServiceUrl();

    String getServiceUrlTls();

    ClusterData setServiceUrl(String serviceUrl);

    ClusterData setServiceUrlTls(String serviceUrlTls);

    String getBrokerServiceUrl();

    ClusterData setBrokerServiceUrl(String brokerServiceUrl);

    String getBrokerServiceUrlTls();

    ClusterData setBrokerServiceUrlTls(String brokerServiceUrlTls);

    String getProxyServiceUrl();

    ClusterData setProxyServiceUrl(String proxyServiceUrl);

    ProxyProtocol getProxyProtocol();

    ClusterData setProxyProtocol(ProxyProtocol proxyProtocol);

    LinkedHashSet<String> getPeerClusterNames();

    String getAuthenticationPlugin();

    ClusterData setAuthenticationPlugin(String authenticationPlugin);

    String getAuthenticationParameters();

    ClusterData setAuthenticationParameters(String authenticationParameters);

    ClusterData setPeerClusterNames(LinkedHashSet<String> peerClusterNames);

    ClusterData setBrokerClientTlsEnabled(boolean enabled);
    boolean isBrokerClientTlsEnabled();

    ClusterData setTlsAllowInsecureConnection(boolean enabled);
    boolean isTlsAllowInsecureConnection();

    ClusterData setBrokerClientTlsEnabledWithKeyStore(boolean enabled);
    boolean isBrokerClientTlsEnabledWithKeyStore();

    ClusterData setBrokerClientTlsTrustStoreType(String trustStoreType);
    String getBrokerClientTlsTrustStoreType();

    ClusterData setBrokerClientTlsTrustStore(String tlsTrustStore);
    String getBrokerClientTlsTrustStore();

    ClusterData setBrokerClientTlsTrustStorePassword(String trustStorePassword);
    String getBrokerClientTlsTrustStorePassword();

    ClusterData setBrokerClientTrustCertsFilePath(String trustCertsFilePath);
    String getBrokerClientTrustCertsFilePath();
}
