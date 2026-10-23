/*
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

    String getBrokerClientCertificateFilePath();

    String getBrokerClientKeyFilePath();

    String getBrokerClientTlsKeyStoreType();

    String getBrokerClientTlsKeyStorePassword();

    String getBrokerClientTlsKeyStore();

    /**
     * The {@code PulsarTlsFactory} class name used for outbound connections to this cluster (PIP-478), or
     * blank to inherit the broker-level {@code brokerClientTlsFactoryClassName}. It applies to the two
     * outbound legs whose client configuration is <em>built from this cluster entry</em>: the
     * binary-protocol replication client and the cross-cluster admin (HTTPS) client.
     *
     * <p>It does not reach the peer-cluster lookup client ({@code NamespaceService.getNamespaceClient}),
     * which takes only the service URL from the cluster entry and all of its TLS configuration — material
     * and factory alike — from the broker-level {@code brokerClient*} settings. That leg was already
     * broker-level in 4.x (it read the broker-level {@code brokerClientSslFactoryPlugin}, never the
     * per-cluster one), so this is unchanged behaviour rather than a PIP-478 narrowing.
     *
     * @return the per-cluster TLS factory class name, or blank/null to inherit the broker-level setting
     */
    String getBrokerClientTlsFactoryClassName();

    /**
     * The configuration parameters passed to {@link #getBrokerClientTlsFactoryClassName()} as its init
     * params (a JSON object or a {@code key=value} list).
     *
     * <p>This value follows the class name rather than inheriting on its own: when this cluster names a
     * factory, this configuration is used even when blank, so factory A's parameters can never be handed to
     * factory B. Only when the class name is blank — so the broker-level factory applies — does the
     * broker-level {@code brokerClientTlsFactoryConfig} apply too.
     *
     * @return the per-cluster TLS factory configuration; used verbatim when this cluster names a factory
     */
    String getBrokerClientTlsFactoryConfig();

    /**
     * @deprecated since 5.0.0: the PIP-337 SSL factory plugin is removed (PIP-478). Retained in the cluster
     *     metadata schema for wire/metadata compatibility, but a configured value is ignored (with a WARN).
     *     Use {@link #getBrokerClientTlsFactoryClassName()} instead.
     */
    @Deprecated
    String getBrokerClientSslFactoryPlugin();

    /**
     * @deprecated since 5.0.0: the PIP-337 SSL factory plugin is removed (PIP-478). Retained for metadata
     *     compatibility but ignored. Use {@link #getBrokerClientTlsFactoryConfig()} instead.
     */
    @Deprecated
    String getBrokerClientSslFactoryPluginParams();

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

        Builder brokerClientCertificateFilePath(String certificateFilePath);

        Builder brokerClientKeyFilePath(String keyFilePath);

        Builder brokerClientTlsKeyStoreType(String keyStoreType);

        Builder brokerClientTlsKeyStorePassword(String keyStorePassword);

        Builder brokerClientTlsKeyStore(String keyStore);

        Builder listenerName(String listenerName);

        /**
         * Select the {@code PulsarTlsFactory} used for outbound connections to this cluster (PIP-478) —
         * the binary-protocol replication client and the cross-cluster admin (HTTPS) client, the two legs
         * whose configuration is built from this cluster entry (see
         * {@link ClusterData#getBrokerClientTlsFactoryClassName()} for the leg it does not reach). Leave
         * blank to inherit the broker-level {@code brokerClientTlsFactoryClassName}.
         *
         * @param tlsFactoryClassName the factory class name, or blank to inherit the broker-level setting
         * @return this builder
         */
        Builder brokerClientTlsFactoryClassName(String tlsFactoryClassName);

        /**
         * Configuration passed to {@link #brokerClientTlsFactoryClassName(String)} as its init params (a
         * JSON object or a {@code key=value} list).
         *
         * <p>It follows the class name rather than inheriting on its own: setting a class name here means
         * this configuration is used even when blank, so one factory's parameters are never handed to
         * another. It is ignored — with a warning — when no class name is set on this cluster.
         *
         * @param tlsFactoryConfig the factory configuration for this cluster's factory
         * @return this builder
         */
        Builder brokerClientTlsFactoryConfig(String tlsFactoryConfig);

        /**
         * @deprecated since 5.0.0: the PIP-337 SSL factory plugin is removed (PIP-478). Setting it writes a
         *     metadata field retained only for compatibility; the value is ignored (with a WARN). Use
         *     {@link #brokerClientTlsFactoryClassName(String)} instead.
         */
        @Deprecated
        Builder brokerClientSslFactoryPlugin(String sslFactoryPlugin);

        /**
         * @deprecated since 5.0.0: the PIP-337 SSL factory plugin is removed (PIP-478). Retained for
         *     metadata compatibility but ignored. Use {@link #brokerClientTlsFactoryConfig(String)} instead.
         */
        @Deprecated
        Builder brokerClientSslFactoryPluginParams(String sslFactoryPluginParams);

        ClusterData build();
    }

    Builder clone();

    static Builder builder() {
        return ReflectionUtils.newBuilder("org.apache.pulsar.common.policies.data.ClusterDataImpl");
    }
}
