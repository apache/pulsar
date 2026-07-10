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

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.LinkedHashSet;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.CustomLog;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;
import org.apache.pulsar.common.util.URIPreconditions;

/**
 * The configuration data for a cluster.
 */
@Schema(
        name = "ClusterData",
        description = "The configuration data for a cluster"
)
@Data
@AllArgsConstructor
@NoArgsConstructor
@CustomLog
public final class ClusterDataImpl implements  ClusterData, Cloneable {
    @Schema(
            name = "serviceUrl",
            description = "The HTTP rest service URL (for admin operations)",
            example = "http://pulsar.example.com:8080"
    )
    private String serviceUrl;
    @Schema(
            name = "serviceUrlTls",
            description = "The HTTPS rest service URL (for admin operations)",
            example = "https://pulsar.example.com:8443"
    )
    private String serviceUrlTls;
    @Schema(
            name = "brokerServiceUrl",
            description = "The broker service url (for produce and consume operations)",
            example = "pulsar://pulsar.example.com:6650"
    )
    private String brokerServiceUrl;
    @Schema(
            name = "brokerServiceUrlTls",
            description = "The secured broker service url (for produce and consume operations)",
            example = "pulsar+ssl://pulsar.example.com:6651"
    )
    private String brokerServiceUrlTls;
    @Schema(
            name = "proxyServiceUrl",
            description = "Proxy-service url when client would like to connect to broker via proxy.",
            example = "pulsar+ssl://ats-proxy.example.com:4443 or "
                    + "pulsar://ats-proxy.example.com:4080"
    )
    private String proxyServiceUrl;
    @Schema(
        name = "authenticationPlugin",
        description = "Authentication plugin when client would like to connect to cluster.",
        example = "org.apache.pulsar.client.impl.auth.AuthenticationToken"
    )
    private String authenticationPlugin;
    @Schema(
        name = "authenticationParameters",
        description = "Authentication parameters when client would like to connect to cluster."
    )
    private String authenticationParameters;
    @Schema(
            name = "proxyProtocol",
            description = "Protocol to decide the type of proxy routing, e.g. SNI-routing",
            example = "SNI"
    )
    private ProxyProtocol proxyProtocol;

    // For given Cluster1(us-west1, us-east1) and Cluster2(us-west2, us-east2)
    // Peer: [us-west1 -> us-west2] and [us-east1 -> us-east2]
    @Schema(
            name = "peerClusterNames",
            description = "A set of peer cluster names"
    )
    private LinkedHashSet<String> peerClusterNames;

    @Schema(
        name = "brokerClientTlsEnabled",
        description = "Enable TLS when talking with other brokers in the same cluster (admin operation)"
                + " or different clusters (replication)"
    )
    private boolean brokerClientTlsEnabled;
    @Schema(
        name = "tlsAllowInsecureConnection",
        description = "Allow TLS connections to servers whose certificate cannot"
                + " be verified to have been signed by a trusted certificate"
                + " authority."
    )
    private boolean tlsAllowInsecureConnection;
    @Schema(
        name = "brokerClientTlsEnabledWithKeyStore",
        description = "Whether the internal client uses KeyStore type to authenticate with other Pulsar brokers"
    )
    private boolean brokerClientTlsEnabledWithKeyStore;
    @Schema(
        name = "brokerClientTlsTrustStoreType",
        description = "TLS TrustStore type configuration for internal client: JKS, PKCS12"
                + " used by the internal client to authenticate with Pulsar brokers",
        example = "JKS"
    )
    private String brokerClientTlsTrustStoreType;
    @Schema(
        name = "brokerClientTlsTrustStore",
        description = "TLS TrustStore path for internal client"
                + " used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsTrustStore;
    @Schema(
        name = "brokerClientTlsTrustStorePassword",
        description = "TLS TrustStore password for internal client"
                + " used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsTrustStorePassword;
    @Schema(
            name = "brokerClientTlsKeyStoreType",
            description = "TLS KeyStore type configuration for internal client: JKS, PKCS12,"
                    + " used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsKeyStoreType;
    @Schema(
            name = "brokerClientTlsKeyStore",
            description = "TLS KeyStore path for internal client,"
                    + " used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsKeyStore;
    @Schema(
            name = "brokerClientTlsKeyStorePassword",
            description = "TLS KeyStore password for internal client,"
                    + " used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsKeyStorePassword;
    @Schema(
            name = "brokerClientTrustCertsFilePath",
            description = "Path for the trusted TLS certificate file for outgoing connection to a server (broker)"
    )
    private String brokerClientTrustCertsFilePath;
    @Schema(
            name = "brokerClientKeyFilePath",
            description = "TLS private key file for internal client, "
                    + "used by the internal client to authenticate with Pulsar brokers")
    private String brokerClientKeyFilePath;
    @Schema(
            name = "brokerClientCertificateFilePath",
            description = "TLS certificate file for internal client, "
                    + "used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientCertificateFilePath;
    @Schema(
            name = "brokerClientSslFactoryPlugin",
            description = "SSL Factory plugin used by internal client to generate the SSL Context and Engine"
    )
    private String brokerClientSslFactoryPlugin;
    @Schema(
            name = "brokerClientSslFactoryPluginParams",
            description =
                    "Parameters used by the internal client's SSL factory plugin to generate the SSL Context and Engine"
    )
    private String brokerClientSslFactoryPluginParams;
    @Schema(
            name = "listenerName",
            description = "listenerName when client would like to connect to cluster",
            example = ""
    )
    private String listenerName;

    public static ClusterDataImplBuilder builder() {
        return new ClusterDataImplBuilder();
    }

    @Override
    public ClusterDataImplBuilder clone() {
        return builder()
                .serviceUrl(serviceUrl)
                .serviceUrlTls(serviceUrlTls)
                .brokerServiceUrl(brokerServiceUrl)
                .brokerServiceUrlTls(brokerServiceUrlTls)
                .proxyServiceUrl(proxyServiceUrl)
                .authenticationPlugin(authenticationPlugin)
                .authenticationParameters(authenticationParameters)
                .proxyProtocol(proxyProtocol)
                .peerClusterNames(peerClusterNames)
                .brokerClientTlsEnabled(brokerClientTlsEnabled)
                .tlsAllowInsecureConnection(tlsAllowInsecureConnection)
                .brokerClientTlsEnabledWithKeyStore(brokerClientTlsEnabledWithKeyStore)
                .brokerClientTlsTrustStoreType(brokerClientTlsTrustStoreType)
                .brokerClientTlsTrustStore(brokerClientTlsTrustStore)
                .brokerClientTlsTrustStorePassword(brokerClientTlsTrustStorePassword)
                .brokerClientTlsKeyStoreType(brokerClientTlsKeyStoreType)
                .brokerClientTlsKeyStore(brokerClientTlsKeyStore)
                .brokerClientTlsKeyStorePassword(brokerClientTlsKeyStorePassword)
                .brokerClientTrustCertsFilePath(brokerClientTrustCertsFilePath)
                .brokerClientCertificateFilePath(brokerClientCertificateFilePath)
                .brokerClientKeyFilePath(brokerClientKeyFilePath)
                .brokerClientSslFactoryPlugin(brokerClientSslFactoryPlugin)
                .brokerClientSslFactoryPluginParams(brokerClientSslFactoryPluginParams)
                .listenerName(listenerName);
    }

    @Data
    public static class ClusterDataImplBuilder implements ClusterData.Builder {
        private String serviceUrl;
        private String serviceUrlTls;
        private String brokerServiceUrl;
        private String brokerServiceUrlTls;
        private String proxyServiceUrl;
        private String authenticationPlugin;
        private String authenticationParameters;
        private ProxyProtocol proxyProtocol;
        private LinkedHashSet<String> peerClusterNames;
        private boolean brokerClientTlsEnabled = false;
        private boolean tlsAllowInsecureConnection = false;
        private boolean brokerClientTlsEnabledWithKeyStore = false;
        private String brokerClientTlsTrustStoreType = "JKS";
        private String brokerClientTlsTrustStore;
        private String brokerClientTlsTrustStorePassword;
        private String brokerClientTlsKeyStoreType = "JKS";
        private String brokerClientTlsKeyStore;
        private String brokerClientTlsKeyStorePassword;
        private String brokerClientCertificateFilePath;
        private String brokerClientKeyFilePath;
        private String brokerClientTrustCertsFilePath;
        private String brokerClientSslFactoryPlugin = DefaultPulsarSslFactory.class.getName();
        private String brokerClientSslFactoryPluginParams;
        private String listenerName;

        ClusterDataImplBuilder() {
        }

        public ClusterDataImplBuilder serviceUrl(String serviceUrl) {
            this.serviceUrl = serviceUrl;
            return this;
        }

        public ClusterDataImplBuilder serviceUrlTls(String serviceUrlTls) {
            this.serviceUrlTls = serviceUrlTls;
            return this;
        }

        public ClusterDataImplBuilder brokerServiceUrl(String brokerServiceUrl) {
            this.brokerServiceUrl = brokerServiceUrl;
            return this;
        }

        public ClusterDataImplBuilder brokerServiceUrlTls(String brokerServiceUrlTls) {
            this.brokerServiceUrlTls = brokerServiceUrlTls;
            return this;
        }

        public ClusterDataImplBuilder proxyServiceUrl(String proxyServiceUrl) {
            this.proxyServiceUrl = proxyServiceUrl;
            return this;
        }

        public ClusterDataImplBuilder authenticationPlugin(String authenticationPlugin) {
            this.authenticationPlugin = authenticationPlugin;
            return this;
        }

        public ClusterDataImplBuilder authenticationParameters(String authenticationParameters) {
            this.authenticationParameters = authenticationParameters;
            return this;
        }

        public ClusterDataImplBuilder proxyProtocol(ProxyProtocol proxyProtocol) {
            this.proxyProtocol = proxyProtocol;
            return this;
        }

        public ClusterDataImplBuilder peerClusterNames(LinkedHashSet<String> peerClusterNames) {
            this.peerClusterNames = peerClusterNames;
            return this;
        }

        public ClusterDataImplBuilder brokerClientTlsEnabled(boolean brokerClientTlsEnabled) {
            this.brokerClientTlsEnabled = brokerClientTlsEnabled;
            return this;
        }

        public ClusterDataImplBuilder tlsAllowInsecureConnection(boolean tlsAllowInsecureConnection) {
            this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
            return this;
        }

        public ClusterDataImplBuilder brokerClientTlsEnabledWithKeyStore(boolean brokerClientTlsEnabledWithKeyStore) {
            this.brokerClientTlsEnabledWithKeyStore = brokerClientTlsEnabledWithKeyStore;
            return this;
        }

        public ClusterDataImplBuilder brokerClientTlsTrustStoreType(String brokerClientTlsTrustStoreType) {
            this.brokerClientTlsTrustStoreType = brokerClientTlsTrustStoreType;
            return this;
        }

        public ClusterDataImplBuilder brokerClientTlsTrustStore(String brokerClientTlsTrustStore) {
            this.brokerClientTlsTrustStore = brokerClientTlsTrustStore;
            return this;
        }

        public ClusterDataImplBuilder brokerClientTlsTrustStorePassword(String brokerClientTlsTrustStorePassword) {
            this.brokerClientTlsTrustStorePassword = brokerClientTlsTrustStorePassword;
            return this;
        }

        @Override
        public ClusterDataImplBuilder brokerClientTlsKeyStoreType(String keyStoreType) {
            this.brokerClientTlsKeyStoreType = keyStoreType;
            return this;
        }

        @Override
        public ClusterDataImplBuilder brokerClientTlsKeyStorePassword(String keyStorePassword) {
            this.brokerClientTlsKeyStorePassword = keyStorePassword;
            return this;
        }

        @Override
        public ClusterDataImplBuilder brokerClientTlsKeyStore(String keyStore) {
            this.brokerClientTlsKeyStore = keyStore;
            return this;
        }

        public ClusterDataImplBuilder brokerClientTrustCertsFilePath(String brokerClientTrustCertsFilePath) {
            this.brokerClientTrustCertsFilePath = brokerClientTrustCertsFilePath;
            return this;
        }

        @Override
        public ClusterDataImplBuilder brokerClientCertificateFilePath(String certificateFilePath) {
            this.brokerClientCertificateFilePath = certificateFilePath;
            return this;
        }

        @Override
        public ClusterDataImplBuilder brokerClientKeyFilePath(String keyFilePath) {
            this.brokerClientKeyFilePath = keyFilePath;
            return this;
        }

        @Override
        public ClusterDataImplBuilder brokerClientSslFactoryPlugin(String sslFactoryPlugin) {
            this.brokerClientSslFactoryPlugin = sslFactoryPlugin;
            return this;
        }

        @Override
        public ClusterDataImplBuilder brokerClientSslFactoryPluginParams(String sslFactoryPluginParams) {
            this.brokerClientSslFactoryPluginParams = sslFactoryPluginParams;
            return this;
        }

        public ClusterDataImplBuilder listenerName(String listenerName) {
            this.listenerName = listenerName;
            return this;
        }

        public ClusterDataImpl build() {
            return new ClusterDataImpl(
                    serviceUrl,
                    serviceUrlTls,
                    brokerServiceUrl,
                    brokerServiceUrlTls,
                    proxyServiceUrl,
                    authenticationPlugin,
                    authenticationParameters,
                    proxyProtocol,
                    peerClusterNames,
                    brokerClientTlsEnabled,
                    tlsAllowInsecureConnection,
                    brokerClientTlsEnabledWithKeyStore,
                    brokerClientTlsTrustStoreType,
                    brokerClientTlsTrustStore,
                    brokerClientTlsTrustStorePassword,
                    brokerClientTlsKeyStoreType,
                    brokerClientTlsKeyStore,
                    brokerClientTlsKeyStorePassword,
                    brokerClientTrustCertsFilePath,
                    brokerClientKeyFilePath,
                    brokerClientCertificateFilePath,
                    brokerClientSslFactoryPlugin,
                    brokerClientSslFactoryPluginParams,
                    listenerName);
        }
    }

    /**
     * Check cluster data properties by rule, if some property is illegal, it will throw
     * {@link IllegalArgumentException}.
     *
     * @throws IllegalArgumentException exist illegal property.
     */
    public void checkPropertiesIfPresent() throws IllegalArgumentException {
        URIPreconditions.checkURIIfPresent(getServiceUrl(),
                uri -> Objects.equals(uri.getScheme(), "http"),
                "Illegal service url, example: http://pulsar.example.com:8080");
        URIPreconditions.checkURIIfPresent(getServiceUrlTls(),
                uri -> Objects.equals(uri.getScheme(), "https"),
                "Illegal service tls url, example: https://pulsar.example.com:8443");
        URIPreconditions.checkURIIfPresent(getBrokerServiceUrl(),
                uri -> Objects.equals(uri.getScheme(), "pulsar"),
                "Illegal broker service url, example: pulsar://pulsar.example.com:6650");
        URIPreconditions.checkURIIfPresent(getBrokerServiceUrlTls(),
                uri -> Objects.equals(uri.getScheme(), "pulsar+ssl"),
                "Illegal broker service tls url, example: pulsar+ssl://pulsar.example.com:6651");
        URIPreconditions.checkURIIfPresent(getProxyServiceUrl(),
                uri -> Objects.equals(uri.getScheme(), "pulsar")
                        || Objects.equals(uri.getScheme(), "pulsar+ssl"),
                "Illegal proxy service url, example: pulsar+ssl://ats-proxy.example.com:4443 "
                        + "or pulsar://ats-proxy.example.com:4080");

        warnIfUrlIsNotPresent();
    }

    private void warnIfUrlIsNotPresent() {
        if (StringUtils.isEmpty(getServiceUrl()) && StringUtils.isEmpty(getServiceUrlTls())) {
            log.warn("Service url not found, "
                    + "please provide either service url, example: http://pulsar.example.com:8080 "
                    + "or service tls url, example: https://pulsar.example.com:8443");
        }
        if (StringUtils.isEmpty(getBrokerServiceUrl()) && StringUtils.isEmpty(getBrokerServiceUrlTls())) {
            log.warn("Broker service url not found, "
                    + "please provide either broker service url, example: pulsar://pulsar.example.com:6650 "
                    + "or broker service tls url, example: pulsar+ssl://pulsar.example.com:6651.");
        }
    }
}
