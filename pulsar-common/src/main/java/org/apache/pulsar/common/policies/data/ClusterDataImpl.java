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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.LinkedHashSet;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.common.util.URIPreconditions;

/**
 * The configuration data for a cluster.
 */
@ApiModel(
        value = "ClusterData",
        description = "The configuration data for a cluster"
)
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class ClusterDataImpl implements  ClusterData, Cloneable {
    @ApiModelProperty(
            name = "serviceUrl",
            value = "The HTTP rest service URL (for admin operations)",
            example = "http://pulsar.example.com:8080"
    )
    private String serviceUrl;
    @ApiModelProperty(
            name = "serviceUrlTls",
            value = "The HTTPS rest service URL (for admin operations)",
            example = "https://pulsar.example.com:8443"
    )
    private String serviceUrlTls;
    @ApiModelProperty(
            name = "brokerServiceUrl",
            value = "The broker service url (for produce and consume operations)",
            example = "pulsar://pulsar.example.com:6650"
    )
    private String brokerServiceUrl;
    @ApiModelProperty(
            name = "brokerServiceUrlTls",
            value = "The secured broker service url (for produce and consume operations)",
            example = "pulsar+ssl://pulsar.example.com:6651"
    )
    private String brokerServiceUrlTls;
    @ApiModelProperty(
            name = "proxyServiceUrl",
            value = "Proxy-service url when client would like to connect to broker via proxy.",
            example = "pulsar+ssl://ats-proxy.example.com:4443 or "
                    + "pulsar://ats-proxy.example.com:4080"
    )
    private String proxyServiceUrl;
    @ApiModelProperty(
        name = "authenticationPlugin",
        value = "Authentication plugin when client would like to connect to cluster.",
        example = "org.apache.pulsar.client.impl.auth.AuthenticationToken"
    )
    private String authenticationPlugin;
    @ApiModelProperty(
        name = "authenticationParameters",
        value = "Authentication parameters when client would like to connect to cluster."
    )
    private String authenticationParameters;
    @ApiModelProperty(
            name = "proxyProtocol",
            value = "protocol to decide type of proxy routing eg: SNI-routing",
            example = "SNI"
    )
    private ProxyProtocol proxyProtocol;

    // For given Cluster1(us-west1, us-east1) and Cluster2(us-west2, us-east2)
    // Peer: [us-west1 -> us-west2] and [us-east1 -> us-east2]
    @ApiModelProperty(
            name = "peerClusterNames",
            value = "A set of peer cluster names"
    )
    private LinkedHashSet<String> peerClusterNames;

    @ApiModelProperty(
        name = "brokerClientTlsEnabled",
        value = "Enable TLS when talking with other brokers in the same cluster (admin operation)"
                + " or different clusters (replication)"
    )
    private boolean brokerClientTlsEnabled;
    @ApiModelProperty(
        name = "tlsAllowInsecureConnection",
        value = "Allow TLS connections to servers whose certificate cannot be"
                + " be verified to have been signed by a trusted certificate"
                + " authority."
    )
    private boolean tlsAllowInsecureConnection;
    @ApiModelProperty(
        name = "brokerClientTlsEnabledWithKeyStore",
        value = "Whether internal client use KeyStore type to authenticate with other Pulsar brokers"
    )
    private boolean brokerClientTlsEnabledWithKeyStore;
    @ApiModelProperty(
        name = "brokerClientTlsTrustStoreType",
        value = "TLS TrustStore type configuration for internal client: JKS, PKCS12"
                + " used by the internal client to authenticate with Pulsar brokers",
        example = "JKS"
    )
    private String brokerClientTlsTrustStoreType;
    @ApiModelProperty(
        name = "brokerClientTlsTrustStore",
        value = "TLS TrustStore path for internal client"
                + " used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsTrustStore;
    @ApiModelProperty(
        name = "brokerClientTlsTrustStorePassword",
        value = "TLS TrustStore password for internal client"
                + " used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsTrustStorePassword;
    @ApiModelProperty(
            name = "brokerClientTlsKeyStoreType",
            value = "TLS KeyStore type configuration for internal client: JKS, PKCS12,"
                    + " used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsKeyStoreType;
    @ApiModelProperty(
            name = "brokerClientTlsKeyStore",
            value = "TLS KeyStore path for internal client, "
                    + " used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsKeyStore;
    @ApiModelProperty(
            name = "brokerClientTlsKeyStorePassword",
            value = "TLS KeyStore password for internal client, "
                    + " used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientTlsKeyStorePassword;
    @ApiModelProperty(
            name = "brokerClientTrustCertsFilePath",
            value = "Path for the trusted TLS certificate file for outgoing connection to a server (broker)"
    )
    private String brokerClientTrustCertsFilePath;
    @ApiModelProperty(
            name = "brokerClientKeyFilePath",
            value = "TLS private key file for internal client, "
                    + "used by the internal client to authenticate with Pulsar brokers")
    private String brokerClientKeyFilePath;
    @ApiModelProperty(
            name = "brokerClientCertificateFilePath",
            value = "TLS certificate file for internal client, "
                    + "used by the internal client to authenticate with Pulsar brokers"
    )
    private String brokerClientCertificateFilePath;
    @ApiModelProperty(
            name = "listenerName",
            value = "listenerName when client would like to connect to cluster",
            example = ""
    )
    private String listenerName;
    @ApiModelProperty(
            name = "migrated",
            value = "flag to check if cluster is migrated to different cluster",
            example = "true/false"
    )
    private boolean migrated;
    @ApiModelProperty(
            name = "migratedClusterUrl",
            value = "url of cluster where current cluster is migrated"
    )
    private ClusterUrl migratedClusterUrl;

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
                .listenerName(listenerName)
                .migrated(migrated)
                .migratedClusterUrl(migratedClusterUrl);
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
        private String listenerName;
        private boolean migrated;
        private ClusterUrl migratedClusterUrl;

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


        public ClusterDataImplBuilder listenerName(String listenerName) {
            this.listenerName = listenerName;
            return this;
        }

        public ClusterDataImplBuilder migrated(boolean migrated) {
            this.migrated = migrated;
            return this;
        }

        public ClusterDataImplBuilder migratedClusterUrl(ClusterUrl migratedClusterUrl) {
            this.migratedClusterUrl = migratedClusterUrl;
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
                    listenerName,
                    migrated,
                    migratedClusterUrl);
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
    }
}
