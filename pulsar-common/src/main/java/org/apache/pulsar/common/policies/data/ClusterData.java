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

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.MoreObjects;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.LinkedHashSet;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.pulsar.client.api.ProxyProtocol;

/**
 * The configuration data for a cluster.
 */
@ApiModel(
    value = "ClusterData",
    description = "The configuration data for a cluster"
)
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class ClusterData {
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
    private boolean brokerClientTlsEnabled = false;
    @ApiModelProperty(
        name = "tlsAllowInsecureConnection",
        value = "Allow TLS connections to servers whose certificate cannot be"
                + " be verified to have been signed by a trusted certificate"
                + " authority."
    )
    private boolean tlsAllowInsecureConnection = false;
    @ApiModelProperty(
        name = "brokerClientTlsEnabledWithKeyStore",
        value = "Whether internal client use KeyStore type to authenticate with other Pulsar brokers"
    )
    private boolean brokerClientTlsEnabledWithKeyStore = false;
    @ApiModelProperty(
        name = "brokerClientTlsTrustStoreType",
        value = "TLS TrustStore type configuration for internal client: JKS, PKCS12"
                + " used by the internal client to authenticate with Pulsar brokers",
        example = "JKS"
    )
    private String brokerClientTlsTrustStoreType = "JKS";
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
        name = "brokerClientTrustCertsFilePath",
        value = "Path for the trusted TLS certificate file for outgoing connection to a server (broker)"
    )
    private String brokerClientTrustCertsFilePath;
    @ApiModelProperty(
            name = "listenerName",
            value = "listenerName when client would like to connect to cluster",
            example = ""
    )
    private String listenerName;

    public ClusterData(String serviceUrl) {
        this(serviceUrl, "");
    }

    public ClusterData(String serviceUrl, String serviceUrlTls) {
        this.serviceUrl = serviceUrl;
        this.serviceUrlTls = serviceUrlTls;
    }

    public ClusterData(String serviceUrl, String serviceUrlTls, String brokerServiceUrl, String brokerServiceUrlTls) {
        this.serviceUrl = serviceUrl;
        this.serviceUrlTls = serviceUrlTls;
        this.brokerServiceUrl = brokerServiceUrl;
        this.brokerServiceUrlTls = brokerServiceUrlTls;
    }

    public ClusterData(String serviceUrl, String serviceUrlTls, String brokerServiceUrl, String brokerServiceUrlTls,
                       String authenticationPlugin, String authenticationParameters) {
        this.serviceUrl = serviceUrl;
        this.serviceUrlTls = serviceUrlTls;
        this.brokerServiceUrl = brokerServiceUrl;
        this.brokerServiceUrlTls = brokerServiceUrlTls;
        this.authenticationPlugin = authenticationPlugin;
        this.authenticationParameters = authenticationParameters;
    }

    public ClusterData(String serviceUrl, String serviceUrlTls, String brokerServiceUrl, String brokerServiceUrlTls,
                       String authenticationPlugin, String authenticationParameters, String listenerName) {
        this.serviceUrl = serviceUrl;
        this.serviceUrlTls = serviceUrlTls;
        this.brokerServiceUrl = brokerServiceUrl;
        this.brokerServiceUrlTls = brokerServiceUrlTls;
        this.authenticationPlugin = authenticationPlugin;
        this.authenticationParameters = authenticationParameters;
        this.listenerName = listenerName;
    }

    public ClusterData(String serviceUrl, String serviceUrlTls, String brokerServiceUrl, String brokerServiceUrlTls,
                       String proxyServiceUrl, String authenticationPlugin, String authenticationParameters,
                       ProxyProtocol proxyProtocol) {
        this.serviceUrl = serviceUrl;
        this.serviceUrlTls = serviceUrlTls;
        this.brokerServiceUrl = brokerServiceUrl;
        this.brokerServiceUrlTls = brokerServiceUrlTls;
        this.authenticationPlugin = authenticationPlugin;
        this.authenticationParameters = authenticationParameters;
        this.proxyServiceUrl = proxyServiceUrl;
        this.proxyProtocol = proxyProtocol;
    }


    public void update(ClusterData other) {
        checkNotNull(other);
        this.serviceUrl = other.serviceUrl;
        this.serviceUrlTls = other.serviceUrlTls;
        this.brokerServiceUrl = other.brokerServiceUrl;
        this.brokerServiceUrlTls = other.brokerServiceUrlTls;
        this.proxyServiceUrl = other.proxyServiceUrl;
        this.proxyProtocol = other.proxyProtocol;
        this.authenticationPlugin = other.authenticationPlugin;
        this.authenticationParameters = other.authenticationParameters;
        this.brokerClientTlsEnabled = other.brokerClientTlsEnabled;
        this.tlsAllowInsecureConnection = other.tlsAllowInsecureConnection;
        this.brokerClientTlsEnabledWithKeyStore = other.brokerClientTlsEnabledWithKeyStore;
        this.brokerClientTlsTrustStoreType = other.brokerClientTlsTrustStoreType;
        this.brokerClientTlsTrustStore = other.brokerClientTlsTrustStore;
        this.brokerClientTlsTrustStorePassword = other.brokerClientTlsTrustStorePassword;
        this.brokerClientTrustCertsFilePath = other.brokerClientTrustCertsFilePath;
        this.listenerName = other.listenerName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ClusterData) {
            ClusterData other = (ClusterData) obj;
            return Objects.equals(serviceUrl, other.serviceUrl) && Objects.equals(serviceUrlTls, other.serviceUrlTls)
                    && Objects.equals(brokerServiceUrl, other.brokerServiceUrl)
                    && Objects.equals(brokerServiceUrlTls, other.brokerServiceUrlTls)
                    && Objects.equals(proxyServiceUrl, other.proxyServiceUrl)
                    && Objects.equals(proxyProtocol, other.proxyProtocol)
                    && Objects.equals(authenticationPlugin, other.authenticationPlugin)
                    && Objects.equals(authenticationParameters, other.authenticationParameters)
                    && Objects.equals(brokerClientTlsEnabled, other.brokerClientTlsEnabled)
                    && Objects.equals(tlsAllowInsecureConnection, other.tlsAllowInsecureConnection)
                    && Objects.equals(brokerClientTlsEnabledWithKeyStore, other.brokerClientTlsEnabledWithKeyStore)
                    && Objects.equals(brokerClientTlsTrustStoreType, other.brokerClientTlsTrustStoreType)
                    && Objects.equals(brokerClientTlsTrustStore, other.brokerClientTlsTrustStore)
                    && Objects.equals(brokerClientTlsTrustStorePassword, other.brokerClientTlsTrustStorePassword)
                    && Objects.equals(brokerClientTrustCertsFilePath, other.brokerClientTrustCertsFilePath)
                    && Objects.equals(listenerName, other.listenerName);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.toString());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("serviceUrl", serviceUrl)
                .add("serviceUrlTls", serviceUrlTls)
                .add("brokerServiceUrl", brokerServiceUrl)
                .add("brokerServiceUrlTls", brokerServiceUrlTls)
                .add("proxyServiceUrl", proxyServiceUrl)
                .add("proxyProtocol", proxyProtocol)
                .add("peerClusterNames", peerClusterNames)
                .add("authenticationPlugin", authenticationPlugin)
                .add("authenticationParameters", authenticationParameters)
                .add("brokerClientTlsEnabled", brokerClientTlsEnabled)
                .add("tlsAllowInsecureConnection", tlsAllowInsecureConnection)
                .add("brokerClientTlsEnabledWithKeyStore", brokerClientTlsEnabledWithKeyStore)
                .add("brokerClientTlsTrustStoreType", brokerClientTlsTrustStoreType)
                .add("brokerClientTlsTrustStore", brokerClientTlsTrustStore)
                .add("brokerClientTlsTrustStorePassword", brokerClientTlsTrustStorePassword)
                .add("brokerClientTrustCertsFilePath", brokerClientTrustCertsFilePath)
                .toString();
    }

}
