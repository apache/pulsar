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

import org.apache.pulsar.client.api.ProxyProtocol;

/**
 * The configuration data for a cluster.
 */
@ApiModel(
    value = "ClusterData",
    description = "The configuration data for a cluster"
)
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

    public ClusterData() {
    }

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
            String proxyServiceUrl, ProxyProtocol proxyProtocol) {
        this.serviceUrl = serviceUrl;
        this.serviceUrlTls = serviceUrlTls;
        this.brokerServiceUrl = brokerServiceUrl;
        this.brokerServiceUrlTls = brokerServiceUrlTls;
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
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getServiceUrlTls() {
        return serviceUrlTls;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public void setServiceUrlTls(String serviceUrlTls) {
        this.serviceUrlTls = serviceUrlTls;
    }

    public String getBrokerServiceUrl() {
        return brokerServiceUrl;
    }

    public void setBrokerServiceUrl(String brokerServiceUrl) {
        this.brokerServiceUrl = brokerServiceUrl;
    }

    public String getBrokerServiceUrlTls() {
        return brokerServiceUrlTls;
    }

    public void setBrokerServiceUrlTls(String brokerServiceUrlTls) {
        this.brokerServiceUrlTls = brokerServiceUrlTls;
    }

    public String getProxyServiceUrl() {
        return proxyServiceUrl;
    }

    public void setProxyServiceUrl(String proxyServiceUrl) {
        this.proxyServiceUrl = proxyServiceUrl;
    }

    public ProxyProtocol getProxyProtocol() {
        return proxyProtocol;
    }

    public void setProxyProtocol(ProxyProtocol proxyProtocol) {
        this.proxyProtocol = proxyProtocol;
    }

    public LinkedHashSet<String> getPeerClusterNames() {
        return peerClusterNames;
    }

    public void setPeerClusterNames(LinkedHashSet<String> peerClusterNames) {
        this.peerClusterNames = peerClusterNames;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ClusterData) {
            ClusterData other = (ClusterData) obj;
            return Objects.equals(serviceUrl, other.serviceUrl) && Objects.equals(serviceUrlTls, other.serviceUrlTls)
                    && Objects.equals(brokerServiceUrl, other.brokerServiceUrl)
                    && Objects.equals(brokerServiceUrlTls, other.brokerServiceUrlTls)
                    && Objects.equals(proxyServiceUrl, other.proxyServiceUrl)
                    && Objects.equals(proxyProtocol, other.proxyProtocol);
        }

        return false;
    }

    @Override
    public int hashCode() {
       return Objects.hash(this.toString());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("serviceUrl", serviceUrl).add("serviceUrlTls", serviceUrlTls)
                .add("brokerServiceUrl", brokerServiceUrl).add("brokerServiceUrlTls", brokerServiceUrlTls)
                .add("proxyServiceUrl", proxyServiceUrl).add("proxyProtocol", proxyProtocol)
                .add("peerClusterNames", peerClusterNames).toString();
    }

}
