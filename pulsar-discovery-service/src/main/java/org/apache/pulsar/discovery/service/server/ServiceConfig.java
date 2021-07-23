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
package org.apache.pulsar.discovery.service.server;

import io.swagger.annotations.ApiModelProperty;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import lombok.Data;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.discovery.service.web.DiscoveryServiceServlet;

import com.google.common.collect.Sets;

/**
 * Service Configuration to start :{@link DiscoveryServiceServlet}
 *
 */
@Data
public class ServiceConfig implements PulsarConfiguration {

    @ApiModelProperty(
            name = "zookeeperServers",
            value = "Local ZooKeeper quorum connection string"
    )
    private String zookeeperServers;
    // Global-Zookeeper quorum connection string
    @Deprecated
    private String globalZookeeperServers;

    @ApiModelProperty(
            name = "configurationStoreServers",
            value = "Configuration store connection string"
    )
    private String configurationStoreServers;

    @ApiModelProperty(
            name = "zookeeperSessionTimeoutMs",
            value = "ZooKeeper session timeout (in million seconds)"
    )
    private int zookeeperSessionTimeoutMs = 30_000;

    @ApiModelProperty(
            name = "zooKeeperCacheExpirySeconds",
            value = "ZooKeeper cache expiry time (in seconds)"
    )
    private int zooKeeperCacheExpirySeconds=300;

    @ApiModelProperty(
            name = "servicePort",
            value = "Port used to server binary-proto request"
    )
    private Optional<Integer> servicePort = Optional.ofNullable(5000);

    @ApiModelProperty(
            name = "servicePortTls",
            value = "Port used to server binary-proto-tls request"
    )
    private Optional<Integer> servicePortTls = Optional.empty();

    @ApiModelProperty(
            name = "webServicePort",
            value = "Port used to server HTTP request"
    )
    private Optional<Integer> webServicePort = Optional.ofNullable(8080);

    @ApiModelProperty(
            name = "webServicePortTls",
            value = "Port used to server HTTPS request"
    )
    private Optional<Integer> webServicePortTls = Optional.empty();

    @ApiModelProperty(
            name = "bindOnLocalhost",
            value = "Control whether to bind directly on localhost rather than on normal hostname"
    )
    private boolean bindOnLocalhost = false;

    @ApiModelProperty(
            name = "superUserRoles",
            value = "Role names that are treated as \"super-user\", meaning they are able to "
                    + "do all admin operations and publish to or consume from all topics"
    )
    private Set<String> superUserRoles = Sets.newTreeSet();

    @ApiModelProperty(
            name = "authorizationAllowWildcardsMatching",
            value = "Allow wildcard matching in authorization (wildcard matching only applicable "
                    + "if wildcard char * presents at first or last position. "
                    + "For example, *.pulsar.service, pulsar.service.*"
    )
    private boolean authorizationAllowWildcardsMatching = false;

    @ApiModelProperty(
            name = "authenticationEnabled",
            value = "Whether enable authentication"
    )
    private boolean authenticationEnabled = false;

    @ApiModelProperty(
            name = "authenticationProviders",
            value = "Authentication provider name list, which is a list of class names"
    )
    private Set<String> authenticationProviders = Sets.newTreeSet();

    @ApiModelProperty(
            name = "authorizationEnabled",
            value = "Whether enforce authorization"
    )
    private boolean authorizationEnabled = false;

    @ApiModelProperty(
            name = "authorizationProvider",
            value = "Authorization provider fully qualified class-name"
    )
    private String authorizationProvider = PulsarAuthorizationProvider.class.getName();

    /***** --- TLS --- ****/
    @Deprecated
    private boolean tlsEnabled = false;

    @ApiModelProperty(
            name = "tlsCertRefreshCheckDurationSec",
            value = "TLS cert refresh duration (in seconds). 0 means checking every new connection."
    )
    private long tlsCertRefreshCheckDurationSec = 300;

    @ApiModelProperty(
            name = "tlsCertificateFilePath",
            value = "Path for the TLS certificate file"
    )
    private String tlsCertificateFilePath;

    @ApiModelProperty(
            name = "tlsKeyFilePath",
            value = "Path for the TLS private key file"
    )
    private String tlsKeyFilePath;

    @ApiModelProperty(
            name = "tlsTrustCertsFilePath",
            value = "Path for the trusted TLS certificate file"
    )
    private String tlsTrustCertsFilePath = "";

    @ApiModelProperty(
            name = "tlsAllowInsecureConnection",
            value = "Accept untrusted TLS certificate from client"
    )
    private boolean tlsAllowInsecureConnection = false;

    @ApiModelProperty(
            name = "tlsProtocols",
            value = "Specify the TLS protocols the broker uses to negotiate during TLS Handshake. "
                    + "Example: [TLSv1.3, TLSv1.2]"
    )
    private Set<String> tlsProtocols = Sets.newTreeSet();

    @ApiModelProperty(
            name = "tlsCiphers",
            value = "Specify the tls cipher the broker will use to negotiate during TLS Handshake. "
                    + "Example: [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]"
    )
    private Set<String> tlsCiphers = Sets.newTreeSet();

    @ApiModelProperty(
            name = "tlsRequireTrustedClientCertOnConnect",
            value = "Specify whether client certificates are required for TLS. "
                    + "Reject the connection if the client certificate is not trusted."
    )
    private boolean tlsRequireTrustedClientCertOnConnect = false;

    /***** --- TLS with KeyStore--- ****/
    @ApiModelProperty(
            name = "tlsEnabledWithKeyStore",
            value = "Enable TLS with KeyStore type configuration in broker"
    )
    private boolean tlsEnabledWithKeyStore = false;

    @ApiModelProperty(
            name = "tlsProvider",
            value = "Full class name of TLS Provider"
    )
    private String tlsProvider = null;

    @ApiModelProperty(
            name = "tlsKeyStoreType",
            value = "TLS KeyStore type configurations in broker are JKS or PKCS12"
    )
    private String tlsKeyStoreType = "JKS";

    @ApiModelProperty(
            name = "tlsKeyStore",
            value = "TLS KeyStore path in broker"
    )
    private String tlsKeyStore = null;

    @ApiModelProperty(
            name = "tlsKeyStorePassword",
            value = "TLS KeyStore password in broker"
    )
    private String tlsKeyStorePassword = null;

    @ApiModelProperty(
            name = "tlsTrustStoreType",
            value = "TLS TrustStore type configuration in broker are JKS or PKCS12"
    )
    private String tlsTrustStoreType = "JKS";

    @ApiModelProperty(
            name = "tlsTrustStore",
            value = "TLS TrustStore path in broker"
    )
    private String tlsTrustStore = null;

    @ApiModelProperty(
            name = "tlsTrustStorePassword",
            value = "TLS TrustStore password in broker"
    )
    private String tlsTrustStorePassword = null;

    @ApiModelProperty(
            name = "properties",
            value = "You can store string in key-value format"
    )
    private Properties properties = new Properties();

    public String getConfigurationStoreServers() {
        return null == configurationStoreServers ? getGlobalZookeeperServers() : configurationStoreServers;
    }
}
