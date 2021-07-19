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

    // Local-Zookeeper quorum connection string
    private String zookeeperServers;
    // Global-Zookeeper quorum connection string
    @Deprecated
    private String globalZookeeperServers;
    // Configuration Store connection string
    private String configurationStoreServers;

    // ZooKeeper session timeout
    private int zookeeperSessionTimeoutMs = 30_000;

    // ZooKeeper cache expiry time in seconds
    private int zooKeeperCacheExpirySeconds=300;

    // Port to use to server binary-proto request
    private Optional<Integer> servicePort = Optional.ofNullable(5000);
    // Port to use to server binary-proto-tls request
    private Optional<Integer> servicePortTls = Optional.empty();
    // Port to use to server HTTP request
    private Optional<Integer> webServicePort = Optional.ofNullable(8080);
    // Port to use to server HTTPS request
    private Optional<Integer> webServicePortTls = Optional.empty();
    // Control whether to bind directly on localhost rather than on normal
    // hostname
    private boolean bindOnLocalhost = false;

    // Role names that are treated as "super-user", meaning they will be able to
    // do all admin operations and publish/consume from all topics
    private Set<String> superUserRoles = Sets.newTreeSet();

    // Allow wildcard matching in authorization
    // (wildcard matching only applicable if wildcard-char:
    // * presents at first or last position eg: *.pulsar.service, pulsar.service.*)
    private boolean authorizationAllowWildcardsMatching = false;

    // Enable authentication
    private boolean authenticationEnabled = false;
    // Authentication provider name list, which is a list of class names
    private Set<String> authenticationProviders = Sets.newTreeSet();
    // Enforce authorization
    private boolean authorizationEnabled = false;
    // Authorization provider fully qualified class-name
    private String authorizationProvider = PulsarAuthorizationProvider.class.getName();

    /***** --- TLS --- ****/
    @Deprecated
    private boolean tlsEnabled = false;
    // Tls cert refresh duration in seconds (set 0 to check on every new connection)
    private long tlsCertRefreshCheckDurationSec = 300;
    // Path for the TLS certificate file
    private String tlsCertificateFilePath;
    // Path for the TLS private key file
    private String tlsKeyFilePath;
    // Path for the trusted TLS certificate file
    private String tlsTrustCertsFilePath = "";
    // Accept untrusted TLS certificate from client
    private boolean tlsAllowInsecureConnection = false;
    // Specify the tls protocols the broker will use to negotiate during TLS Handshake.
    // Example:- [TLSv1.3, TLSv1.2]
    private Set<String> tlsProtocols = Sets.newTreeSet();
    // Specify the tls cipher the broker will use to negotiate during TLS Handshake.
    // Example:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]
    private Set<String> tlsCiphers = Sets.newTreeSet();
    // Specify whether Client certificates are required for TLS
    // Reject the Connection if the Client Certificate is not trusted.
    private boolean tlsRequireTrustedClientCertOnConnect = false;

    /***** --- TLS with KeyStore--- ****/
    // Enable TLS with KeyStore type configuration in broker
    private boolean tlsEnabledWithKeyStore = false;
    // TLS Provider
    private String tlsProvider = null;
    // TLS KeyStore type configuration in broker: JKS, PKCS12
    private String tlsKeyStoreType = "JKS";
    // TLS KeyStore path in broker
    private String tlsKeyStore = null;
    // TLS KeyStore password in broker
    private String tlsKeyStorePassword = null;
    // TLS TrustStore type configuration in broker: JKS, PKCS12
    private String tlsTrustStoreType = "JKS";
    // TLS TrustStore path in broker
    private String tlsTrustStore = null;
    // TLS TrustStore password in broker"
    private String tlsTrustStorePassword = null;

    private Properties properties = new Properties();

    public String getConfigurationStoreServers() {
        return null == configurationStoreServers ? getGlobalZookeeperServers() : configurationStoreServers;
    }
}
