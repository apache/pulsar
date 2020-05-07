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
package org.apache.pulsar.websocket.service;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

import com.google.common.collect.Sets;

public class WebSocketProxyConfiguration implements PulsarConfiguration {

    // Number of threads used by Proxy server
    public static final int PROXY_SERVER_EXECUTOR_THREADS = 2 * Runtime.getRuntime().availableProcessors();
    // Number of threads used by Websocket service
    public static final int WEBSOCKET_SERVICE_THREADS = 20;
    // Number of threads used by Global ZK
    public static final int GLOBAL_ZK_THREADS = 8;

    // Name of the cluster to which this broker belongs to
    @FieldContext(required = true)
    private String clusterName;

    // Pulsar cluster url to connect to broker (optional if configurationStoreServers present)
    private String serviceUrl;
    private String serviceUrlTls;
    private String brokerServiceUrl;
    private String brokerServiceUrlTls;

    // Path for the file used to determine the rotation status for the broker
    // when responding to service discovery health checks
    private String statusFilePath;

    // Configuration Store connection string
    @Deprecated
    private String globalZookeeperServers;
    private String configurationStoreServers;
    // Zookeeper session timeout in milliseconds
    private long zooKeeperSessionTimeoutMillis = 30000;
    // ZooKeeper cache expiry time in seconds
    private int zooKeeperCacheExpirySeconds = 300;

    // Port to use to server HTTP request
    private Optional<Integer> webServicePort = Optional.of(8080);
    // Port to use to server HTTPS request
    private Optional<Integer> webServicePortTls = Optional.empty();
    // Hostname or IP address the service binds on, default is 0.0.0.0.
    private String bindAddress;
    // --- Authentication ---
    // Enable authentication
    private boolean authenticationEnabled;
    // Autentication provider name list, which is a list of class names
    private Set<String> authenticationProviders = Sets.newTreeSet();
    // Enforce authorization
    private boolean authorizationEnabled;
    // Authorization provider fully qualified class-name
    private String authorizationProvider = PulsarAuthorizationProvider.class.getName();

    // Role names that are treated as "super-user", meaning they will be able to
    // do all admin operations and publish/consume from all topics
    private Set<String> superUserRoles = Sets.newTreeSet();

    // Allow wildcard matching in authorization
    // (wildcard matching only applicable if wildcard-char:
    // * presents at first or last position eg: *.pulsar.service, pulsar.service.*)
    private boolean authorizationAllowWildcardsMatching = false;

    // Authentication settings of the proxy itself. Used to connect to brokers
    private String brokerClientAuthenticationPlugin;
    private String brokerClientAuthenticationParameters;
    // Path for the trusted TLS certificate file for outgoing connection to a server (broker)
    private String brokerClientTrustCertsFilePath = "";

    // Number of IO threads in Pulsar Client used in WebSocket proxy
    private int webSocketNumIoThreads = Runtime.getRuntime().availableProcessors();

    // Number of threads to use in HTTP server
    private int numHttpServerThreads = Math.max(6, Runtime.getRuntime().availableProcessors());

    // Number of connections per Broker in Pulsar Client used in WebSocket proxy
    private int webSocketConnectionsPerBroker = Runtime.getRuntime().availableProcessors();
    // Time in milliseconds that idle WebSocket session times out
    private int webSocketSessionIdleTimeoutMillis = 300000;

    // When this parameter is not empty, unauthenticated users perform as anonymousUserRole
    private String anonymousUserRole = null;

    /***** --- TLS --- ****/
    @Deprecated
    private boolean tlsEnabled = false;

    private boolean brokerClientTlsEnabled = false;
    // Path for the TLS certificate file
    private String tlsCertificateFilePath;
    // Path for the TLS private key file
    private String tlsKeyFilePath;
    // Path for the trusted TLS certificate file
    private String tlsTrustCertsFilePath = "";
    // Accept untrusted TLS certificate from client
    private boolean tlsAllowInsecureConnection = false;
    // Specify whether Client certificates are required for TLS
    // Reject the Connection if the Client Certificate is not trusted.
    private boolean tlsRequireTrustedClientCertOnConnect = false;
    // Tls cert refresh duration in seconds (set 0 to check on every new connection) 
    private long tlsCertRefreshCheckDurationSec = 300;

    private Properties properties = new Properties();

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public String getServiceUrlTls() {
        return serviceUrlTls;
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

    public String getStatusFilePath() {
        return statusFilePath;
    }

    public void setStatusFilePath(String statusFilePath) {
        this.statusFilePath = statusFilePath;
    }

    @Deprecated
    public String getGlobalZookeeperServers() {
        return globalZookeeperServers;
    }

    @Deprecated
    public void setGlobalZookeeperServers(String globalZookeeperServers) {
        this.globalZookeeperServers = globalZookeeperServers;
    }

    public String getConfigurationStoreServers() {
        return null == configurationStoreServers ? getGlobalZookeeperServers() : configurationStoreServers;
    }

    public void setConfigurationStoreServers(String configurationStoreServers) {
        this.configurationStoreServers = configurationStoreServers;
    }

    public long getZooKeeperSessionTimeoutMillis() {
        return zooKeeperSessionTimeoutMillis;
    }

    public void setZooKeeperSessionTimeoutMillis(long zooKeeperSessionTimeoutMillis) {
        this.zooKeeperSessionTimeoutMillis = zooKeeperSessionTimeoutMillis;
    }

    public int getZooKeeperCacheExpirySeconds() {
        return zooKeeperCacheExpirySeconds;
    }

    public void setZooKeeperCacheExpirySeconds(int zooKeeperCacheExpirySeconds) {
        this.zooKeeperCacheExpirySeconds = zooKeeperCacheExpirySeconds;
    }

    public Optional<Integer> getWebServicePort() {
        return webServicePort;
    }

    public void setWebServicePort(Optional<Integer> webServicePort) {
        this.webServicePort = webServicePort;
    }

    public Optional<Integer> getWebServicePortTls() {
        return webServicePortTls;
    }

    public void setWebServicePortTls(Optional<Integer> webServicePortTls) {
        this.webServicePortTls = webServicePortTls;
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public boolean isAuthenticationEnabled() {
        return authenticationEnabled;
    }

    public void setAuthenticationEnabled(boolean authenticationEnabled) {
        this.authenticationEnabled = authenticationEnabled;
    }

    public void setAuthenticationProviders(Set<String> providersClassNames) {
        authenticationProviders = providersClassNames;
    }

    public Set<String> getAuthenticationProviders() {
        return authenticationProviders;
    }

    public boolean isAuthorizationEnabled() {
        return authorizationEnabled;
    }

    public void setAuthorizationEnabled(boolean authorizationEnabled) {
        this.authorizationEnabled = authorizationEnabled;
    }

    public String getAuthorizationProvider() {
        return authorizationProvider;
    }

    public void setAuthorizationProvider(String authorizationProvider) {
        this.authorizationProvider = authorizationProvider;
    }

    public boolean getAuthorizationAllowWildcardsMatching() {
        return authorizationAllowWildcardsMatching;
    }

    public void setAuthorizationAllowWildcardsMatching(boolean authorizationAllowWildcardsMatching) {
        this.authorizationAllowWildcardsMatching = authorizationAllowWildcardsMatching;
    }

    public Set<String> getSuperUserRoles() {
        return superUserRoles;
    }

    public void setSuperUserRoles(Set<String> superUserRoles) {
        this.superUserRoles = superUserRoles;
    }

    public String getBrokerClientAuthenticationPlugin() {
        return brokerClientAuthenticationPlugin;
    }

    public void setBrokerClientAuthenticationPlugin(String brokerClientAuthenticationPlugin) {
        this.brokerClientAuthenticationPlugin = brokerClientAuthenticationPlugin;
    }

    public String getBrokerClientTrustCertsFilePath() {
        return brokerClientTrustCertsFilePath;
    }

    public void setBrokerClientTrustCertsFilePath(String brokerClientTrustCertsFilePath) {
        this.brokerClientTrustCertsFilePath = brokerClientTrustCertsFilePath;
    }

    public String getBrokerClientAuthenticationParameters() {
        return brokerClientAuthenticationParameters;
    }

    public void setBrokerClientAuthenticationParameters(String brokerClientAuthenticationParameters) {
        this.brokerClientAuthenticationParameters = brokerClientAuthenticationParameters;
    }

    @Deprecated
    public int getNumIoThreads() {
        return getWebSocketNumIoThreads();
    }

    @Deprecated
    public void setNumIoThreads(int numIoThreads) {
        setWebSocketNumIoThreads(numIoThreads);
    }

    public int getWebSocketNumIoThreads() {
        return webSocketNumIoThreads;
    }

    public void setWebSocketNumIoThreads(int webSocketNumIoThreads) {
        this.webSocketNumIoThreads = webSocketNumIoThreads;
    }

    public int getNumHttpServerThreads() {
        return numHttpServerThreads;
    }

    public void setNumHttpServerThreads(int numHttpServerThreads) {
        this.numHttpServerThreads = numHttpServerThreads;
    }

    @Deprecated
    public int getConnectionsPerBroker() {
        return getWebSocketConnectionsPerBroker();
    }

    @Deprecated
    public void setConnectionsPerBroker(int connectionsPerBroker) {
        setWebSocketConnectionsPerBroker(connectionsPerBroker);
    }

    public int getWebSocketConnectionsPerBroker() {
        return webSocketConnectionsPerBroker;
    }

    public void setWebSocketConnectionsPerBroker(int webSocketConnectionsPerBroker) {
        this.webSocketConnectionsPerBroker = webSocketConnectionsPerBroker;
    }

    public int getWebSocketSessionIdleTimeoutMillis() {
        return webSocketSessionIdleTimeoutMillis;
    }

    public void setWebSocketSessionIdleTimeoutMillis(int webSocketSessionIdleTimeoutMillis) {
        this.webSocketSessionIdleTimeoutMillis = webSocketSessionIdleTimeoutMillis;
    }

    public String getAnonymousUserRole() {
        return anonymousUserRole;
    }

    public void setAnonymousUserRole(String anonymousUserRole) {
        this.anonymousUserRole = anonymousUserRole;
    }

    public boolean isBrokerClientTlsEnabled() {
        return brokerClientTlsEnabled || tlsEnabled;
    }

    public void setBrokerClientTlsEnabled(boolean brokerClientTlsEnabled) {
        this.brokerClientTlsEnabled = brokerClientTlsEnabled;
    }

    public String getTlsCertificateFilePath() {
        return tlsCertificateFilePath;
    }

    public void setTlsCertificateFilePath(String tlsCertificateFilePath) {
        this.tlsCertificateFilePath = tlsCertificateFilePath;
    }

    public String getTlsKeyFilePath() {
        return tlsKeyFilePath;
    }

    public void setTlsKeyFilePath(String tlsKeyFilePath) {
        this.tlsKeyFilePath = tlsKeyFilePath;
    }

    public String getTlsTrustCertsFilePath() {
        return tlsTrustCertsFilePath;
    }

    public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
    }

    public boolean isTlsAllowInsecureConnection() {
        return tlsAllowInsecureConnection;
    }

    public void setTlsAllowInsecureConnection(boolean tlsAllowInsecureConnection) {
        this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public boolean getTlsRequireTrustedClientCertOnConnect() {
        return tlsRequireTrustedClientCertOnConnect;
    }

    public void setTlsRequireTrustedClientCertOnConnect(boolean tlsRequireTrustedClientCertOnConnect) {
        this.tlsRequireTrustedClientCertOnConnect = tlsRequireTrustedClientCertOnConnect;
    }
    
    public long getTlsCertRefreshCheckDurationSec() {
        return tlsCertRefreshCheckDurationSec;
    }

    public void setTlsCertRefreshCheckDurationSec(long tlsCertRefreshCheckDurationSec) {
        this.tlsCertRefreshCheckDurationSec = tlsCertRefreshCheckDurationSec;
    }
}
