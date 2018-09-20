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
package org.apache.pulsar.proxy.server;

import java.util.Properties;
import java.util.Set;

import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

import com.google.common.collect.Sets;

public class ProxyConfiguration implements PulsarConfiguration {

    // Local-Zookeeper quorum connection string
    private String zookeeperServers;
    @Deprecated
    // Global-Zookeeper quorum connection string
    private String globalZookeeperServers;

    // Configuration Store connection string
    private String configurationStoreServers;

    // ZooKeeper session timeout
    private int zookeeperSessionTimeoutMs = 30_000;

    // if Service Discovery is Disabled this url should point to the discovery service provider.
    private String brokerServiceURL;
    private String brokerServiceURLTLS;

    // These settings are unnecessary if `zookeeperServers` is specified
    private String brokerWebServiceURL;
    private String brokerWebServiceURLTLS;

    // function worker web services
    private String functionWorkerWebServiceURL;
    private String functionWorkerWebServiceURLTLS;

    // Port to use to server binary-proto request
    private int servicePort = 6650;
    // Port to use to server binary-proto-tls request
    private int servicePortTls = 6651;

    // Port to use to server HTTP request
    private int webServicePort = 8080;
    // Port to use to server HTTPS request
    private int webServicePortTls = 8443;

    // Path for the file used to determine the rotation status for the broker
    // when responding to service discovery health checks
    private String statusFilePath;

    // Role names that are treated as "super-user", meaning they will be able to
    // do all admin operations and publish/consume from all topics
    private Set<String> superUserRoles = Sets.newTreeSet();

    // Enable authentication
    private boolean authenticationEnabled = false;
    // Authentication provider name list, which is a list of class names
    private Set<String> authenticationProviders = Sets.newTreeSet();
    // Enforce authorization
    private boolean authorizationEnabled = false;
    // Authorization provider fully qualified class-name
    private String authorizationProvider = PulsarAuthorizationProvider.class.getName();
    // Forward client authData to Broker for re authorization
    // make sure authentication is enabled for this to take effect
    private boolean forwardAuthorizationCredentials = false;

    // Max concurrent inbound Connections
    private int maxConcurrentInboundConnections = 10000;

    // Max concurrent outbound Connections
    private int maxConcurrentLookupRequests = 50000;

    // Authentication settings of the proxy itself. Used to connect to brokers
    private String brokerClientAuthenticationPlugin;
    private String brokerClientAuthenticationParameters;
    private String brokerClientTrustCertsFilePath;

    /***** --- TLS --- ****/
    // Enable TLS for the proxy handler
    private boolean tlsEnabledInProxy = false;

    // Enable TLS when talking with the brokers
    private boolean tlsEnabledWithBroker = false;

    // Path for the TLS certificate file
    private String tlsCertificateFilePath;
    // Path for the TLS private key file
    private String tlsKeyFilePath;
    // Path for the trusted TLS certificate file
    private String tlsTrustCertsFilePath;
    // Accept untrusted TLS certificate from client
    private boolean tlsAllowInsecureConnection = false;
    // Validates hostname when proxy creates tls connection with broker
    private boolean tlsHostnameVerificationEnabled = false;
    // Specify the tls protocols the broker will use to negotiate during TLS Handshake.
    // Example:- [TLSv1.2, TLSv1.1, TLSv1]
    private Set<String> tlsProtocols = Sets.newTreeSet();
    // Specify the tls cipher the broker will use to negotiate during TLS Handshake.
    // Example:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]
    private Set<String> tlsCiphers = Sets.newTreeSet();
    // Specify whether Client certificates are required for TLS
    // Reject the Connection if the Client Certificate is not trusted.
    private boolean tlsRequireTrustedClientCertOnConnect = false;
    
    private Properties properties = new Properties();

    public boolean forwardAuthorizationCredentials() {
        return forwardAuthorizationCredentials;
    }

    public void setForwardAuthorizationCredentials(boolean forwardAuthorizationCredentials) {
        this.forwardAuthorizationCredentials = forwardAuthorizationCredentials;
    }

    public String getBrokerServiceURLTLS() {
        return brokerServiceURLTLS;
    }

    public void setBrokerServiceURLTLS(String discoveryServiceURLTLS) {
        this.brokerServiceURLTLS = discoveryServiceURLTLS;
    }

    public String getBrokerServiceURL() {
        return brokerServiceURL;
    }

    public void setBrokerServiceURL(String discoveryServiceURL) {
        this.brokerServiceURL = discoveryServiceURL;
    }

    public String getBrokerWebServiceURL() {
        return brokerWebServiceURL;
    }

    public void setBrokerWebServiceURL(String brokerWebServiceURL) {
        this.brokerWebServiceURL = brokerWebServiceURL;
    }

    public String getBrokerWebServiceURLTLS() {
        return brokerWebServiceURLTLS;
    }

    public void setBrokerWebServiceURLTLS(String brokerWebServiceURLTLS) {
        this.brokerWebServiceURLTLS = brokerWebServiceURLTLS;
    }

    public String getFunctionWorkerWebServiceURL() {
        return functionWorkerWebServiceURL;
    }

    public String getFunctionWorkerWebServiceURLTLS() {
        return functionWorkerWebServiceURLTLS;
    }

    public String getZookeeperServers() {
        return zookeeperServers;
    }

    public void setZookeeperServers(String zookeeperServers) {
        this.zookeeperServers = zookeeperServers;
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

    public int getZookeeperSessionTimeoutMs() {
        return zookeeperSessionTimeoutMs;
    }

    public void setZookeeperSessionTimeoutMs(int zookeeperSessionTimeoutMs) {
        this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
    }

    public int getServicePort() {
        return servicePort;
    }

    public void setServicePort(int servicePort) {
        this.servicePort = servicePort;
    }

    public int getServicePortTls() {
        return servicePortTls;
    }

    public void setServicePortTls(int servicePortTls) {
        this.servicePortTls = servicePortTls;
    }

    public int getWebServicePort() {
        return webServicePort;
    }

    public void setWebServicePort(int webServicePort) {
        this.webServicePort = webServicePort;
    }

    public int getWebServicePortTls() {
        return webServicePortTls;
    }

    public void setWebServicePortTls(int webServicePortTls) {
        this.webServicePortTls = webServicePortTls;
    }

    public String getStatusFilePath() {
        return statusFilePath;
    }

    public void setStatusFilePath(String statusFilePath) {
        this.statusFilePath = statusFilePath;
    }

    public boolean isTlsEnabledInProxy() {
        return tlsEnabledInProxy;
    }

    public void setTlsEnabledInProxy(boolean tlsEnabledInProxy) {
        this.tlsEnabledInProxy = tlsEnabledInProxy;
    }

    public boolean isTlsEnabledWithBroker() {
        return tlsEnabledWithBroker;
    }

    public void setTlsEnabledWithBroker(boolean tlsEnabledWithBroker) {
        this.tlsEnabledWithBroker = tlsEnabledWithBroker;
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

    public boolean isTlsHostnameVerificationEnabled() {
        return tlsHostnameVerificationEnabled;
    }

    public void setTlsHostnameVerificationEnabled(boolean tlsHostnameVerificationEnabled) {
        this.tlsHostnameVerificationEnabled = tlsHostnameVerificationEnabled;
    }

    public String getBrokerClientAuthenticationPlugin() {
        return brokerClientAuthenticationPlugin;
    }

    public void setBrokerClientAuthenticationPlugin(String brokerClientAuthenticationPlugin) {
        this.brokerClientAuthenticationPlugin = brokerClientAuthenticationPlugin;
    }

    public String getBrokerClientAuthenticationParameters() {
        return brokerClientAuthenticationParameters;
    }

    public void setBrokerClientAuthenticationParameters(String brokerClientAuthenticationParameters) {
        this.brokerClientAuthenticationParameters = brokerClientAuthenticationParameters;
    }

    public String getBrokerClientTrustCertsFilePath() {
        return this.brokerClientTrustCertsFilePath;
    }

    public void setBrokerClientTrustCertsFilePath(String brokerClientTlsTrustCertsFilePath) {
        this.brokerClientTrustCertsFilePath = brokerClientTlsTrustCertsFilePath;
    }

    public boolean isAuthenticationEnabled() {
        return authenticationEnabled;
    }

    public void setAuthenticationEnabled(boolean authenticationEnabled) {
        this.authenticationEnabled = authenticationEnabled;
    }

    public Set<String> getAuthenticationProviders() {
        return authenticationProviders;
    }

    public void setAuthenticationProviders(Set<String> authenticationProviders) {
        this.authenticationProviders = authenticationProviders;
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

    public Set<String> getSuperUserRoles() {
        return superUserRoles;
    }

    public void setSuperUserRoles(Set<String> superUserRoles) {
        this.superUserRoles = superUserRoles;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public Set<String> getTlsProtocols() {
        return tlsProtocols;
    }

    public void setTlsProtocols(Set<String> tlsProtocols) {
        this.tlsProtocols = tlsProtocols;
    }

    public Set<String> getTlsCiphers() {
        return tlsCiphers;
    }

    public void setTlsCiphers(Set<String> tlsCiphers) {
        this.tlsCiphers = tlsCiphers;
    }

    public int getMaxConcurrentInboundConnections() {
        return maxConcurrentInboundConnections;
    }

    public void setMaxConcurrentInboundConnections(int maxConcurrentInboundConnections) {
        this.maxConcurrentInboundConnections = maxConcurrentInboundConnections;
    }

    public int getMaxConcurrentLookupRequests() {
        return maxConcurrentLookupRequests;
    }

    public void setMaxConcurrentLookupRequests(int maxConcurrentLookupRequests) {
        this.maxConcurrentLookupRequests = maxConcurrentLookupRequests;
    }

    public boolean getTlsRequireTrustedClientCertOnConnect() {
        return tlsRequireTrustedClientCertOnConnect;
    }

    public void setTlsRequireTrustedClientCertOnConnect(boolean tlsRequireTrustedClientCertOnConnect) {
        this.tlsRequireTrustedClientCertOnConnect = tlsRequireTrustedClientCertOnConnect;
    }
}
