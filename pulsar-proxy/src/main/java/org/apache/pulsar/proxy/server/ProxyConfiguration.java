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

import org.apache.pulsar.common.configuration.PulsarConfiguration;

import com.google.common.collect.Sets;

public class ProxyConfiguration implements PulsarConfiguration {

    // Local-Zookeeper quorum connection string
    private String zookeeperServers;
    // Global-Zookeeper quorum connection string
    private String globalZookeeperServers;

    // ZooKeeper session timeout
    private int zookeeperSessionTimeoutMs = 30_000;

    // Port to use to server binary-proto request
    private int servicePort = 6650;
    // Port to use to server binary-proto-tls request
    private int servicePortTls = 6651;

    // Port to use to server HTTP request
    private int webServicePort = 8080;
    // Port to use to server HTTPS request
    private int webServicePortTls = 8443;

    // Role names that are treated as "super-user", meaning they will be able to
    // do all admin operations and publish/consume from all topics
    private Set<String> superUserRoles = Sets.newTreeSet();

    // Enable authentication
    private boolean authenticationEnabled = false;
    // Authentication provider name list, which is a list of class names
    private Set<String> authenticationProviders = Sets.newTreeSet();
    // Enforce authorization
    private boolean authorizationEnabled = false;

    // Authentication settings of the proxy itself. Used to connect to brokers
    private String brokerClientAuthenticationPlugin;
    private String brokerClientAuthenticationParameters;

    /***** --- TLS --- ****/
    // Enable TLS for the proxy handler
    private boolean tlsEnabledInProxy = false;

    // Enable TLS when talking with the brokers
    private boolean tlsEnabledWithBroker = false;

    // Path for the TLS certificate file
    private String tlsCertificateFilePath;
    // Path for the TLS private key file
    private String tlsKeyFilePath;

    private Properties properties = new Properties();

    public String getZookeeperServers() {
        return zookeeperServers;
    }

    public void setZookeeperServers(String zookeeperServers) {
        this.zookeeperServers = zookeeperServers;
    }

    public String getGlobalZookeeperServers() {
        return globalZookeeperServers;
    }

    public void setGlobalZookeeperServers(String globalZookeeperServers) {
        this.globalZookeeperServers = globalZookeeperServers;
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
}
