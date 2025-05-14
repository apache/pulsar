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
package org.apache.pulsar.client.admin.internal;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;

public class PulsarAdminBuilderImpl implements PulsarAdminBuilder {

    @Getter
    protected ClientConfigurationData conf;

    private ClassLoader clientBuilderClassLoader = null;
    private boolean acceptGzipCompression = true;

    @Override
    public PulsarAdmin build() throws PulsarClientException {
        return new PulsarAdminImpl(conf.getServiceUrl(), conf, clientBuilderClassLoader, acceptGzipCompression);
    }

    public PulsarAdminBuilderImpl() {
        this.conf = new ClientConfigurationData();
        this.conf.setConnectionsPerBroker(16);
    }

    private PulsarAdminBuilderImpl(ClientConfigurationData conf) {
        this.conf = conf;
    }

    @Override
    public PulsarAdminBuilder clone() {
        PulsarAdminBuilderImpl pulsarAdminBuilder = new PulsarAdminBuilderImpl(conf.clone());
        pulsarAdminBuilder.clientBuilderClassLoader = clientBuilderClassLoader;
        pulsarAdminBuilder.acceptGzipCompression = acceptGzipCompression;
        return pulsarAdminBuilder;
    }

    @Override
    public PulsarAdminBuilder loadConf(Map<String, Object> config) {
        conf = ConfigurationDataUtils.loadData(config, conf, ClientConfigurationData.class);
        setAuthenticationFromPropsIfAvailable(conf);
        if (config.containsKey("acceptGzipCompression")) {
            Object acceptGzipCompressionObj = config.get("acceptGzipCompression");
            if (acceptGzipCompressionObj instanceof Boolean) {
                acceptGzipCompression = (Boolean) acceptGzipCompressionObj;
            } else {
                acceptGzipCompression = Boolean.parseBoolean(acceptGzipCompressionObj.toString());
            }
        }
        // in ClientConfigurationData, the maxConnectionsPerHost maps to connectionsPerBroker
        if (config.containsKey("maxConnectionsPerHost")) {
            Object maxConnectionsPerHostObj = config.get("maxConnectionsPerHost");
            if (maxConnectionsPerHostObj instanceof Integer) {
                maxConnectionsPerHost((Integer) maxConnectionsPerHostObj);
            } else {
                maxConnectionsPerHost(Integer.parseInt(maxConnectionsPerHostObj.toString()));
            }
        }
        return this;
    }

    @Override
    public PulsarAdminBuilder serviceHttpUrl(String serviceHttpUrl) {
        conf.setServiceUrl(serviceHttpUrl);
        return this;
    }

    @Override
    public PulsarAdminBuilder authentication(Authentication authentication) {
        conf.setAuthentication(authentication);
        return this;
    }

    @Override
    public PulsarAdminBuilder authentication(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException {
        conf.setAuthentication(AuthenticationFactory.create(authPluginClassName, authParams));
        return this;
    }

    @Override
    public PulsarAdminBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        conf.setAuthentication(AuthenticationFactory.create(authPluginClassName, authParamsString));
        return this;
    }

    private void setAuthenticationFromPropsIfAvailable(ClientConfigurationData clientConfig) {
        String authPluginClass = clientConfig.getAuthPluginClassName();
        String authParams = clientConfig.getAuthParams();
        Map<String, String> authParamMap = clientConfig.getAuthParamMap();
        if (StringUtils.isBlank(authPluginClass) || (StringUtils.isBlank(authParams) && authParamMap == null)) {
            return;
        }
        try {
            if (StringUtils.isNotBlank(authParams)) {
                authentication(authPluginClass, authParams);
            } else if (authParamMap != null) {
                authentication(authPluginClass, authParamMap);
            }
        } catch (UnsupportedAuthenticationException ex) {
            throw new RuntimeException("Failed to create authentication: " + ex.getMessage(), ex);
        }
    }

    @Override
    public PulsarAdminBuilder tlsKeyFilePath(String tlsKeyFilePath) {
        conf.setTlsKeyFilePath(tlsKeyFilePath);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsCertificateFilePath(String tlsCertificateFilePath) {
        conf.setTlsCertificateFilePath(tlsCertificateFilePath);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        conf.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
        return this;
    }

    @Override
    public PulsarAdminBuilder allowTlsInsecureConnection(boolean allowTlsInsecureConnection) {
        conf.setTlsAllowInsecureConnection(allowTlsInsecureConnection);
        return this;
    }

    @Override
    public PulsarAdminBuilder enableTlsHostnameVerification(boolean enableTlsHostnameVerification) {
        conf.setTlsHostnameVerificationEnable(enableTlsHostnameVerification);
        return this;
    }

    @Override
    public PulsarAdminBuilder useKeyStoreTls(boolean useKeyStoreTls) {
        conf.setUseKeyStoreTls(useKeyStoreTls);
        return this;
    }

    @Override
    public PulsarAdminBuilder sslProvider(String sslProvider) {
        conf.setSslProvider(sslProvider);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsKeyStoreType(String tlsKeyStoreType) {
        conf.setTlsKeyStoreType(tlsKeyStoreType);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsKeyStorePath(String tlsTrustStorePath) {
        conf.setTlsKeyStorePath(tlsTrustStorePath);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsKeyStorePassword(String tlsKeyStorePassword) {
        conf.setTlsKeyStorePassword(tlsKeyStorePassword);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsTrustStoreType(String tlsTrustStoreType) {
        conf.setTlsTrustStoreType(tlsTrustStoreType);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsTrustStorePath(String tlsTrustStorePath) {
        conf.setTlsTrustStorePath(tlsTrustStorePath);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsTrustStorePassword(String tlsTrustStorePassword) {
        conf.setTlsTrustStorePassword(tlsTrustStorePassword);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsCiphers(Set<String> tlsCiphers) {
        conf.setTlsCiphers(tlsCiphers);
        return this;
    }

    @Override
    public PulsarAdminBuilder sslFactoryPlugin(String sslFactoryPlugin) {
        if (StringUtils.isNotBlank(sslFactoryPlugin)) {
            conf.setSslFactoryPlugin(sslFactoryPlugin);
        }
        return this;
    }

    @Override
    public PulsarAdminBuilder sslFactoryPluginParams(String sslFactoryPluginParams) {
        conf.setSslFactoryPluginParams(sslFactoryPluginParams);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsProtocols(Set<String> tlsProtocols) {
        conf.setTlsProtocols(tlsProtocols);
        return this;
    }

    @Override
    public PulsarAdminBuilder connectionTimeout(int connectionTimeout, TimeUnit connectionTimeoutUnit) {
        this.conf.setConnectionTimeoutMs((int) connectionTimeoutUnit.toMillis(connectionTimeout));
        return this;
    }

    @Override
    public PulsarAdminBuilder readTimeout(int readTimeout, TimeUnit readTimeoutUnit) {
        this.conf.setReadTimeoutMs((int) readTimeoutUnit.toMillis(readTimeout));
        return this;
    }

    @Override
    public PulsarAdminBuilder requestTimeout(int requestTimeout, TimeUnit requestTimeoutUnit) {
        this.conf.setRequestTimeoutMs((int) requestTimeoutUnit.toMillis(requestTimeout));
        return this;
    }

    @Override
    public PulsarAdminBuilder autoCertRefreshTime(int autoCertRefreshTime, TimeUnit autoCertRefreshTimeUnit) {
        this.conf.setAutoCertRefreshSeconds((int) autoCertRefreshTimeUnit.toSeconds(autoCertRefreshTime));
        return this;
    }

    @Override
    public PulsarAdminBuilder setContextClassLoader(ClassLoader clientBuilderClassLoader) {
        this.clientBuilderClassLoader = clientBuilderClassLoader;
        return this;
    }

    @Override
    public PulsarAdminBuilder acceptGzipCompression(boolean acceptGzipCompression) {
        this.acceptGzipCompression = acceptGzipCompression;
        return this;
    }

    @Override
    public PulsarAdminBuilder maxConnectionsPerHost(int maxConnectionsPerHost) {
        // reuse the same configuration as the client, however for the admin client, the connection
        // is usually established to a cluster address and not to a broker address
        this.conf.setConnectionsPerBroker(maxConnectionsPerHost);
        return this;
    }

    @Override
    public PulsarAdminBuilder connectionMaxIdleSeconds(int connectionMaxIdleSeconds) {
        this.conf.setConnectionMaxIdleSeconds(connectionMaxIdleSeconds);
        return this;
    }
}
