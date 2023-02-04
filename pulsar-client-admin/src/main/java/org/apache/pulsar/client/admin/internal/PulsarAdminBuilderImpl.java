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
package org.apache.pulsar.client.admin.internal;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

    protected ClientConfigurationData conf;
    private int connectTimeout = PulsarAdminImpl.DEFAULT_CONNECT_TIMEOUT_SECONDS;
    private int readTimeout = PulsarAdminImpl.DEFAULT_READ_TIMEOUT_SECONDS;
    private int requestTimeout = PulsarAdminImpl.DEFAULT_REQUEST_TIMEOUT_SECONDS;
    private int autoCertRefreshTime = PulsarAdminImpl.DEFAULT_CERT_REFRESH_SECONDS;
    private TimeUnit connectTimeoutUnit = TimeUnit.SECONDS;
    private TimeUnit readTimeoutUnit = TimeUnit.SECONDS;
    private TimeUnit requestTimeoutUnit = TimeUnit.SECONDS;
    private TimeUnit autoCertRefreshTimeUnit = TimeUnit.SECONDS;
    private ClassLoader clientBuilderClassLoader = null;

    @Override
    public PulsarAdmin build() throws PulsarClientException {
        return new PulsarAdminImpl(conf.getServiceUrl(), conf, connectTimeout, connectTimeoutUnit, readTimeout,
                readTimeoutUnit, requestTimeout, requestTimeoutUnit, autoCertRefreshTime,
                autoCertRefreshTimeUnit, clientBuilderClassLoader);
    }

    public PulsarAdminBuilderImpl() {
        this.conf = new ClientConfigurationData();
    }

    private PulsarAdminBuilderImpl(ClientConfigurationData conf) {
        this.conf = conf;
    }

    @Override
    public PulsarAdminBuilder clone() {
        return new PulsarAdminBuilderImpl(conf.clone());
    }

    @Override
    public PulsarAdminBuilder loadConf(Map<String, Object> config) {
        conf = ConfigurationDataUtils.loadData(config, conf, ClientConfigurationData.class);
        setAuthenticationFromPropsIfAvailable(conf);
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
    public PulsarAdminBuilder tlsProtocols(Set<String> tlsProtocols) {
        conf.setTlsProtocols(tlsProtocols);
        return this;
    }

    @Override
    public PulsarAdminBuilder connectionTimeout(int connectionTimeout, TimeUnit connectionTimeoutUnit) {
        this.connectTimeout = connectionTimeout;
        this.connectTimeoutUnit = connectionTimeoutUnit;
        return this;
    }

    @Override
    public PulsarAdminBuilder readTimeout(int readTimeout, TimeUnit readTimeoutUnit) {
        this.readTimeout = readTimeout;
        this.readTimeoutUnit = readTimeoutUnit;
        return this;
    }

    @Override
    public PulsarAdminBuilder requestTimeout(int requestTimeout, TimeUnit requestTimeoutUnit) {
        this.requestTimeout = requestTimeout;
        this.requestTimeoutUnit = requestTimeoutUnit;
        return this;
    }

    @Override
    public PulsarAdminBuilder autoCertRefreshTime(int autoCertRefreshTime, TimeUnit autoCertRefreshTimeUnit) {
        this.autoCertRefreshTime = autoCertRefreshTime;
        this.autoCertRefreshTimeUnit = autoCertRefreshTimeUnit;
        return this;
    }

    @Override
    public PulsarAdminBuilder setContextClassLoader(ClassLoader clientBuilderClassLoader) {
        this.clientBuilderClassLoader = clientBuilderClassLoader;
        return this;
    }
}
