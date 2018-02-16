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
package org.apache.pulsar.client.impl;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;

@SuppressWarnings("deprecation")
public class ClientBuilderImpl implements ClientBuilder {

    private static final long serialVersionUID = 1L;

    String serviceUrl;
    final ClientConfiguration conf = new ClientConfiguration();

    @Override
    public PulsarClient build() throws PulsarClientException {
        if (serviceUrl == null) {
            throw new IllegalArgumentException("service URL needs to be specified on the ClientBuilder object");
        }

        return new PulsarClientImpl(serviceUrl, conf);
    }

    @Override
    public ClientBuilder clone() {
        try {
            return (ClientBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ClientBuilderImpl");
        }
    }

    @Override
    public ClientBuilder serviceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
        return this;
    }

    @Override
    public ClientBuilder authentication(Authentication authentication) {
        conf.setAuthentication(authentication);
        return this;
    }

    @Override
    public ClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        conf.setAuthentication(authPluginClassName, authParamsString);
        return this;
    }

    @Override
    public ClientBuilder authentication(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException {
        conf.setAuthentication(authPluginClassName, authParams);
        return this;
    }

    @Override
    public ClientBuilder operationTimeout(int operationTimeout, TimeUnit unit) {
        conf.setOperationTimeout(operationTimeout, unit);
        return this;
    }

    @Override
    public ClientBuilder ioThreads(int numIoThreads) {
        conf.setIoThreads(numIoThreads);
        return this;
    }

    @Override
    public ClientBuilder listenerThreads(int numListenerThreads) {
        conf.setListenerThreads(numListenerThreads);
        return this;
    }

    @Override
    public ClientBuilder connectionsPerBroker(int connectionsPerBroker) {
        conf.setConnectionsPerBroker(connectionsPerBroker);
        return this;
    }

    @Override
    public ClientBuilder enableTcpNoDelay(boolean useTcpNoDelay) {
        conf.setUseTcpNoDelay(useTcpNoDelay);
        return this;
    }

    @Override
    public ClientBuilder enableTls(boolean useTls) {
        conf.setUseTls(useTls);
        return this;
    }

    @Override
    public ClientBuilder enableTlsHostnameVerification(boolean enableTlsHostnameVerification) {
        conf.setTlsHostnameVerificationEnable(enableTlsHostnameVerification);
        return this;
    }

    @Override
    public ClientBuilder tlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        conf.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
        return this;
    }

    @Override
    public ClientBuilder allowTlsInsecureConnection(boolean tlsAllowInsecureConnection) {
        conf.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
        return this;
    }

    @Override
    public ClientBuilder statsInterval(long statsInterval, TimeUnit unit) {
        conf.setStatsInterval(statsInterval, unit);
        return this;
    }

    @Override
    public ClientBuilder maxConcurrentLookupRequests(int concurrentLookupRequests) {
        conf.setConcurrentLookupRequest(concurrentLookupRequests);
        return this;
    }

    @Override
    public ClientBuilder maxNumberOfRejectedRequestPerConnection(int maxNumberOfRejectedRequestPerConnection) {
        conf.setMaxNumberOfRejectedRequestPerConnection(maxNumberOfRejectedRequestPerConnection);
        return this;
    }
}
