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
package org.apache.pulsar.client.impl.v5;

import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationData;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;

/**
 * Adapts between v5 Authentication and v4 Authentication interfaces.
 */
final class AuthenticationAdapter {

    private AuthenticationAdapter() {
    }

    /**
     * Create a v5 Authentication wrapping a v4 token auth.
     */
    static Authentication token(String token) {
        return new V5AuthWrapper(new AuthenticationToken(token));
    }

    /**
     * Create a v5 Authentication wrapping a v4 token supplier auth.
     */
    static Authentication token(Supplier<String> tokenSupplier) {
        return new V5AuthWrapper(new AuthenticationToken(tokenSupplier));
    }

    /**
     * Create a v5 Authentication wrapping a v4 TLS auth.
     */
    static Authentication tls(String certFilePath, String keyFilePath) {
        return new V5AuthWrapper(new AuthenticationTls(certFilePath, keyFilePath));
    }

    /**
     * Create a v5 Authentication by loading a v4 auth plugin by class name.
     */
    static Authentication create(String className, String params) throws PulsarClientException {
        try {
            var v4Auth = org.apache.pulsar.client.api.AuthenticationFactory.create(className, params);
            return new V5AuthWrapper(v4Auth);
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new PulsarClientException(e.getMessage(), e);
        }
    }

    /**
     * Create a v5 Authentication by loading a v4 auth plugin by class name and params map.
     */
    static Authentication create(String className, Map<String, String> params)
            throws PulsarClientException {
        try {
            var v4Auth = org.apache.pulsar.client.api.AuthenticationFactory.create(className, params);
            return new V5AuthWrapper(v4Auth);
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new PulsarClientException(e.getMessage(), e);
        }
    }

    /**
     * Unwrap a v5 Authentication to get the v4 Authentication.
     */
    static org.apache.pulsar.client.api.Authentication toV4(Authentication v5Auth) {
        if (v5Auth instanceof V5AuthWrapper wrapper) {
            return wrapper.v4Auth;
        }
        throw new IllegalArgumentException("Unknown v5 Authentication type: " + v5Auth.getClass());
    }

    /**
     * V5 Authentication that wraps a v4 Authentication.
     */
    private static final class V5AuthWrapper implements Authentication {
        final org.apache.pulsar.client.api.Authentication v4Auth;

        V5AuthWrapper(org.apache.pulsar.client.api.Authentication v4Auth) {
            this.v4Auth = v4Auth;
        }

        @Override
        public String authMethodName() {
            return v4Auth.getAuthMethodName();
        }

        @Override
        public AuthenticationData authData() throws PulsarClientException {
            try {
                AuthenticationDataProvider v4Data = v4Auth.getAuthData();
                return v4Data == null ? null : new V5AuthDataWrapper(v4Data);
            } catch (org.apache.pulsar.client.api.PulsarClientException e) {
                throw new PulsarClientException(e.getMessage(), e);
            }
        }

        @Override
        public AuthenticationData authData(String brokerHostName) throws PulsarClientException {
            try {
                AuthenticationDataProvider v4Data = v4Auth.getAuthData(brokerHostName);
                return v4Data == null ? null : new V5AuthDataWrapper(v4Data);
            } catch (org.apache.pulsar.client.api.PulsarClientException e) {
                throw new PulsarClientException(e.getMessage(), e);
            }
        }

        @Override
        public void initialize() throws PulsarClientException {
            try {
                v4Auth.start();
            } catch (org.apache.pulsar.client.api.PulsarClientException e) {
                throw new PulsarClientException(e.getMessage(), e);
            }
        }

        @Override
        public void close() {
            try {
                v4Auth.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Bridges a v4 {@link AuthenticationDataProvider} into the V5 {@link AuthenticationData}
     * surface. The two interfaces are intentionally aligned so this is a 1:1 delegation.
     */
    private static final class V5AuthDataWrapper implements AuthenticationData {
        private final AuthenticationDataProvider v4Data;

        V5AuthDataWrapper(AuthenticationDataProvider v4Data) {
            this.v4Data = v4Data;
        }

        @Override
        public boolean hasDataForHttp() {
            return v4Data.hasDataForHttp();
        }

        @Override
        public Set<Map.Entry<String, String>> getHttpHeaders() {
            try {
                Set<Map.Entry<String, String>> headers = v4Data.getHttpHeaders();
                return headers != null ? headers : Set.of();
            } catch (Exception e) {
                throw new RuntimeException("Failed to obtain HTTP auth headers", e);
            }
        }

        @Override
        public boolean hasDataForTls() {
            return v4Data.hasDataForTls();
        }

        @Override
        public Certificate[] getTlsCertificates() {
            return v4Data.getTlsCertificates();
        }

        @Override
        public PrivateKey getTlsPrivateKey() {
            return v4Data.getTlsPrivateKey();
        }

        @Override
        public boolean hasDataFromCommand() {
            return v4Data.hasDataFromCommand();
        }

        @Override
        public String getCommandData() {
            return v4Data.getCommandData();
        }
    }
}
