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
package org.apache.pulsar.client.impl.auth.oauth2;

import java.net.URL;
import java.time.Clock;
import org.apache.pulsar.client.api.Authentication;

/**
 * Factory class that allows to create {@link Authentication} instances
 * for OAuth 2.0 authentication methods.
 */
public final class AuthenticationFactoryOAuth2 {

    /**
     * Authenticate with client credentials.
     *
     * @param issuerUrl      the issuer URL
     * @param credentialsUrl the credentials URL
     * @param audience       An optional field. The audience identifier used by some Identity Providers, like Auth0.
     * @return an Authentication object
     */
    public static Authentication clientCredentials(URL issuerUrl, URL credentialsUrl, String audience) {
        return clientCredentialsBuilder().issuerUrl(issuerUrl).credentialsUrl(credentialsUrl).audience(audience)
                .build();
    }

    /**
     * Authenticate with client credentials.
     *
     * @param issuerUrl      the issuer URL
     * @param credentialsUrl the credentials URL
     * @param audience       An optional field. The audience identifier used by some Identity Providers, like Auth0.
     * @param scope          An optional field. The value of the scope parameter is expressed as a list of
     *                       space-delimited,
     *                       case-sensitive strings. The strings are defined by the authorization server.
     *                       If the value contains multiple space-delimited strings, their order does not matter,
     *                       and each string adds an additional access range to the requested scope.
     *                       From here: https://datatracker.ietf.org/doc/html/rfc6749#section-4.4.2
     * @return an Authentication object
     */
    public static Authentication clientCredentials(URL issuerUrl, URL credentialsUrl, String audience, String scope) {
        return clientCredentialsBuilder().issuerUrl(issuerUrl).credentialsUrl(credentialsUrl).audience(audience)
                .scope(scope).build();
    }

    public static ClientCredentialsBuilder clientCredentialsBuilder() {
        return new ClientCredentialsBuilder();
    }

    public static class ClientCredentialsBuilder {

        private URL issuerUrl;
        private URL credentialsUrl;
        private String audience;
        private String scope;
        private Integer connectTimeout;
        private Integer readTimeout;
        private String trustCertsFilePath;

        private ClientCredentialsBuilder() {
        }

        public ClientCredentialsBuilder issuerUrl(URL issuerUrl) {
            this.issuerUrl = issuerUrl;
            return this;
        }

        public ClientCredentialsBuilder credentialsUrl(URL credentialsUrl) {
            this.credentialsUrl = credentialsUrl;
            return this;
        }

        public ClientCredentialsBuilder audience(String audience) {
            this.audience = audience;
            return this;
        }

        public ClientCredentialsBuilder scope(String scope) {
            this.scope = scope;
            return this;
        }

        public ClientCredentialsBuilder connectTimeout(Integer connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public ClientCredentialsBuilder readTimeout(Integer readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public ClientCredentialsBuilder trustCertsFilePath(String trustCertsFilePath) {
            this.trustCertsFilePath = trustCertsFilePath;
            return this;
        }

        public Authentication build() {
            ClientCredentialsFlow flow = ClientCredentialsFlow.builder()
                    .issuerUrl(issuerUrl)
                    .privateKey(credentialsUrl.toExternalForm())
                    .audience(audience)
                    .scope(scope)
                    .connectTimeout(connectTimeout)
                    .readTimeout(readTimeout)
                    .trustCertsFilePath(trustCertsFilePath)
                    .build();
            return new AuthenticationOAuth2(flow, Clock.systemDefaultZone());
        }

    }


}
