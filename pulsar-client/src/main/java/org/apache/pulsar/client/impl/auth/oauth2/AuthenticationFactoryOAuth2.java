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

import io.netty.resolver.NameResolver;
import java.net.InetAddress;
import java.net.URL;
import java.time.Clock;
import java.time.Duration;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.httpclient.AuthenticationHttpClientConfig;
import org.apache.pulsar.client.impl.auth.httpclient.AuthenticationHttpClientFactory;
import org.asynchttpclient.AsyncHttpClient;

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
        return clientCredentials(issuerUrl, credentialsUrl, audience, null);
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

    /**
     * A builder to create an authentication with client credentials.
     *
     * @return the builder
     */
    public static ClientCredentialsBuilder clientCredentialsBuilder() {
        return new ClientCredentialsBuilder();
    }

    public static class ClientCredentialsBuilder {

        private URL issuerUrl;
        private URL credentialsUrl;
        private String audience;
        private String scope;
        private Duration connectTimeout;
        private Duration readTimeout;
        private String trustCertsFilePath;
        private AsyncHttpClient httpClient;
        private NameResolver<InetAddress> nameResolver;

        private ClientCredentialsBuilder() {
        }

        /**
         * Required issuer URL.
         *
         * @param issuerUrl the issuer URL
         * @return the builder
         */
        public ClientCredentialsBuilder issuerUrl(URL issuerUrl) {
            this.issuerUrl = issuerUrl;
            return this;
        }

        /**
         * Required credentials URL.
         *
         * @param credentialsUrl the credentials URL
         * @return the builder
         */
        public ClientCredentialsBuilder credentialsUrl(URL credentialsUrl) {
            this.credentialsUrl = credentialsUrl;
            return this;
        }

        /**
         * Optional audience identifier used by some Identity Providers, like Auth0.
         *
         * @param audience the audiance
         * @return the builder
         */
        public ClientCredentialsBuilder audience(String audience) {
            this.audience = audience;
            return this;
        }

        /**
         * Optional scope expressed as a list of space-delimited, case-sensitive strings.
         * The strings are defined by the authorization server.
         * If the value contains multiple space-delimited strings, their order does not matter,
         * and each string adds an additional access range to the requested scope.
         * From here: https://datatracker.ietf.org/doc/html/rfc6749#section-4.4.2
         *
         * @param scope the scope
         * @return the builder
         */
        public ClientCredentialsBuilder scope(String scope) {
            this.scope = scope;
            return this;
        }

        /**
         * Optional HTTP connection timeout.
         *
         * @param connectTimeout the connect timeout
         * @return the builder
         */
        public ClientCredentialsBuilder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Optional HTTP read timeout.
         *
         * @param readTimeout the read timeout
         * @return the builder
         */
        public ClientCredentialsBuilder readTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        /**
         * Optional path to the file containing the trusted certificate(s) of the token issuer.
         *
         * @param trustCertsFilePath the path to the file containing the trusted certificate(s)
         * @return the builder
         */
        public ClientCredentialsBuilder trustCertsFilePath(String trustCertsFilePath) {
            this.trustCertsFilePath = trustCertsFilePath;
            return this;
        }

        /**
         * Optional custom HTTP client.
         *
         * @param httpClient the HTTP client
         * @return the builder
         */
        public ClientCredentialsBuilder httpClient(AsyncHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        /**
         * Optional custom name resolver.
         *
         * @param nameResolver the name resolver
         * @return the builder
         */
        public ClientCredentialsBuilder nameResolver(NameResolver<InetAddress> nameResolver) {
            this.nameResolver = nameResolver;
            return this;
        }

        /**
         * Authenticate with client credentials.
         *
         * @return an Authentication object
         */
        public Authentication build() {
            AsyncHttpClient finalHttpClient = this.httpClient;
            NameResolver<InetAddress> finalNameResolver = this.nameResolver;

            if (finalHttpClient == null || finalNameResolver == null) {
                AuthenticationHttpClientConfig.ConfigBuilder configBuilder =
                        AuthenticationHttpClientConfig.builder();

                if (connectTimeout != null) {
                    configBuilder.connectTimeout((int) connectTimeout.toMillis());
                }

                if (readTimeout != null) {
                    configBuilder.readTimeout((int) readTimeout.toMillis());
                }

                if (trustCertsFilePath != null) {
                    configBuilder.trustCertsFilePath(trustCertsFilePath);
                }

                AuthenticationHttpClientFactory clientFactory = new AuthenticationHttpClientFactory(
                        configBuilder.build(),
                        null
                );

                if (finalHttpClient == null) {
                    finalHttpClient = clientFactory.createHttpClient();
                }

                if (finalNameResolver == null) {
                    finalNameResolver = clientFactory.getNameResolver();
                }
            }
            ClientCredentialsFlow flow = ClientCredentialsFlow.builder()
                    .issuerUrl(issuerUrl)
                    .privateKey(credentialsUrl == null ? null : credentialsUrl.toExternalForm())
                    .audience(audience)
                    .scope(scope)
                    .httpClient(finalHttpClient)
                    .nameResolver(finalNameResolver)
                    .build();
            return new AuthenticationOAuth2(flow, Clock.systemDefaultZone());
        }

    }
}