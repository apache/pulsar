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
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.DefaultMetadataResolver;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenEndpointAuthMethod;

/**
 * Factory class that allows to create {@link Authentication} instances
 * for OAuth 2.0 authentication methods.
 *
 * <p>Use {@link #clientCredentialsBuilder()} to build an {@link Authentication} object
 * for the client credentials flow, with optional early token refresh support.
 */
public final class AuthenticationFactoryOAuth2 {

    /**
     * Authenticate with client credentials.
     *
     * @param issuerUrl      the issuer URL
     * @param credentialsUrl the credentials URL
     * @param audience       An optional field. The audience identifier used by some Identity Providers, like Auth0.
     * @return an Authentication object
     * @deprecated use {@link #clientCredentialsBuilder()}, instead.
     */
    @Deprecated
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
     * @deprecated use {@link #clientCredentialsBuilder()}, instead.
     */
    @Deprecated
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

    /**
     * A builder to create an authentication with client credentials using standard OAuth 2.0 metadata path
     * as defined in RFC 8414 ("/.well-known/oauth-authorization-server").
     *
     * @return the builder pre-configured to use standard OAuth 2.0 metadata path
     */
    public static ClientCredentialsBuilder clientCredentialsWithStandardAuthzServerBuilder() {
        return new ClientCredentialsBuilder()
                .wellKnownMetadataPath(DefaultMetadataResolver.OAUTH_WELL_KNOWN_METADATA_PATH);
    }

    public static class ClientCredentialsBuilder {

        private URL issuerUrl;
        private URL credentialsUrl;
        private TokenEndpointAuthMethod tokenEndpointAuthMethod = TokenEndpointAuthMethod.CLIENT_SECRET_POST;
        private String clientId;
        private String tlsCertFile;
        private String tlsKeyFile;
        private String audience;
        private String scope;
        private Duration connectTimeout;
        private Duration readTimeout;
        private String trustCertsFilePath;
        private String wellKnownMetadataPath;
        private Duration autoCertRefreshDuration;
        private double earlyTokenRefreshPercent = AuthenticationOAuth2.EARLY_TOKEN_REFRESH_PERCENT_DEFAULT;
        private ScheduledExecutorService scheduler;

        private ClientCredentialsBuilder() {
        }

        /**
         * Optional token endpoint auth method.
         * Defaults to {@code client_secret_post}.
         *
         * @param tokenEndpointAuthMethod the token endpoint auth method
         * @return the builder
         */
        public ClientCredentialsBuilder tokenEndpointAuthMethod(TokenEndpointAuthMethod tokenEndpointAuthMethod) {
            this.tokenEndpointAuthMethod = tokenEndpointAuthMethod;
            return this;
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
         * Only used by {@code client_secret_post}
         *
         * @param credentialsUrl the credentials URL
         * @return the builder
         */
        public ClientCredentialsBuilder credentialsUrl(URL credentialsUrl) {
            this.credentialsUrl = credentialsUrl;
            return this;
        }

        /**
         * Optional path to the file for a client certificate.
         * Required when {@code tokenEndpointAuthMethod} is {@code tls_client_auth}
         *
         * @param tlsCertFile the path to the file for a client certificate
         * @return the builder
         */
        public ClientCredentialsBuilder tlsCertFile(String tlsCertFile) {
            this.tlsCertFile = tlsCertFile;
            return this;
        }

        /**
         * Optional path to the file for a client private key.
         * Required when {@code tokenEndpointAuthMethod} is {@code tls_client_auth}
         *
         * @param tlsKeyFile the path to the file for a client private key
         * @return the builder
         */
        public ClientCredentialsBuilder tlsKeyFile(String tlsKeyFile) {
            this.tlsKeyFile = tlsKeyFile;
            return this;
        }

        /**
         * Optional client identifier issued by the authorization server.
         * Only used by {@code tls_client_auth}.
         * Defaults to {@code pulsar-client} when not provided.
         *
         * @param clientId the client identifier
         * @return the builder
         */
        public ClientCredentialsBuilder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        /**
         * Optional audience identifier used by some Identity Providers, like Auth0.
         *
         * @param audience the audience
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
         * Optional well-known metadata path.
         *
         * @param wellKnownMetadataPath the well-known metadata path (must start with "/.well-known/")
         * @return the builder
         */
        public ClientCredentialsBuilder wellKnownMetadataPath(String wellKnownMetadataPath) {
            this.wellKnownMetadataPath = wellKnownMetadataPath;
            return this;
        }

        /**
         * Optional certificate refresh interval.
         *
         * @param autoCertRefreshDuration the Certificate refresh interval
         * @return the builder
         */
        public ClientCredentialsBuilder autoCertRefreshDuration(Duration autoCertRefreshDuration) {
            this.autoCertRefreshDuration = autoCertRefreshDuration;
            return this;
        }

        /**
         * The fraction of the token's {@code expires_in} time at which the client starts attempting
         * a background refresh. Must be greater than 0. Values &ge; 1 disable early refresh (the default).
         *
         * <p>For example, {@code 0.8} means the client will attempt to refresh after 80% of the
         * token lifetime has elapsed, leaving a 20% buffer to tolerate a temporary OAuth server
         * outage while the existing token is still valid. During an outage the client keeps retrying
         * in the background with exponential backoff, continuing to serve requests with the current
         * token until it actually expires.
         *
         * @param earlyTokenRefreshPercent fractional value in (0, 1) to enable, or &ge; 1 to disable
         * @return the builder
         */
        public ClientCredentialsBuilder earlyTokenRefreshPercent(double earlyTokenRefreshPercent) {
            if (earlyTokenRefreshPercent <= 0) {
                throw new IllegalArgumentException("earlyTokenRefreshPercent must be greater than 0.");
            }
            this.earlyTokenRefreshPercent = earlyTokenRefreshPercent;
            return this;
        }

        /**
         * Optional scheduler for background token refresh tasks. If not set and early refresh is
         * enabled, a shared internal daemon-thread scheduler is used automatically.
         * {@link AuthenticationOAuth2} will never shut down a caller-supplied scheduler.
         *
         * @param scheduler the scheduler to use for background token refresh
         * @return the builder
         */
        public ClientCredentialsBuilder scheduler(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        /**
         * Builds the {@link Authentication} object.
         *
         * @return an Authentication object
         */
        public Authentication build() {
            Flow flow;
            if (tokenEndpointAuthMethod == TokenEndpointAuthMethod.CLIENT_SECRET_POST) {
                flow = ClientCredentialsFlow.builder()
                        .issuerUrl(issuerUrl)
                        .privateKey(credentialsUrl == null ? null : credentialsUrl.toExternalForm())
                        .audience(audience)
                        .scope(scope)
                        .connectTimeout(connectTimeout)
                        .readTimeout(readTimeout)
                        .trustCertsFilePath(trustCertsFilePath)
                        .certFile(tlsCertFile)
                        .keyFile(tlsKeyFile)
                        .autoCertRefreshDuration(autoCertRefreshDuration)
                        .wellKnownMetadataPath(wellKnownMetadataPath)
                        .build();
            } else if (tokenEndpointAuthMethod == TokenEndpointAuthMethod.TLS_CLIENT_AUTH) {
                if (StringUtils.isBlank(tlsCertFile) || StringUtils.isBlank(tlsKeyFile)) {
                    throw new IllegalArgumentException("Required configuration parameters: tlsCertFile, tlsKeyFile");
                }
                flow = TlsClientAuthFlow.builder()
                        .issuerUrl(issuerUrl)
                        .clientId(clientId)
                        .certFile(tlsCertFile)
                        .keyFile(tlsKeyFile)
                        .audience(audience)
                        .scope(scope)
                        .connectTimeout(connectTimeout)
                        .readTimeout(readTimeout)
                        .trustCertsFilePath(trustCertsFilePath)
                        .wellKnownMetadataPath(wellKnownMetadataPath)
                        .autoCertRefreshDuration(autoCertRefreshDuration)
                        .build();
            } else {
                throw new IllegalArgumentException("Unsupported auth method: " + tokenEndpointAuthMethod);
            }
            return new AuthenticationOAuth2(flow, earlyTokenRefreshPercent, scheduler);
        }

    }


}
