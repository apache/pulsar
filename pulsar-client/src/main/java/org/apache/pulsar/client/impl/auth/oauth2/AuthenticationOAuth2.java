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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationInitContext;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.client.impl.auth.httpclient.AuthenticationHttpClientConfig;
import org.apache.pulsar.client.impl.auth.httpclient.AuthenticationHttpClientFactory;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenResult;

/**
 * Pulsar client authentication provider based on OAuth 2.0.
 */
@Slf4j
public class AuthenticationOAuth2 implements Authentication, EncodedAuthenticationParameterSupport {

    public static final String CONFIG_PARAM_TYPE = "type";
    public static final String TYPE_CLIENT_CREDENTIALS = "client_credentials";
    public static final String AUTH_METHOD_NAME = "token";
    public static final String CONFIG_PARAM_CONNECT_TIMEOUT = "connectTimeout";
    public static final String CONFIG_PARAM_READ_TIMEOUT = "readTimeout";
    public static final String CONFIG_PARAM_TRUST_CERTS_FILE_PATH = "trustCertsFilePath";
    public static final String CONFIG_PARAM_ISSUER_URL = "issuerUrl";
    public static final String CONFIG_PARAM_KEY_FILE = "privateKey";

    protected static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    protected static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(30);
    public static final double EXPIRY_ADJUSTMENT = 0.9;
    private static final long serialVersionUID = 1L;

    final Clock clock;
    Flow flow;
    transient CachedToken cachedToken;
    private Map<String, String> params;
    private URL issuerUrl;
    private String privateKeyUrl;

    public AuthenticationOAuth2() {
        this.clock = Clock.systemDefaultZone();
    }

    AuthenticationOAuth2(Flow flow, Clock clock) {
        this.flow = flow;
        this.clock = clock;
    }

    @Override
    public String getAuthMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public void configure(String encodedAuthParamString) {
        if (StringUtils.isBlank(encodedAuthParamString)) {
            throw new IllegalArgumentException("No authentication parameters were provided");
        }
        try {
            this.params = AuthenticationUtil.configureFromJsonString(encodedAuthParamString);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed authentication parameters", e);
        }
        String type = this.params.getOrDefault(CONFIG_PARAM_TYPE, TYPE_CLIENT_CREDENTIALS);
        if (!type.equals(TYPE_CLIENT_CREDENTIALS)) {
            throw new IllegalArgumentException("Unsupported authentication type: " + type);
        }
        this.issuerUrl = parseParameterUrl(params, CONFIG_PARAM_ISSUER_URL);
        this.privateKeyUrl = parseParameterString(params, CONFIG_PARAM_KEY_FILE);
    }

    private void initializeFlow(AuthenticationInitContext context) {
        AuthenticationHttpClientConfig config = buildHttpConfig(params);
        AuthenticationHttpClientFactory httpClientFactory =
                new AuthenticationHttpClientFactory(config, context);
        this.flow = ClientCredentialsFlow.fromParameters(
                params, httpClientFactory.getNameResolver(),
                httpClientFactory.createHttpClient(), issuerUrl, privateKeyUrl);
    }

    static String parseParameterString(Map<String, String> params, String name) {
        String s = params.get(name);
        if (StringUtils.isEmpty(s)) {
            throw new IllegalArgumentException("Required configuration parameter: " + name);
        }
        return s;
    }


    static URL parseParameterUrl(Map<String, String> params, String name) {
        String s = params.get(name);
        if (StringUtils.isEmpty(s)) {
            throw new IllegalArgumentException("Required configuration parameter: " + name);
        }
        try {
            return new URL(s);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Malformed configuration parameter: " + name);
        }
    }


    private static int getParameterDurationToMillis(String name, Duration value, Duration defaultValue) {
        Duration duration;
        if (value == null) {
            log.info("Configuration for [{}] is using the default value: [{}]", name, defaultValue);
            duration = defaultValue;
        } else {
            log.info("Configuration for [{}] is: [{}]", name, value);
            duration = value;
        }

        return (int) duration.toMillis();
    }


    static int parseParameterDuration(Map<String, String> params, String name, Duration defaultValue) {
        String value = params.get(name);
        if (StringUtils.isNotBlank(value)) {
            try {
                return getParameterDurationToMillis(name, Duration.parse(value), defaultValue);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Malformed configuration parameter: " + name, e);
            }
        }
        return (int) defaultValue.toMillis();
    }


    private AuthenticationHttpClientConfig buildHttpConfig(
            Map<String, String> params) {
        int connectTimeout = parseParameterDuration(params, CONFIG_PARAM_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
        int readTimeout = parseParameterDuration(params, CONFIG_PARAM_READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
        String trustCertsFilePath = params.get(CONFIG_PARAM_TRUST_CERTS_FILE_PATH);
        return new AuthenticationHttpClientConfig(readTimeout, connectTimeout, trustCertsFilePath);
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
        throw new NotImplementedException("Deprecated; use EncodedAuthenticationParameterSupport");
    }

    @Override
    public void start() throws PulsarClientException {
        start(null);
    }

    @Override
    public void start(AuthenticationInitContext context) throws PulsarClientException {
        if (flow == null) {
            initializeFlow(context);
        }
        flow.initialize();
    }

    @Override
    public synchronized AuthenticationDataProvider getAuthData() throws PulsarClientException {
        if (this.cachedToken == null || this.cachedToken.isExpired()) {
            TokenResult tr = this.flow.authenticate();
            this.cachedToken = new CachedToken(tr);
        }
        return this.cachedToken.getAuthData();
    }

    @Override
    public void close() throws IOException {
        try {
            flow.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Data
    class CachedToken {
        private final TokenResult latest;
        private final Instant expiresAt;
        private final AuthenticationDataOAuth2 authData;

        public CachedToken(TokenResult latest) {
            this.latest = latest;
            int adjustedExpiresIn = (int) (latest.getExpiresIn() * EXPIRY_ADJUSTMENT);
            this.expiresAt = AuthenticationOAuth2.this.clock.instant().plusSeconds(adjustedExpiresIn);
            this.authData = new AuthenticationDataOAuth2(latest.getAccessToken());
        }

        public boolean isExpired() {
            return AuthenticationOAuth2.this.clock.instant().isAfter(this.expiresAt);
        }
    }
}

