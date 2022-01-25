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
package org.apache.pulsar.client.impl.auth.oauth2;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.BackoffBuilder;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenResult;

/**
 * Pulsar client authentication provider based on OAuth 2.0.
 *
 * The class has an option to preemptively refresh the token by configuring the {@link #expiryAdjustment}. This value
 * must be greater than 0. When it is less than 1, it is treated as a percentage and is multiplied by the most recent
 * token's `expires_in` value to determine how early this class should start attempting to retrieve another token. When
 * it is greater than or equal to 1, this feature is turned off, and the client will only refresh the token when the
 * client attempts to use an already expired token.
 *
 * The current implementation of this class can block the calling thread.
 *
 * This class is intended to be called from multiple threads, and is therefore designed to be threadsafe.
 */
@Slf4j
public class AuthenticationOAuth2 implements Authentication, EncodedAuthenticationParameterSupport {

    public static final String CONFIG_PARAM_TYPE = "type";
    public static final String TYPE_CLIENT_CREDENTIALS = "client_credentials";
    public static final String AUTH_METHOD_NAME = "token";
    private static final long serialVersionUID = 1L;

    private final transient ScheduledThreadPoolExecutor scheduler;
    private final transient Backoff backoff;
    private final double expiryAdjustment;
    final Clock clock;
    volatile Flow flow;
    transient volatile CachedToken cachedToken;

    private AuthenticationOAuth2(Clock clock, double expiryAdjustment) {
        if (expiryAdjustment <= 0) {
            throw new IllegalArgumentException("ExpiryAdjustment must be greater than 0.");
        }
        this.clock = clock;
        this.expiryAdjustment = expiryAdjustment;
        boolean isPreemptiveTokenRefresh = expiryAdjustment < 1;
        this.backoff = isPreemptiveTokenRefresh ? new BackoffBuilder()
                .setInitialTime(100, TimeUnit.MILLISECONDS)
                .setMax(5, TimeUnit.MINUTES)
                .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                .create() : null;
        this.scheduler =  isPreemptiveTokenRefresh ? new ScheduledThreadPoolExecutor(1) : null;
    }

    public AuthenticationOAuth2() {
        this(Clock.systemDefaultZone(), 0.9);
    }

    AuthenticationOAuth2(Flow flow, Clock clock, double expiryAdjustment) {
        this(clock, expiryAdjustment);
        this.flow = flow;
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
        Map<String, String> params;
        try {
            params = AuthenticationUtil.configureFromJsonString(encodedAuthParamString);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed authentication parameters", e);
        }

        String type = params.getOrDefault(CONFIG_PARAM_TYPE, TYPE_CLIENT_CREDENTIALS);
        switch(type) {
            case TYPE_CLIENT_CREDENTIALS:
                this.flow = ClientCredentialsFlow.fromParameters(params);
                break;
            default:
                throw new IllegalArgumentException("Unsupported authentication type: " + type);
        }
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
        throw new NotImplementedException("Deprecated; use EncodedAuthenticationParameterSupport");
    }

    @Override
    public void start() throws PulsarClientException {
        flow.initialize();
    }

    /**
     * The first time that this method is called, it retrieves a token. All subsequent
     * calls should get a cached value. However, if there is an issue with the Identity
     * Provider, there is a chance that the background thread responsible for keeping
     * the refresh token hot will
     * @return The authentication data identifying this client that will be sent to the broker
     * @throws PulsarClientException
     */
    @Override
    public synchronized AuthenticationDataProvider getAuthData() throws PulsarClientException {
        if (this.cachedToken == null || this.cachedToken.isExpired()) {
            this.authenticate();
        }
        return this.cachedToken.getAuthData();
    }

    /**
     * Retrieve the token (synchronously), and then schedule refresh runnable.
     */
    private void authenticate() throws PulsarClientException {
        if (log.isDebugEnabled()) {
            log.debug("Attempting to retrieve OAuth2 token now.");
        }
        TokenResult tr = this.flow.authenticate();
        this.cachedToken = new CachedToken(tr);
        handleSuccessfulTokenRefresh();
    }

    private void handleSuccessfulTokenRefresh() {
        if (scheduler != null) {
            backoff.reset();
            long expiresInMillis = TimeUnit.SECONDS.toMillis(cachedToken.latest.getExpiresIn());
            scheduleRefresh((long) (expiresInMillis * expiryAdjustment));
        }
    }

    /**
     * Attempt to refresh the token. If successful, schedule the next refresh task according to the
     * {@link #expiryAdjustment}. If failed, schedule another attempt to refresh the token according to the
     * {@link #backoff} policy.
     */
    private void refreshToken() {
        try {
            this.authenticate();
        } catch (Throwable e) {
            long delayMillis = backoff.next();
            log.error("Error refreshing token. Will retry in {} millis.", delayMillis, e);
            scheduleRefresh(delayMillis);
        }
    }

    private void scheduleRefresh(long delayMillis) {
        scheduler.schedule(this::refreshToken, delayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        try {
            flow.close();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
        }
    }

    @Data
    class CachedToken {
        private final TokenResult latest;
        private final Instant expiresAt;
        private final AuthenticationDataOAuth2 authData;

        public CachedToken(TokenResult latest) {
            this.latest = latest;
            this.expiresAt = AuthenticationOAuth2.this.clock.instant().plusSeconds(latest.getExpiresIn());
            this.authData = new AuthenticationDataOAuth2(latest.getAccessToken());
        }

        public boolean isExpired() {
            return AuthenticationOAuth2.this.clock.instant().isAfter(this.expiresAt);
        }
    }
}

