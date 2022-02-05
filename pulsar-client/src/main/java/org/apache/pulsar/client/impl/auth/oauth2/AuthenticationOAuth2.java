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
import java.util.concurrent.ScheduledFuture;
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
 * The first call to {@link #getAuthData()} will result in a blocking network call to retrieve the OAuth2.0 token from
 * the Identity Provider. After that, there are two behaviors, depending on {@link #earlyTokenRefreshPercent}:
 *
 * 1. If {@link #earlyTokenRefreshPercent} is less than 1, this authentication class will schedule a runnable to refresh
 * the token in n seconds where n is the result of multiplying {@link #earlyTokenRefreshPercent} and the `expires_in`
 * value returned by the Identity Provider. If the call to the Identity Provider fails, this class will retry attempting
 * to refresh the token using an exponential backoff. If the token is not refreshed before it expires, the Pulsar client
 * will make one final blocking call to the Identity Provider. If that call fails, this class will pass the failure to
 * the Pulsar client. This proactive approach to token management is good for use cases that want to avoid latency
 * spikes from calls to the Identity Provider and that want to be able to withstand short Identity Provider outages. The
 * tradeoff is that this class consumes slightly more resources.
 *
 * 2. If {@link #earlyTokenRefreshPercent} is greater than or equal to 1, this class will not retrieve a new token until
 * the {@link #getAuthData()} method is called while the cached token is expired. If the call to the Identity Provider
 * fails, this class will pass the failure to the Pulsar client. This lazy approach is good for use cases that are not
 * latency sensitive and that will not use the token frequently.
 *
 * {@link #earlyTokenRefreshPercent} must be greater than 0. It defaults to 1, which means that early token refresh is
 * disabled by default.
 *
 * The current implementation of this class can block the calling thread.
 *
 * This class is intended to be called from multiple threads, and is therefore designed to be thread-safe.
 */
@Slf4j
public class AuthenticationOAuth2 implements Authentication, EncodedAuthenticationParameterSupport {

    public static final String CONFIG_PARAM_TYPE = "type";
    public static final String TYPE_CLIENT_CREDENTIALS = "client_credentials";
    public static final int EARLY_TOKEN_REFRESH_PERCENT_DEFAULT = 1; // feature disabled by default
    public static final String AUTH_METHOD_NAME = "token";
    private static final long serialVersionUID = 1L;
    private final transient ScheduledThreadPoolExecutor scheduler;
    private final boolean createdScheduler;
    private final double earlyTokenRefreshPercent;

    final Clock clock;
    volatile Flow flow;
    private transient volatile CachedToken cachedToken;

    // Only ever updated in synchronized block on class.
    private boolean isClosed = false;

    // Only ever updated on the single scheduler thread. Do not need to be volatile.
    private transient Backoff backoff;
    private transient ScheduledFuture<?> nextRefreshAttempt;

    // No args constructor used when creating class with reflection
    public AuthenticationOAuth2() {
        this(Clock.systemDefaultZone(), EARLY_TOKEN_REFRESH_PERCENT_DEFAULT, null);
    }

    AuthenticationOAuth2(Flow flow,
                         double earlyTokenRefreshPercent,
                         ScheduledThreadPoolExecutor scheduler) {
        this(flow, Clock.systemDefaultZone(), earlyTokenRefreshPercent, scheduler);
    }

    AuthenticationOAuth2(Flow flow,
                         Clock clock,
                         double earlyTokenRefreshPercent,
                         ScheduledThreadPoolExecutor scheduler) {
        this(clock, earlyTokenRefreshPercent, scheduler);
        this.flow = flow;
    }

    /**
     * @param clock - clock to use when determining token expiration.
     * @param earlyTokenRefreshPercent - see javadoc for {@link AuthenticationOAuth2}. Must be greater than 0.
     * @param scheduler - The scheduler to use for background refreshes of the token. If null and the
     *                  {@link #earlyTokenRefreshPercent} is less than 1, the client will create an internal scheduler.
     *                  Otherwise, it will use the passed in scheduler. If the caller supplies a scheduler, the
     *                  {@link AuthenticationOAuth2} will not close it.
     */
    private AuthenticationOAuth2(Clock clock, double earlyTokenRefreshPercent, ScheduledThreadPoolExecutor scheduler) {
        if (earlyTokenRefreshPercent <= 0) {
            throw new IllegalArgumentException("EarlyTokenRefreshPercent must be greater than 0.");
        }
        this.earlyTokenRefreshPercent = earlyTokenRefreshPercent;
        this.clock = clock;
        if (scheduler == null && earlyTokenRefreshPercent < 1) {
            this.scheduler = new ScheduledThreadPoolExecutor(1);
            this.createdScheduler = true;
        } else {
            this.scheduler = scheduler;
            this.createdScheduler = false;
        }
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
        if (TYPE_CLIENT_CREDENTIALS.equals(type)) {
            this.flow = ClientCredentialsFlow.fromParameters(params);
        } else {
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
        if (isClosed) {
            throw new PulsarClientException.AlreadyClosedException("Authentication already closed.");
        }
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

    /**
     * When we successfully get a token, we need to schedule the next attempt to refresh it.
     * This is done completely based on the "expires_in" value returned by the identity provider.
     * The code is run on the single scheduler thread in order to ensure that the backoff and the nextRefreshAttempt are
     * updated safely.
     */
    private void handleSuccessfulTokenRefresh() {
        if (scheduler != null) {
            scheduler.execute(() -> {
                if (earlyTokenRefreshPercent < 1) {
                    backoff = buildBackoff(cachedToken.latest.getExpiresIn());
                    long expiresInMillis = TimeUnit.SECONDS.toMillis(cachedToken.latest.getExpiresIn());
                    scheduleRefresh((long) (expiresInMillis * earlyTokenRefreshPercent));
                }
            });
        }
    }

    /**
     * Attempt to refresh the token. If successful, schedule the next refresh task according to the
     * {@link #earlyTokenRefreshPercent}. If failed, schedule another attempt to refresh the token according to the
     * {@link #backoff} policy.
     */
    private void refreshToken() {
        try {
            this.authenticate();
        } catch (PulsarClientException | RuntimeException e) {
            long delayMillis = backoff.next();
            log.error("Error refreshing token. Will retry in {} millis.", delayMillis, e);
            scheduleRefresh(delayMillis);
        }
    }

    /**
     * Schedule the task to refresh the token.
     * NOTE: this method must be run on the {@link #scheduler} thread in order to ensure {@link #nextRefreshAttempt}
     * is accessed and updated safely.
     * @param delayMillis the time, in milliseconds, to wait before starting to attempt to refresh the token.
     */
    private void scheduleRefresh(long delayMillis) {
        nextRefreshAttempt = scheduler.schedule(this::refreshToken, delayMillis, TimeUnit.MILLISECONDS);
    }

    private Backoff buildBackoff(int expiresInSeconds) {
        return new BackoffBuilder()
                .setInitialTime(1, TimeUnit.SECONDS)
                .setMax(10, TimeUnit.MINUTES)
                // Attempt a final token refresh attempt 2 seconds before the token actually expires, if necessary.
                .setMandatoryStop(expiresInSeconds - 2, TimeUnit.SECONDS)
                .create();
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            isClosed = true;
            if (flow != null) {
                flow.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (createdScheduler) {
                this.scheduler.shutdownNow();
            } else if (scheduler != null) {
                // Cancel the all subsequent refresh attempts by canceling the next token refresh attempt. By running
                // this command on the single scheduler thread, we remove the chance for a race condition that could
                // allow a currently executing refresh attempt to schedule another refresh attempt.
                scheduler.execute(() -> {
                    if (nextRefreshAttempt != null) {
                        nextRefreshAttempt.cancel(false);
                    }
                });
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

