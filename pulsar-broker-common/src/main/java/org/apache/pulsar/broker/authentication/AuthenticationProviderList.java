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
package org.apache.pulsar.broker.authentication;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.apache.pulsar.common.api.AuthData;

/**
 * An authentication provider wraps a list of auth providers.
 */
@Slf4j
public class AuthenticationProviderList implements AuthenticationProvider {

    private AuthenticationMetrics authenticationMetrics;

    private interface AuthProcessor<T, W> {

        T apply(W process) throws AuthenticationException;

    }

    private enum ErrorCode {
        UNKNOWN,
        AUTH_REQUIRED,
    }

    private static <T, W> T applyAuthProcessor(List<W> processors, AuthenticationMetrics metrics,
                                               AuthProcessor<T, W> authFunc)
        throws AuthenticationException {
        AuthenticationException authenticationException = null;
        String errorCode = ErrorCode.UNKNOWN.name();
        for (W ap : processors) {
            try {
                return authFunc.apply(ap);
            } catch (AuthenticationException ae) {
                if (log.isDebugEnabled()) {
                    log.debug("Authentication failed for auth provider " + ap.getClass() + ": ", ae);
                }
                // Store the exception so we can throw it later instead of a generic one
                authenticationException = ae;
                errorCode = ap.getClass().getSimpleName() + "-INVALID-AUTH";
            }
        }

        if (null == authenticationException) {
            metrics.recordFailure(AuthenticationProviderList.class.getSimpleName(),
                    "authentication-provider-list", ErrorCode.AUTH_REQUIRED);
            throw new AuthenticationException("Authentication required");
        } else {
            metrics.recordFailure(AuthenticationProviderList.class.getSimpleName(),
                    "authentication-provider-list", errorCode);
            throw authenticationException;
        }
    }

    private static class AuthenticationListState implements AuthenticationState {

        private final List<AuthenticationState> states;
        private volatile AuthenticationState authState;
        private final AuthenticationMetrics metrics;

        AuthenticationListState(List<AuthenticationState> states, AuthenticationMetrics metrics) {
            if (states == null || states.isEmpty()) {
                throw new IllegalArgumentException("Authentication state requires at least one state");
            }
            this.states = states;
            this.authState = states.get(0);
            this.metrics = metrics;
        }

        private AuthenticationState getAuthState() throws AuthenticationException {
            if (authState != null) {
                return authState;
            } else {
                throw new AuthenticationException("Authentication state is not initialized");
            }
        }

        @Override
        public String getAuthRole() throws AuthenticationException {
            return getAuthState().getAuthRole();
        }

        @Override
        public CompletableFuture<AuthData> authenticateAsync(AuthData authData) {
            // First, attempt to authenticate with the current auth state
            CompletableFuture<AuthData> authChallengeFuture = new CompletableFuture<>();
            authState
                    .authenticateAsync(authData)
                    .whenComplete((authChallenge, ex) -> {
                        if (ex == null) {
                            // Current authState is still correct. Just need to return the authChallenge.
                            authChallengeFuture.complete(authChallenge);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Authentication failed for auth provider " + authState.getClass() + ": ", ex);
                            }
                            authenticateRemainingAuthStates(authChallengeFuture, authData, ex,
                                    states.isEmpty() ? -1 : 0);
                        }
                    });
            return authChallengeFuture;
        }

        private void authenticateRemainingAuthStates(CompletableFuture<AuthData> authChallengeFuture,
                                                     AuthData clientAuthData,
                                                     Throwable previousException,
                                                     int index) {
            if (index < 0 || index >= states.size()) {
                if (previousException == null) {
                    previousException = new AuthenticationException("Authentication required");
                }
                metrics.recordFailure(AuthenticationProviderList.class.getSimpleName(),
                        "authentication-provider-list",
                        ErrorCode.AUTH_REQUIRED);
                authChallengeFuture.completeExceptionally(previousException);
                return;
            }
            AuthenticationState state = states.get(index);
            if (state == authState) {
                // Skip the current auth state
                authenticateRemainingAuthStates(authChallengeFuture, clientAuthData, null, index + 1);
            } else {
                state.authenticateAsync(clientAuthData)
                        .whenComplete((authChallenge, ex) -> {
                            if (ex == null) {
                                // Found the correct auth state
                                authState = state;
                                authChallengeFuture.complete(authChallenge);
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("Authentication failed for auth provider "
                                            + authState.getClass() + ": ", ex);
                                }
                                authenticateRemainingAuthStates(authChallengeFuture, clientAuthData, ex, index + 1);
                            }
                        });
            }
        }

        @Override
        public AuthData authenticate(AuthData authData) throws AuthenticationException {
            return applyAuthProcessor(
                states,
                metrics,
                as -> {
                    AuthData ad = as.authenticate(authData);
                    AuthenticationListState.this.authState = as;
                    return ad;
                }
            );
        }

        @Override
        public AuthenticationDataSource getAuthDataSource() {
            return authState.getAuthDataSource();
        }

        @Override
        public boolean isComplete() {
            return authState.isComplete();
        }

        @Override
        public long getStateId() {
            if (null != authState) {
                return authState.getStateId();
            } else {
                return states.get(0).getStateId();
            }
        }

        @Override
        public boolean isExpired() {
            return authState.isExpired();
        }

        @Override
        public AuthData refreshAuthentication() throws AuthenticationException {
            return getAuthState().refreshAuthentication();
        }
    }

    private final List<AuthenticationProvider> providers;

    public AuthenticationProviderList(List<AuthenticationProvider> providers) {
        this.providers = providers;
    }

    public List<AuthenticationProvider> getProviders() {
        return providers;
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        initialize(Context.builder().config(config).build());
    }

    @Override
    public void initialize(Context context) throws IOException {
        authenticationMetrics = new AuthenticationMetrics(context.getOpenTelemetry(),
                getClass().getSimpleName(), getAuthMethodName());
        for (AuthenticationProvider ap : providers) {
            ap.initialize(context);
        }
    }

    @Override
    public String getAuthMethodName() {
        return providers.get(0).getAuthMethodName();
    }

    @Override
    public void incrementFailureMetric(Enum<?> errorCode) {
        authenticationMetrics.recordFailure(errorCode);
    }

    @Override
    public CompletableFuture<String> authenticateAsync(AuthenticationDataSource authData) {
        CompletableFuture<String> roleFuture = new CompletableFuture<>();
        authenticateRemainingAuthProviders(roleFuture, authData, null, providers.isEmpty() ? -1 : 0);
        return roleFuture;
    }

    private void authenticateRemainingAuthProviders(CompletableFuture<String> roleFuture,
                                                    AuthenticationDataSource authData,
                                                    Throwable previousException,
                                                    int index) {
        if (index < 0 || index >= providers.size()) {
            if (previousException == null) {
                previousException = new AuthenticationException("Authentication required");
            }
            authenticationMetrics.recordFailure(AuthenticationProvider.class.getSimpleName(),
                    "authentication-provider-list", ErrorCode.AUTH_REQUIRED);
            roleFuture.completeExceptionally(previousException);
            return;
        }
        AuthenticationProvider provider = providers.get(index);
        provider.authenticateAsync(authData)
                .whenComplete((role, ex) -> {
                    if (ex == null) {
                        roleFuture.complete(role);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Authentication failed for auth provider " + provider.getClass() + ": ", ex);
                        }
                        authenticateRemainingAuthProviders(roleFuture, authData, ex, index + 1);
                    }
                });
        }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        return applyAuthProcessor(
            providers,
            authenticationMetrics,
            provider -> provider.authenticate(authData)
        );
    }

    @Override
    public AuthenticationState newAuthState(AuthData authData, SocketAddress remoteAddress, SSLSession sslSession)
        throws AuthenticationException {
        final List<AuthenticationState> states = new ArrayList<>(providers.size());

        AuthenticationException authenticationException = null;
        for (AuthenticationProvider provider : providers) {
            try {
                AuthenticationState state = provider.newAuthState(authData, remoteAddress, sslSession);
                states.add(state);
            } catch (AuthenticationException ae) {
                if (log.isDebugEnabled()) {
                    log.debug("Authentication failed for auth provider " + provider.getClass() + ": ", ae);
                }
                // Store the exception so we can throw it later instead of a generic one
                authenticationException = ae;
            }
        }
        if (states.isEmpty()) {
            log.debug("Failed to initialize a new auth state from {}", remoteAddress, authenticationException);
            if (authenticationException != null) {
                throw authenticationException;
            } else {
                throw new AuthenticationException("Failed to initialize a new auth state from " + remoteAddress);
            }
        } else {
            return new AuthenticationListState(states, authenticationMetrics);
        }
    }

    @Override
    public AuthenticationState newHttpAuthState(HttpServletRequest request) throws AuthenticationException {
        final List<AuthenticationState> states = new ArrayList<>(providers.size());

        AuthenticationException authenticationException = null;
        for (AuthenticationProvider provider : providers) {
            try {
                AuthenticationState state = provider.newHttpAuthState(request);
                states.add(state);
            } catch (AuthenticationException ae) {
                if (log.isDebugEnabled()) {
                    log.debug("Authentication failed for auth provider " + provider.getClass() + ": ", ae);
                }
                // Store the exception so we can throw it later instead of a generic one
                authenticationException = ae;
            }
        }
        if (states.isEmpty()) {
            log.debug("Failed to initialize a new http auth state from {}",
                    request.getRemoteHost(), authenticationException);
            if (authenticationException != null) {
                throw authenticationException;
            } else {
                throw new AuthenticationException(
                        "Failed to initialize a new http auth state from " + request.getRemoteHost());
            }
        } else {
            return new AuthenticationListState(states, authenticationMetrics);
        }
    }

    @Override
    public boolean authenticateHttpRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
        Boolean authenticated = applyAuthProcessor(
            providers,
            authenticationMetrics,
            provider -> {
                try {
                    return provider.authenticateHttpRequest(request, response);
                } catch (Exception e) {
                    if (e instanceof AuthenticationException) {
                        throw (AuthenticationException) e;
                    } else {
                        throw new AuthenticationException("Failed to authentication http request");
                    }
                }
            }
        );
        return authenticated;
    }

    @Override
    public void close() throws IOException {
        for (AuthenticationProvider provider : providers) {
            provider.close();
        }
    }
}
