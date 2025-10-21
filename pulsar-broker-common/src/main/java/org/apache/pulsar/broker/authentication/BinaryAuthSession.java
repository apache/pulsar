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

import static org.apache.pulsar.common.naming.Constants.WEBSOCKET_DUMMY_ORIGINAL_PRINCIPLE;
import java.util.concurrent.CompletableFuture;
import javax.naming.AuthenticationException;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.api.AuthData;
import org.jspecify.annotations.NonNull;

@Slf4j
@Getter
public class BinaryAuthSession {
    private static final byte[] emptyArray = new byte[0];

    private AuthenticationState authState;
    private String authMethod;
    private String authRole = null;
    private volatile AuthenticationDataSource authenticationData;
    private AuthenticationProvider authenticationProvider;

    // In case of proxy, if the authentication credentials are forwardable,
    // it will hold the credentials of the original client
    private String originalAuthMethod;
    private String originalPrincipal = null;
    private AuthenticationState originalAuthState;
    private volatile AuthenticationDataSource originalAuthData;
    // Keep temporarily in order to verify after verifying proxy's authData
    private AuthData originalAuthDataCopy;

    private final BinaryAuthContext context;

    private AuthResult defaultAuthResult;

    public BinaryAuthSession(@NonNull BinaryAuthContext context) {
        this.context = context;
    }

    public CompletableFuture<AuthResult> doAuthentication() {
        var connect = context.getCommandConnect();
        try {
            var authData = connect.hasAuthData() ? connect.getAuthData() : emptyArray;
            var clientData = AuthData.of(authData);
            // init authentication
            if (connect.hasAuthMethodName()) {
                authMethod = connect.getAuthMethodName();
            } else if (connect.hasAuthMethod()) {
                // Legacy client is passing enum
                authMethod = connect.getAuthMethod().name().substring(10).toLowerCase();
            } else {
                authMethod = "none";
            }

            defaultAuthResult = AuthResult.builder().clientProtocolVersion(connect.getProtocolVersion())
                    .clientVersion(connect.hasClientVersion() ? connect.getClientVersion() : "")
                    .build();

            authenticationProvider = context.getAuthenticationService().getAuthenticationProvider(authMethod);
            // Not find provider named authMethod. Most used for tests.
            // In AuthenticationDisabled, it will set authMethod "none".
            if (authenticationProvider == null) {
                authRole = context.getAuthenticationService().getAnonymousUserRole()
                        .orElseThrow(() ->
                                new AuthenticationException(
                                        "No anonymous role, and no authentication provider configured"));
                return CompletableFuture.completedFuture(defaultAuthResult);
            }

            authState =
                    authenticationProvider.newAuthState(clientData, context.getRemoteAddress(),
                            context.getSslSession());

            if (log.isDebugEnabled()) {
                String role = "";
                if (authState != null && authState.isComplete()) {
                    role = authState.getAuthRole();
                } else {
                    role = "authentication incomplete or null";
                }
                log.debug("[{}] Authenticate role : {}", context.getRemoteAddress(), role);
            }

            if (connect.hasOriginalPrincipal() && context.isAuthenticateOriginalAuthData()
                    && !WEBSOCKET_DUMMY_ORIGINAL_PRINCIPLE.equals(connect.getOriginalPrincipal())) {
                // Flow:
                // 1. Initialize original authentication.
                // 2. Authenticate the proxy's authentication data.
                // 3. Authenticate the original authentication data.
                if (connect.hasOriginalAuthMethod()) {
                    originalAuthMethod = connect.getOriginalAuthMethod();
                } else {
                    originalAuthMethod = "none";
                }

                var originalAuthenticationProvider =
                        context.getAuthenticationService().getAuthenticationProvider(originalAuthMethod);

                /**
                 * When both the broker and the proxy are configured with anonymousUserRole
                 * if the client does not configure an authentication method
                 * the proxy side will set the value of anonymousUserRole to clientAuthRole when it creates a connection
                 * and the value of clientAuthMethod will be none.
                 * Similarly, should also set the value of authRole to anonymousUserRole on the broker side.
                 */
                if (originalAuthenticationProvider == null) {
                    authRole = context.getAuthenticationService().getAnonymousUserRole()
                            .orElseThrow(() ->
                                    new AuthenticationException("No anonymous role, and can't find "
                                            + "AuthenticationProvider for original role using auth method "
                                            + "[" + originalAuthMethod + "] is not available"));
                    originalPrincipal = authRole;
                    return CompletableFuture.completedFuture(defaultAuthResult);
                }

                originalAuthDataCopy = AuthData.of(connect.getOriginalAuthData().getBytes());
                originalAuthState = originalAuthenticationProvider.newAuthState(
                        originalAuthDataCopy,
                        context.getRemoteAddress(),
                        context.getSslSession());
            } else if (connect.hasOriginalPrincipal()) {
                originalPrincipal = connect.getOriginalPrincipal();

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Setting original role (forwarded from proxy): {}",
                            context.getRemoteAddress(), originalPrincipal);
                }
            }

            return authChallenge(clientData, false, connect.getProtocolVersion(),
                    connect.hasClientVersion() ? connect.getClientVersion() : "");
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }


    // According to auth result, send Connected, AuthChallenge, or Error command.
    public CompletableFuture<AuthResult> authChallenge(AuthData clientData,
                                                       boolean useOriginalAuthState,
                                                       int clientProtocolVersion,
                                                        String clientVersion) {
        // The original auth state can only be set on subsequent auth attempts (and only
        // in presence of a proxy and if the proxy is forwarding the credentials).
        // In this case, the re-validation needs to be done against the original client
        // credentials.
        AuthenticationState authState = useOriginalAuthState ? originalAuthState : this.authState;
        String authRole = useOriginalAuthState ? originalPrincipal : this.authRole;
        if (log.isDebugEnabled()) {
            log.debug("Authenticate using original auth state : {}, role = {}", useOriginalAuthState, authRole);
        }
        return authState
                .authenticateAsync(clientData)
                .thenComposeAsync((authChallenge) -> authChallengeSuccessCallback(authChallenge,
                                useOriginalAuthState, authRole, clientProtocolVersion, clientVersion),
                        context.getExecutor());
    }

    public CompletableFuture<AuthResult> authChallengeSuccessCallback(AuthData authChallenge,
                                                                      boolean useOriginalAuthState,
                                                                      String authRole,
                                                                      int clientProtocolVersion,
                                                                      String clientVersion) {
        try {
            if (authChallenge == null) {
                // Authentication has completed. It was either:
                // 1. the 1st time the authentication process was done, in which case we'll send
                //    a `CommandConnected` response
                // 2. an authentication refresh, in which case we need to refresh authenticationData
                AuthenticationState authState = useOriginalAuthState ? originalAuthState : this.authState;
                String newAuthRole = authState.getAuthRole();
                AuthenticationDataSource newAuthDataSource = authState.getAuthDataSource();

                if (context.getIsConnectingSupplier().get()) {
                    // Set the auth data and auth role
                    if (!useOriginalAuthState) {
                        this.authRole = newAuthRole;
                        this.authenticationData = newAuthDataSource;
                    }
                    // First time authentication is done
                    if (originalAuthState != null) {
                        // We only set originalAuthState when we are going to use it.
                        return authenticateOriginalData().thenApply(
                                __ -> defaultAuthResult);
                    } else {
                        return CompletableFuture.completedFuture(defaultAuthResult);
                    }
                } else {
                    // If the connection was already ready, it means we're doing a refresh
                    if (!StringUtils.isEmpty(authRole)) {
                        if (!authRole.equals(newAuthRole)) {
                            log.warn("[{}] Principal cannot change during an authentication refresh expected={} got={}",
                                    context.getRemoteAddress(), authRole, newAuthRole);
                            return CompletableFuture.failedFuture(
                                    new AuthenticationException("Auth role not match previous"));
                        }
                    }
                    // Refresh authentication data
                    if (!useOriginalAuthState) {
                        this.authenticationData = newAuthDataSource;
                    } else {
                        this.originalAuthData = newAuthDataSource;
                    }
                    log.info("[{}] Refreshed authentication credentials for role {}",
                            context.getRemoteAddress(), authRole);
                }
            } else {
                // auth not complete, continue auth with client side.
                return CompletableFuture.completedFuture(AuthResult.builder()
                        .clientProtocolVersion(clientProtocolVersion)
                        .clientVersion(clientVersion)
                        .authMethod(authMethod)
                        .authData(authChallenge)
                        .build());
            }
        } catch (Exception | AssertionError e) {
            return CompletableFuture.failedFuture(e);
        }

        return CompletableFuture.completedFuture(defaultAuthResult);
    }

    private CompletableFuture<Void> authenticateOriginalData() {
        return originalAuthState
                .authenticateAsync(originalAuthDataCopy)
                .thenComposeAsync((authChallenge) -> {
                    if (authChallenge != null) {
                        // The protocol does not yet handle an auth challenge here.
                        // See https://github.com/apache/pulsar/issues/19291.
                        return CompletableFuture.failedFuture(
                                new AuthenticationException("Failed to authenticate original auth data "
                                        + "due to unsupported authChallenge."));
                    } else {
                        try {
                            // No need to retain these bytes anymore
                            originalAuthDataCopy = null;
                            originalAuthData = originalAuthState.getAuthDataSource();
                            originalPrincipal = originalAuthState.getAuthRole();
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Authenticated original role (forwarded from proxy): {}",
                                        context.getRemoteAddress(), originalPrincipal);
                            }
                            return CompletableFuture.completedFuture(null);
                        } catch (Exception | AssertionError e) {
                            return CompletableFuture.failedFuture(e);
                        }
                    }
                }, context.getExecutor());
    }

    @Builder
    @Getter
    public static class AuthResult {
        int clientProtocolVersion;
        String clientVersion;
        AuthData authData;
        String authMethod;
    }
}
