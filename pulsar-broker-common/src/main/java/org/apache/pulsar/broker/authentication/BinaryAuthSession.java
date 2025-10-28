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

/**
 * Represents a per-connection authentication session for a client using the broker's binary protocol.
 *
 * <p>This class manages the complete authentication lifecycle for a single client connection.
 * It tracks the current {@code AuthenticationState}, authentication provider, method, and role,
 * as well as the resolved {@code AuthenticationDataSource}. When a proxy is involved, it can
 * also store the original credentials and authentication state forwarded by the proxy.
 *
 * <p>{@code BinaryAuthSession} handles both the initial authentication (CONNECT) and
 * subsequent re-authentication or credential refresh flows. All asynchronous operations
 * are executed using the {@link BinaryAuthContext#getExecutor() executor} provided by the
 * associated {@link BinaryAuthContext}.
 *
 * <p>The session supports two main connection scenarios:
 *
 * <h3>Direct client-to-broker connections:</h3>
 * <ul>
 *   <li>The client and broker may exchange authentication data multiple times until
 *       authentication is complete.</li>
 *   <li>If credentials expire, the broker requests the client to refresh them,
 *       ensuring that the role remains consistent across refreshes.</li>
 * </ul>
 *
 * <h3>Client-to-broker connections via a proxy:</h3>
 * <ul>
 *   <li>The proxy may optionally forward the original client's authentication data.</li>
 *   <li>The broker first authenticates the proxy, then optionally validates the
 *       original client's credentials if forwarded.</li>
 *   <li>{@code originalAuthState} is non-null when the proxy has forwarded the original
 *       authentication data and the broker is configured to authenticate it.</li>
 *   <li>Proxy authentication does not expire. The proxy acts as a transparent intermediary,
 *       and subsequent client credential refreshes occur directly between the client and broker.</li>
 * </ul>
 */
@Slf4j
@Getter
public class BinaryAuthSession {
    private static final byte[] emptyArray = new byte[0];

    /// Current authentication state of the connected client.
    private AuthenticationState authState;
    // Authentication method used by the connected client.
    private String authMethod;
    // Role of the connected client as determined by authentication.
    private String authRole = null;
    // Authentication data for the connected client (volatile for visibility across threads).
    private volatile AuthenticationDataSource authenticationData;
    // Authentication provider associated with this session.
    private AuthenticationProvider authenticationProvider;

    // Original authentication method forwarded by proxy, if any.
    private String originalAuthMethod;
    // Original principal forwarded by proxy, if any.
    private String originalPrincipal = null;
    // Original authentication state forwarded by proxy, if any.
    private AuthenticationState originalAuthState;
    // Original authentication data forwarded by proxy (volatile for thread visibility).
    private volatile AuthenticationDataSource originalAuthData;
    // Keep temporarily in order to verify after verifying proxy's authData
    private AuthData originalAuthDataCopy;

    // Context holding connection-specific data needed for authentication.
    private final BinaryAuthContext context;

    // Default authentication result returned after successful initial authentication
    private AuthResult defaultAuthResult;
    // Indicates whether the client supports authentication refresh.
    private boolean supportsAuthRefresh;

    public BinaryAuthSession(@NonNull BinaryAuthContext context) {
        this.context = context;
    }

    /**
     * Performs the initial authentication process for the client connection.
     * <p>
     * This method handles both standard CONNECT authentication and optional original credentials
     * forwarded by a proxy. Authentication may be asynchronous and results in a {@link AuthResult}.
     *
     * @return a {@link CompletableFuture} that completes with the authentication result
     */
    public CompletableFuture<AuthResult> doAuthentication() {
        var connect = context.getCommandConnect();
        try {
            supportsAuthRefresh = connect.getFeatureFlags().hasSupportsAuthRefresh() && connect.getFeatureFlags()
                    .isSupportsAuthRefresh();
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

    /**
     * Processes the authentication step when the broker receives an authentication response from the client.
     *
     * <p>If {@code useOriginalAuthState} is {@code true}, the authentication is performed
     * against the original credentials forwarded by a proxy. Otherwise, the primary
     * session {@code authState} is used.
     */
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

    /**
     * Callback invoked when an authentication step completes on the {@link AuthenticationState}.
     *
     * <p>If {@code authChallenge} is non-null, the authentication exchange is not yet complete.
     * An {@link AuthResult} containing the challenge bytes is returned for the broker or proxy
     * to send to the client. This method does <b>not</b> send data itself.
     *
     * <p>If {@code authChallenge} is null, the authentication step is complete. In that case, this
     * method will:
     * <ul>
     *     <li>For the initial connection: set the resolved authentication data and role, and
     *         optionally authenticate original proxy-forwarded credentials.</li>
     *     <li>For a refresh: validate that the role remains the same and update stored authentication
     *         data accordingly.</li>
     * </ul>
     */
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

                if (context.getIsInitialConnectSupplier().get()) {
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
                                    new AuthenticationException("Auth role does not match previous role"));
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

    /**
     * Performs authentication of the original client credentials forwarded by a proxy.
     */
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

    /**
     * Returns whether the current effective authentication state for this session has expired.
     *
     * <p>If the session has an {@code originalAuthState} forwarded by a proxy, that state is
     * checked first. Otherwise, the session's primary {@code authState} is used.
     */
    public boolean isExpired() {
        if (originalAuthState != null) {
            return originalAuthState.isExpired();
        }
        return authState.isExpired();
    }

    /**
     * Determines whether the session supports authentication refresh.
     *
     * <p>Refresh is not supported when:
     * <ul>
     *     <li>the client indicated it does not support auth refresh via feature flags</li>
     *     <li>the session is a proxied connection with an original principal but the proxy did not forward original
     *     credentials (so re-validation of the original user is impossible)</li>
     * </ul>
     */
    public boolean supportsAuthenticationRefresh() {
        if (originalPrincipal != null && originalAuthState == null) {
            // This case is only checked when the authState is expired because we've reached a point where
            // authentication needs to be refreshed, but the protocol does not support it unless the proxy forwards
            // the originalAuthData.
            log.info(
                    "[{}] Cannot revalidate user credential when using proxy and"
                            + " not forwarding the credentials.",
                    context.getRemoteAddress());
            return false;
        }

        if (!supportsAuthRefresh) {
            log.warn("[{}] Client doesn't support auth credentials refresh",
                    context.getRemoteAddress());
            return false;
        }

        return true;
    }

    /**
     * Refreshes the authentication credentials for this session.
     *
     * <p>If the session has an {@code originalAuthState} (i.e., credentials forwarded by a proxy
     * and broker is configured to authenticate them), the refresh is performed on that state.
     * Otherwise, the primary {@code authState} is refreshed.
     *
     * <p>The returned {@link AuthResult} contains the updated authentication data and the
     * corresponding authentication method.
     */
    public AuthResult refreshAuthentication() throws AuthenticationException {
        if (originalAuthState != null) {
            return AuthResult.builder()
                    .authData(originalAuthState.refreshAuthentication())
                    .authMethod(originalAuthMethod)
                    .build();
        }
        return AuthResult.builder()
                .authData(authState.refreshAuthentication())
                .authMethod(authMethod)
                .build();
    }

    /**
     * Result container for an authentication operation performed by {@link BinaryAuthSession}.
     *
     * <p>Holds the optional client protocol/version metadata and the authentication payload
     * produced by the underlying authentication provider. This object is returned to the
     * broker/proxy to indicate either a completed authentication (no authData) or a pending
     * authentication exchange that requires sending {@code authData} back to the client.
     */
    @Builder
    @Getter
    public static class AuthResult {
        /**
         * Client protocol version used to format protocol-level responses.
         *
         * <p>This value is used by the broker or proxy when building response frames so the
         * client can interpret any returned authentication bytes correctly.
         */
        int clientProtocolVersion;

        /**
         * Human-readable client version string, if provided by the client.
         */
        String clientVersion;

        /**
         * Authentication data produced by the authentication provider.
         *
         * <p>When non-null, these bytes represent a challenge or credentials that must be
         * sent to the client to continue the authentication handshake. When null, no further
         * client exchange is required and authentication is considered complete.
         */
        AuthData authData;

        /**
         * Identifier of the authentication method associated with {@code authData}.
         */
        String authMethod;
    }
}
