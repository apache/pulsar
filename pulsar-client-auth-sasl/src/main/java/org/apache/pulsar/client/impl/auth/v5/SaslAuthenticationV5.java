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
package org.apache.pulsar.client.impl.auth.v5;

import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_ROLE_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_ROLE_TOKEN_EXPIRED;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_HEADER_STATE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_HEADER_TYPE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_CLIENT_INIT;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_COMPLETE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_NEGOTIATE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_SERVER;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_SERVER_CHECK_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_TYPE_VALUE;
import java.io.Serializable;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.AuthChallenge;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.ChallengeResponse;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.SaslConstants;

/**
 * v5-native SASL authentication for the Pulsar binary protocol (PIP-478). SASL is a multi-round
 * challenge/response handshake, so this body implements {@link BinaryAuthDataProvider} for the initial
 * {@code CommandConnect} credential and {@link BinaryAuthChallengeHandler} for each subsequent
 * {@code CommandAuthChallenge}. The per-exchange SASL conversation state (an
 * {@link AuthenticationDataProvider} wrapping a {@code PulsarSaslClient}) is created on the initial call
 * and kept in the call-context state slot so each broker handshake stays isolated.
 *
 * <p>The same body also serves the SASL-over-HTTP flow: it implements {@link HttpAuthChallengeHandler}
 * (the SASL-style {@code 401}-carrying-custom-headers multi-round exchange) and {@link HttpAuthHeadersProvider}
 * (the validated role-token replay onto the real request). The framework HTTP auth driver runs the
 * {@code 401}→resubmit→{@code 200} loop; this body only computes each round's headers, porting the v4
 * {@code AuthenticationSasl.newRequestHeader} / {@code getHeaders} state machine. Its per-exchange
 * conversation (the {@code PulsarSaslClient} wrapper plus the captured role token) lives in the HTTP
 * call-context state slot, so concurrent HTTP handshakes stay isolated. A fresh per-exchange SASL provider
 * is obtained through the serializable {@link SaslProviderFactory} supplied by the shim (it reads the
 * shim's JAAS subject and server type).
 *
 * <p>Although the v5 {@code Authentication} SPI deliberately does not extend {@link Serializable}, this
 * concrete built-in body is serializable so the v4 {@code AuthenticationSasl} shim (whose interface
 * requires {@code Serializable}, for Functions/connector frameworks) round-trips. The provider factory is
 * itself serializable.
 */
public class SaslAuthenticationV5 implements Authentication, BinaryAuthDataProvider,
        BinaryAuthChallengeHandler, HttpAuthChallengeHandler, HttpAuthHeadersProvider, Serializable {

    private static final long serialVersionUID = 1L;

    /** The stable auth-method name. */
    public static final String AUTH_METHOD_NAME = SaslConstants.AUTH_METHOD_NAME;

    private final SaslProviderFactory providerFactory;

    // PIP-478 FIX D: the client's bounded blocking executor, late-bound at initializeAsync(...). The SASL
    // provider creation and evaluateChallenge/authenticate (GSSAPI/Kerberos) work is off-loaded onto it so
    // it never runs on the Netty event loop. Null when used outside a client, which is not a degraded inline
    // path: V5AuthContexts.supplyBlocking substitutes the shared blocking pool rather than running inline.
    private transient volatile Executor blockingExecutor;

    // PIP-478 FIX C: the cross-request SASL-over-HTTP role-token cache, restoring the v4
    // AuthenticationSasl.saslRoleToken semantics. The framework HTTP auth driver reuses ONE body instance
    // across requests but gives each request a fresh call-context/conversation, so the validated role token
    // must live on the body to be replayed (State=ServerCheckToken) instead of restarting a full Kerberos
    // negotiation every request. Cleared and re-negotiated on SaslAuthRoleTokenExpired.
    private transient volatile String cachedRoleToken;

    /**
     * @param providerFactory creates a fresh per-exchange SASL data provider for a broker host
     */
    public SaslAuthenticationV5(SaslProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public String authMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
        this.blockingExecutor = ctx.blockingExecutor();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
        // FIX D: run the SASL provider creation + initial evaluateChallenge off the event loop.
        return V5AuthContexts.supplyBlocking(blockingExecutor, () -> {
            try {
                AuthenticationDataProvider provider = providerFactory.create(ctx.brokerHost());
                ctx.setStateObject(AuthenticationDataProvider.class, provider);
                AuthData initData = provider.authenticate(AuthData.INIT_AUTH_DATA);
                return new BinaryAuthData(initData.getBytes());
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    @Override
    public CompletableFuture<ChallengeResponse> respondToChallengeAsync(AuthenticationCallContext ctx,
                                                                        AuthChallenge authChallenge) {
        // FIX D: run evaluateChallenge (GSSAPI) off the event loop.
        return V5AuthContexts.supplyBlocking(blockingExecutor, () -> {
            try {
                AuthenticationDataProvider provider =
                        ctx.getStateObject(AuthenticationDataProvider.class).orElse(null);
                if (provider == null) {
                    // No prior state (e.g. a broker-pushed challenge without a preceding connect call on this
                    // context); start a fresh SASL exchange.
                    provider = providerFactory.create(ctx.brokerHost());
                    ctx.setStateObject(AuthenticationDataProvider.class, provider);
                }
                AuthData response = provider.authenticate(AuthData.of(authChallenge.bytes()));
                return new ChallengeResponse(response.getBytes());
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    // ---- SASL-over-HTTP: HttpAuthChallengeHandler + HttpAuthHeadersProvider (PIP-478) ----
    //
    // Ports AuthenticationSasl.newRequestHeader / getHeaders. The framework driver runs the
    // 401->resubmit->200 loop (bodiless GET to the original URI); respondToHttpChallengeAsync computes each
    // round's request headers from the server's prior response, and getHttpHeadersAsync produces the real
    // request's headers (the validated role token). Cross-round state (the PulsarSaslClient conversation
    // plus the role token) lives in the call-context state slot, keyed by SaslHttpConversation.

    /**
     * The per-exchange SASL-over-HTTP conversation held in the HTTP call-context state slot: the
     * {@link AuthenticationDataProvider} wrapping this exchange's {@code PulsarSaslClient}, and the role
     * token once the server has issued it.
     */
    private static final class SaslHttpConversation {
        private AuthenticationDataProvider provider;
        private String roleToken;
    }

    @Override
    public CompletableFuture<HttpAuthHeaders> respondToHttpChallengeAsync(HttpAuthCallContext ctx) {
        // FIX D: run the SASL negotiation (evaluateChallenge/GSSAPI) off the event loop.
        return V5AuthContexts.supplyBlocking(blockingExecutor, () -> {
            try {
                SaslHttpConversation conv = conversation(ctx);
                // The server carries its prior response's SASL headers here (empty on the first round).
                boolean hasPrevious = ctx.serverChallengeHeaders().isPresent();
                // Capture the role token if the server has issued it (the terminal 200 also carries it).
                String issuedRoleToken = header(ctx, SASL_AUTH_ROLE_TOKEN);
                if (issuedRoleToken != null) {
                    conv.roleToken = issuedRoleToken;
                }

                Map<String, String> headers = new LinkedHashMap<>();
                // The SASL data provider's HTTP headers (SASL-Type: Kerberos), on every request. Guarded on
                // hasDataForHttp() as the v4 path is: AuthenticationDataProvider.getHttpHeaders() defaults to
                // returning null, so an implementation that overrides neither would NPE here. The built-in
                // SaslAuthenticationDataProvider overrides both, but SaslProviderFactory is a public seam.
                addHttpHeaders(conv.provider, headers);

                // Role token exists but the server rejected it: drop it (including the cross-request
                // cache, FIX C) and restart the SASL exchange.
                if (isRoleTokenRejected(conv, ctx, hasPrevious)) {
                    hasPrevious = false;
                    conv.roleToken = null;
                    cachedRoleToken = null;
                    conv.provider = providerFactory.create(host(ctx));
                }

                // Role token in hand: replay it, asking the server to check / negotiate / complete.
                if (conv.roleToken != null) {
                    headers.put(SASL_AUTH_ROLE_TOKEN, conv.roleToken);
                    if (!hasPrevious) {
                        headers.put(SASL_HEADER_STATE, SASL_STATE_SERVER_CHECK_TOKEN);
                    } else if (SASL_STATE_COMPLETE.equalsIgnoreCase(header(ctx, SASL_HEADER_STATE))) {
                        headers.put(SASL_HEADER_STATE, SASL_STATE_COMPLETE);
                    } else {
                        headers.put(SASL_HEADER_STATE, SASL_STATE_NEGOTIATE);
                    }
                    return HttpAuthHeaders.of(headers);
                }

                // No role token yet: run the SASL negotiation.
                if (!hasPrevious) {
                    headers.put(SASL_HEADER_STATE, SASL_STATE_CLIENT_INIT);
                    AuthData initData = conv.provider.authenticate(AuthData.INIT_AUTH_DATA);
                    headers.put(SASL_AUTH_TOKEN, Base64.getEncoder().encodeToString(initData.getBytes()));
                } else {
                    AuthData brokerData = AuthData.of(Base64.getDecoder().decode(header(ctx, SASL_AUTH_TOKEN)));
                    AuthData clientData = conv.provider.authenticate(brokerData);
                    headers.put(SASL_STATE_SERVER, header(ctx, SASL_STATE_SERVER));
                    headers.put(SASL_HEADER_TYPE, SASL_TYPE_VALUE);
                    headers.put(SASL_HEADER_STATE, SASL_STATE_NEGOTIATE);
                    headers.put(SASL_AUTH_TOKEN, Base64.getEncoder().encodeToString(clientData.getBytes()));
                }
                return HttpAuthHeaders.of(headers);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    @Override
    public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
        try {
            SaslHttpConversation conv = ctx.getStateObject(SaslHttpConversation.class).orElse(null);
            // The role token arrives on the terminal 200 response; fall back to any captured during rounds.
            String roleToken = header(ctx, SASL_AUTH_ROLE_TOKEN);
            if (roleToken == null && conv != null) {
                roleToken = conv.roleToken;
            }
            if (roleToken == null) {
                return CompletableFuture.failedFuture(new PulsarClientException.AuthenticationException(
                        "SASL over HTTP exchange completed without issuing a role token"));
            }
            if (conv != null) {
                conv.roleToken = roleToken;
            }
            // FIX C: publish the validated role token to the cross-request cache so the next request replays
            // it (State=ServerCheckToken) instead of restarting a full Kerberos negotiation (v4 semantics).
            cachedRoleToken = roleToken;
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(SASL_HEADER_TYPE, SASL_TYPE_VALUE);
            headers.put(SASL_AUTH_ROLE_TOKEN, roleToken);
            headers.put(SASL_HEADER_STATE, SASL_STATE_COMPLETE);
            return CompletableFuture.completedFuture(HttpAuthHeaders.of(headers));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private SaslHttpConversation conversation(HttpAuthCallContext ctx) throws Exception {
        SaslHttpConversation conv = ctx.getStateObject(SaslHttpConversation.class).orElse(null);
        if (conv == null) {
            conv = new SaslHttpConversation();
            conv.provider = providerFactory.create(host(ctx));
            // FIX C: seed this request's conversation from the cross-request role-token cache so a
            // previously validated token is replayed (State=ServerCheckToken) rather than renegotiated.
            conv.roleToken = cachedRoleToken;
            ctx.setStateObject(SaslHttpConversation.class, conv);
        }
        return conv;
    }

    /**
     * Whether a role token this exchange replayed has been rejected and must be discarded.
     *
     * <p>A server can say so in two ways. The explicit one is a Kerberos SASL response whose {@code State}
     * reports the token expired — what {@code AuthenticationSasl} checks, and all it checks. The other is a
     * bare refusal: a broker whose signer secret has rotated, or a peer broker holding a divergent secret,
     * rejects the replayed token with a response carrying no SASL headers at all.
     *
     * <p>Handling only the explicit form leaves a <em>poisoned</em> token cached: every request replays it,
     * burns the whole round budget, and fails, for the life of the client — and the cache exists precisely
     * so the token is replayed. Treating an unadorned response to a replay round as a rejection costs at
     * most one extra negotiation in the case where a server answers oddly, and restores progress in the
     * case where it does not.
     *
     * @param conv        this exchange's conversation
     * @param ctx         the call context, carrying the server's prior response headers
     * @param hasPrevious whether a prior round happened — without one there is no response to read
     */
    private static boolean isRoleTokenRejected(SaslHttpConversation conv, HttpAuthCallContext ctx,
            boolean hasPrevious) {
        if (conv.roleToken == null || !hasPrevious) {
            return false;
        }
        String type = header(ctx, SASL_HEADER_TYPE);
        if (SASL_TYPE_VALUE.equalsIgnoreCase(type)) {
            return SASL_AUTH_ROLE_TOKEN_EXPIRED.equalsIgnoreCase(header(ctx, SASL_HEADER_STATE));
        }
        // Not a SASL response at all, in answer to a round that replayed the token: a bare refusal.
        return true;
    }

    /**
     * Copy a data provider's HTTP headers into {@code headers}, contributing nothing when it has none.
     *
     * <p>Mirrors the v4 {@code AuthenticationSasl} guard. {@link AuthenticationDataProvider#getHttpHeaders()}
     * is a default method returning {@code null} (and {@code hasDataForHttp()} returns {@code false}), so
     * calling it unguarded NPEs for any provider that overrides neither. The built-in
     * {@code SaslAuthenticationDataProvider} overrides both, but {@link SaslProviderFactory} is a public
     * extension seam, so the guard is not merely defensive.
     *
     * @param provider the SASL data provider for this exchange
     * @param headers  the header map to add to
     * @throws Exception if the provider fails to produce its headers
     */
    private static void addHttpHeaders(AuthenticationDataProvider provider, Map<String, String> headers)
            throws Exception {
        if (!provider.hasDataForHttp()) {
            return;
        }
        Set<Map.Entry<String, String>> httpHeaders = provider.getHttpHeaders();
        if (httpHeaders != null) {
            httpHeaders.forEach(e -> headers.put(e.getKey(), e.getValue()));
        }
    }

    private static String header(HttpAuthCallContext ctx, String name) {
        return ctx.serverChallengeHeaders().flatMap(h -> h.get(name)).orElse(null);
    }

    private static String host(HttpAuthCallContext ctx) {
        return ctx.requestUri() == null ? null : ctx.requestUri().getHost();
    }

    @Override
    public void close() {
    }

    /**
     * Creates a fresh per-exchange SASL {@link AuthenticationDataProvider} (wrapping a new
     * {@code PulsarSaslClient}) for a given broker host. Implementations must be serializable.
     */
    public interface SaslProviderFactory extends Serializable {
        /**
         * @param brokerHost the broker host the SASL client authenticates against
         * @return a fresh SASL data provider for this exchange
         * @throws Exception if the SASL client could not be created
         */
        AuthenticationDataProvider create(String brokerHost) throws Exception;
    }
}
