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

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.HttpAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;

/**
 * The single framework-side HTTP authentication driver — the shared {@code 401}→resubmit→{@code 200}
 * state machine that both HTTP client APIs (the AsyncHttpClient topic-lookup client and the JAX-RS admin
 * client) reach through a thin {@link HttpChallengeTransport} adapter (PIP-478).
 *
 * <p>The binary protocol runs its multi-round loop in one place ({@code ClientCnx}); HTTP is reached
 * through two client APIs, so the loop lives here instead of inside the plugin (the v4 hazard). The driver
 * discovers a plugin's HTTP challenge handling <em>only</em> by capability lookup
 * ({@code capability(HttpAuthChallengeHandler.class)}) — never {@code instanceof} on the plugin — and, for
 * the SASL style, reproduces v4 behaviour exactly: it re-issues each round as a bodiless {@code GET} to
 * the original URI, surfaces the server's challenge through
 * {@link org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext#serverChallengeHeaders()}, and attaches
 * the plugin's computed headers to the resubmitted request. On the terminal {@code 200} it asks the
 * plugin's {@link HttpAuthHeadersProvider} for the real request's headers (the validated role-token
 * replay).
 *
 * <p><b>Bounded exchange (normative, PIP-478).</b> At most {@link #MAX_CHALLENGE_ROUNDS} rounds per
 * request; the <em>original request's</em> timeout budget covers the whole exchange (challenge rounds do
 * not reset it). A plugin failure (a handler future completing exceptionally) fails the request with that
 * error — the driver never retries a plugin failure; any retry is the caller's ordinary request-retry
 * policy applied to the whole request. Body replay is sidestepped entirely because every round is a
 * bodiless {@code GET}.
 *
 * <p>One instance is created per plugin and reused across requests; each {@link #authenticateAsync} call
 * owns a fresh {@link HttpAuthCallContextImpl} (and its state slot) for that one request's retry sequence,
 * so concurrent exchanges never share conversation state.
 */
public final class HttpAuthenticationDriver {

    /** The fixed per-request cap on challenge rounds (internal constant, PIP-478). */
    public static final int MAX_CHALLENGE_ROUNDS = 10;

    private final Authentication v5;
    private final ClientAuthenticationServices services;
    private final String clientInstanceId;
    private final HttpChallengeTransport defaultTransport;
    private final AuthMetrics authMetrics;
    private volatile boolean initialized;

    /**
     * @param v5               the v5-native authentication body whose HTTP capabilities drive the exchange
     * @param services         the client's framework services (may be {@code null} — e.g. the admin path,
     *                         which does not bind them; the SASL body does not require them)
     * @param defaultTransport the transport used when a caller does not supply its own (the built-in SASL
     *                         plugin's own HTTP client, for the admin path); may be {@code null} when every
     *                         caller supplies a transport
     */
    public HttpAuthenticationDriver(Authentication v5, ClientAuthenticationServices services,
            HttpChallengeTransport defaultTransport) {
        this.v5 = v5;
        this.services = services;
        this.clientInstanceId = services == null ? null : services.clientInstanceId();
        this.defaultTransport = defaultTransport;
        this.authMetrics = AuthMetrics.create(services == null ? null : services.openTelemetry());
    }

    /**
     * @return {@code true} if the plugin exposes {@link HttpAuthChallengeHandler}, i.e. this driver can run
     *         the SASL-style HTTP exchange for it
     */
    public boolean supportsHttpChallenge() {
        return v5.capability(HttpAuthChallengeHandler.class).isPresent();
    }

    /**
     * @return the transport used when {@link #authenticateAsync} is called with a {@code null} transport
     */
    public HttpChallengeTransport defaultTransport() {
        return defaultTransport;
    }

    /**
     * Run the bounded SASL-style HTTP exchange for one request and produce the authentication headers to
     * attach to the real request (the validated role token). Never throws synchronously.
     *
     * @param uri       the original request URI; every round is re-issued as a bodiless {@code GET} to it
     * @param transport the transport for this call, or {@code null} to use the {@link #defaultTransport()}
     * @param budget    the original request's timeout budget, spanning the whole exchange
     * @return a future of the real request's authentication headers, or an exceptionally-completed future
     *         on plugin failure, a non-{@code 200}/{@code 401} response, exhausting the round cap, or
     *         exceeding the timeout budget
     */
    public CompletableFuture<HttpAuthHeaders> authenticateAsync(URI uri, HttpChallengeTransport transport,
            Duration budget) {
        try {
            HttpAuthChallengeHandler handler = v5.capability(HttpAuthChallengeHandler.class).orElse(null);
            if (handler == null) {
                return CompletableFuture.failedFuture(new PulsarClientException.UnsupportedAuthenticationException(
                        "v5 authentication body " + v5.getClass().getName() + " does not expose "
                                + "HttpAuthChallengeHandler; the framework HTTP auth driver cannot run its "
                                + "challenge exchange (PIP-478)"));
            }
            HttpAuthHeadersProvider provider = v5.capability(HttpAuthHeadersProvider.class).orElse(null);
            if (provider == null) {
                return CompletableFuture.failedFuture(new PulsarClientException.UnsupportedAuthenticationException(
                        "v5 authentication body " + v5.getClass().getName() + " exposes HttpAuthChallengeHandler "
                                + "but not HttpAuthHeadersProvider; it cannot replay the validated credential onto "
                                + "the real request (PIP-478)"));
            }
            HttpChallengeTransport t = transport != null ? transport : defaultTransport;
            if (t == null) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "No HTTP challenge transport supplied and no default transport is configured"));
            }
            ensureInitialized();
            HttpAuthCallContextImpl ctx = new HttpAuthCallContextImpl(uri);
            long budgetNanos = Math.max(0L, budget == null ? 0L : budget.toNanos());
            long deadlineNanos = System.nanoTime() + budgetNanos;
            CompletableFuture<HttpAuthHeaders> credential = challengeRound(ctx, handler, t, uri, null, 0,
                    deadlineNanos).thenCompose(ignored -> provider.getHttpHeadersAsync(ctx));
            // Metrics (PIP-478): time the HTTP credential acquisition and count failures by error class.
            return authMetrics.timeCredential(credential, httpAuthMethod());
        } catch (Throwable th) {
            return CompletableFuture.failedFuture(th);
        }
    }

    // The HTTP capabilities carry no method name of their own; fall back to the binary capability's name
    // (a plugin serving HTTP usually also serves the binary transport) or the plugin's class name.
    private String httpAuthMethod() {
        return v5.capability(BinaryAuthDataProvider.class)
                .map(BinaryAuthDataProvider::authMethodName)
                .orElseGet(() -> v5.getClass().getSimpleName());
    }

    private CompletableFuture<Void> challengeRound(HttpAuthCallContextImpl ctx, HttpAuthChallengeHandler handler,
            HttpChallengeTransport transport, URI uri, HttpAuthHeaders serverChallenge, int round,
            long deadlineNanos) {
        ctx.setServerChallengeHeaders(serverChallenge);
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return CompletableFuture.failedFuture(new PulsarClientException.AuthenticationException(
                    "SASL over HTTP exchange exceeded its timeout budget before round " + round));
        }
        Duration remaining = Duration.ofNanos(remainingNanos);
        // Bound the WHOLE round — the plugin's challenge evaluation (respondToHttpChallengeAsync, which for
        // SASL/GSSAPI is the security-context step) AND the transport GET together — against the remaining
        // deadline, so a hang in EITHER stage fails within the shared budget. boundedGet keeps its own inner
        // per-transport bound; this outer bound also covers the evaluation that runs before the GET.
        CompletableFuture<HttpChallengeTransport.Result> roundFuture = handler.respondToHttpChallengeAsync(ctx)
                .thenCompose(reqHeaders -> boundedGet(transport, uri,
                        reqHeaders == null ? HttpAuthHeaders.empty() : reqHeaders, remaining, round));
        return boundedByRemaining(roundFuture, remaining, round)
                .thenCompose(result -> {
                    int status = result.statusCode();
                    if (status == java.net.HttpURLConnection.HTTP_OK) {
                        // Terminal: surface the role-token headers so getHttpHeadersAsync can replay them.
                        ctx.setServerChallengeHeaders(result.responseHeaders());
                        return CompletableFuture.completedFuture(null);
                    }
                    if (status == java.net.HttpURLConnection.HTTP_UNAUTHORIZED) {
                        int next = round + 1;
                        if (next >= MAX_CHALLENGE_ROUNDS) {
                            return CompletableFuture.failedFuture(new PulsarClientException.AuthenticationException(
                                    "SASL over HTTP exchange exceeded the maximum of " + MAX_CHALLENGE_ROUNDS
                                            + " challenge rounds"));
                        }
                        return challengeRound(ctx, handler, transport, uri, result.responseHeaders(), next,
                                deadlineNanos);
                    }
                    return CompletableFuture.failedFuture(new PulsarClientException.AuthenticationException(
                            "SASL over HTTP auth request failed with status " + status));
                });
    }

    /**
     * Issue one transport round bounded by the remaining exchange budget. The deadline check at the top of
     * each round only fires <em>between</em> rounds; a transport whose underlying client applies no read
     * timeout (the admin path's JAX-RS transport) would otherwise leave the exchange pending forever
     * against a peer that accepts the connection but never responds. This is the inner, transport-only bound;
     * {@link #challengeRound} additionally wraps the whole round (evaluation + GET) via
     * {@link #boundedByRemaining}, so the plugin's challenge evaluation is covered too.
     */
    private static CompletableFuture<HttpChallengeTransport.Result> boundedGet(HttpChallengeTransport transport,
            URI uri, HttpAuthHeaders headers, Duration remaining, int round) {
        return boundedByRemaining(transport.get(uri, headers, remaining), remaining, round);
    }

    /**
     * Bound {@code future} against the remaining exchange budget, surfacing a timeout as an
     * {@link PulsarClientException.AuthenticationException} that names the round. Shared by the inner
     * transport bound ({@link #boundedGet}) and the outer whole-round bound applied in
     * {@link #challengeRound}.
     */
    private static CompletableFuture<HttpChallengeTransport.Result> boundedByRemaining(
            CompletableFuture<HttpChallengeTransport.Result> future, Duration remaining, int round) {
        return future
                .orTimeout(remaining.toNanos(), TimeUnit.NANOSECONDS)
                .exceptionallyCompose(t -> {
                    Throwable cause = t instanceof CompletionException ? t.getCause() : t;
                    if (cause instanceof TimeoutException) {
                        return CompletableFuture.failedFuture(new PulsarClientException.AuthenticationException(
                                "SASL over HTTP round " + round + " timed out after " + remaining.toMillis()
                                        + " ms"));
                    }
                    return CompletableFuture.failedFuture(cause);
                });
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    // The built-in SASL body completes initializeAsync immediately (it reads its JAAS subject
                    // through the shim's provider factory, not the framework services), so join() never blocks.
                    v5.initializeAsync(V5AuthContexts.initContext(services, clientInstanceId)).join();
                    initialized = true;
                }
            }
        }
    }
}
