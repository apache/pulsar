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

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.client.api.v5.auth.AuthChallenge;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.common.api.AuthData;

/**
 * The single, shared binary-transport {@link AuthenticationExchange} that both v5→v4 adapters drive
 * (PIP-478). Consolidating the routing here guarantees the two adapters —
 * {@link V5BinaryAuthenticationDriver} (built-in shims, in this module) and
 * {@code V5ToV4AuthenticationAdapter} (application plugins, in {@code pulsar-client-v5}) — cannot drift
 * on the normative rules below.
 *
 * <p>One exchange owns one {@link AuthenticationCallContext} (and its state slot); multi-round conversation
 * state survives across the (non-refresh) challenge rounds that one exchange services. A broker REFRESH
 * restarts authentication: {@code ClientCnx} opens a <em>fresh</em> exchange (a new call context) for it
 * rather than reusing this one, so conversation state does not carry across a refresh. The exchange is
 * <em>not</em> thread-safe and is not shared across connections; {@code ClientCnx} serializes its rounds (it
 * issues the next round only after the previous future completes).
 *
 * <p>Binary challenge routing (PIP-478 normative rules):
 * <ol>
 *   <li>Initial connect → {@link BinaryAuthDataProvider#getAuthDataAsync}.</li>
 *   <li>The <b>refresh sentinel</b> ({@link AuthData#REFRESH_AUTH_DATA_BYTES}, {@code "PulsarAuthRefresh"})
 *       — and <em>only</em> that exact payload — re-produces the current credential through
 *       {@link BinaryAuthDataProvider#getAuthDataAsync}. In practice {@code ClientCnx} answers a REFRESH by
 *       opening a fresh exchange and calling {@code getAuthDataAsync} on it directly, so the refresh branch of
 *       {@link #authenticateAsync} is a defensive equivalent (both re-produce the credential) rather than the
 *       primary path.</li>
 *   <li>Any other {@code CommandAuthChallenge} — including one carrying an empty payload, a legitimate
 *       SASL/custom round — is answered by {@link BinaryAuthChallengeHandler}.</li>
 * </ol>
 *
 * <p>Every future-returning method reports failures by completing the returned future exceptionally,
 * mapped to the matching v4 {@link PulsarClientException} subtype (v4 exception types are public API); it
 * never throws on the calling thread (PIP-478 error model).
 */
public final class BinaryAuthenticationExchange implements AuthenticationExchange {

    private final Authentication v5;
    private final AuthenticationCallContext callContext;
    private final AuthMetrics metrics;

    /**
     * @param v5          the v5-native authentication body to drive over the binary transport
     * @param callContext the per-connection call context (and its state slot) this exchange owns
     */
    public BinaryAuthenticationExchange(Authentication v5, AuthenticationCallContext callContext) {
        this(v5, callContext, AuthMetrics.NOOP);
    }

    /**
     * @param v5          the v5-native authentication body to drive over the binary transport
     * @param callContext the per-connection call context (and its state slot) this exchange owns
     * @param metrics     the client-auth metrics (credential-acquisition latency + failures); {@code null}
     *                    maps to {@link AuthMetrics#NOOP}
     */
    public BinaryAuthenticationExchange(Authentication v5, AuthenticationCallContext callContext,
            AuthMetrics metrics) {
        this.v5 = v5;
        this.callContext = callContext;
        this.metrics = metrics == null ? AuthMetrics.NOOP : metrics;
    }

    @Override
    public CompletableFuture<AuthData> getAuthDataAsync() {
        // Never throw synchronously (PIP-478 error model): a body that throws while producing its future is
        // caught here and reported as a failed future, mapped to the v4 exception type.
        try {
            Optional<BinaryAuthDataProvider> provider = v5.capability(BinaryAuthDataProvider.class);
            if (provider.isEmpty()) {
                return CompletableFuture.failedFuture(new PulsarClientException.UnsupportedAuthenticationException(
                        "v5 authentication body " + v5.getClass().getName() + " does not expose "
                                + "BinaryAuthDataProvider; the Pulsar binary transport requires it (PIP-478)"));
            }
            BinaryAuthDataProvider binaryProvider = provider.get();
            CompletableFuture<AuthData> credential = binaryProvider.getAuthDataAsync(callContext)
                    .thenApply(d -> AuthData.of(d == null ? new byte[0] : d.bytes()))
                    .exceptionally(t -> {
                        throw new CompletionException(toV4Exception(unwrap(t)));
                    });
            // Metrics (PIP-478): time the credential acquisition and count failures by error class.
            return metrics.timeCredential(credential, binaryProvider.authMethodName());
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(toV4Exception(unwrap(t)));
        }
    }

    @Override
    public CompletableFuture<AuthData> authenticateAsync(AuthData challengeOrRefresh) {
        // Rule 2: the refresh sentinel — and only the exact "PulsarAuthRefresh" payload — re-produces the
        // current credential through BinaryAuthDataProvider; it never reaches the challenge handler.
        if (isRefreshSentinel(challengeOrRefresh)) {
            return getAuthDataAsync();
        }
        try {
            // Rule 3: any other CommandAuthChallenge (including an empty payload) goes to the challenge
            // handler; absent capability is a hard failure (matching v4, where a plugin without
            // authenticate(AuthData) cannot answer a challenge).
            Optional<BinaryAuthChallengeHandler> handler = v5.capability(BinaryAuthChallengeHandler.class);
            if (handler.isEmpty()) {
                return CompletableFuture.failedFuture(new PulsarClientException.AuthenticationException(
                        "v5 authentication body " + v5.getClass().getName() + " received a binary auth "
                                + "challenge but does not expose BinaryAuthChallengeHandler (PIP-478)"));
            }
            return handler.get()
                    .respondToChallengeAsync(callContext, new AuthChallenge(challengeOrRefresh.getBytes()))
                    .thenApply(r -> AuthData.of(r == null ? new byte[0] : r.bytes()))
                    .exceptionally(t -> {
                        throw new CompletionException(toV4Exception(unwrap(t)));
                    });
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(toV4Exception(unwrap(t)));
        }
    }

    /**
     * Whether an {@code authenticateAsync} payload is the broker's refresh sentinel (PIP-478 rule 2).
     * The refresh sentinel is <em>exactly</em> {@link AuthData#REFRESH_AUTH_DATA_BYTES}; every other
     * payload — empty, {@code INIT}, or an arbitrary challenge — is a challenge to be answered by the
     * challenge handler under rule 3, never a re-produce-the-credential request.
     *
     * @param challenge the payload from {@code authenticateAsync}
     * @return {@code true} only for the exact refresh sentinel payload
     */
    public static boolean isRefreshSentinel(AuthData challenge) {
        return challenge != null && Arrays.equals(challenge.getBytes(), AuthData.REFRESH_AUTH_DATA_BYTES);
    }

    /**
     * Unwrap a {@link CompletionException} / {@link ExecutionException} wrapper down to its cause.
     *
     * @param t the throwable to unwrap
     * @return the wrapped cause, or {@code t} itself when it is not a wrapper (or has no cause)
     */
    public static Throwable unwrap(Throwable t) {
        if ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
            return t.getCause();
        }
        return t;
    }

    /**
     * Translate a v5-side throwable into the matching v4 {@link PulsarClientException} subtype, since v4
     * exception types are part of the public API.
     *
     * @param t the throwable raised by a v5 call
     * @return the translated v4 exception
     */
    public static PulsarClientException toV4Exception(Throwable t) {
        if (t instanceof org.apache.pulsar.client.api.v5.PulsarClientException.AuthenticationException) {
            // The v4 AuthenticationException only exposes a (String) constructor, so preserve the original
            // v5 throwable as the cause explicitly.
            PulsarClientException.AuthenticationException e =
                    new PulsarClientException.AuthenticationException(t.getMessage());
            e.initCause(t);
            return e;
        }
        if (t instanceof org.apache.pulsar.client.api.v5.PulsarClientException.UnsupportedAuthenticationException) {
            PulsarClientException.UnsupportedAuthenticationException e =
                    new PulsarClientException.UnsupportedAuthenticationException(t.getMessage());
            e.initCause(t);
            return e;
        }
        if (t instanceof org.apache.pulsar.client.api.v5.PulsarClientException.GettingAuthenticationDataException) {
            return new PulsarClientException.GettingAuthenticationDataException(t);
        }
        if (t instanceof PulsarClientException pce) {
            return pce;
        }
        return new PulsarClientException(t);
    }
}
