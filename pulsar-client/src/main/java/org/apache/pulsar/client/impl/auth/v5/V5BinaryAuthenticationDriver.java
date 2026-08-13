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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.common.api.AuthData;

/**
 * Drives a v5-native {@link Authentication} body over the Pulsar binary transport, exposing it as the
 * legacy {@link BinaryAuthenticationDriver} that {@code ClientCnx} consumes (PIP-478). This is
 * the single exchange pattern shared by the built-in v4 auth shims (Token, Basic, OAuth2, Athenz, SASL);
 * each shim wraps its v5 body in this driver rather than re-implementing the exchange.
 *
 * <p>The per-connection routing (initial connect / refresh sentinel / challenge) and the v5→v4 exception
 * mapping live in the shared {@link BinaryAuthenticationExchange}, which this driver and the
 * {@code pulsar-client-v5} application-plugin adapter both use so the two cannot drift on the normative
 * binary rules. One {@link BinaryAuthenticationExchange} owns one call context (and its state slot) for
 * the lifetime of a single connection attempt, so multi-round conversation state survives across rounds.
 */
public final class V5BinaryAuthenticationDriver implements BinaryAuthenticationDriver {

    private final Authentication v5;
    private final String clientInstanceId;
    private final ClientAuthenticationServices services;
    private final AuthMetrics authMetrics;
    private CompletableFuture<Void> initialization;

    /**
     * @param v5 the v5-native authentication body to drive over the binary transport
     */
    public V5BinaryAuthenticationDriver(Authentication v5) {
        this(v5, null, null);
    }

    /**
     * @param v5       the v5-native authentication body to drive over the binary transport
     * @param services the client's late-bound framework services (may be {@code null} before binding),
     *                 which flow to the body's {@code initializeAsync(...)} so a credential-fetching body
     *                 can off-load its blocking fetch onto the bounded blocking executor
     */
    public V5BinaryAuthenticationDriver(Authentication v5, ClientAuthenticationServices services) {
        this(v5, services, services == null ? null : services.clientInstanceId());
    }

    /**
     * @param v5               the v5-native authentication body to drive over the binary transport
     * @param services         the client's late-bound framework services (may be {@code null})
     * @param clientInstanceId a stable id for logging correlation (may be {@code null})
     */
    public V5BinaryAuthenticationDriver(Authentication v5, ClientAuthenticationServices services,
            String clientInstanceId) {
        this.v5 = v5;
        this.services = services;
        this.clientInstanceId = clientInstanceId;
        this.authMetrics = AuthMetrics.create(services == null ? null : services.openTelemetry());
    }

    @Override
    public AuthenticationExchange newAuthenticationExchange(String brokerHostName) {
        BinaryAuthenticationExchange exchange = new BinaryAuthenticationExchange(v5,
                V5AuthContexts.binaryCallContext(brokerHostName), authMetrics);
        CompletableFuture<Void> initialization = initializedAsync();
        // ClientCnx calls this on the Netty event loop, so initialization — which the SPI allows to do I/O —
        // must not be waited for here. The built-in bodies complete it immediately (they only stash the bound
        // blocking executor), which is the common case and returns the bare exchange; anything slower gates
        // the exchange's own futures instead, where ClientCnx already handles asynchrony.
        if (initialization.isDone() && !initialization.isCompletedExceptionally()) {
            return exchange;
        }
        return new InitGatedExchange(initialization, exchange);
    }

    /**
     * The body's one-shot initialization, memoized. A failed initialization is not cached — the next
     * connection attempt retries, matching the previous behaviour where a throwing {@code join()} left
     * {@code initialized} false, so a reconnect after a transient failure can still succeed.
     *
     * @return a future completing when the body is ready to serve credentials
     */
    private synchronized CompletableFuture<Void> initializedAsync() {
        CompletableFuture<Void> current = initialization;
        if (current != null) {
            return current;
        }
        CompletableFuture<Void> started;
        try {
            started = v5.initializeAsync(V5AuthContexts.initContext(services, clientInstanceId));
        } catch (Throwable t) {
            // The SPI forbids throwing synchronously, but a third-party body may still do it.
            return CompletableFuture.failedFuture(t);
        }
        initialization = started;
        // Attach the failure-clearing callback only after the field is installed: an already-failed future
        // runs it synchronously, so clearing beforehand would be undone by the assignment and the failure
        // would be cached forever. Returning `started` rather than the field matters for the same reason —
        // the callback may already have nulled it out.
        started.whenComplete((ignored, ex) -> {
            if (ex != null) {
                forgetFailedInitialization(started);
            }
        });
        return started;
    }

    private synchronized void forgetFailedInitialization(CompletableFuture<Void> failed) {
        // Only if it is still the current one: a later attempt may already have installed its own.
        if (initialization == failed) {
            initialization = null;
        }
    }

    /**
     * Defers an exchange's rounds until the body's initialization completes, so a body that does I/O in
     * {@code initializeAsync} delays the handshake rather than blocking the event loop. Failures propagate
     * through the returned futures, which is what {@code ClientCnx} already expects.
     */
    private record InitGatedExchange(CompletableFuture<Void> initialization, AuthenticationExchange delegate)
            implements AuthenticationExchange {

        @Override
        public String authMethodName() {
            return delegate.authMethodName();
        }

        @Override
        public CompletableFuture<AuthData> getAuthDataAsync() {
            return initialization.thenCompose(ignored -> delegate.getAuthDataAsync());
        }

        @Override
        public CompletableFuture<AuthData> authenticateAsync(AuthData challenge) {
            return initialization.thenCompose(ignored -> delegate.authenticateAsync(challenge));
        }
    }

    private BinaryAuthDataProvider requireProvider() {
        return v5.capability(BinaryAuthDataProvider.class)
                .orElseThrow(() -> new IllegalStateException(
                        "v5 authentication body " + v5.getClass().getName() + " does not expose "
                                + "BinaryAuthDataProvider; it cannot authenticate a Pulsar binary-protocol "
                                + "connection (PIP-478)"));
    }

    /**
     * @return the auth method name, from the required binary capability
     */
    public String authMethodName() {
        return requireProvider().authMethodName();
    }
}
