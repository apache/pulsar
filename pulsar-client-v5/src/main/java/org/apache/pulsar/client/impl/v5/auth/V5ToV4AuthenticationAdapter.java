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
package org.apache.pulsar.client.impl.v5.auth;

import static java.nio.charset.StandardCharsets.UTF_8;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.time.Clock;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServicesAware;
import org.apache.pulsar.client.impl.auth.v5.AuthMetrics;
import org.apache.pulsar.client.impl.auth.v5.BinaryAuthenticationExchange;
import org.apache.pulsar.common.api.AuthData;

/**
 * Exposes a new-SPI v5 {@link Authentication} plugin through the legacy v4
 * {@link org.apache.pulsar.client.api.Authentication} interface that {@code ClientCnx} drives
 * (PIP-478).
 *
 * <p>It also implements {@link AsyncAuthenticationDriver} so that the {@code ClientCnx} carve-out
 * (PIP-478) can authenticate via the asynchronous (non event-loop-blocking) path. Both the
 * async driver and the synchronous v4 {@link #getAuthData(String)} path are exchange-scoped: one
 * {@link AsyncAuthenticationDriver.AuthenticationExchange} — and one {@link AuthenticationCallContext}
 * with its state slot — backs a single connection attempt, so a plugin's challenge/response
 * conversation state survives across all of that connection's rounds (initial data, refresh, and every
 * {@code CommandAuthChallenge}).
 *
 * <p>The per-connection routing (initial connect / refresh sentinel / challenge) and the v5→v4 exception
 * mapping are the shared {@link BinaryAuthenticationExchange} — the same implementation the built-in
 * shims' {@code V5BinaryAuthenticationDriver} uses — so the two adapters cannot drift on the normative
 * binary rules. Binary transport requires the {@link BinaryAuthDataProvider} capability (PIP-478 binary
 * routing rule 1); a wrapped v5 plugin that does not expose it fails loudly — {@link #start()} throws a
 * v4-mapped {@link PulsarClientException.UnsupportedAuthenticationException} — rather than synthesizing
 * an empty {@code "none"} credential.
 */
// Implements the deprecated v4 Authentication SPI by design (configure(Map)).
@SuppressWarnings("deprecation")
public class V5ToV4AuthenticationAdapter
        implements org.apache.pulsar.client.api.Authentication, AsyncAuthenticationDriver,
        ClientAuthenticationServicesAware {

    private static final long serialVersionUID = 1L;

    private final transient Authentication v5;
    // Late-bound by the client via bindClientAuthenticationServices(...) before start() (PIP-478);
    // null until then (e.g. when the adapter is exercised outside a client), yielding an init context with
    // no framework services.
    private transient volatile ClientAuthenticationServices services;
    // Built from the bound services' OpenTelemetry in start(); NOOP until then.
    private transient volatile AuthMetrics authMetrics = AuthMetrics.NOOP;

    /**
     * Create an adapter that exposes a v5 authentication plugin through the v4 interface. The framework
     * services the plugin's init context reports are late-bound by the client through
     * {@link #bindClientAuthenticationServices} before {@link #start()}.
     *
     * @param v5 the v5 authentication plugin to expose
     */
    public V5ToV4AuthenticationAdapter(Authentication v5) {
        this.v5 = v5;
    }

    @Override
    public void bindClientAuthenticationServices(ClientAuthenticationServices services) {
        this.services = services;
    }

    @Override
    public String getAuthMethodName() {
        return requireBinaryProvider().authMethodName();
    }

    @Override
    public void configure(Map<String, String> authParams) {
        v5.configure(authParams);
    }

    @Override
    public void start() throws PulsarClientException {
        ClientAuthenticationServices bound = this.services;
        this.authMetrics = AuthMetrics.create(bound == null ? null : bound.openTelemetry());
        AuthenticationInitContext initContext = bound == null
                ? new SimpleAuthInitContext(null, null, null, Clock.systemUTC(), OpenTelemetry.noop(), null)
                : new SimpleAuthInitContext(bound.httpClientFactory(), bound.scheduler(), bound.blockingExecutor(),
                        bound.clock() == null ? Clock.systemUTC() : bound.clock(),
                        bound.openTelemetry() == null ? OpenTelemetry.noop() : bound.openTelemetry(),
                        bound.clientInstanceId());
        try {
            v5.initializeAsync(initContext).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        } catch (Exception e) {
            throw BinaryAuthenticationExchange.toV4Exception(BinaryAuthenticationExchange.unwrap(e));
        }
        // Binary transport requires the BinaryAuthDataProvider capability (PIP-478 binary routing
        // rule 1). Fail loudly here rather than synthesizing a "none"/empty credential.
        if (v5.capability(BinaryAuthDataProvider.class).isEmpty()) {
            throw new PulsarClientException.UnsupportedAuthenticationException(
                    "v5 authentication plugin " + v5.getClass().getName() + " does not expose "
                            + "BinaryAuthDataProvider; the Pulsar binary transport requires it (PIP-478)");
        }
    }

    @Override
    public AuthenticationDataProvider getAuthData(String brokerHostName) throws PulsarClientException {
        AuthenticationExchange exchange = newAuthenticationExchange(brokerHostName);
        try {
            AuthData initial = exchange.getAuthDataAsync().get();
            return new SynthesizedV4DataProvider(exchange, initial, v5, brokerHostName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        } catch (Exception e) {
            throw BinaryAuthenticationExchange.toV4Exception(BinaryAuthenticationExchange.unwrap(e));
        }
    }

    @Override
    public AuthenticationExchange newAuthenticationExchange(String brokerHostName) {
        return new BinaryAuthenticationExchange(v5, new SimpleAuthCallContext(brokerHostName), authMetrics);
    }

    /**
     * The v5 {@link BinaryAuthDataProvider} the wrapped plugin exposes for the binary transport.
     *
     * @return the capability
     * @throws IllegalStateException if the plugin does not support the binary transport; normally
     *         {@link #start()} has already failed loudly before this can be reached
     */
    private BinaryAuthDataProvider requireBinaryProvider() {
        return v5.capability(BinaryAuthDataProvider.class)
                .orElseThrow(() -> new IllegalStateException(
                        "v5 authentication plugin " + v5.getClass().getName() + " does not expose "
                                + "BinaryAuthDataProvider; it cannot authenticate a Pulsar binary-protocol "
                                + "connection (PIP-478)"));
    }

    @Override
    public void close() throws IOException {
        try {
            v5.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * v5 {@link Authentication} plugins are not serializable. Serialization of this adapter is therefore
     * unsupported. Configure authentication on a remote/forked context via the
     * {@code authPluginClassName} + {@code authParams} string form instead.
     *
     * @param out the object output stream
     * @throws NotSerializableException always
     */
    private void writeObject(ObjectOutputStream out) throws NotSerializableException {
        throw new NotSerializableException("V5 authentication plugins are not serializable. "
                + "Configure authentication using authPluginClassName + authParams instead of a "
                + "pre-built Authentication instance.");
    }

    private static boolean isInitSentinel(AuthData challenge) {
        return challenge != null && Arrays.equals(challenge.getBytes(), AuthData.INIT_AUTH_DATA_BYTES);
    }

    /**
     * A v4 {@link AuthenticationDataProvider} synthesized from a single
     * {@link AsyncAuthenticationDriver.AuthenticationExchange}, so that the synchronous v4 {@code ClientCnx}
     * code path keeps working — including multi-round challenge/response, which rides
     * {@link #authenticate(AuthData)} through the same exchange (and thus the same
     * {@link AuthenticationCallContext} state slot).
     *
     * <p>It also bridges the single-pass HTTP transport: the v4 HTTP lookup path (which drives
     * {@link #hasDataForHttp()} / {@link #getHttpHeaders()}) is served from the wrapped plugin's v5
     * {@link HttpAuthHeadersProvider} capability, so a bearer/role credential (for example
     * {@code Authorization: Bearer <jwt>}) is attached to the lookup {@code GET}. Multi-round
     * SASL-over-HTTP challenge/response is out of scope here — it rides the framework HTTP challenge
     * driver, not this bridge — so a plugin that exposes only an
     * {@link org.apache.pulsar.client.api.v5.auth.HttpAuthChallengeHandler} contributes no static header
     * and {@link #hasDataForHttp()} stays {@code false}.
     */
    private static final class SynthesizedV4DataProvider implements AuthenticationDataProvider {

        private static final long serialVersionUID = 1L;

        private final transient AuthenticationExchange exchange;
        private final transient byte[] initialData;
        private final transient Authentication v5;
        private final transient String brokerHostName;

        SynthesizedV4DataProvider(AuthenticationExchange exchange, AuthData initial, Authentication v5,
                                  String brokerHostName) {
            this.exchange = exchange;
            this.initialData = initial == null || initial.getBytes() == null ? null : initial.getBytes();
            this.v5 = v5;
            this.brokerHostName = brokerHostName;
        }

        @Override
        public boolean hasDataFromCommand() {
            return initialData != null;
        }

        @Override
        public String getCommandData() {
            return initialData == null ? null : new String(initialData, UTF_8);
        }

        @Override
        public boolean hasDataForHttp() {
            return v5.capability(HttpAuthHeadersProvider.class).isPresent();
        }

        @Override
        public Set<Map.Entry<String, String>> getHttpHeaders() throws Exception {
            Optional<HttpAuthHeadersProvider> provider = v5.capability(HttpAuthHeadersProvider.class);
            if (provider.isEmpty()) {
                return null;
            }
            // Blocking .get() is intentional: this v4 hook runs on the client's blockingAuthExecutor
            // (HttpClient.computeAuthHeaders offloads the whole v4 branch), never the event loop.
            HttpAuthCallContext ctx = new SynthesizedHttpAuthCallContext(brokerHostName);
            try {
                HttpAuthHeaders headers = provider.get().getHttpHeadersAsync(ctx).get();
                return headers == null || headers.isEmpty() ? null : headers.asMap().entrySet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (ExecutionException e) {
                Throwable cause = BinaryAuthenticationExchange.unwrap(e);
                if (cause instanceof Exception ex) {
                    throw ex;
                }
                throw new PulsarClientException(cause);
            }
        }

        /**
         * Route a challenge (or the connect/refresh sentinel) through the bound exchange so that v5
         * challenge handlers work through the plain synchronous v4 path, sharing the exchange's state
         * slot across rounds. The init sentinel reuses the already-computed initial credential to avoid
         * re-running the initial round (which, for challenge/response plugins, would re-seed the
         * conversation).
         *
         * @param data the challenge payload, or the {@code INIT}/{@code REFRESH} sentinel
         * @return the response auth data
         * @throws javax.naming.AuthenticationException if the exchange fails
         */
        @Override
        public AuthData authenticate(AuthData data) throws javax.naming.AuthenticationException {
            if (isInitSentinel(data)) {
                return AuthData.of(initialData == null ? new byte[0] : initialData);
            }
            try {
                AuthData result = exchange.authenticateAsync(data).get();
                return AuthData.of(result == null ? new byte[0] : result.getBytes());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw namingAuthException(e);
            } catch (ExecutionException e) {
                throw namingAuthException(BinaryAuthenticationExchange.unwrap(e));
            }
        }

        private static javax.naming.AuthenticationException namingAuthException(Throwable cause) {
            javax.naming.AuthenticationException e = new javax.naming.AuthenticationException(
                    cause == null ? null : cause.getMessage());
            e.initCause(cause);
            return e;
        }
    }

    /**
     * A single-pass {@link HttpAuthCallContext} for the v4 HTTP lookup path, which surfaces only the
     * request host (from {@link org.apache.pulsar.client.api.Authentication#getAuthData(String)}) rather
     * than a full request URI. The host is presented as an {@code https://<host>} URI; single-pass
     * {@link HttpAuthHeadersProvider}s (token/Athenz/OAuth2) do not consult it. There is no prior server
     * challenge on this path, so {@link #serverChallengeHeaders()} is always empty; the state slot exists
     * to satisfy the contract but is not shared across requests.
     */
    private static final class SynthesizedHttpAuthCallContext implements HttpAuthCallContext {

        private final URI requestUri;
        private final Map<Class<?>, Object> state = new ConcurrentHashMap<>();

        SynthesizedHttpAuthCallContext(String brokerHostName) {
            this.requestUri = URI.create("https://" + (brokerHostName == null ? "" : brokerHostName));
        }

        @Override
        public URI requestUri() {
            return requestUri;
        }

        @Override
        public Optional<HttpAuthHeaders> serverChallengeHeaders() {
            return Optional.empty();
        }

        @Override
        public <T> Optional<T> getStateObject(Class<T> clazz) {
            return Optional.ofNullable(clazz.cast(state.get(clazz)));
        }

        @Override
        public <T> void setStateObject(Class<T> clazz, T value) {
            if (value == null) {
                state.remove(clazz);
            } else {
                state.put(clazz, value);
            }
        }
    }
}
