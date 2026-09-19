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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.CustomLog;
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
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.common.api.AuthData;

/**
 * Bridges an arbitrary v4 {@link org.apache.pulsar.client.api.Authentication} plugin so it can be used
 * through the new asynchronous v5 {@link Authentication} SPI (PIP-478).
 *
 * <p>The right v5 capability set is selected by inspecting the v4 plugin via {@link #wrap}, which
 * returns one of the package-private subclasses:
 * <ul>
 *     <li>{@link LegacyV4TlsAdapter} — for the built-in TLS plugins ({@code AuthenticationTls} /
 *         {@code AuthenticationKeyStoreTls}), identified by the {@code "tls"} auth-method name.</li>
 *     <li>{@link LegacyV4ChallengeResponseAdapter} — for plugins that drive a multi-stage
 *         challenge/response flow (e.g. SASL).</li>
 *     <li>{@link LegacyV4CredentialAdapter} — for simple credential plugins that provide command
 *         and/or HTTP auth data (e.g. {@code AuthenticationToken}).</li>
 * </ul>
 *
 * <p><b>Offload discipline (PIP-478).</b> {@link #wrap} is side-effect-free (it never invokes the v4
 * plugin's credential methods) and {@link #configure} only stores its parameters; all v4
 * {@code configure}/{@code start}/probe work runs later on
 * {@link AuthenticationInitContext#blockingExecutor()} inside {@link #initializeAsync}. A capability
 * method invoked before initialization has completed returns a failed future (never runs the v4 call
 * inline on the caller thread). Any v4 {@link org.apache.pulsar.client.api.PulsarClientException} is
 * translated into the matching v5 {@link PulsarClientException} subtype, completing the returned future
 * exceptionally.
 */
// Bridges to the deprecated v4 Authentication SPI by design (getAuthData()/configure(Map)).
@SuppressWarnings("deprecation")
@CustomLog
public abstract class LegacyV4AuthenticationAdapter implements Authentication {


    /**
     * The wrapped v4 authentication plugin.
     */
    protected final org.apache.pulsar.client.api.Authentication v4;

    /**
     * Whether this adapter is responsible for the wrapped plugin's {@code configure}/{@code start}/
     * {@code close} lifecycle. False when the client that resolved this adapter already owns the v4
     * instance and drives that lifecycle itself; see {@link #wrapAlreadyStarted}.
     */
    private final boolean ownsV4Lifecycle;

    private volatile AuthenticationInitContext initContext;
    private volatile Map<String, String> configuredParams;

    LegacyV4AuthenticationAdapter(org.apache.pulsar.client.api.Authentication v4) {
        this(v4, true);
    }

    LegacyV4AuthenticationAdapter(org.apache.pulsar.client.api.Authentication v4, boolean ownsV4Lifecycle) {
        this.v4 = v4;
        this.ownsV4Lifecycle = ownsV4Lifecycle;
    }

    /**
     * Wrap a v4 authentication plugin in the appropriate v5 adapter subtype.
     *
     * <p>Selection is <b>side-effect-free</b>: it branches only on the v4 plugin's (constant)
     * auth-method name and never calls a credential method. Probing the plugin for TLS material
     * ({@code v4.getAuthData(null).hasDataForTls()}) would run v4 I/O on the caller thread, breaking the
     * PIP-478 offload discipline, so it is not done here: the built-in TLS plugins report method name
     * {@code "tls"} and are routed to {@link LegacyV4TlsAdapter}; folding an arbitrary third-party
     * {@code hasDataForTls()} plugin's material into the client TLS configuration is a builder-time
     * concern handled by the client (PIP-478).
     *
     * <p>The built-in {@code AuthenticationDisabled} is resolved to {@link NoAuthentication} rather than to
     * a bridging adapter. Bridging it works, but it would put the default unauthenticated deployment — the
     * one connection path where there is provably nothing to fetch — through an executor hop and a
     * start-time probe on every connect. The match is on the concrete built-in rather than on the
     * {@code "none"} method name deliberately: a third-party plugin may report {@code "none"} and still
     * serve a credential, and discarding it here would downgrade that connection to anonymous silently.
     *
     * @param v4 the v4 authentication plugin to wrap
     * @return a v5 {@link Authentication} that delegates to the v4 plugin, or a built-in v5-native body
     *         for the method names that have one
     */
    public static Authentication wrap(org.apache.pulsar.client.api.Authentication v4) {
        return wrap(v4, true);
    }

    /**
     * Wrap a v4 plugin whose {@code configure}/{@code start} the caller has already performed and whose
     * {@code close} the caller will perform.
     *
     * <p>This is the client's own path. The client keeps the configured v4 plugin in its configuration and
     * starts it during construction — the v4 instance remains observable, and several client-side features
     * (notably folding an auth plugin's TLS material into the client's TLS policy) read it directly. The
     * adapter that lets the client <em>drive</em> that plugin through the v5 model must therefore not run
     * the lifecycle a second time: {@code AuthenticationOAuth2.start()} initializes the token flow, so a
     * second {@code start()} is not a harmless no-op.
     *
     * @param v4 the already-configured, already-started v4 authentication plugin
     * @return a v5 {@link Authentication} that delegates to the v4 plugin without re-running its lifecycle
     */
    public static Authentication wrapAlreadyStarted(org.apache.pulsar.client.api.Authentication v4) {
        return wrap(v4, false);
    }

    private static Authentication wrap(org.apache.pulsar.client.api.Authentication v4, boolean ownsLifecycle) {
        if (v4 == null) {
            throw new IllegalArgumentException("v4 authentication must not be null");
        }
        if (v4 instanceof AuthenticationDisabled) {
            return NoAuthentication.INSTANCE;
        }
        String methodName = v4.getAuthMethodName();
        if (TlsAuthentication.DEFAULT_AUTH_METHOD_NAME.equalsIgnoreCase(methodName)) {
            return new LegacyV4TlsAdapter(v4, ownsLifecycle);
        }
        if ("sasl".equalsIgnoreCase(methodName)) {
            return new LegacyV4ChallengeResponseAdapter(v4, ownsLifecycle);
        }
        return new LegacyV4CredentialAdapter(v4, ownsLifecycle);
    }

    /**
     * Unwrap the v4 {@link org.apache.pulsar.client.api.Authentication} that a {@link #wrap}-produced
     * v5 adapter delegates to, if any. This is the inverse of {@link #wrap} — it recovers the wrapped v4
     * plugin for any bridged adapter (used by the v5 client builder to inspect a bridged plugin's TLS
     * material and decide whether it can run raw on the v4 client or must stay wrapped so its credential
     * I/O off-loads).
     *
     * @param v5 the v5 authentication (possibly a {@code wrap()}-produced adapter)
     * @return the wrapped v4 plugin if {@code v5} is a legacy adapter, otherwise empty
     */
    public static Optional<org.apache.pulsar.client.api.Authentication> unwrapV4(Authentication v5) {
        if (v5 instanceof LegacyV4AuthenticationAdapter adapter) {
            return Optional.of(adapter.v4);
        }
        if (v5 instanceof LegacyV4TlsAdapter tlsAdapter) {
            return Optional.of(tlsAdapter.v4);
        }
        return Optional.empty();
    }

    @Override
    public void configure(Map<String, String> authParams) {
        // Store only. The actual (potentially blocking) v4.configure(...) is applied on the blocking
        // executor in initializeAsync(), per the PIP-478 offload discipline.
        this.configuredParams = authParams;
    }

    @Override
    public CompletableFuture<Void> initializeAsync(AuthenticationInitContext initContext) {
        this.initContext = initContext;
        return supplyOffloaded(() -> {
            if (ownsV4Lifecycle) {
                Map<String, String> cfg = configuredParams;
                if (cfg != null) {
                    v4.configure(cfg);
                }
                v4.start();
            }
            onStarted();
            return null;
        });
    }

    @Override
    public void close() throws Exception {
        // Closing a plugin this adapter did not start would close it twice: its owner closes it too, and a
        // v4 plugin is not required to be idempotent.
        if (ownsV4Lifecycle) {
            v4.close();
        }
    }

    /**
     * Hook invoked on the blocking executor immediately after {@code v4.start()} during
     * {@link #initializeAsync}. Subclasses that need to probe the started plugin (e.g. to learn which
     * capabilities it actually supports) override this; the base implementation is a no-op.
     *
     * @throws Exception if the probe fails, completing {@link #initializeAsync}'s future exceptionally
     */
    protected void onStarted() throws Exception {
    }

    /**
     * Return the {@link AuthenticationInitContext} captured during {@link #initializeAsync}.
     *
     * @return the init context, or {@code null} if not yet initialized
     */
    protected AuthenticationInitContext initContext() {
        return initContext;
    }

    /**
     * Offload a (potentially blocking) v4 call to the dedicated blocking executor captured at
     * initialization, translating v4 authentication exceptions into the matching v5 subtype.
     *
     * <p>There is deliberately no <em>inline</em> fallback: the caller thread is a Netty event loop, and
     * running a v4 plugin's credential I/O there is the stall PIP-478 exists to remove. When no
     * client-owned executor is bound — a connection opened outside a {@code PulsarClient}, such as the
     * proxy's broker connections — the work goes to {@link V5AuthContexts#sharedBlockingExecutor()}
     * rather than failing, since refusing would reject connections the v4 client accepted.
     *
     * @param supplier the blocking work
     * @param <T> the result type
     * @return a future completing with the supplier's result, or exceptionally with a v5 exception
     */
    protected <T> CompletableFuture<T> supplyOffloaded(ThrowingSupplier<T> supplier) {
        AuthenticationInitContext ctx = this.initContext;
        if (ctx == null) {
            // Not initialized yet: the wrapped plugin has not been configured or started, so there is
            // nothing valid to call. This is a different situation from "initialized, but the caller owns
            // no executor" below, and it still fails fast.
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Legacy v4 authentication adapter " + getClass().getName() + " was used before "
                            + "initializeAsync() completed; v4 plugin calls must run on the blocking "
                            + "executor, never inline (PIP-478)"));
        }
        Executor executor = ctx.blockingExecutor();
        if (executor == null) {
            executor = V5AuthContexts.sharedBlockingExecutor();
        }
        CompletableFuture<T> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                future.complete(supplier.get());
            } catch (Throwable t) {
                future.completeExceptionally(toV5Exception(t));
            }
        });
        return future;
    }

    /**
     * Translate a v4-side throwable into the matching v5 {@link PulsarClientException} subtype. A
     * throwable that is already a v5 exception (e.g. one this adapter deliberately raised) passes
     * through unchanged, preserving its subtype.
     *
     * @param t the throwable raised by a v4 call
     * @return the translated v5 exception
     */
    protected static Throwable toV5Exception(Throwable t) {
        if (t instanceof PulsarClientException) {
            return t;
        }
        if (t instanceof org.apache.pulsar.client.api.PulsarClientException.AuthenticationException) {
            return new PulsarClientException.AuthenticationException(t);
        }
        if (t instanceof org.apache.pulsar.client.api.PulsarClientException.GettingAuthenticationDataException) {
            return new PulsarClientException.GettingAuthenticationDataException(t);
        }
        if (t instanceof javax.naming.AuthenticationException) {
            return new PulsarClientException.AuthenticationException(t);
        }
        return new PulsarClientException.GettingAuthenticationDataException(t);
    }

    /**
     * A supplier that may throw a checked exception.
     *
     * @param <T> the result type
     */
    @FunctionalInterface
    protected interface ThrowingSupplier<T> {
        /**
         * Compute the result.
         *
         * @return the result
         * @throws Exception if the computation fails
         */
        T get() throws Exception;
    }

    /**
     * Adapter for simple credential plugins that supply command and/or HTTP auth data
     * (for example {@code AuthenticationToken}).
     *
     * <p>Which of the two single-pass capabilities this adapter advertises is decided by probing the
     * started v4 plugin (PIP-478): {@link #capability(Class)} returns {@link Optional#empty()} for a
     * kind the wrapped plugin does not actually support, rather than declaring it and serving empty
     * data. A capability that is claimed but yields no v4 data at call time completes exceptionally with
     * {@link PulsarClientException.UnsupportedAuthenticationException}.
     */
    static final class LegacyV4CredentialAdapter extends LegacyV4AuthenticationAdapter
            implements BinaryAuthDataProvider, BinaryAuthChallengeHandler, HttpAuthHeadersProvider {

        private volatile boolean supportsHttp;

        LegacyV4CredentialAdapter(org.apache.pulsar.client.api.Authentication v4, boolean ownsV4Lifecycle) {
            super(v4, ownsV4Lifecycle);
        }

        @Override
        protected void onStarted() throws Exception {
            // Probe (post-start, on the blocking executor) which credential kinds the v4 plugin actually
            // provides, so capability(...) only advertises the ones it supports. If the probe itself
            // fails, keep the binary path (the reason this adapter was selected) and disable the
            // unverifiable HTTP path.
            try {
                AuthenticationDataProvider probe = v4.getAuthData(null);
                supportsHttp = probe != null && probe.hasDataForHttp();
            } catch (Exception e) {
                log.debug().attr("plugin", v4.getClass().getName()).exception(e)
                        .log("Could not probe v4 auth plugin for HTTP support; assuming binary only");
                supportsHttp = false;
            }
        }

        @Override
        public <T> Optional<T> capability(Class<T> kind) {
            // This adapter can serve only the two single-pass capabilities, and only those the probe
            // confirmed the v4 plugin actually supports. Anything else is unsupported.
            if (kind == BinaryAuthDataProvider.class || kind == BinaryAuthChallengeHandler.class) {
                // Both are always advertised, because in v4 every plugin could authenticate a binary
                // connection: the client called authenticate(INIT_AUTH_DATA) and sent whatever came back,
                // empty included, and answered any challenge through the same provider. Gating either on a
                // probe would refuse connections v4 accepted — a plugin whose credential is presented at
                // the TLS layer reports no command data yet still has to connect. The probe below therefore
                // decides only the HTTP capability, which has no such universal fallback.
                return Optional.of(kind.cast(this));
            }
            if (kind == HttpAuthHeadersProvider.class) {
                return supportsHttp ? Optional.of(kind.cast(this)) : Optional.empty();
            }
            return Optional.empty();
        }

        @Override
        public String authMethodName() {
            return v4.getAuthMethodName();
        }

        @Override
        public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext callContext) {
            return supplyOffloaded(() -> {
                AuthenticationDataProvider d = v4.getAuthData(callContext.brokerHost());
                if (d == null) {
                    throw new PulsarClientException.UnsupportedAuthenticationException(
                            "v4 plugin " + v4.getClass().getName() + " returned no AuthenticationDataProvider");
                }
                // The provider is retained for this exchange so a following challenge round continues the
                // same conversation, and the initial credential comes from authenticate(INIT_AUTH_DATA) —
                // verbatim what the v4 client did. Its default implementation returns getCommandData(), so a
                // single-pass plugin is unaffected, while a challenge/response plugin (which serves its
                // first frame only through authenticate) keeps working.
                callContext.setStateObject(AuthenticationDataProvider.class, d);
                AuthData initial = d.authenticate(AuthData.INIT_AUTH_DATA);
                return new BinaryAuthData(initial == null ? new byte[0] : initial.getBytes());
            });
        }

        @Override
        public CompletableFuture<ChallengeResponse> respondToChallengeAsync(AuthenticationCallContext callContext,
                AuthChallenge challenge) {
            return supplyOffloaded(() -> {
                AuthenticationDataProvider d = callContext.getStateObject(AuthenticationDataProvider.class)
                        .orElse(null);
                if (d == null) {
                    d = v4.getAuthData(callContext.brokerHost());
                    callContext.setStateObject(AuthenticationDataProvider.class, d);
                }
                AuthData response = d.authenticate(AuthData.of(challenge.bytes()));
                return new ChallengeResponse(response == null ? null : response.getBytes());
            });
        }

        @Override
        public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext callContext) {
            return supplyOffloaded(() -> {
                AuthenticationDataProvider d = v4.getAuthData();
                if (d == null || !d.hasDataForHttp() || d.getHttpHeaders() == null) {
                    throw new PulsarClientException.UnsupportedAuthenticationException(
                            "v4 plugin " + v4.getClass().getName() + " provided no HTTP auth headers "
                                    + "(hasDataForHttp()/getHttpHeaders() returned nothing)");
                }
                Map<String, String> headers = new HashMap<>();
                for (Map.Entry<String, String> e : d.getHttpHeaders()) {
                    headers.put(e.getKey(), e.getValue());
                }
                return HttpAuthHeaders.of(headers);
            });
        }
    }

    /**
     * Adapter for plugins that drive a multi-stage challenge/response flow such as SASL, where the v4
     * {@link AuthenticationDataProvider#authenticate(AuthData)} method is used to exchange tokens.
     *
     * <p>The per-exchange v4 {@link AuthenticationDataProvider} is stored in the call-context state slot
     * so the same provider instance handles the whole challenge/response exchange.
     */
    static final class LegacyV4ChallengeResponseAdapter extends LegacyV4AuthenticationAdapter
            implements BinaryAuthDataProvider, BinaryAuthChallengeHandler {

        LegacyV4ChallengeResponseAdapter(org.apache.pulsar.client.api.Authentication v4,
                boolean ownsV4Lifecycle) {
            super(v4, ownsV4Lifecycle);
        }

        @Override
        public String authMethodName() {
            return v4.getAuthMethodName();
        }

        @Override
        public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext callContext) {
            return supplyOffloaded(() -> {
                AuthenticationDataProvider d = v4.getAuthData(callContext.brokerHost());
                callContext.setStateObject(AuthenticationDataProvider.class, d);
                AuthData result = d.authenticate(AuthData.INIT_AUTH_DATA);
                byte[] bytes = result == null ? new byte[0] : result.getBytes();
                return new BinaryAuthData(bytes);
            });
        }

        @Override
        public CompletableFuture<ChallengeResponse> respondToChallengeAsync(AuthenticationCallContext callContext,
                AuthChallenge challenge) {
            return supplyOffloaded(() -> {
                AuthenticationDataProvider d = callContext.getStateObject(AuthenticationDataProvider.class)
                        .orElse(null);
                if (d == null) {
                    d = v4.getAuthData(callContext.brokerHost());
                    callContext.setStateObject(AuthenticationDataProvider.class, d);
                }
                AuthData response = d.authenticate(AuthData.of(challenge.bytes()));
                return new ChallengeResponse(response == null ? null : response.getBytes());
            });
        }
    }

    /**
     * Adapter for v4 plugins that supply TLS material (for example {@code AuthenticationTls} or
     * {@code AuthenticationKeyStoreTls}). The binary-protocol handshake carries no payload — the TLS
     * material is applied at the transport layer — so this reuses {@link TlsAuthentication}.
     */
    static final class LegacyV4TlsAdapter extends TlsAuthentication {

        private final org.apache.pulsar.client.api.Authentication v4;
        private final boolean ownsV4Lifecycle;

        LegacyV4TlsAdapter(org.apache.pulsar.client.api.Authentication v4, boolean ownsV4Lifecycle) {
            this.v4 = v4;
            this.ownsV4Lifecycle = ownsV4Lifecycle;
        }

        @Override
        public void configure(Map<String, String> authParams) {
            v4.configure(authParams);
        }

        /**
         * Start the wrapped plugin. A custom plugin reporting the {@code tls} method name is adapted here and
         * would otherwise never be started: its {@code configure(...)} was forwarded but its {@code start()}
         * was not, so any credential loading it does in {@code start()} never happened. The built-in
         * {@code AuthenticationTls} has an empty {@code start()}, which is why this went unnoticed.
         *
         * @param initContext the framework services
         * @return completion of the v5 initialization
         */
        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext initContext) {
            if (ownsV4Lifecycle) {
                try {
                    v4.start();
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
            return super.initializeAsync(initContext);
        }

        /**
         * Close the wrapped plugin, mirroring {@link #initializeAsync}. Without this a custom {@code tls}
         * plugin holding resources (a reloading key store, an HSM session) leaks for the client's lifetime.
         *
         * @throws Exception if the wrapped plugin fails to close
         */
        @Override
        public void close() throws Exception {
            if (!ownsV4Lifecycle) {
                return;
            }
            try {
                v4.close();
            } finally {
                super.close();
            }
        }
    }
}
