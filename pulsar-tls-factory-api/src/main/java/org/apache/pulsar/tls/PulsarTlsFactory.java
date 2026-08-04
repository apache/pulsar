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
package org.apache.pulsar.tls;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * The v5 TLS SPI: a purpose-driven, typed <strong>instance factory</strong> that replaces PIP-337's
 * {@code PulsarSslFactory} / {@code PulsarSslConfiguration} (PIP-478).
 *
 * <p>Consumers request a fully configured TLS object of a well-known class for a {@link TlsPurpose};
 * <em>how</em> the factory sources key material and builds the object — files, a KMS API, an
 * HSM-backed {@code KeyManagerFactory} — is entirely factory-internal. Nothing material-shaped appears
 * in this SPI and key material never crosses a Pulsar API.
 *
 * <p><b>Well-known instance classes.</b> A factory need only implement whichever it can build
 * directly:
 * <ul>
 *   <li>{@code io.netty.handler.ssl.SslContext} — binary protocol and AsyncHttpClient-based HTTP;
 *       optional (on {@code empty()} the framework wraps the JDK {@code SSLContext} fallback with
 *       Netty's {@code JdkSslContext} adapter).</li>
 *   <li>Jetty's {@code org.eclipse.jetty.util.ssl.SslContextFactory.Server} — the web servers;
 *       optional (on {@code empty()} the framework synthesizes it from an {@code SSLContext}
 *       subscription and Jetty's {@code reload(...)}).</li>
 *   <li>Jetty's {@code org.eclipse.jetty.util.ssl.SslContextFactory.Client} — the proxy's admin
 *       {@code HttpClient} (proxy&rarr;broker); optional (on {@code empty()} the framework synthesizes it
 *       from an {@code SSLContext} subscription and Jetty's {@code reload(...)}, mirroring the Server).</li>
 *   <li>{@code javax.net.ssl.SSLContext} — the universal fallback the framework synthesizes the
 *       richer objects from; required for a purpose only <em>unless</em> the factory natively supplies
 *       every richer class that purpose consumes.</li>
 *   <li>{@code javax.net.ssl.SSLParameters} — the optional engine-policy companion to the
 *       {@code SSLContext} fallback, consulted <em>only</em> on the synthesis path. It carries the
 *       engine-level baseline a bare {@code SSLContext} cannot express (enabled protocols and cipher
 *       suites, client-auth mode, endpoint identification, algorithm constraints) — the JDK API has no
 *       setter for a context's default parameters. {@code empty()} means the consumer's own configuration
 *       applies, as before. Requires zero non-JDK dependencies.</li>
 * </ul>
 * As long as a factory supplies at least the JDK {@code SSLContext} for a purpose, the framework can
 * derive the Netty and Jetty objects from it.
 *
 * <p><b>{@code SSLParameters} merge order (synthesis path).</b> When the framework synthesizes the Netty /
 * Jetty objects from the {@code SSLContext} fallback, it also requests
 * {@code createInstance(purpose, SSLParameters.class)} and merges deterministically:
 * <ol>
 *   <li>the factory's {@code SSLParameters} (non-null members only) form the engine baseline;</li>
 *   <li>{@code endpointIdentificationAlgorithm} — the factory's value wins when set, otherwise the
 *       consumer's hostname-verification configuration applies {@code "HTTPS"} on client purposes;</li>
 *   <li>SNI server names are always set per connection from the target endpoint, overriding any factory
 *       baseline (a factory should not pin SNI);</li>
 *   <li>on server purposes a factory-supplied {@code SSLParameters} is authoritative for
 *       {@code needClientAuth}/{@code wantClientAuth}, otherwise the consumer's client-auth flag maps as
 *       usual.</li>
 * </ol>
 * On subscriptions the parameters are re-requested with each {@code SSLContext} delivery, so engine policy
 * may rotate with material. The synthesized Jetty factory consults only the protocols, cipher suites, and
 * client-auth members (Jetty exposes no setters for the finer members); the full member set applies on the
 * Netty synthesis path.
 *
 * <p><b>{@code Optional.empty()} means exactly one thing:</b> the factory does not support the
 * requested {@code (purpose, class)} combination. It is NOT a purpose-resolution signal — how a
 * factory maps a purpose to configured material is factory-internal. Two guardrails follow: a factory
 * that has resolved a purpose to material (or to the system default) and <em>supports the requested
 * class</em> must <strong>never</strong> return {@code empty()} for it — a resolved-but-unbuildable
 * request completes the future <em>exceptionally</em> instead, so a real configuration error cannot be
 * masked by the framework quietly falling back to {@code SSLContext} synthesis; and when nothing is
 * configured for a purpose a factory applies the role's terminal rule — an unconfigured client purpose
 * resolves to the system default, an unconfigured server purpose is a configuration error — unless it
 * documents divergent resolution.
 *
 * <p><b>Never throws synchronously.</b> Like every future-returning method in this SPI,
 * {@code createInstance} reports all failures — including argument validation and build errors — by
 * completing the returned future exceptionally; it never throws on the calling thread.
 *
 * <p><b>Thread-safety and lifecycle ordering.</b> {@link #initialize} is called <em>exactly once</em> and
 * completes before any {@code createInstance} call. After initialization, {@code createInstance} may be
 * invoked <em>concurrently</em> — for the same and for different purposes — as many connections resolve
 * TLS in parallel; implementations must be thread-safe. {@link #close} is called at most once and after
 * the owning component has stopped issuing {@code createInstance} calls.
 *
 * <p><b>Reload callbacks</b> (subscribing overload) are serial per subscription, never concurrent,
 * never invoked on a consumer event loop, and the first delivery <em>happens-before</em> the returned
 * future completes. A failed rebuild on rotation keeps serving the last-good instance (logged at WARN,
 * recorded in the reload-failure metric) and retries on the next observed material change; a consumer
 * callback that throws is caught and logged, and the subscription stays live.
 */
public interface PulsarTlsFactory extends AutoCloseable {

    /**
     * Initialize the factory with its runtime services. Completes before the first
     * {@code createInstance} call; a failure is fatal to the owning component's startup.
     *
     * @param context the factory parameters and framework runtime services
     * @return a future completing when the factory is ready
     */
    CompletableFuture<Void> initialize(TlsFactoryInitContext context);

    /**
     * Build a one-shot instance of {@code instanceClass} for the given purpose.
     *
     * @param purpose       the TLS purpose to resolve material for
     * @param instanceClass the well-known instance class to build
     * @param <T>           the instance type
     * @return a future of a fully configured instance for the purpose, or {@link Optional#empty()}
     *         when this factory does not support the {@code (purpose, class)} combination; a failure to
     *         build a SUPPORTED combination completes the future exceptionally instead
     */
    <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose, Class<T> instanceClass);

    /**
     * One-shot variant carrying the destination endpoint as a per-request HINT. Client-side consumers
     * pass the target host/port when they know it (one connection, one instance); a factory that serves
     * per-destination material (multi-cluster deployments, per-target workload identities) may key on
     * it.
     *
     * <p>The default implementation ignores the endpoint and delegates to
     * {@link #createInstance(TlsPurpose, Class)} — most factories, including the default file-based one,
     * never look at it. The endpoint does NOT replace hostname verification or SNI: those are applied at
     * engine creation from the same peer address.
     *
     * <p>Never throws synchronously: like every future-returning method here, it reports all failures by
     * completing the returned future exceptionally. The default delegation is guarded so that even a
     * delegate that throws on the calling thread surfaces as a failed future rather than propagating.
     *
     * @param purpose       the TLS purpose to resolve material for
     * @param endpoint      the destination host/port hint
     * @param instanceClass the well-known instance class to build
     * @param <T>           the instance type
     * @return a future of a fully configured instance, or {@link Optional#empty()} when unsupported
     */
    default <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
            TlsPurpose purpose, TlsEndpoint endpoint, Class<T> instanceClass) {
        try {
            return createInstance(purpose, instanceClass);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    /**
     * Like the one-shot form, but additionally subscribes to reloads: {@code onLoadOrReload} receives
     * the instance on initial load and a REBUILT instance whenever the underlying material changes. The
     * returned future completes after the first delivery. Subscriptions are purpose-scoped and carry no
     * endpoint — they serve server-side listeners, which have no destination.
     *
     * @param purpose        the TLS purpose to resolve material for
     * @param instanceClass  the well-known instance class to build
     * @param onLoadOrReload receives the instance on first load and on every subsequent rebuild
     * @param <T>            the instance type
     * @return a future of the subscribing handle, or {@link Optional#empty()} when unsupported
     */
    <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
            TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload);

    /**
     * Release the factory and all its instances. The component that created the factory owns and closes
     * it; a factory instance supplied programmatically to the v5 builder is adopted and closed with the
     * client.
     */
    @Override
    void close();
}
