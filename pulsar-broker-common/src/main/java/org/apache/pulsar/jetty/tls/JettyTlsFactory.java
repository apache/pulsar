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
package org.apache.pulsar.jetty.tls;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.tls.impl.TlsContexts;
import org.apache.pulsar.common.util.tls.JcaProviders;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * The framework's Jetty integration for the PIP-478 TLS SPI. Both {@link SslContextFactory.Server} and
 * {@link SslContextFactory.Client} are well-known SPI classes: the framework first asks the
 * {@link PulsarTlsFactory} to supply one natively (a custom factory may build and own it, reload
 * included); only when the factory returns {@code empty()} for the Jetty class does the framework
 * synthesize a <em>vanilla</em> (never subclassed) one from an {@code SSLContext} subscription. The
 * default {@code FileBasedTlsFactory} returns {@code empty()} for the Jetty classes, so the synthesized
 * path is the usual one.
 *
 * <p><b>Factory-supplied (native) instances.</b> When the factory supplies the Jetty factory directly it
 * is handed back <em>unstarted</em> (the connector / {@code HttpClient} starts it), is the factory's
 * same-instance-per-purpose, and the factory owns its reload on material rotation. The framework only
 * holds the returned {@link TlsHandle} for disposal and never drives {@code setSslContext}/{@code reload}
 * or overlays consumer configuration on such an instance.
 *
 * <p>This deliberately abandons the unsound {@code getSslContext()} override of the superseded PIP-337
 * {@code JettySslContextFactory}. Instead it uses
 * Jetty's documented hot-reload API — the same one {@code KeyStoreScanner} uses: an {@code SSLContext}
 * subscription drives {@link SslContextFactory#setSslContext(SSLContext)} before start, and on each
 * later delivery (once started) {@link SslContextFactory#reload(Consumer)} atomically swaps the context and re-selects
 * protocols/ciphers. Existing connections keep their sessions; new connections use the new context.
 *
 * <p><b>{@code SSLParameters} companion (PIP-478).</b> Because these are synthesized paths (the factory
 * returned {@code empty()} for the Jetty class and supplies only the {@code SSLContext}), the framework also
 * asks the factory for its optional {@code javax.net.ssl.SSLParameters} companion — the engine-level baseline
 * a bare {@code SSLContext} cannot express — and maps its non-null members onto the Jetty setters: enabled
 * protocols ({@link SslContextFactory#setIncludeProtocols}) and cipher suites
 * ({@link SslContextFactory#setIncludeCipherSuites}), and (server side, merge rule 4) the authoritative
 * client-auth mode ({@link SslContextFactory.Server#setNeedClientAuth} /
 * {@link SslContextFactory.Server#setWantClientAuth}). The initial companion is overlaid before start (at build
 * time); on each subsequent <em>rotation</em> delivery it is re-requested <em>asynchronously</em> — the reload
 * callback composes {@code createInstance(purpose, SSLParameters.class)} with {@code whenComplete} and only then
 * calls {@code reload(...)}, so engine policy rotates with material (pip-478.md:736) yet the companion is
 * <b>never joined on the delivery thread</b>. Joining there would self-deadlock a custom factory that dispatches
 * companion creation to the same single-thread scheduler that runs the poll delivery — the exact hazard
 * {@code TlsContextAcquisition.SynthesizingSubscription} avoids by composing the companion asynchronously.
 *
 * <p>Because those asynchronous re-requests complete on arbitrary threads and may finish out of order, the
 * synthesized reload mirrors the two ordering guarantees of {@code SynthesizingSubscription} (see
 * {@code JettyReloadCoordinator}): each delivery captures a strictly increasing <em>generation</em> and applies
 * its {@code reload(...)} only while that generation is still the latest, so a companion superseded by a newer
 * rotation is dropped rather than pinning the listener to a stale context/baseline; and the last
 * successfully-resolved companion is <em>retained</em> (seeded from the build-time baseline) so a transient
 * companion-fetch failure during rotation reloads with that last-good baseline rather than downgrading engine
 * policy (protocols/ciphers and, server side, client-auth) to consumer defaults. Consumer defaults are still
 * re-applied <em>in full</em> first inside every {@code reload(...)} lambda — protocols, cipher suites and, server
 * side, client-auth are each re-pinned or cleared unconditionally, since the long-lived {@code SslContextFactory}
 * persists setter state across {@code reload()} — so any companion member legitimately dropped by a newer delivery
 * reverts to the consumer default rather than staying stuck from the previous companion.
 * {@code empty()} leaves the consumer's configuration in force, exactly as before.
 */
@CustomLog
public final class JettyTlsFactory {

    static {
        // Install Conscrypt process-wide, if present, before any TLS object is built here. Jetty resolves a
        // JSSE provider by name, so the provider has to be registered by the time a factory is configured.
        JcaProviders.ensureConscryptRegistered();
    }

    private JettyTlsFactory() {
    }

    /**
     * A Jetty server factory (factory-supplied native, or framework-synthesized and self-reloading)
     * together with the handle backing it; dispose the {@link #subscription()} when the web service stops.
     *
     * @param sslContextFactory the (unstarted) Jetty server factory
     * @param subscription      the backing handle — the {@code SSLContext} reload subscription on the
     *                          synthesized path, or the native-instance handle when the factory supplied it
     */
    public record ReloadableServerTls(SslContextFactory.Server sslContextFactory,
                                      TlsHandle<?> subscription) {
    }

    /**
     * A Jetty client factory (factory-supplied native, or framework-synthesized and self-reloading)
     * together with the handle backing it; dispose the {@link #subscription()} when the owning HTTP client
     * / servlet is destroyed.
     *
     * @param sslContextFactory the (unstarted) Jetty client factory
     * @param subscription      the backing handle — the {@code SSLContext} reload subscription on the
     *                          synthesized path, or the native-instance handle when the factory supplied it
     */
    public record ReloadableClientTls(SslContextFactory.Client sslContextFactory,
                                      TlsHandle<?> subscription) {
    }

    /**
     * Build a self-reloading {@link SslContextFactory.Server} for a purpose, handed back <em>unstarted</em>
     * (Jetty starts it with the connector lifecycle) with its initial {@code SSLContext} already set.
     *
     * <p><b>Must not be called from the factory's blocking executor.</b> This builder blocks on the
     * factory's acquisition, and the default factory dispatches acquisitions to
     * {@code TlsFactoryInitContext.blockingExecutor()} — so calling it from that executor waits on work
     * queued behind itself, which deadlocks when the executor is single-threaded. Call it from the owning
     * component's startup thread.
     *
     * @param factory                  the TLS factory to subscribe to
     * @param purpose                  the server purpose (e.g. {@link TlsPurpose#WEB})
     * @param sslProviderString        the legacy engine/JSSE provider string passed to Jetty's
     *                                 {@code SslContextFactory}, or {@code null}/empty for the default. Jetty is
     *                                 handed a pre-built {@code SSLContext}, so it never loads key material
     *                                 itself and takes no PIP-478 {@code jcaProvider}.
     * @param requireTrustedClientCert whether to require (vs. request) a trusted client certificate
     * @param allowInsecureConnection  whether an untrusted client cert is accepted under optional client auth
     * @param ciphers                  enabled cipher suites, or {@code null} for defaults
     * @param protocols                enabled protocols, or {@code null} for defaults
     * @return the reloading factory and its subscription handle
     */
    public static ReloadableServerTls createReloadingServerFactory(PulsarTlsFactory factory, TlsPurpose purpose,
                                                                   Executor reloadExecutor,
                                                                   String sslProviderString,
                                                                   boolean requireTrustedClientCert,
                                                                   boolean allowInsecureConnection,
                                                                   Set<String> ciphers, Set<String> protocols) {
        // Ask the factory first: a custom factory may natively supply the Jetty server factory (a
        // well-known class). When it does, hand it back unstarted with the factory owning its reload and
        // configuration; the framework overlays no consumer config on it. The default file-based factory
        // returns empty() here, so the synthesized, self-reloading fallback below runs (the usual path).
        Optional<TlsHandle<SslContextFactory.Server>> nativeFactory =
                acquireNativeJettyFactory(factory, purpose, SslContextFactory.Server.class);
        if (nativeFactory.isPresent()) {
            TlsHandle<SslContextFactory.Server> handle = nativeFactory.get();
            return new ReloadableServerTls(handle.get(), handle);
        }

        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        // Consumer config plus the factory's engine-policy companion (if any), before start. The resolved
        // companion seeds the coordinator's last-good baseline so a companion-fetch failure on the very first
        // rotation retains it rather than downgrading to consumer defaults.
        SSLParameters initialBaseline = configureServerBaseline(sslContextFactory, factory, purpose,
                sslProviderString, requireTrustedClientCert, allowInsecureConnection, ciphers, protocols);

        // Each rotation re-requests the companion asynchronously (off the delivery thread) and applies it under
        // a generation guard so an out-of-order companion cannot pin a stale context, retaining the last-good
        // baseline across a transient companion failure. See JettyReloadCoordinator.
        JettyReloadCoordinator coordinator = new JettyReloadCoordinator(factory, purpose, reloadExecutor,
                sslContextFactory, initialBaseline, (newContext, baseline) -> {
                    sslContextFactory.setSslContext(newContext);
                    // Re-apply the consumer defaults first so a companion member dropped by this delivery
                    // (notably client-auth) reverts to the consumer default rather than staying stuck from the
                    // previous companion, then overlay the generation-guarded, last-good companion.
                    applyServerConfig(sslContextFactory, sslProviderString, requireTrustedClientCert,
                            allowInsecureConnection, ciphers, protocols);
                    if (baseline != null) {
                        applyServerBaseline(sslContextFactory, baseline);
                    }
                });

        TlsHandle<SSLContext> subscription =
                awaitAcquisition(factory.createInstance(purpose, SSLContext.class, coordinator::onDelivery), purpose)
                        .orElseThrow(() -> new IllegalStateException(
                                "TLS factory supplied no SSLContext for purpose " + purpose));
        return new ReloadableServerTls(sslContextFactory, coordinator.bind(subscription));
    }

    /**
     * Build an {@link SslContextFactory.Client} for a purpose (used by the proxy's {@code AdminProxyHandler},
     * whose Jetty {@code HttpClient} outlives broker-client material rotation), handed back
     * <em>unstarted</em>. Mirroring {@link #createReloadingServerFactory}, the framework first asks the
     * factory to supply the Jetty client factory natively; a custom factory that does so owns its reload and
     * endpoint identification, and the framework overlays nothing on it. Otherwise a vanilla, self-reloading
     * one is synthesized: an {@code SSLContext} subscription drives
     * {@link SslContextFactory#setSslContext(SSLContext)} before start, and on each later delivery (once
     * started) {@link SslContextFactory#reload(Consumer)} atomically swaps the context so new connections use
     * the rotated material. Dispose the returned {@link ReloadableClientTls#subscription()} when the owning
     * client is destroyed.
     *
     * <p><b>Must not be called from the factory's blocking executor.</b> This builder blocks on the
     * factory's acquisition, and the default factory dispatches acquisitions to
     * {@code TlsFactoryInitContext.blockingExecutor()} — so calling it from that executor waits on work
     * queued behind itself, which deadlocks when the executor is single-threaded. Call it from the owning
     * component's startup thread.
     *
     * @param factory                   the TLS factory to acquire from / subscribe to
     * @param purpose                   the client purpose (e.g. {@link TlsPurpose#BROKER_CLIENT})
     * @param sslProviderString         the legacy engine/JSSE provider string passed to Jetty's
     *                                 {@code SslContextFactory}, or {@code null}/empty for the default. Jetty is
     *                                 handed a pre-built {@code SSLContext}, so it never loads key material
     *                                 itself and takes no PIP-478 {@code jcaProvider}.
     * @param enableHostnameVerification whether to verify the peer hostname; when {@code false} the
     *                                  synthesized client's endpoint identification is disabled (a
     *                                  factory-supplied native client owns this itself and is left untouched)
     * @return the client factory and the handle backing it
     */
    public static ReloadableClientTls createReloadingClientFactory(PulsarTlsFactory factory, TlsPurpose purpose,
                                                                   Executor reloadExecutor,
                                                                   String sslProviderString,
                                                                   boolean enableHostnameVerification) {
        // Ask the factory first: a custom factory may natively supply the Jetty client factory (a well-known
        // class, mirroring the Server variant) to customize proxy->broker admin TLS. When it does, hand it
        // back unstarted with the factory owning its reload and its own endpoint identification; the
        // framework overlays no consumer config on it. The default file-based factory returns empty() here,
        // so the SSLContext-synthesized, self-reloading fallback below runs (the usual path, including
        // rotation of the long-lived admin HttpClient's broker-client material).
        Optional<TlsHandle<SslContextFactory.Client>> nativeFactory =
                acquireNativeJettyFactory(factory, purpose, SslContextFactory.Client.class);
        if (nativeFactory.isPresent()) {
            TlsHandle<SslContextFactory.Client> handle = nativeFactory.get();
            return new ReloadableClientTls(handle.get(), handle);
        }

        SslContextFactory.Client client = new SslContextFactory.Client();
        if (StringUtils.isNotBlank(sslProviderString)) {
            client.setProvider(sslProviderString);
        }
        // Pin the {TLSv1.3, TLSv1.2} floor and overlay the factory's engine-policy companion (if any), before
        // start; the resolved companion seeds the coordinator's last-good baseline (see the Server variant).
        SSLParameters initialBaseline = configureClientBaseline(client, factory, purpose);
        // Hostname verification is a consumer (proxy) concern on the synthesized path: disable endpoint
        // identification when the consumer has it off (a native factory, handled above, owns this itself). This
        // is a per-consumer setting applied once at build, not part of the per-delivery companion overlay.
        if (!enableHostnameVerification) {
            client.setEndpointIdentificationAlgorithm(null);
        }

        // Rotations re-request the companion asynchronously (off the delivery thread) and apply it under a
        // generation guard, retaining the last-good baseline across a transient companion failure (client-auth
        // is a server concept; endpoint identification stays as set at build). See JettyReloadCoordinator.
        JettyReloadCoordinator coordinator = new JettyReloadCoordinator(factory, purpose, reloadExecutor, client,
                initialBaseline,
                (newContext, baseline) -> {
                    client.setSslContext(newContext);
                    // Re-apply the consumer defaults first (the {TLSv1.3, TLSv1.2} floor and an unrestricted cipher
                    // list), then overlay the generation-guarded, last-good companion so a companion member dropped
                    // by this delivery reverts to those defaults rather than staying stuck from the previous one.
                    applyClientConfig(client);
                    if (baseline != null) {
                        applyClientBaseline(client, baseline);
                    }
                });

        TlsHandle<SSLContext> subscription =
                awaitAcquisition(factory.createInstance(purpose, SSLContext.class, coordinator::onDelivery), purpose)
                        .orElseThrow(() -> new IllegalStateException(
                                "TLS factory supplied no SSLContext for purpose " + purpose));
        return new ReloadableClientTls(client, coordinator.bind(subscription));
    }

    /**
     * Apply the consumer config, then overlay the factory's engine-policy companion, onto a synthesized server
     * factory — at build time only (off any event loop; the companion is joined here). Each subsequent rotation
     * delivery re-requests the companion asynchronously and re-applies it inside the {@code reload(...)} lambda
     * (see {@link JettyReloadCoordinator}), never joined on the delivery thread.
     *
     * @return the resolved companion baseline, or {@code null} when the factory supplied none — used to seed the
     *         coordinator's retained last-good baseline.
     */
    private static SSLParameters configureServerBaseline(SslContextFactory.Server sslContextFactory,
                                                         PulsarTlsFactory factory, TlsPurpose purpose,
                                                         String sslProviderString, boolean requireTrustedClientCert,
                                                         boolean allowInsecureConnection, Set<String> ciphers,
                                                         Set<String> protocols) {
        applyServerConfig(sslContextFactory, sslProviderString, requireTrustedClientCert, allowInsecureConnection,
                ciphers, protocols);
        SSLParameters baseline = resolveBaselineParameters(factory, purpose).orElse(null);
        if (baseline != null) {
            applyServerBaseline(sslContextFactory, baseline);
        }
        return baseline;
    }

    /**
     * Pin the {@code {TLSv1.3, TLSv1.2}} floor, then overlay the factory companion's protocols/ciphers, onto a
     * synthesized client factory — at build time only (off any event loop; the companion is joined here). Each
     * subsequent rotation delivery re-requests the companion asynchronously and re-pins the floor + overlays it
     * inside the {@code reload(...)} lambda (see {@link JettyReloadCoordinator}), never joined on the delivery
     * thread. Client-auth is a server concept and is not mapped here; endpoint identification is a per-consumer
     * setting applied once at build.
     *
     * @return the resolved companion baseline, or {@code null} when the factory supplied none — used to seed the
     *         coordinator's retained last-good baseline.
     */
    private static SSLParameters configureClientBaseline(SslContextFactory.Client client, PulsarTlsFactory factory,
                                                         TlsPurpose purpose) {
        applyClientConfig(client);
        SSLParameters baseline = resolveBaselineParameters(factory, purpose).orElse(null);
        if (baseline != null) {
            applyClientBaseline(client, baseline);
        }
        return baseline;
    }

    /**
     * The synthesized client's consumer defaults, shared by the build-time overlay and every reload lambda so both
     * agree on what a dropped companion member reverts to: the {@code {TLSv1.3, TLSv1.2}} floor, and an unrestricted
     * cipher list (an empty include list means "no restriction" in Jetty). Both are cleared/re-pinned unconditionally
     * because the factory is long-lived and persists setter state across {@code reload()}. The provider and endpoint
     * identification are per-consumer settings applied once at build, not part of this per-delivery reset.
     */
    private static void applyClientConfig(SslContextFactory.Client client) {
        client.setIncludeProtocols(TlsContexts.DEFAULT_ENABLED_PROTOCOLS.toArray(new String[0]));
        client.setIncludeCipherSuites();
    }

    private static void applyServerConfig(SslContextFactory.Server sslContextFactory, String sslProviderString,
                                          boolean requireTrustedClientCertOnConnect, boolean allowInsecureConnection,
                                          Set<String> ciphers, Set<String> protocols) {
        if (ciphers != null && !ciphers.isEmpty()) {
            sslContextFactory.setIncludeCipherSuites(ciphers.toArray(new String[0]));
        } else {
            // Clear the include list explicitly (not just rely on the fresh-factory default), for the same reason
            // needClientAuth is reset below: this method is re-applied on each reload of a long-lived factory, so a
            // prior companion delivery that pinned cipher suites must revert to the consumer default before a later
            // companion (or its absence) is overlaid. An empty include list means "no restriction" in Jetty --
            // SslContextFactory.processIncludeExcludePatterns falls back to the SSLContext's enabled suites.
            sslContextFactory.setIncludeCipherSuites();
        }
        // Pin the enabled protocols even when unconfigured, matching the {TLSv1.3, TLSv1.2} floor the native
        // Netty path applies rather than deferring to the provider default. A factory-supplied companion,
        // applied afterwards by applyServerBaseline, still overrides this.
        if (protocols != null && !protocols.isEmpty()) {
            sslContextFactory.setIncludeProtocols(protocols.toArray(new String[0]));
        } else {
            sslContextFactory.setIncludeProtocols(TlsContexts.DEFAULT_ENABLED_PROTOCOLS.toArray(new String[0]));
        }
        if (StringUtils.isNotBlank(sslProviderString)) {
            sslContextFactory.setProvider(sslProviderString);
        }
        if (requireTrustedClientCertOnConnect) {
            sslContextFactory.setNeedClientAuth(true);
            sslContextFactory.setTrustAll(false);
        } else {
            // PIP-478: optional client auth requests but does not require a client cert. An untrusted
            // client cert is accepted at the handshake only when tlsAllowInsecureConnection=true, aligning the
            // web listener with the Netty binary listener's semantics and diverging from the pre-5.0 Jetty
            // path, which trusted any presented client cert whenever client auth was optional (see PIP-478
            // Security Considerations).
            //
            // The actual enforcement is the trust managers baked into the SSLContext the framework hands to
            // Jetty via setSslContext (built from the WEB TlsPolicy's allowInsecureConnection: CA-validating
            // when secure, insecure-trust-all when insecure). Jetty's own setTrustAll only takes effect when
            // Jetty builds the SSLContext itself, so it is inert on this setSslContext path; we still scope it
            // to the insecure flag so the two never disagree (defence in depth) rather than leaving the
            // inherited unconditional setTrustAll(true).
            // Clear needClientAuth explicitly (not just rely on the fresh-factory default): this method is
            // re-applied on each reload, so a prior companion delivery that set needClientAuth must be reset to
            // the consumer default here before a later companion (or its absence) is overlaid.
            sslContextFactory.setNeedClientAuth(false);
            sslContextFactory.setWantClientAuth(true);
            sslContextFactory.setTrustAll(allowInsecureConnection);
        }
        // https://jetty.org/docs/jetty/12.1/operations-guide/protocols/index.html#ssl-sni
        // Set to false for backwards compatibility with Jetty 9.x
        sslContextFactory.setSniRequired(false);
    }

    /**
     * Ask the factory to supply a native Jetty {@code jettyClass} for {@code purpose} (one-shot). Jetty's
     * {@link SslContextFactory.Server} and {@link SslContextFactory.Client} are well-known SPI classes, so a
     * custom factory MAY build one directly; the default {@code FileBasedTlsFactory} returns
     * {@link Optional#empty()}, in which case the caller synthesizes the factory from an {@code SSLContext}
     * subscription. A supplied instance is the factory's own (unstarted, same-instance-per-purpose,
     * factory-driven reload) — the caller returns it verbatim and keeps the {@link TlsHandle} for disposal.
     * Resolved at factory-build time (off any event loop), mirroring {@link #resolveBaselineParameters}.
     */
    private static <T extends SslContextFactory> Optional<TlsHandle<T>> acquireNativeJettyFactory(
            PulsarTlsFactory factory, TlsPurpose purpose, Class<T> jettyClass) {
        return awaitAcquisition(factory.createInstance(purpose, jettyClass), purpose);
    }

    /**
     * Request the factory's optional {@code SSLParameters} companion for a purpose (one-shot) at factory-build
     * time, joining off any event loop, and return the supplied instance or {@link Optional#empty()} when the
     * factory supplies none. Rotation deliveries do NOT use this: they re-request the companion asynchronously
     * inside {@link JettyReloadCoordinator} (never joined on the delivery thread) and unwrap it via
     * {@link #extractBaseline}.
     */
    private static Optional<SSLParameters> resolveBaselineParameters(PulsarTlsFactory factory, TlsPurpose purpose) {
        return Optional.ofNullable(
                extractBaseline(awaitAcquisition(factory.createInstance(purpose, SSLParameters.class), purpose)));
    }

    /**
     * Unwrap the factory's {@code SSLParameters} companion from its handle, disposing the handle afterwards (a
     * companion carries no reference-counted state). Returns {@code null} when the factory supplied none
     * ({@link Optional#empty()}). Used both by the build-time {@link #resolveBaselineParameters} and by the
     * asynchronous per-rotation re-request in {@link JettyReloadCoordinator}.
     */
    private static SSLParameters extractBaseline(Optional<TlsHandle<SSLParameters>> handle) {
        if (handle.isEmpty()) {
            return null;
        }
        TlsHandle<SSLParameters> paramsHandle = handle.get();
        try {
            return paramsHandle.get();
        } finally {
            paramsHandle.dispose();
        }
    }

    /**
     * Await a factory acquisition made during a synchronous builder, unwrapping {@link CompletionException}
     * so a configuration failure surfaces as its own cause. The underlying messages ("No TLS material
     * configured for server purpose WEB") are the actionable ones and should not arrive wrapped.
     *
     * @param pending the acquisition
     * @param purpose the purpose being acquired, for the failure message
     * @return the acquisition result
     */
    private static <T> T awaitAcquisition(CompletableFuture<T> pending, TlsPurpose purpose) {
        try {
            return pending.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("Failed to acquire TLS material for purpose " + purpose, cause);
        }
    }

    /**
     * Overlay a factory-supplied engine baseline onto a synthesized server factory (PIP-478 merge order):
     * enabled protocols/cipher suites when the companion sets them, and the companion's client-auth mode as
     * authoritative (rule 4) — {@code needClientAuth} wins over {@code wantClientAuth}, neither means none.
     */
    private static void applyServerBaseline(SslContextFactory.Server sslContextFactory, SSLParameters baseline) {
        if (baseline.getProtocols() != null) {
            sslContextFactory.setIncludeProtocols(baseline.getProtocols());
        }
        if (baseline.getCipherSuites() != null) {
            sslContextFactory.setIncludeCipherSuites(baseline.getCipherSuites());
        }
        if (baseline.getNeedClientAuth()) {
            sslContextFactory.setNeedClientAuth(true);
        } else if (baseline.getWantClientAuth()) {
            sslContextFactory.setNeedClientAuth(false);
            sslContextFactory.setWantClientAuth(true);
        } else {
            sslContextFactory.setNeedClientAuth(false);
            sslContextFactory.setWantClientAuth(false);
        }
    }

    /**
     * Overlay a factory-supplied engine baseline onto a synthesized client factory: enabled protocols/cipher
     * suites only (client-auth is a server concept; SNI/hostname verification remain per-connection).
     */
    private static void applyClientBaseline(SslContextFactory.Client client, SSLParameters baseline) {
        if (baseline.getProtocols() != null) {
            client.setIncludeProtocols(baseline.getProtocols());
        }
        if (baseline.getCipherSuites() != null) {
            client.setIncludeCipherSuites(baseline.getCipherSuites());
        }
    }

    /**
     * Per-subscription reload coordinator for the synthesized Jetty paths. It ports the two ordering guarantees
     * of {@code TlsContextAcquisition.SynthesizingSubscription} to Jetty's hot-reload API, which the naive
     * "compose {@code createInstance(SSLParameters)} then {@code reload}" delivery lacked:
     *
     * <ul>
     *   <li><b>Generation guard.</b> Each delivery captures a strictly increasing generation (deliveries are
     *       serial per subscription, per the SPI contract). A rotation's companion re-request is composed
     *       asynchronously and may complete on an arbitrary thread and out of order, so its {@code reload(...)}
     *       is applied — under a lock — only while its generation is still the latest; a companion superseded
     *       by a newer rotation is dropped rather than pinning the listener to a stale context/baseline.</li>
     *   <li><b>Last-good baseline retention.</b> The last successfully-resolved companion is retained (seeded
     *       from the build-time baseline). A transient companion-fetch failure during rotation reloads with that
     *       last-good baseline rather than consumer defaults, so it cannot silently drop protocols/ciphers or
     *       (server side) client-auth. The retained baseline is updated only for a successfully-resolved,
     *       non-superseded companion, under the same lock.</li>
     * </ul>
     *
     * <p>The companion is never joined on the delivery thread (the self-deadlock hazard described on the class
     * javadoc); only the <em>first</em> delivery — which fires synchronously inside the subscribe call, before the
     * built factory is even returned — sets the already-overlaid context directly, mirroring
     * {@code SynthesizingSubscription}'s pre-fetched first delivery. Every later delivery is a rotation and
     * re-requests the companion, <em>including one that arrives before the connector/client starts</em>; whether
     * the factory is started only decides how the result is applied (Jetty's {@code reload(...)} once started, the
     * same setters directly before), so a factory started after such a rotation starts with that rotation's
     * material AND its companion policy rather than the stale build-time baseline.
     */
    private static final class JettyReloadCoordinator {

        private final PulsarTlsFactory factory;
        private final TlsPurpose purpose;
        // Where publish() runs. The companion request is composed, never joined — but for the DEFAULT
        // factory it completes synchronously (SSLParameters is an unsupported class, so createInstance
        // returns an already-completed empty future), which would run the whole Jetty reload inline on the
        // delivery thread, i.e. while the factory holds its source monitor. Dispatching keeps the source
        // monitor out of the Jetty lock chain and honours the delivery contract's "cheap non-blocking store".
        private final Executor reloadExecutor;
        private final SslContextFactory target;
        // Applies the rotated context + (nullable) companion baseline inside Jetty's reload lambda.
        private final BiConsumer<SSLContext, SSLParameters> applyReload;
        // Last successfully-resolved companion, seeded from the pre-fetched build-time baseline. Retained across
        // a transient companion-fetch failure so a rotation cannot silently downgrade engine policy.
        private volatile SSLParameters lastBaseline;
        // One-shot: only the first (synchronous, pre-fetched) delivery reuses the build-time baseline; every
        // later delivery is a rotation and re-requests the companion, started or not.
        private final AtomicBoolean firstDelivery = new AtomicBoolean(true);
        private final AtomicLong deliveryGeneration = new AtomicLong();
        private long lastPublishedGeneration;
        // A rotation's companion request is composed asynchronously, so its completion can land after the
        // owner disposed the subscription (and, on the server path, after Jetty stopped the connector).
        // Reloading a stopped/abandoned factory then would be a pointless mutation at best.
        private final AtomicBoolean disposed = new AtomicBoolean();

        JettyReloadCoordinator(PulsarTlsFactory factory, TlsPurpose purpose, Executor reloadExecutor,
                               SslContextFactory target, SSLParameters initialBaseline,
                               BiConsumer<SSLContext, SSLParameters> applyReload) {
            this.factory = factory;
            this.purpose = purpose;
            this.reloadExecutor = reloadExecutor == null ? Runnable::run : reloadExecutor;
            this.target = target;
            this.lastBaseline = initialBaseline;
            this.applyReload = applyReload;
        }

        /**
         * Wrap the backing subscription so disposing it also stops this coordinator. Synchronized on the same
         * monitor as {@link #publish}, so dispose returns only once any in-flight publish has finished —
         * setting the flag alone would still let a publisher that had already passed the check reload a
         * factory its owner has torn down.
         */
        TlsHandle<SSLContext> bind(TlsHandle<SSLContext> subscription) {
            return new TlsHandle<>() {
                @Override
                public SSLContext get() {
                    return subscription.get();
                }

                @Override
                public void dispose() {
                    synchronized (JettyReloadCoordinator.this) {
                        disposed.set(true);
                    }
                    subscription.dispose();
                }
            };
        }

        // Serial per subscription (SPI contract), so each delivery gets a strictly increasing generation.
        void onDelivery(SSLContext newContext) {
            long generation = deliveryGeneration.incrementAndGet();
            if (firstDelivery.compareAndSet(true, false)) {
                // The first delivery fires synchronously inside the subscribe call, before the built factory is
                // returned (let alone started): the build-time companion was already overlaid and seeds
                // lastBaseline, so only the context is missing — set it directly, mirroring
                // SynthesizingSubscription's pre-fetched first delivery.
                target.setSslContext(newContext);
                return;
            }
            // Rotation delivery. Re-request the companion so engine policy rotates WITH the material
            // (pip-478.md:736), resolved ASYNCHRONOUSLY off this delivery thread and never joined here (the
            // self-deadlock hazard). This runs for rotations delivered BEFORE start too — those must refresh the
            // companion as well, otherwise the connector would start with the rotated context but the stale
            // build-time baseline; only the mechanism used to apply the result differs (see publish). The
            // generation guard in publish drops a superseded companion; the last-good baseline is maintained
            // there, never here, so a superseded companion completing late cannot regress it.
            factory.createInstance(purpose, SSLParameters.class).whenCompleteAsync((companion, err) ->
                    publish(newContext, err != null ? null : extractBaseline(companion), generation, err == null),
                    reloadExecutor);
        }

        // Apply the rotation for `generation`, dropping a result already superseded by a newer delivery.
        // Synchronized because rotation companions may complete on arbitrary threads and out of order; the guard
        // keeps the applied context, the reload callback, AND the retained last-good baseline monotonic in
        // delivery order.
        private synchronized void publish(SSLContext newContext, SSLParameters baseline, long generation,
                                          boolean baselineResolved) {
            if (generation < lastPublishedGeneration || disposed.get()) {
                return;
            }
            lastPublishedGeneration = generation;
            if (baselineResolved) {
                lastBaseline = baseline;
            } else {
                // A transient companion-fetch failure must not downgrade engine policy: reload with the last-good
                // baseline, read under the same guard so a stale companion can never have regressed it.
                baseline = lastBaseline;
            }
            final SSLParameters effectiveBaseline = baseline;
            try {
                // Always go through reload(), started or not. Checking isStarted() and then applying directly
                // was a TOCTOU: a connector starting in that window takes its load()-time snapshot before the
                // direct setters land, so the rotated context is silently not served — no exception, the
                // delivery counted a success, and the listener stays on pre-rotation material until the NEXT
                // material change, which at a 90-day renewal cadence can outlive the certificate.
                // reload() takes the same internal lock as Jetty's doStart()/load(), so the check and the
                // apply stop being separable; on a not-yet-started factory it applies the configuration and
                // start() loads from it afterwards.
                target.reload(f -> applyReload.accept(newContext, effectiveBaseline));
            } catch (Exception e) {
                log.warn().attr("purpose", purpose).exception(e)
                        .log("Failed to reload Jetty SslContextFactory; keeping the running context");
            }
        }
    }
}
