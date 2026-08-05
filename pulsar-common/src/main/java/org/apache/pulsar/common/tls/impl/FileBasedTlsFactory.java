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
package org.apache.pulsar.common.tls.impl;

import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCountUtil;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsEndpoint;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * The default, file-based {@link PulsarTlsFactory} (PIP-478).
 *
 * <p>Immutable after construction: the complete {@code TlsPurpose -> TlsPolicy} map and the
 * factory-wide {@link FileBasedTlsFactorySettings} are fixed by the constructor and never mutated. The
 * owning component (the v5 client builder, or {@code DefaultBrokerTlsFactory} on the server side)
 * composes the final map before constructing the factory.
 *
 * <p>Natively supplies the Netty {@code SslContext} (on the configured engine — JDK or OpenSSL) and the
 * JDK {@code SSLContext}; returns {@code empty()} for every other class (notably Jetty's
 * {@code SslContextFactory.Server} and {@code SslContextFactory.Client}), which the framework synthesizes
 * from the JDK {@code SSLContext}. It also
 * returns {@code empty()} for the {@code javax.net.ssl.SSLParameters} companion (PIP-478): its engine policy
 * (protocols, ciphers, client-auth mode, hostname verification) is baked natively into the Netty contexts it
 * builds, so it exposes no separate baseline for the framework to overlay on those.
 *
 * <p>A JDK {@code SSLContext} cannot itself carry that policy — enabled protocols and cipher suites,
 * algorithm constraints and the endpoint-identification algorithm are all per-engine {@code SSLParameters}
 * settings — so the JDK context this factory returns is a <em>material carrier</em>: it holds the key and
 * trust managers (built through the pinned providers) and nothing else. Every consumer of it applies the
 * engine policy itself from the same {@link TlsPolicy}, which is what {@code JettyTlsFactory} does. A
 * consumer that instead created engines straight from this {@code SSLContext} would get the provider defaults,
 * so consumers must not do that.
 *
 * <p><b>Purpose resolution.</b> A request resolves the requested purpose directly against the configured
 * {@code TlsPurpose -> TlsPolicy} map. When nothing is configured for the purpose, a {@code CLIENT}-role
 * purpose resolves to the system default (OS trust store, no client certificate) and a {@code SERVER}-role
 * purpose fails the request. A resolved-but-unbuildable request completes the future <em>exceptionally</em>
 * — never {@code empty()}, which strictly means "unsupported {@code (purpose, class)} combination".
 *
 * <p><b>Rotation.</b> One {@link TlsMaterialSource} is shared per configured purpose. Server-side
 * subscribers are pushed rebuilt instances by a background poll (interval from the settings, on the
 * framework scheduler); client-side one-shot callers re-stat on each request and pick up rotation
 * naturally. Because a re-stat consumes the source's change signal, an acquisition that observes a change
 * first also fans it out to that purpose's subscribers, so a rotation is never swallowed by whoever happened
 * to look first. A failed rebuild keeps the last-good instance, logs at WARN, and retries on the next
 * change; a subscriber callback that throws is caught and logged, and the subscription stays live.
 */
@CustomLog
public class FileBasedTlsFactory implements PulsarTlsFactory {

    private final Map<TlsPurpose, RegisteredSource> registry;
    private final FileBasedTlsFactorySettings settings;

    private volatile TlsFactoryInitContext initContext;
    private volatile TlsReloadMetrics metrics;
    private volatile RegisteredSource systemDefaultSource;
    private volatile ScheduledFuture<?> pollFuture;
    private volatile boolean closed;
    // Stands in for scheduleWithFixedDelay's no-overlap guarantee once the poll body is dispatched.
    private final AtomicBoolean pollInFlight = new AtomicBoolean();

    /**
     * Construct an immutable file-based factory.
     *
     * @param policies the complete purpose&rarr;policy map (defensively copied)
     * @param settings the factory-wide engine/refresh settings
     */
    public FileBasedTlsFactory(Map<TlsPurpose, TlsPolicy> policies, FileBasedTlsFactorySettings settings) {
        this(policies, settings, Map.of());
    }

    /**
     * Construct an immutable file-based factory that additionally folds an authentication plugin's TLS
     * material over the configured file policy for the named purposes (the server-side {@code BROKER_CLIENT}
     * fold — PIP-478). For a purpose present in {@code authMaterialSuppliers}, the supplier's
     * {@code Authentication} material overrides the file policy's key/cert per
     * {@link AuthProvidedMaterialSource} (auth-cert-wins); other purposes use the file policy unchanged.
     *
     * @param policies              the complete purpose&rarr;policy map (defensively copied)
     * @param settings              the factory-wide engine/refresh settings
     * @param authMaterialSuppliers per-purpose broker-client authentication material suppliers (may be empty)
     */
    public FileBasedTlsFactory(Map<TlsPurpose, TlsPolicy> policies, FileBasedTlsFactorySettings settings,
            Map<TlsPurpose, Supplier<AuthenticationDataProvider>> authMaterialSuppliers) {
        Objects.requireNonNull(policies, "policies must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        Objects.requireNonNull(authMaterialSuppliers, "authMaterialSuppliers must not be null");
        Map<TlsPurpose, RegisteredSource> built = new LinkedHashMap<>();
        for (Map.Entry<TlsPurpose, TlsPolicy> entry : policies.entrySet()) {
            TlsPurpose purpose = Objects.requireNonNull(entry.getKey(), "purpose key must not be null");
            TlsPolicy policy = Objects.requireNonNull(entry.getValue(), "policy must not be null");
            TlsMaterialSource fileSource = new TlsMaterialSource(policy);
            Supplier<AuthenticationDataProvider> authSupplier = authMaterialSuppliers.get(purpose);
            MaterialSource source = authSupplier == null
                    ? fileSource
                    : new AuthProvidedMaterialSource(fileSource, authSupplier);
            built.put(purpose, new RegisteredSource(purpose, policy, source));
        }
        this.registry = Map.copyOf(built);
    }

    /**
     * Adapt a component's broker-client {@link Authentication} to a per-refresh
     * {@link AuthenticationDataProvider} supplier for use with the {@code BROKER_CLIENT} fold constructor.
     * The supplier re-reads {@code getAuthData()} on each poll so credential rotation is observed; a checked
     * {@link PulsarClientException} is rethrown unchecked and handled by the factory's keep-last-good poll.
     *
     * @param authentication the broker-client authentication plugin (never {@code null})
     * @return a supplier of the plugin's current authentication data
     */
    public static Supplier<AuthenticationDataProvider> authMaterialSupplier(Authentication authentication) {
        Objects.requireNonNull(authentication, "authentication must not be null");
        return () -> {
            try {
                return resolveAuthData(authentication);
            } catch (PulsarClientException e) {
                throw new RuntimeException("Failed to obtain broker-client authentication TLS material", e);
            }
        };
    }

    // The BROKER_CLIENT TLS fold is host-agnostic — TLS key material does not vary by peer — so the
    // host-less getAuthData() is exactly what we want; isolate its deprecation here.
    @SuppressWarnings("deprecation")
    private static AuthenticationDataProvider resolveAuthData(Authentication authentication)
            throws PulsarClientException {
        return authentication.getAuthData();
    }

    @Override
    public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
        try {
            Objects.requireNonNull(context, "context must not be null");
            // Required, not optional: every acquisition path in this factory reads files and parses key
            // material. Without an executor the work would run inline on the caller's thread, which the SPI
            // contract says may be a consumer event loop. Fail at wiring time rather than stalling a channel.
            Objects.requireNonNull(context.blockingExecutor(),
                    "TlsFactoryInitContext.blockingExecutor() must not be null: FileBasedTlsFactory performs "
                            + "blocking file and key-material loading and must never run it on a consumer thread");
            this.initContext = context;
            // Cancel any poll scheduled by a prior initialize() so a second call does not orphan the first:
            // the field is overwritten below and the old task would otherwise run forever.
            ScheduledFuture<?> previousPoll = this.pollFuture;
            if (previousPoll != null) {
                previousPoll.cancel(false);
            }
            // Same reasoning for the metrics: a prior initialize() registered an observable gauge that stays
            // registered on the meter until close(), so overwriting the field without closing it would leave
            // an orphaned callback reporting stale per-purpose timestamps forever.
            TlsReloadMetrics previousMetrics = this.metrics;
            if (previousMetrics != null) {
                previousMetrics.close();
            }
            this.metrics = TlsReloadMetrics.create(context.openTelemetry(), context.clock());
            int interval = settings.refreshIntervalSeconds();
            if (interval > 0 && context.scheduler() != null) {
                // The scheduler only TRIGGERS the poll; the work runs on the blocking executor. The poll
                // stats files, parses PEM/keystores and rebuilds contexts, which is exactly the work this
                // method refuses to run without a blocking executor a few lines above — running it on a
                // shared framework scheduler thread would occupy that thread for the duration, and with
                // material on slow or network-backed storage that is not bounded by anything.
                this.pollFuture = context.scheduler().scheduleWithFixedDelay(
                        this::triggerPoll, interval, interval, TimeUnit.SECONDS);
            }
            log.debug().attr("purposes", registry.keySet()).attr("refreshIntervalSeconds", interval)
                    .log("Initialized FileBasedTlsFactory");
            return CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose, Class<T> instanceClass) {
        return createOneShot(purpose, instanceClass);
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
            TlsPurpose purpose, TlsEndpoint endpoint, Class<T> instanceClass) {
        // The default file-based factory serves purpose-scoped material and ignores the endpoint hint.
        return createOneShot(purpose, instanceClass);
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
            TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
        if (onLoadOrReload == null) {
            // SPI contract: argument-validation failures complete the returned future exceptionally — this
            // method never throws on the calling thread.
            return CompletableFuture.failedFuture(new NullPointerException("onLoadOrReload must not be null"));
        }
        if (!isSupported(instanceClass)) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("FileBasedTlsFactory is closed"));
        }
        return runAsync(() -> {
            RegisteredSource source = resolve(purpose);
            rejectReentrantAcquisition(source, purpose);
            synchronized (source) {
                // FIX F: close() runs releaseAll() under this same source lock, and may have completed after
                // the outer closed-check but before this task acquired the lock. Re-check here so we never
                // build+cache+deliver a context (and register a subscription) whose factory-owned reference
                // would then never be released — that would leak past shutdown.
                if (closed) {
                    throw new IllegalStateException("FileBasedTlsFactory is closed");
                }
                T instance = loadInitialInstance(source, instanceClass);
                Subscription<T> subscription = new Subscription<>(instanceClass, onLoadOrReload);
                // Initial delivery happens-before the returned future completes (same thread, ordered).
                subscription.deliver(instance);
                source.subscriptions.add(subscription);
                return Optional.of((TlsHandle<T>) new SubscriptionHandle<>(source, subscription));
            }
        });
    }

    private <T> CompletableFuture<Optional<TlsHandle<T>>> createOneShot(TlsPurpose purpose, Class<T> instanceClass) {
        if (!isSupported(instanceClass)) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("FileBasedTlsFactory is closed"));
        }
        return runAsync(() -> {
            RegisteredSource source = resolve(purpose);
            rejectReentrantAcquisition(source, purpose);
            synchronized (source) {
                // FIX F: re-check under the source lock (see createInstance above) so a task racing a
                // concurrent close()/releaseAll() never builds+retains a context that then leaks past shutdown.
                if (closed) {
                    throw new IllegalStateException("FileBasedTlsFactory is closed");
                }
                T instance = loadInitialInstance(source, instanceClass);
                retain(instance);
                return Optional.of((TlsHandle<T>) new OneShotHandle<>(instance));
            }
        });
    }

    /**
     * Load (or reuse the cached) instance of {@code instanceClass} for the resolved source, recording a
     * {@code pulsar.tls.reload} attempt for the source's purpose. A resolution failure upstream (an
     * unconfigured server purpose) is a startup configuration error and is not counted here — only actual
     * material loads are. Must be called while holding the source monitor.
     *
     * <p>Success is counted only for a <em>real</em> (re)load, mirroring the poll path: the first build of a
     * context class for this source, or a refresh that found changed material. A repeat one-shot acquisition
     * that re-serves the memoized context (every new connection, on the acquire-per-connection paths) is not a
     * reload — counting it would inflate {@code pulsar.tls.reload} and keep advancing
     * {@code pulsar.tls.last_reload_success}, masking exactly the stalled rotation the Monitoring section
     * alerts on. Failures stay unconditional: an attempt that threw is always worth counting.
     */
    private <T> T loadInitialInstance(RegisteredSource source, Class<T> instanceClass) throws Exception {
        // Hoisted out of the try so the catch can tell whether this call already CONSUMED a rotation before
        // failing (see the comment on the catch below). Null means currentMaterial() itself threw, which
        // commits no baseline and so consumes nothing.
        MaterialSource.RefreshOutcome outcome = null;
        try {
            outcome = source.currentMaterial();
            // Sampled before acquireInstance, which is what populates the memo (both calls run under the
            // source monitor held by the caller, so no other acquisition can interleave).
            boolean firstBuild = source.cachedInstance(instanceClass) == null;
            T instance = source.acquireInstance(instanceClass, outcome.material(), settings);
            // Rotation fan-out: currentMaterial() CONSUMES the source's change signal (refresh() commits the new
            // mtime baseline), so whoever refreshes first is the only caller that ever sees changed=true. Without
            // this, a one-shot acquisition (or an initial subscribing acquisition) that observes a rotation first
            // leaves every later poll reporting changed=false and the subscribers on this same purpose wedged on
            // the pre-rotation instance forever — e.g. an https:// client whose FrameworkHttpClientFactory
            // subscribes to CLIENT_DEFAULT while another path acquires one-shot on it. Deliver the observed
            // change here, exactly as the poll would have; a delivery failure arms pendingRedeliver so the next
            // poll retries. No extra metric is recorded — this shares the single recordLoad below.
            // Also drive the retry when a previous rotation's fan-out failed: the poll is the usual driver,
            // but it is disabled entirely when refreshIntervalSeconds <= 0, and even when enabled an
            // acquisition may get here first. Without this, a subscriber wedged by a failed rebuild would
            // stay on the pre-rotation instance until the files changed again.
            boolean retryPending = source.pendingRedeliver();
            boolean delivered = true;
            if (outcome.changed() || retryPending) {
                delivered = source.deliverToSubscribers(outcome.material(), settings);
            }
            if (outcome.changed() || retryPending || firstBuild) {
                recordLoad(source.purpose, delivered);
            }
            return instance;
        } catch (Exception e) {
            recordLoad(source.purpose, false);
            // The material parsed (the baseline is committed) but building or delivering the context failed.
            // currentMaterial() has therefore already CONSUMED this rotation's change signal, so every later
            // poll would report changed=false and — without arming the redeliver flag here — return early,
            // leaving the subscribers wedged on the pre-rotation instance until the files change again.
            // deliverToSubscribers arms the flag itself on a partial delivery failure; this covers the case
            // where the failure happened before it was ever reached.
            if (outcome != null && outcome.changed()) {
                source.armRedeliver();
            }
            // Keep-last-good (PIP-478): a one-shot (or initial subscribing) acquisition during a
            // non-atomic rotation window — e.g. the cert file briefly unreadable while it is being replaced —
            // must not fail when a prior load already built a context of this class. Serve that last-good
            // instance and WARN; the reload-failure metric was recorded above. Fail only when there is no
            // last-good to fall back on (a genuine startup misconfiguration, per the fail-fast contract).
            T cached = source.cachedInstance(instanceClass);
            if (cached != null) {
                log.warn().attr("purpose", source.purpose).exception(e)
                        .log("Failed to load TLS material; serving the last-good instance");
                return cached;
            }
            throw e;
        }
    }

    private void recordLoad(TlsPurpose purpose, boolean success) {
        TlsReloadMetrics m = this.metrics;
        if (m != null) {
            m.recordLoad(purpose, success);
        }
    }

    @Override
    public void close() {
        closed = true;
        ScheduledFuture<?> poll = this.pollFuture;
        if (poll != null) {
            poll.cancel(false);
        }
        for (RegisteredSource source : registry.values()) {
            source.releaseAll();
        }
        RegisteredSource systemDefault = this.systemDefaultSource;
        if (systemDefault != null) {
            systemDefault.releaseAll();
        }
        TlsReloadMetrics m = this.metrics;
        if (m != null) {
            m.close();
        }
    }

    /**
     * Resolve a requested purpose to the {@link RegisteredSource} that owns its material, applying the
     * role's terminal-resolution rule when nothing is configured for the purpose.
     *
     * @throws TlsMaterialUnavailableException when a server-role purpose has no material configured
     */
    private RegisteredSource resolve(TlsPurpose purpose) {
        Objects.requireNonNull(purpose, "purpose must not be null");
        RegisteredSource source = registry.get(purpose);
        if (source != null) {
            return source;
        }
        if (purpose.role() == TlsPurpose.Role.CLIENT) {
            return systemDefaultSource();
        }
        throw new TlsMaterialUnavailableException(
                "No TLS material configured for server purpose " + purpose);
    }

    private RegisteredSource systemDefaultSource() {
        RegisteredSource existing = this.systemDefaultSource;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (this.systemDefaultSource == null) {
                // System default: verify hostnames, OS trust store, no client certificate. A constant
                // source (no files, never rotates).
                TlsPolicy defaultPolicy = TlsPolicy.builder().build();
                this.systemDefaultSource = new RegisteredSource(
                        TlsPurpose.CLIENT_DEFAULT, defaultPolicy, null);
            }
            return this.systemDefaultSource;
        }
    }

    /**
     * Hand the poll to the blocking executor, preserving {@code scheduleWithFixedDelay}'s no-overlap
     * property: dispatching breaks the scheduler's own guarantee that one run finishes before the next is
     * scheduled, so an in-flight flag stands in for it. A poll slower than the interval therefore skips
     * ticks rather than queueing them up behind itself.
     */
    private void triggerPoll() {
        if (closed || !pollInFlight.compareAndSet(false, true)) {
            return;
        }
        try {
            pollExecutor().execute(() -> {
                try {
                    pollSafely();
                } finally {
                    pollInFlight.set(false);
                }
            });
        } catch (Throwable t) {
            // Rejected (executor shutting down, queue full): clear the flag so a later tick can retry.
            pollInFlight.set(false);
            log.debug().exception(t).log("Could not dispatch the TLS material poll");
        }
    }

    private Executor pollExecutor() {
        TlsFactoryInitContext context = this.initContext;
        return context != null ? context.blockingExecutor() : Runnable::run;
    }

    private void pollSafely() {
        if (closed) {
            return;
        }
        try {
            for (RegisteredSource source : registry.values()) {
                source.poll(settings, metrics);
            }
        } catch (Throwable t) {
            log.warn().exception(t).log("Unexpected error during TLS material poll");
        }
    }

    private <R> CompletableFuture<R> runAsync(Callable<R> task) {
        CompletableFuture<R> future = new CompletableFuture<>();
        TlsFactoryInitContext context = this.initContext;
        // initialize() rejects a null blockingExecutor, so the caller-thread fallback is reachable only before
        // initialize() has run — a direct-construction path (tests, or a component acquiring during its own
        // startup) that is by definition not on a consumer event loop.
        Executor executor = context != null ? context.blockingExecutor() : Runnable::run;
        try {
            executor.execute(() -> {
                try {
                    R result = task.call();
                    if (!future.complete(result)) {
                        // The caller cancelled (or otherwise completed) the future while the acquisition was
                        // still running, so nothing will ever receive this handle. The task has already
                        // retained the context on its behalf, and with the finalizer-free OPENSSL_REFCNT
                        // engine an unbalanced retain is a permanent native leak — dispose the orphan here.
                        disposeOrphan(result);
                    }
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * Release a handle that was built but could not be delivered to its caller. Both acquisition forms
     * produce an {@code Optional<TlsHandle<?>>}, and disposing it is exactly what the caller would have done:
     * it drops the one-shot's retained reference, or unregisters the subscription and releases its borrows.
     */
    private static void disposeOrphan(Object result) {
        if (result instanceof Optional<?> optional && optional.orElse(null) instanceof TlsHandle<?> handle) {
            try {
                handle.dispose();
            } catch (Throwable t) {
                log.warn().exception(t).log("Failed to dispose an undelivered TLS handle");
            }
        }
    }

    private static boolean isSupported(Class<?> instanceClass) {
        return instanceClass == SslContext.class || instanceClass == SSLContext.class;
    }

    /**
     * Fail an acquisition that re-enters the factory from inside a reload callback for the same purpose.
     *
     * <p>Consumer callbacks run while the source monitor is held (see
     * {@link RegisteredSource#deliverToSubscribers}), so a callback that acquires the same purpose and
     * blocks on the returned future deadlocks: the acquisition task needs the monitor its own caller is
     * holding. The contract has always said not to do that, but a documented constraint that fails as a
     * hang is a poor contract — this turns it into an immediate, self-describing error.
     *
     * <p>Only same-purpose re-entry is caught, because that is the case that cannot succeed. A callback
     * acquiring a DIFFERENT purpose takes a different monitor and is merely inadvisable.
     */
    private static void rejectReentrantAcquisition(RegisteredSource source, TlsPurpose purpose) {
        if (Thread.holdsLock(source)) {
            throw new IllegalStateException("Re-entrant TLS acquisition for purpose " + purpose
                    + ": a reload callback must not call createInstance(...) for the purpose it is being "
                    + "delivered for and block on the result — the callback already holds that purpose's "
                    + "monitor, so the acquisition could never complete. Do the work outside the callback; "
                    + "the callback contract is a cheap, non-blocking store.");
        }
    }

    private static void retain(Object instance) {
        if (instance != null) {
            ReferenceCountUtil.retain(instance);
        }
    }

    private static void release(Object instance) {
        if (instance != null) {
            ReferenceCountUtil.release(instance);
        }
    }

    /**
     * A configured purpose's material source together with the cached, factory-owned context instances
     * and the active subscriptions. All state is guarded by the instance monitor.
     */
    private static final class RegisteredSource {
        private final TlsPurpose purpose;
        private final TlsPolicy policy;
        // Null for the system-default source, whose constant material never rotates.
        private final MaterialSource source;
        private final CopyOnWriteArrayList<Subscription<?>> subscriptions = new CopyOnWriteArrayList<>();

        private SslContext nettyContext;
        private TlsMaterial nettyMaterial;
        private SSLContext jdkContext;
        private TlsMaterial jdkMaterial;
        // Set when a rotation's rebuild/delivery to some subscriber failed, so the next poll re-attempts the
        // rebuild even if the source now reports changed=false (PIP-478 L1 reload-wedge guard).
        private boolean pendingRedeliver;

        RegisteredSource(TlsPurpose purpose, TlsPolicy policy, MaterialSource source) {
            this.purpose = purpose;
            this.policy = policy;
            this.source = source;
        }

        /**
         * Refresh the material, reporting whether it changed in value. The system-default source has no
         * backing files, so it never changes — its single context build is counted via the first-build check
         * in {@link FileBasedTlsFactory#loadInitialInstance}.
         */
        synchronized MaterialSource.RefreshOutcome currentMaterial() throws Exception {
            if (source == null) {
                return new MaterialSource.RefreshOutcome(TlsMaterial.SYSTEM_DEFAULT, false);
            }
            return source.refresh();
        }

        /**
         * Return the cached context of the requested class, rebuilding it (and releasing the superseded
         * one) only when the material changed in value. The returned instance is the factory-owned memo;
         * callers that hand it to a consumer {@link FileBasedTlsFactory#retain(Object) retain} it.
         */
        synchronized <T> T acquireInstance(Class<T> instanceClass, TlsMaterial material,
                                           FileBasedTlsFactorySettings settings) throws Exception {
            if (instanceClass == SslContext.class) {
                if (nettyContext == null || !material.equals(nettyMaterial)) {
                    SslContext built = purpose.role() == TlsPurpose.Role.SERVER
                            ? TlsContexts.buildNettyServerContext(material, policy, settings.engineProvider(),
                                    settings.requireTrustedClientCert())
                            : TlsContexts.buildNettyClientContext(material, policy, settings.engineProvider());
                    release(nettyContext);
                    nettyContext = built;
                    nettyMaterial = material;
                }
                return instanceClass.cast(nettyContext);
            }
            if (instanceClass == SSLContext.class) {
                if (jdkContext == null || !material.equals(jdkMaterial)) {
                    jdkContext = TlsContexts.buildJdkContext(material, policy);
                    jdkMaterial = material;
                }
                return instanceClass.cast(jdkContext);
            }
            throw new IllegalArgumentException("Unsupported instance class " + instanceClass);
        }

        /**
         * The last-good cached context of the requested class, or {@code null} if none has been built yet.
         * Used by the keep-last-good one-shot path when a fresh load fails mid-rotation.
         */
        synchronized <T> T cachedInstance(Class<T> instanceClass) {
            if (instanceClass == SslContext.class && nettyContext != null) {
                return instanceClass.cast(nettyContext);
            }
            if (instanceClass == SSLContext.class && jdkContext != null) {
                return instanceClass.cast(jdkContext);
            }
            return null;
        }

        synchronized void poll(FileBasedTlsFactorySettings settings, TlsReloadMetrics metrics) {
            if (source == null || subscriptions.isEmpty()) {
                return;
            }
            MaterialSource.RefreshOutcome outcome;
            try {
                outcome = source.refresh();
            } catch (Exception e) {
                // Keep-last-good: leave every subscription on its current instance and retry next change.
                recordLoad(metrics, false);
                log.warn().attr("purpose", purpose).exception(e)
                        .log("Failed to reload TLS material; keeping the last-good instance");
                return;
            }
            // A poll that finds no change is not a reload — do not count it, so the reload counter and the
            // last-success gauge reflect real (re)load events only. But if a prior rotation's rebuild/delivery
            // to some subscriber failed (pendingRedeliver), retry it even when the source now reports
            // changed=false: otherwise, once the source commits the new baseline, every later poll sees
            // changed=false and that subscriber stays wedged on the old instance until the next file change
            // (L1). The retry rebuilds against the current last-good material.
            if (!outcome.changed() && !pendingRedeliver) {
                return;
            }
            recordLoad(metrics, deliverToSubscribers(outcome.material(), settings));
        }

        /**
         * Rebuild and hand {@code material} to every live subscription, reporting whether all succeeded. Called
         * both by the background poll and by an acquisition that consumed the source's change signal first
         * (see {@link FileBasedTlsFactory#loadInitialInstance}), so a rotation reaches the subscribers no matter
         * which caller observed it. Records no metric itself — each caller counts the (re)load once.
         *
         * <p><b>Consumer callbacks run while this source's monitor is held</b>, which keeps a rotation's
         * rebuild-and-publish atomic against a concurrent acquisition, and makes deliveries serial per
         * subscription — a guarantee the framework's own Jetty coordinator relies on for its generation
         * ordering. The cost is a reentrancy constraint on consumers: a reload callback must not call
         * {@code createInstance} for the same purpose and block on the returned future, because that
         * acquisition needs this same monitor. That is now enforced rather than merely documented —
         * {@link FileBasedTlsFactory#rejectReentrantAcquisition} fails such an acquisition immediately with
         * an actionable message instead of letting it hang. Consumers are expected to do only a volatile
         * store in the callback (swap the context they hand to new connections) and never to block in it.
         */
        synchronized boolean deliverToSubscribers(TlsMaterial material, FileBasedTlsFactorySettings settings) {
            boolean allRebuilt = true;
            for (Subscription<?> subscription : subscriptions) {
                try {
                    Object rebuilt = acquireInstance(subscription.instanceClass, material, settings);
                    subscription.deliverErased(rebuilt);
                } catch (Exception e) {
                    allRebuilt = false;
                    log.warn().attr("purpose", purpose).attr("class", subscription.instanceClass.getName())
                            .exception(e).log("Failed to rebuild rotated TLS instance; keeping the last-good one");
                }
            }
            // Re-attempt on the next poll until every subscriber has the rotated instance (L1).
            pendingRedeliver = !allRebuilt;
            return allRebuilt;
        }

        private void recordLoad(TlsReloadMetrics metrics, boolean success) {
            if (metrics != null) {
                metrics.recordLoad(purpose, success);
            }
        }

        /**
         * Arm the redeliver retry for a rotation whose change signal was consumed but whose fan-out never
         * ran (the context build failed first). The next poll — or, when polling is disabled, the next
         * acquisition — then re-attempts the rebuild even though the source now reports {@code changed=false}.
         */
        synchronized void armRedeliver() {
            pendingRedeliver = true;
        }

        /** @return whether a rotation still owes its subscribers a rebuilt instance. */
        synchronized boolean pendingRedeliver() {
            return pendingRedeliver;
        }

        synchronized void removeSubscription(Subscription<?> subscription) {
            if (subscriptions.remove(subscription)) {
                subscription.releaseCurrent();
            }
        }

        synchronized void releaseAll() {
            for (Subscription<?> subscription : subscriptions) {
                subscription.releaseCurrent();
            }
            subscriptions.clear();
            release(nettyContext);
            nettyContext = null;
            nettyMaterial = null;
            jdkContext = null;
            jdkMaterial = null;
        }
    }

    /** A live server-side subscription: its instance class, callback, and last-delivered instances. */
    private static final class Subscription<T> {
        private final Class<T> instanceClass;
        private final Consumer<T> callback;
        // Volatile: written under the source monitor (deliver), but read lock-free by SubscriptionHandle.get()
        // from arbitrary consumer threads — the handle contract promises the most recent delivery, which
        // needs the happens-before edge (a stale read can outlive the deferred-release window below).
        private volatile T current;
        // Deferred release (PIP-478 use-after-free): the instance superseded by the most recent delivery
        // is kept alive one extra generation rather than released immediately. Consumers hold a bare volatile
        // borrow of the delivered instance and later call newHandler/newEngine on it off a different thread;
        // releasing the superseded Netty/OpenSSL context to refcount 0 on the poll thread the instant the new
        // one is published would free the native SSL_CTX out from under such an in-flight borrow. Retaining it
        // for one further rotation gives every reader a full poll interval to finish. Pairs with per-use
        // pinning at the consumers (see TlsContextAcquisition.withPinnedContext) for the descheduling case.
        private T previous;

        Subscription(Class<T> instanceClass, Consumer<T> callback) {
            this.instanceClass = instanceClass;
            this.callback = callback;
        }

        void deliver(T instance) {
            retain(instance);
            // Release the instance delivered two rotations ago (N-1) on this (N+1th) delivery, not the one
            // just superseded, so the just-superseded instance survives one more generation for readers.
            release(previous);
            previous = current;
            current = instance;
            safeInvoke(instance);
        }

        @SuppressWarnings("unchecked")
        void deliverErased(Object instance) {
            deliver((T) instance);
        }

        T current() {
            return current;
        }

        void releaseCurrent() {
            release(current);
            current = null;
            release(previous);
            previous = null;
        }

        private void safeInvoke(T instance) {
            try {
                callback.accept(instance);
            } catch (Throwable t) {
                // A throwing consumer callback must not kill the subscription; later deliveries proceed.
                log.warn().exception(t).log("TLS reload callback threw; subscription stays live");
            }
        }
    }

    /** A one-shot handle: exposes the built instance and releases its retained reference on dispose. */
    private static final class OneShotHandle<T> implements TlsHandle<T> {
        private final T instance;
        // FIX E: an AtomicBoolean, not a volatile flag — a plain check-then-set lets two concurrent
        // dispose() callers both pass the guard and over-decrement the cached context's refcount.
        private final AtomicBoolean disposed = new AtomicBoolean(false);

        OneShotHandle(T instance) {
            this.instance = instance;
        }

        @Override
        public T get() {
            return instance;
        }

        @Override
        public void dispose() {
            if (disposed.compareAndSet(false, true)) {
                release(instance);
            }
        }
    }

    /** A subscribing handle: exposes the most-recently-delivered instance and unregisters on dispose. */
    private static final class SubscriptionHandle<T> implements TlsHandle<T> {
        private final RegisteredSource source;
        private final Subscription<T> subscription;
        // AtomicBoolean rather than a plain volatile check-then-set, matching OneShotHandle. The release is
        // in fact idempotent without it — removeSubscription() only releases when subscriptions.remove()
        // returns true, which exactly one racing disposer sees — but leaving the two handles with opposite
        // patterns, one of them carrying a comment calling the other a bug, is a trap for the next reader.
        private final AtomicBoolean disposed = new AtomicBoolean();

        SubscriptionHandle(RegisteredSource source, Subscription<T> subscription) {
            this.source = source;
            this.subscription = subscription;
        }

        @Override
        public T get() {
            return subscription.current();
        }

        @Override
        public void dispose() {
            if (disposed.compareAndSet(false, true)) {
                source.removeSubscription(subscription);
            }
        }
    }

    /** Thrown when a resolved server purpose has no configured material (a resolved-but-unbuildable case). */
    static final class TlsMaterialUnavailableException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        TlsMaterialUnavailableException(String message) {
            super(message);
        }
    }
}
