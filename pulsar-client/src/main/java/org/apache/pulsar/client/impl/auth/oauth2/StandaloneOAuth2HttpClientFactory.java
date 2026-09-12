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
package org.apache.pulsar.client.impl.auth.oauth2;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.CustomLog;
import org.apache.pulsar.client.impl.auth.v5.FrameworkHttpClientFactory;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.http.PulsarHttpClient;
import org.apache.pulsar.http.PulsarHttpClientConfig;
import org.apache.pulsar.http.PulsarHttpClientFactory;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * A self-contained {@link PulsarHttpClientFactory} for an OAuth2 {@link FlowBase} that runs <em>standalone</em>
 * — outside any {@code PulsarClient} / {@code PulsarAdmin} that would late-bind the framework HTTP client
 * factory (PIP-478). This is the path the proxy, the broker's broker-client and the CLI tools use
 * when they create {@code AuthenticationOAuth2} directly via {@code AuthenticationFactory.create(...).start()}
 * (e.g. {@code ProxyServiceStarter}); {@code FlowBase} self-provisions its own HTTP client for exactly this
 * case, providing backward-compatible standalone support on the new SPI.
 *
 * <p>It wraps a framework {@link FrameworkHttpClientFactory} whose Netty event loop / timer / DNS resolver are
 * self-provisioned by AsyncHttpClient (HTTP-only, mirroring {@code PulsarAdminImpl.bindAuthenticationServices}),
 * and — when the flow carries its own IdP TLS material — a standalone {@link FileBasedTlsFactory} serving that
 * material on the {@link TlsPurpose#CLIENT_OAUTH2} purpose with rotation (mirroring the admin connector's
 * {@code AsyncHttpConnector.resolveNewTlsFactory}, which likewise owns a single-thread scheduler for a client
 * with no shared funnel). With no IdP TLS material the framework client uses the platform default trust store
 * for the IdP, matching v4 behaviour for a system-trusted IdP. All owned resources are released by
 * {@link #close()}.
 */
@CustomLog
final class StandaloneOAuth2HttpClientFactory implements PulsarHttpClientFactory, AutoCloseable {

    private final FrameworkHttpClientFactory delegate;
    // The standalone IdP TLS factory and the scheduler that drives its rotation polling / blocking loads;
    // both null when the flow carries no IdP TLS material (the framework client then uses the platform default
    // trust store for the IdP).
    private final PulsarTlsFactory idpTlsFactory;
    private final ScheduledExecutorService tlsExecutor;

    StandaloneOAuth2HttpClientFactory(Optional<TlsPolicy> idpTlsPolicy, int refreshIntervalSeconds,
            String instanceId) {
        ScheduledExecutorService executor = null;
        PulsarTlsFactory tlsFactory = null;
        try {
            if (idpTlsPolicy.isPresent()) {
                // Daemon threads: the standalone factory may be created by a short-lived tool; a leaked
                // (never-closed) rotation scheduler with non-daemon threads would block JVM exit.
                executor = Executors.newSingleThreadScheduledExecutor(
                        new DefaultThreadFactory("oauth2-idp-tls", true));
                tlsFactory = buildIdpTlsFactory(idpTlsPolicy.get(), refreshIntervalSeconds, executor);
            }
            PulsarTlsFactory factoryForSupplier = tlsFactory;
            // HTTP-only: AsyncHttpClient provisions its own event loop / timer / DNS resolver (as the removed
            // private OAuth2 client did), so the shared-resource suppliers return null.
            this.delegate = new FrameworkHttpClientFactory(() -> null, () -> null, () -> null,
                    () -> factoryForSupplier, new ClientConfigurationData(), instanceId);
            this.tlsExecutor = executor;
            this.idpTlsFactory = tlsFactory;
        } catch (RuntimeException | Error e) {
            // Ctor-throw cleanup: buildIdpTlsFactory or the delegate build failed after we allocated the
            // IdP TLS factory / scheduler; release them here since close() will never run on this instance.
            if (tlsFactory != null) {
                closeQuietly(tlsFactory);
            }
            if (executor != null) {
                executor.shutdownNow();
            }
            throw e;
        }
    }

    private static PulsarTlsFactory buildIdpTlsFactory(TlsPolicy policy, int refreshIntervalSeconds,
            ScheduledExecutorService executor) {
        FileBasedTlsFactory factory = new FileBasedTlsFactory(
                Map.of(TlsPurpose.CLIENT_OAUTH2, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(refreshIntervalSeconds).build(),
                Map.of());
        try {
            factory.initialize(initContext(executor)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closeQuietly(factory);
            throw new IllegalStateException("Interrupted initializing OAuth2 IdP TLS factory", e);
        } catch (ExecutionException e) {
            closeQuietly(factory);
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalStateException("Failed to initialize OAuth2 IdP TLS factory: " + cause.getMessage(),
                    cause);
        }
        return factory;
    }

    private static TlsFactoryInitContext initContext(ScheduledExecutorService executor) {
        return new TlsFactoryInitContext() {
            @Override
            public Map<String, String> params() {
                return Map.of();
            }

            @Override
            public ScheduledExecutorService scheduler() {
                return executor;
            }

            @Override
            public Executor blockingExecutor() {
                return executor;
            }

            @Override
            public Clock clock() {
                return Clock.systemUTC();
            }

            @Override
            public OpenTelemetry openTelemetry() {
                return OpenTelemetry.noop();
            }
        };
    }

    @Override
    public PulsarHttpClient newHttpClient(PulsarHttpClientConfig config) {
        return delegate.newHttpClient(config);
    }

    @Override
    public void close() {
        delegate.close();
        if (idpTlsFactory != null) {
            closeQuietly(idpTlsFactory);
        }
        if (tlsExecutor != null) {
            tlsExecutor.shutdownNow();
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn().exception(e).log("Failed to close standalone OAuth2 IdP TLS factory");
        }
    }
}
