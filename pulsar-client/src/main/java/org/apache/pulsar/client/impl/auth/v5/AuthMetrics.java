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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.util.concurrent.CompletableFuture;

/**
 * The OpenTelemetry client-authentication instruments emitted at the v5 credential-acquisition seam
 * (PIP-478, the Metrics section). The instruments observe the async credential methods —
 * {@code BinaryAuthDataProvider.getAuthDataAsync} on the binary transport and
 * {@code HttpAuthHeadersProvider.getHttpHeadersAsync} on HTTP — the exact calls that in v4 could stall the
 * Netty event loop invisibly while a slow IdP/ZTS endpoint was hit.
 *
 * <ul>
 *   <li>{@value #CREDENTIAL_DURATION} — histogram (seconds) {@code {auth_method}}: latency of async
 *       credential acquisition;</li>
 *   <li>{@value #AUTH_FAILURE} — counter {@code {auth_method, error=terminal|transient}}: authentication
 *       failures by error class (terminal = credential rejected/unsupported, a retry won't help; transient
 *       = e.g. the identity provider was unreachable, a retry might recover).</li>
 * </ul>
 *
 * <p>The instruments come from the {@link OpenTelemetry} the authentication init context carries; with a
 * {@link OpenTelemetry#noop() no-op} root (the framework default when no real one is wired) every
 * instrument is a no-op, so recording is always safe and cheap. Thread-safe: OpenTelemetry instruments
 * tolerate concurrent recording, and one instance is shared across all of a plugin's concurrent exchanges.
 */
public final class AuthMetrics {

    static final String INSTRUMENTATION_SCOPE_NAME = "org.apache.pulsar.client.auth";
    static final String CREDENTIAL_DURATION = "pulsar.client.auth.credential.duration";
    static final String AUTH_FAILURE = "pulsar.client.auth.failure";

    private static final AttributeKey<String> AUTH_METHOD_KEY = AttributeKey.stringKey("auth_method");
    private static final AttributeKey<String> ERROR_KEY = AttributeKey.stringKey("error");
    private static final String ERROR_TERMINAL = "terminal";
    private static final String ERROR_TRANSIENT = "transient";
    private static final String UNKNOWN_METHOD = "unknown";

    /** A shared no-op instance for callers with no telemetry (e.g. a plugin used outside a client). */
    public static final AuthMetrics NOOP = new AuthMetrics(OpenTelemetry.noop());

    private final DoubleHistogram credentialDuration;
    private final LongCounter authFailure;

    private AuthMetrics(OpenTelemetry openTelemetry) {
        Meter meter = openTelemetry.getMeter(INSTRUMENTATION_SCOPE_NAME);
        this.credentialDuration = meter.histogramBuilder(CREDENTIAL_DURATION)
                .setUnit("s")
                .setDescription("Latency of async credential acquisition (getAuthDataAsync / getHttpHeadersAsync)")
                .build();
        this.authFailure = meter.counterBuilder(AUTH_FAILURE)
                .setDescription("Authentication failures by error class (terminal/transient)")
                .build();
    }

    /**
     * Create the instruments over the given telemetry root. A {@code null} or {@link OpenTelemetry#noop()}
     * root yields the shared {@link #NOOP} instance.
     *
     * @param openTelemetry the telemetry root (may be {@code null})
     * @return the metrics instance
     */
    public static AuthMetrics create(OpenTelemetry openTelemetry) {
        return openTelemetry == null || openTelemetry == OpenTelemetry.noop()
                ? NOOP : new AuthMetrics(openTelemetry);
    }

    /**
     * Instrument a credential-acquisition future: record its latency into {@value #CREDENTIAL_DURATION} on
     * completion and, when it completes exceptionally, increment {@value #AUTH_FAILURE} classified by error.
     * The returned future completes identically to the source (the timing hook is side-effect-free on the
     * value), so this never changes credential behaviour.
     *
     * @param future     the credential-acquisition future to observe
     * @param authMethod the auth method name for the {@code auth_method} attribute (may be {@code null})
     * @param <T>        the credential type
     * @return a future that completes identically to {@code future}
     */
    public <T> CompletableFuture<T> timeCredential(CompletableFuture<T> future, String authMethod) {
        String method = authMethod == null || authMethod.isBlank() ? UNKNOWN_METHOD : authMethod;
        long startNanos = System.nanoTime();
        return future.whenComplete((value, throwable) -> {
            credentialDuration.record((System.nanoTime() - startNanos) / 1_000_000_000.0,
                    Attributes.of(AUTH_METHOD_KEY, method));
            if (throwable != null) {
                authFailure.add(1, Attributes.of(AUTH_METHOD_KEY, method, ERROR_KEY, classify(throwable)));
            }
        });
    }

    // Terminal = credential rejected or unsupported (a retry with the same input will not help); transient
    // = the credential could not be obtained (e.g. the IdP was unreachable), which a retry might recover.
    private static String classify(Throwable throwable) {
        Throwable cause = BinaryAuthenticationExchange.unwrap(throwable);
        if (cause instanceof org.apache.pulsar.client.api.PulsarClientException.AuthenticationException
                || cause instanceof org.apache.pulsar.client.api.PulsarClientException
                        .UnsupportedAuthenticationException
                || cause instanceof org.apache.pulsar.client.api.v5.PulsarClientException.AuthenticationException
                || cause instanceof org.apache.pulsar.client.api.v5.PulsarClientException
                        .UnsupportedAuthenticationException) {
            return ERROR_TERMINAL;
        }
        return ERROR_TRANSIENT;
    }
}
