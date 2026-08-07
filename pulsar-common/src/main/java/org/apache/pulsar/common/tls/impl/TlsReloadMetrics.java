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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * The OpenTelemetry TLS-reload instruments emitted by the default {@link FileBasedTlsFactory} (PIP-478,
 * the Metrics section). The factory obtains its {@link OpenTelemetry} handle from
 * {@code TlsFactoryInitContext.openTelemetry()} and records one attempt per <em>actual</em> material
 * (re)load — the initial load of a purpose, and each rotation where the on-disk material changed and was
 * rebuilt. A steady poll that finds no change is not a reload and is not counted, so the counter reflects
 * real (re)load events and the last-success gauge stops advancing exactly when rotation silently fails or
 * silently stops happening — the signal the Monitoring section alerts on.
 *
 * <ul>
 *   <li>{@value #RELOAD_COUNTER} — counter {@code {purpose, result=success|failure}}: TLS material
 *       load/reload attempts per purpose, client and server side;</li>
 *   <li>{@value #LAST_RELOAD_SUCCESS_GAUGE} — gauge (unix seconds) {@code {purpose}}: time of the last
 *       successful load/reload, so alerts can catch silently-failing rotation long before a certificate
 *       expires.</li>
 * </ul>
 *
 * <p>With a {@link OpenTelemetry#noop() no-op} telemetry root (the framework default for components that
 * do not wire a real one) every instrument is a no-op, so recording is always safe and cheap. The
 * instance is thread-safe: {@link #recordLoad} runs on the factory's blocking-executor / poll threads
 * while the gauge callback runs on the OpenTelemetry collection thread.
 */
final class TlsReloadMetrics implements AutoCloseable {

    static final String INSTRUMENTATION_SCOPE_NAME = "org.apache.pulsar.tls";
    static final String RELOAD_COUNTER = "pulsar.tls.reload";
    static final String LAST_RELOAD_SUCCESS_GAUGE = "pulsar.tls.last_reload_success";

    private static final AttributeKey<String> PURPOSE_KEY = AttributeKey.stringKey("purpose");
    private static final AttributeKey<String> RESULT_KEY = AttributeKey.stringKey("result");
    private static final String RESULT_SUCCESS = "success";
    private static final String RESULT_FAILURE = "failure";

    private final Clock clock;
    private final LongCounter reloadCounter;
    private final ObservableLongGauge lastReloadSuccessGauge;
    // Purpose name -> unix seconds of the last successful load/reload; read by the gauge callback.
    private final Map<String, Long> lastSuccessEpochSeconds = new ConcurrentHashMap<>();

    private TlsReloadMetrics(OpenTelemetry openTelemetry, Clock clock) {
        this.clock = clock == null ? Clock.systemUTC() : clock;
        Meter meter = openTelemetry.getMeter(INSTRUMENTATION_SCOPE_NAME);
        this.reloadCounter = meter.counterBuilder(RELOAD_COUNTER)
                .setDescription("TLS material load/reload attempts per purpose, client and server side")
                .build();
        this.lastReloadSuccessGauge = meter.gaugeBuilder(LAST_RELOAD_SUCCESS_GAUGE)
                .setUnit("s")
                .setDescription("Time (unix seconds) of the last successful TLS material load/reload per purpose")
                .ofLongs()
                .buildWithCallback(measurement -> lastSuccessEpochSeconds.forEach(
                        (purpose, seconds) -> measurement.record(seconds, Attributes.of(PURPOSE_KEY, purpose))));
    }

    /**
     * Create the instruments over the given telemetry root. A {@code null} root maps to
     * {@link OpenTelemetry#noop()}, which yields no-op instruments.
     */
    static TlsReloadMetrics create(OpenTelemetry openTelemetry, Clock clock) {
        return new TlsReloadMetrics(openTelemetry == null ? OpenTelemetry.noop() : openTelemetry, clock);
    }

    /**
     * Record a single material load/reload attempt for a purpose. On success, advances the purpose's
     * last-successful-load timestamp.
     *
     * @param purpose the purpose whose material was (re)loaded
     * @param success whether the load/reload succeeded
     */
    void recordLoad(TlsPurpose purpose, boolean success) {
        String purposeName = Objects.requireNonNull(purpose, "purpose").name();
        reloadCounter.add(1, Attributes.of(PURPOSE_KEY, purposeName,
                RESULT_KEY, success ? RESULT_SUCCESS : RESULT_FAILURE));
        if (success) {
            lastSuccessEpochSeconds.put(purposeName, clock.instant().getEpochSecond());
        }
    }

    @Override
    public void close() {
        lastReloadSuccessGauge.close();
    }
}
