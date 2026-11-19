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

import io.netty.handler.ssl.SslProvider;
import java.util.Objects;
import org.apache.pulsar.common.tls.TlsClientAuthMode;

/**
 * The factory-wide (not per-purpose) settings for a {@link FileBasedTlsFactory} (PIP-478).
 *
 * <p>These are the knobs that a {@code TlsPolicy} deliberately does not carry, because they describe
 * how the factory builds and refreshes contexts rather than which material to load:
 * <ul>
 *   <li><b>engine selection</b> — the Netty {@link SslProvider} the native contexts are built on
 *       (JDK, or an OpenSSL-based provider where the {@code netty-tcnative} binding is present);</li>
 *   <li><b>server client-auth mode</b> — whether server contexts require, merely request, or do not request
 *       a client certificate; a single {@link TlsClientAuthMode} resolved from the {@code tlsClientAuthMode},
 *       deprecated {@code tlsRequireTrustedClientCertOnConnect} and authentication properties (see
 *       {@link TlsClientAuthMode#resolve}), applied to every server purpose. That it
 *       is one setting for the binary, proxy and web listeners is deliberate PIP-337 parity rather than a
 *       simplification: v4 keyed all three off that same property, {@code JettySslContextFactory}
 *       included, so per-listener client-auth was never configurable;</li>
 *   <li><b>refresh interval</b> — how often the factory polls its material sources for rotation; a
 *       value {@code <= 0} disables background polling.</li>
 * </ul>
 */
public final class FileBasedTlsFactorySettings {

    /** Default rotation poll interval, in seconds (PIP-478). */
    public static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 60;

    /**
     * Map a {@code tlsCertRefreshCheckDurationSec} configuration value (broker, proxy, websocket proxy or
     * functions worker — they all default it to 300) onto {@link #refreshIntervalSeconds()}, preserving the v4
     * meaning of a non-positive value: {@code 0} means <em>no background poll</em>, not "use the default".
     * Every v4 listener guarded its refresh task with {@code > 0}, so substituting
     * {@link #DEFAULT_REFRESH_INTERVAL_SECONDS} here would start polling for an operator who asked for none.
     * Shared by all four servers so the mapping cannot drift between them again.
     *
     * <p>Note that {@code 0} therefore disables rotation for the subscribing server purposes, exactly as it
     * did on the PIP-337 path. The config key's "set 0 to check on every new connection" wording describes
     * only the one-shot acquisition paths, which re-stat per request; it has not applied to the server
     * listeners since they moved to a shared, periodically-refreshed context.
     *
     * @param tlsCertRefreshCheckDurationSec the configured value
     * @return the poll interval in seconds, clamped to {@code int}, or {@code 0} to disable polling
     */
    public static int refreshIntervalSecondsFromConfig(long tlsCertRefreshCheckDurationSec) {
        return tlsCertRefreshCheckDurationSec <= 0
                ? 0
                : (int) Math.min(tlsCertRefreshCheckDurationSec, Integer.MAX_VALUE);
    }

    private final SslProvider engineProvider;
    private final TlsClientAuthMode clientAuthMode;
    private final int refreshIntervalSeconds;

    private FileBasedTlsFactorySettings(Builder builder) {
        this.engineProvider = builder.engineProvider;
        this.clientAuthMode = builder.clientAuthMode;
        this.refreshIntervalSeconds = builder.refreshIntervalSeconds;
    }

    /** @return default settings: JDK engine, client-auth requested (not required), 60s poll */
    public static FileBasedTlsFactorySettings defaults() {
        return builder().build();
    }

    /** @return a new {@link Builder} */
    public static Builder builder() {
        return new Builder();
    }

    /** @return the Netty SSL provider for the native contexts (never {@code null}) */
    public SslProvider engineProvider() {
        return engineProvider;
    }

    /** @return whether server contexts require, request or do not request a client certificate */
    public TlsClientAuthMode clientAuthMode() {
        return clientAuthMode;
    }

    /** @return the rotation poll interval in seconds ({@code <= 0} disables polling) */
    public int refreshIntervalSeconds() {
        return refreshIntervalSeconds;
    }

    /**
     * Builder for {@link FileBasedTlsFactorySettings}. Defaults to the JDK engine, client-auth
     * requested (not required), and the {@link #DEFAULT_REFRESH_INTERVAL_SECONDS} poll interval.
     */
    public static final class Builder {
        private SslProvider engineProvider = SslProvider.JDK;
        private TlsClientAuthMode clientAuthMode = TlsClientAuthMode.OPTIONAL;
        private int refreshIntervalSeconds = DEFAULT_REFRESH_INTERVAL_SECONDS;

        private Builder() {
        }

        /**
         * @param engineProvider the Netty SSL provider (engine selection)
         * @return this builder
         */
        public Builder engineProvider(SslProvider engineProvider) {
            this.engineProvider = Objects.requireNonNull(engineProvider, "engineProvider must not be null");
            return this;
        }

        /**
         * @param clientAuthMode whether server contexts require, request or do not request a client certificate
         * @return this builder
         */
        public Builder clientAuthMode(TlsClientAuthMode clientAuthMode) {
            this.clientAuthMode = Objects.requireNonNull(clientAuthMode, "clientAuthMode must not be null");
            return this;
        }

        /**
         * @param refreshIntervalSeconds the rotation poll interval in seconds ({@code <= 0} disables it)
         * @return this builder
         */
        public Builder refreshIntervalSeconds(int refreshIntervalSeconds) {
            this.refreshIntervalSeconds = refreshIntervalSeconds;
            return this;
        }

        /** @return a new immutable {@link FileBasedTlsFactorySettings} */
        public FileBasedTlsFactorySettings build() {
            return new FileBasedTlsFactorySettings(this);
        }
    }
}
