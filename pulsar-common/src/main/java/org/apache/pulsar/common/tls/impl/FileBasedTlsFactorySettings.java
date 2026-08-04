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

/**
 * The factory-wide (not per-purpose) settings for a {@link FileBasedTlsFactory} (PIP-478).
 *
 * <p>These are the knobs that a {@code TlsPolicy} deliberately does not carry, because they describe
 * how the factory builds and refreshes contexts rather than which material to load:
 * <ul>
 *   <li><b>engine selection</b> — the Netty {@link SslProvider} the native contexts are built on
 *       (JDK, or an OpenSSL-based provider where the {@code netty-tcnative} binding is present);</li>
 *   <li><b>server client-auth requirement</b> — whether server contexts require (vs. merely request) a
 *       trusted client certificate; a single flag mirroring the broker-wide
 *       {@code tlsRequireTrustedClientCert} property, applied to every server purpose;</li>
 *   <li><b>refresh interval</b> — how often the factory polls its material sources for rotation; a
 *       value {@code <= 0} disables background polling.</li>
 * </ul>
 */
public final class FileBasedTlsFactorySettings {

    /** Default rotation poll interval, in seconds (PIP-478). */
    public static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 60;

    private final SslProvider engineProvider;
    private final boolean requireTrustedClientCert;
    private final int refreshIntervalSeconds;

    private FileBasedTlsFactorySettings(Builder builder) {
        this.engineProvider = builder.engineProvider;
        this.requireTrustedClientCert = builder.requireTrustedClientCert;
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

    /** @return whether server contexts require (vs. merely request) a trusted client certificate */
    public boolean requireTrustedClientCert() {
        return requireTrustedClientCert;
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
        private boolean requireTrustedClientCert = false;
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
         * @param requireTrustedClientCert whether server contexts require a trusted client certificate
         * @return this builder
         */
        public Builder requireTrustedClientCert(boolean requireTrustedClientCert) {
            this.requireTrustedClientCert = requireTrustedClientCert;
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
