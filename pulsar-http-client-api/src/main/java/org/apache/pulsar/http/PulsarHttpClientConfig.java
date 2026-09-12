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
package org.apache.pulsar.http;

import java.time.Duration;
import java.util.Objects;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * Per-instance configuration for a {@link PulsarHttpClient} (PIP-478).
 *
 * <p>A plugin describes <em>what kind</em> of HTTP client it needs; the framework constructs, pools and
 * closes the instance. This deliberately does NOT carry cert/key/trust material directly — the TLS
 * material lives in the configured {@code PulsarTlsFactory} and is looked up by {@link #tlsPurpose()}.
 * For a plugin that needs its own trust domain (the OAuth2 IdP being the canonical case), the plugin
 * selects a distinct purpose — {@link TlsPurpose#CLIENT_OAUTH2}, or a minted variant such as
 * {@code TlsPurpose.client("oauth2.myPlugin")} — and the operator configures a
 * {@code TlsPolicy} for that purpose; the plugin never handles raw material. Insecure-connection and
 * hostname-verification behaviour are baked into the built TLS objects by the factory per purpose, so
 * they are not configured here.
 */
public final class PulsarHttpClientConfig {

    private final TlsPurpose tlsPurpose;
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final Duration requestTimeout;
    private final String userAgent;
    private final long maxResponseBodyBytes;

    private PulsarHttpClientConfig(Builder b) {
        this.tlsPurpose = b.tlsPurpose;
        this.connectTimeout = b.connectTimeout;
        this.readTimeout = b.readTimeout;
        this.requestTimeout = b.requestTimeout;
        this.userAgent = b.userAgent;
        this.maxResponseBodyBytes = b.maxResponseBodyBytes;
    }

    /**
     * @return the TLS purpose the framework resolves this client's TLS material by
     */
    public TlsPurpose tlsPurpose() {
        return tlsPurpose;
    }

    /**
     * @return the connection timeout
     */
    public Duration connectTimeout() {
        return connectTimeout;
    }

    /**
     * @return the read (socket) timeout
     */
    public Duration readTimeout() {
        return readTimeout;
    }

    /**
     * @return the overall request timeout
     */
    public Duration requestTimeout() {
        return requestTimeout;
    }

    /**
     * @return the {@code User-Agent} header value to send
     */
    public String userAgent() {
        return userAgent;
    }

    /**
     * @return the maximum buffered response body size in bytes
     */
    public long maxResponseBodyBytes() {
        return maxResponseBodyBytes;
    }

    /**
     * Create a builder for the given TLS purpose.
     *
     * @param tlsPurpose the TLS purpose used to resolve TLS material
     * @return a new {@link Builder}
     */
    public static Builder builder(TlsPurpose tlsPurpose) {
        return new Builder(tlsPurpose);
    }

    /**
     * Builder for {@link PulsarHttpClientConfig}.
     */
    public static final class Builder {
        private static final long DEFAULT_MAX_RESPONSE_BODY_BYTES = 16L * 1024 * 1024;

        private final TlsPurpose tlsPurpose;
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration readTimeout = Duration.ofSeconds(30);
        private Duration requestTimeout = Duration.ofSeconds(30);
        private String userAgent = "Pulsar-Java-v5";
        private long maxResponseBodyBytes = DEFAULT_MAX_RESPONSE_BODY_BYTES;

        private Builder(TlsPurpose tlsPurpose) {
            this.tlsPurpose = Objects.requireNonNull(tlsPurpose, "tlsPurpose must not be null");
        }

        /**
         * @param connectTimeout the connection timeout
         * @return this builder
         */
        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * @param readTimeout the read timeout
         * @return this builder
         */
        public Builder readTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        /**
         * @param requestTimeout the overall request timeout
         * @return this builder
         */
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        /**
         * @param userAgent the {@code User-Agent} value
         * @return this builder
         */
        public Builder userAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        /**
         * @param maxResponseBodyBytes the maximum buffered response body size in bytes
         * @return this builder
         */
        public Builder maxResponseBodyBytes(long maxResponseBodyBytes) {
            this.maxResponseBodyBytes = maxResponseBodyBytes;
            return this;
        }

        /**
         * @return a new immutable {@link PulsarHttpClientConfig}
         */
        public PulsarHttpClientConfig build() {
            return new PulsarHttpClientConfig(this);
        }
    }
}
