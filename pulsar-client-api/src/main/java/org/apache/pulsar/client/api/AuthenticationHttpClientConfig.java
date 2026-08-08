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
package org.apache.pulsar.client.api;

import java.time.Duration;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Configuration for authentication HTTP clients.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public final class AuthenticationHttpClientConfig {
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final String trustCertsFilePath;
    private final String tlsCertificateFilePath;
    private final String tlsKeyFilePath;
    private final Duration autoCertRefreshDuration;

    private AuthenticationHttpClientConfig(Builder builder) {
        this.connectTimeout = builder.connectTimeout;
        this.readTimeout = builder.readTimeout;
        this.trustCertsFilePath = builder.trustCertsFilePath;
        this.tlsCertificateFilePath = builder.tlsCertificateFilePath;
        this.tlsKeyFilePath = builder.tlsKeyFilePath;
        this.autoCertRefreshDuration = builder.autoCertRefreshDuration;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public Duration getReadTimeout() {
        return readTimeout;
    }

    public String getTrustCertsFilePath() {
        return trustCertsFilePath;
    }

    public String getTlsCertificateFilePath() {
        return tlsCertificateFilePath;
    }

    public String getTlsKeyFilePath() {
        return tlsKeyFilePath;
    }

    public Duration getAutoCertRefreshDuration() {
        return autoCertRefreshDuration;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link AuthenticationHttpClientConfig}.
     */
    public static final class Builder {
        private Duration connectTimeout;
        private Duration readTimeout;
        private String trustCertsFilePath;
        private String tlsCertificateFilePath;
        private String tlsKeyFilePath;
        private Duration autoCertRefreshDuration;

        private Builder() {
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder readTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public Builder trustCertsFilePath(String trustCertsFilePath) {
            this.trustCertsFilePath = trustCertsFilePath;
            return this;
        }

        public Builder tlsCertificateFilePath(String tlsCertificateFilePath) {
            this.tlsCertificateFilePath = tlsCertificateFilePath;
            return this;
        }

        public Builder tlsKeyFilePath(String tlsKeyFilePath) {
            this.tlsKeyFilePath = tlsKeyFilePath;
            return this;
        }

        public Builder autoCertRefreshDuration(Duration autoCertRefreshDuration) {
            this.autoCertRefreshDuration = autoCertRefreshDuration;
            return this;
        }

        public AuthenticationHttpClientConfig build() {
            return new AuthenticationHttpClientConfig(this);
        }
    }
}
