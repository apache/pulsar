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
package org.apache.pulsar.client.api.v5.config;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * TLS configuration for the Pulsar client connection.
 *
 * <p>Construct via {@link #builder()}, or use {@link #ofInsecure()} for development.
 */
@EqualsAndHashCode
@ToString
public final class TlsPolicy {

    private final String trustCertsFilePath;
    private final String keyFilePath;
    private final String certificateFilePath;
    private final boolean allowInsecureConnection;
    private final boolean enableHostnameVerification;

    private TlsPolicy(String trustCertsFilePath, String keyFilePath, String certificateFilePath,
                      boolean allowInsecureConnection, boolean enableHostnameVerification) {
        this.trustCertsFilePath = trustCertsFilePath;
        this.keyFilePath = keyFilePath;
        this.certificateFilePath = certificateFilePath;
        this.allowInsecureConnection = allowInsecureConnection;
        this.enableHostnameVerification = enableHostnameVerification;
    }

    /**
     * @return path to the trusted CA certificate file (PEM), or {@code null} when not set
     */
    public String trustCertsFilePath() {
        return trustCertsFilePath;
    }

    /**
     * @return path to the client private key file (PEM), or {@code null} when not using mTLS
     */
    public String keyFilePath() {
        return keyFilePath;
    }

    /**
     * @return path to the client certificate file (PEM), or {@code null} when not using mTLS
     */
    public String certificateFilePath() {
        return certificateFilePath;
    }

    /**
     * @return whether connections to brokers with untrusted certificates are allowed
     */
    public boolean allowInsecureConnection() {
        return allowInsecureConnection;
    }

    /**
     * @return whether the broker hostname is verified against the certificate
     */
    public boolean enableHostnameVerification() {
        return enableHostnameVerification;
    }

    /**
     * Create an insecure TLS policy that accepts any certificate (for development only).
     *
     * @return a {@link TlsPolicy} with insecure connections allowed and hostname verification disabled
     */
    public static TlsPolicy ofInsecure() {
        return new TlsPolicy(null, null, null, true, false);
    }

    /**
     * @return a new builder for constructing a {@link TlsPolicy}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link TlsPolicy}.
     */
    public static final class Builder {
        private String trustCertsFilePath;
        private String keyFilePath;
        private String certificateFilePath;
        private boolean allowInsecureConnection = false;
        private boolean enableHostnameVerification = true;

        private Builder() {
        }

        /**
         * Path to the trusted CA certificate file (PEM format).
         *
         * @param trustCertsFilePath the trust store path
         * @return this builder
         */
        public Builder trustCertsFilePath(String trustCertsFilePath) {
            this.trustCertsFilePath = trustCertsFilePath;
            return this;
        }

        /**
         * Path to the client private key file (PEM format) for mutual TLS.
         *
         * @param keyFilePath the client key path
         * @return this builder
         */
        public Builder keyFilePath(String keyFilePath) {
            this.keyFilePath = keyFilePath;
            return this;
        }

        /**
         * Path to the client certificate file (PEM format) for mutual TLS.
         *
         * @param certificateFilePath the client certificate path
         * @return this builder
         */
        public Builder certificateFilePath(String certificateFilePath) {
            this.certificateFilePath = certificateFilePath;
            return this;
        }

        /**
         * Whether to allow connecting to brokers with untrusted certificates.
         * Default {@code false}. Enable only for development.
         *
         * @param allowInsecureConnection whether to skip certificate validation
         * @return this builder
         */
        public Builder allowInsecureConnection(boolean allowInsecureConnection) {
            this.allowInsecureConnection = allowInsecureConnection;
            return this;
        }

        /**
         * Whether to verify the broker hostname against the certificate. Default
         * {@code true}.
         *
         * @param enableHostnameVerification whether to verify the hostname
         * @return this builder
         */
        public Builder enableHostnameVerification(boolean enableHostnameVerification) {
            this.enableHostnameVerification = enableHostnameVerification;
            return this;
        }

        /**
         * @return a new {@link TlsPolicy} instance
         */
        public TlsPolicy build() {
            return new TlsPolicy(trustCertsFilePath, keyFilePath, certificateFilePath,
                    allowInsecureConnection, enableHostnameVerification);
        }
    }
}
