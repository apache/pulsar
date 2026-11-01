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
package org.apache.pulsar.common.tls;

/**
 * Whether a TLS server asks connecting clients for a certificate (TLS client authentication), mirroring the
 * {@code tlsClientAuthMode} setting of the broker, proxy, websocket proxy and functions worker.
 */
public enum TlsClientAuthMode {
    /**
     * Do not request a client certificate. The server sends no TLS {@code CertificateRequest}, so no client
     * certificate is available to authentication providers. Suits deployments that authenticate clients with
     * tokens, OAuth 2.0 or other non-TLS methods.
     */
    NONE,
    /**
     * Request a client certificate but accept a connection without one. A presented certificate must still be
     * trusted (unless insecure connections are allowed).
     */
    OPTIONAL,
    /** Require a trusted client certificate; a connection without one is rejected. */
    REQUIRE;

    /**
     * Resolve the effective mode from the configured {@code tlsClientAuthMode} and the deprecated
     * {@code tlsRequireTrustedClientCertOnConnect} setting. An unset mode is derived: {@link #REQUIRE} when the
     * deprecated setting is {@code true}, {@link #OPTIONAL} when TLS client-certificate authentication is enabled,
     * and {@link #NONE} otherwise, because a server that neither requires nor uses a client certificate has no
     * reason to ask for one.
     *
     * @param configured                        the configured mode, or {@code null} when unset
     * @param requireTrustedClientCertOnConnect the deprecated {@code tlsRequireTrustedClientCertOnConnect} setting
     * @param clientCertAuthenticationEnabled   whether an authentication provider that authenticates clients by
     *                                          their TLS certificate is enabled
     * @return the effective mode
     * @throws IllegalArgumentException if {@code requireTrustedClientCertOnConnect} is {@code true} while the
     *                                  configured mode is {@link #NONE} or {@link #OPTIONAL}
     */
    public static TlsClientAuthMode resolve(TlsClientAuthMode configured, boolean requireTrustedClientCertOnConnect,
                                            boolean clientCertAuthenticationEnabled) {
        if (configured == null) {
            if (requireTrustedClientCertOnConnect) {
                return REQUIRE;
            }
            return clientCertAuthenticationEnabled ? OPTIONAL : NONE;
        }
        if (requireTrustedClientCertOnConnect && configured != REQUIRE) {
            throw new IllegalArgumentException("tlsRequireTrustedClientCertOnConnect=true conflicts with "
                    + "tlsClientAuthMode=" + configured + "; set tlsClientAuthMode=REQUIRE and remove the "
                    + "deprecated tlsRequireTrustedClientCertOnConnect setting");
        }
        return configured;
    }
}
