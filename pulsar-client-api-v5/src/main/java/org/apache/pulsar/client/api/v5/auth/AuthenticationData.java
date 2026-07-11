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
package org.apache.pulsar.client.api.v5.auth;

import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Map;
import java.util.Set;

/**
 * Provides authentication credentials for different transport mechanisms.
 */
public interface AuthenticationData {

    // --- HTTP authentication ---

    /**
     * Check whether this authentication data provides HTTP headers.
     *
     * @return {@code true} if HTTP authentication headers are available, {@code false} otherwise
     */
    default boolean hasDataForHttp() {
        return false;
    }

    /**
     * Get the HTTP authentication headers to include in requests.
     *
     * @return a set of header name-value entries for HTTP authentication, or an empty set if none
     */
    default Set<Map.Entry<String, String>> getHttpHeaders() {
        return Set.of();
    }

    // --- TLS mutual authentication ---

    /**
     * Check whether this authentication data provides TLS client certificates.
     *
     * @return {@code true} if TLS certificate data is available, {@code false} otherwise
     */
    default boolean hasDataForTls() {
        return false;
    }

    /**
     * Get the TLS client certificate chain for mutual authentication.
     *
     * @return the client certificate chain, or {@code null} if not available
     */
    default Certificate[] getTlsCertificates() {
        return null;
    }

    /**
     * Get the TLS client private key for mutual authentication.
     *
     * @return the client private key, or {@code null} if not available
     */
    default PrivateKey getTlsPrivateKey() {
        return null;
    }

    // --- Binary protocol authentication ---

    /**
     * Check whether this authentication data provides binary protocol command data.
     *
     * @return {@code true} if command data is available for the Pulsar binary protocol, {@code false} otherwise
     */
    default boolean hasDataFromCommand() {
        return false;
    }

    /**
     * Get the authentication data to include in binary protocol commands.
     *
     * @return the command data string for binary protocol authentication, or {@code null} if not available
     */
    default String getCommandData() {
        return null;
    }
}
