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
package org.apache.pulsar.client.api.internal;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.api.AuthData;

/**
 * Stable-internal marker that lets {@code ClientCnx} drive authentication asynchronously (PIP-478).
 *
 * <p>This is the only carve-out from the otherwise-untouched v4 client. When the configured
 * {@link org.apache.pulsar.client.api.Authentication} instance also implements this interface,
 * {@code ClientCnx} routes connect / refresh / challenge handling through these non-blocking methods
 * and pipes the result back onto the Netty event loop; plain v4 instances keep the existing
 * synchronous path verbatim. It is generic across all challenge types (initial connect, the broker's
 * {@code REFRESH_AUTH_DATA} sentinel, SASL multi-round, and custom challenge/response).
 *
 * <p><b>Exchange-scoped.</b> Authentication is driven per <em>exchange</em> — one connection attempt.
 * {@code ClientCnx} calls {@link #newAuthenticationExchange(String)} once per connect and drives that
 * connection's rounds through the single returned {@link AuthenticationExchange} — the initial credential
 * via {@link AuthenticationExchange#getAuthDataAsync()} and every ordinary challenge round (SASL
 * multi-round, custom challenge/response) via {@link AuthenticationExchange#authenticateAsync(AuthData)}.
 * The exchange is where per-connection conversation state lives (for the v5 bridge, one call-context and
 * its state slot), so binding it to the exchange object — rather than passing the host on each call —
 * makes the state slot's lifetime exactly one exchange. The broker's {@code REFRESH_AUTH_DATA} sentinel
 * does <em>not</em> ride the current exchange: {@code ClientCnx} terminates it and starts a <em>fresh</em>
 * exchange whose {@link AuthenticationExchange#getAuthDataAsync()} re-produces the current credential, so
 * conversation state does NOT survive a REFRESH. A fresh exchange is likewise created for every connection
 * attempt, so concurrent handshakes to different brokers never collide.
 *
 * <p>The {@code .internal.} subpackage signals "stable internal" — application code should not
 * implement this interface; it is observed by the framework.
 */
public interface AsyncAuthenticationDriver {

    /**
     * Begin an authentication exchange for a single connection attempt to the given broker.
     *
     * <p>The returned {@link AuthenticationExchange} owns all state for this one connection setup and is
     * driven by {@code ClientCnx} for every round of the handshake. Create a new exchange per connection
     * attempt; do not share one across connections.
     *
     * @param brokerHostName the broker host being connected to (for per-host credentials)
     * @return a new, single-use exchange scoped to this connection attempt
     */
    AuthenticationExchange newAuthenticationExchange(String brokerHostName);

    /**
     * A single authentication exchange: the credential rounds of one {@code ClientCnx} connection setup.
     *
     * <p>An exchange is <em>not</em> thread-safe and is not reused across connection attempts. Its rounds
     * are serialized by the caller ({@code ClientCnx} issues the next round only after the previous
     * future completes), so an implementation needs no internal synchronization for its per-exchange
     * conversation state. Every future-returning method reports failures by completing the returned
     * future exceptionally; it never throws on the calling thread.
     */
    interface AuthenticationExchange {

        /**
         * Asynchronously produce the initial authentication data for {@code CommandConnect}.
         *
         * @return a future of the auth data; completes exceptionally on failure
         */
        CompletableFuture<AuthData> getAuthDataAsync();

        /**
         * Asynchronously compute the response to a broker {@code CommandAuthChallenge} — an ordinary
         * challenge round (SASL multi-round or custom challenge/response), answered from this exchange's
         * conversation state.
         *
         * <p>The broker's {@code REFRESH_AUTH_DATA} sentinel is <em>not</em> delivered here: {@code ClientCnx}
         * services a refresh by starting a fresh exchange and calling {@link #getAuthDataAsync()} on it, so
         * in the {@code ClientCnx} flow this method only ever sees a genuine challenge payload.
         *
         * @param challenge the challenge payload from the broker (not the refresh sentinel)
         * @return a future of the response auth data; completes exceptionally on failure
         */
        CompletableFuture<AuthData> authenticateAsync(AuthData challenge);
    }
}
