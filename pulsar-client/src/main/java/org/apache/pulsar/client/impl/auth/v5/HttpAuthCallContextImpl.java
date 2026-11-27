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

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;

/**
 * The {@link HttpAuthCallContext} the {@link HttpAuthenticationDriver} owns for the lifetime of a single
 * HTTP request's retry sequence (PIP-478). The state slot (a {@link ConcurrentHashMap} keyed by
 * class) carries the plugin's cross-round conversation state — for SASL, the {@code PulsarSaslClient}
 * conversation and the captured role token.
 *
 * <p>The driver serializes the rounds of one exchange (it issues the next round only after the previous
 * round's future completes), so slot access within an exchange needs no synchronization; the
 * concurrent-map backing simply tolerates the possibility that different completion threads touch it.
 * The server's prior challenge headers are set by the driver before each {@code respondToHttpChallenge}
 * call (empty on the first round) and, on the terminal {@code 200}, before the final
 * {@code getHttpHeaders} call.
 */
final class HttpAuthCallContextImpl implements HttpAuthCallContext {

    private final URI requestUri;
    private final ConcurrentHashMap<Class<?>, Object> stateSlot = new ConcurrentHashMap<>();
    private volatile HttpAuthHeaders serverChallengeHeaders;

    HttpAuthCallContextImpl(URI requestUri) {
        this.requestUri = requestUri;
    }

    @Override
    public URI requestUri() {
        return requestUri;
    }

    @Override
    public Optional<HttpAuthHeaders> serverChallengeHeaders() {
        return Optional.ofNullable(serverChallengeHeaders);
    }

    /**
     * Set (driver-only) the server's challenge headers seen for the round about to be computed. A
     * {@code null} value — the first round, before any challenge — is surfaced as
     * {@link Optional#empty()}, and an {@link HttpAuthHeaders#isEmpty() empty} value likewise, so the
     * plugin's "no prior response" branch fires consistently.
     *
     * @param headers the prior response's headers, or {@code null} before the first request
     */
    void setServerChallengeHeaders(HttpAuthHeaders headers) {
        this.serverChallengeHeaders = (headers == null || headers.isEmpty()) ? null : headers;
    }

    @Override
    public <T> Optional<T> getStateObject(Class<T> clazz) {
        return Optional.ofNullable(clazz.cast(stateSlot.get(clazz)));
    }

    @Override
    public <T> void setStateObject(Class<T> clazz, T value) {
        if (value == null) {
            stateSlot.remove(clazz);
        } else {
            stateSlot.put(clazz, value);
        }
    }
}
