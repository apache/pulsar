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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;

/**
 * The thin, per-client-API request/response adapter the {@link HttpAuthenticationDriver} runs its
 * SASL-style {@code 401}→resubmit→{@code 200} rounds over (PIP-478).
 *
 * <p>The driver holds the single shared state machine; the transport is the only per-API piece. Each
 * round is re-issued as a <em>bodiless {@code GET} to the original request URI</em> — the v4 SASL takeover
 * preserved exactly — so no request body is ever replayed. Two adapters exist: an AsyncHttpClient-backed
 * one for the HTTP topic-lookup client ({@code HttpClient}) and a JAX-RS-backed one for the admin client
 * (backed by the built-in SASL plugin's own HTTP client, matching what {@code AuthenticationSasl} does
 * today).
 *
 * <p>Implementations MUST NOT throw synchronously: every failure — including argument validation — is
 * reported by completing the returned future exceptionally.
 */
public interface HttpChallengeTransport {

    /**
     * Send a bodiless {@code GET} to {@code uri} carrying {@code requestHeaders}, and report the
     * response's status code and headers.
     *
     * @param uri            the original request URI the exchange is re-issued against
     * @param requestHeaders the auth headers the plugin computed for this round
     * @param timeout        the remaining slice of the exchange's timeout budget for this single request
     * @return a future of the {@link Result}; completes exceptionally on transport failure
     */
    CompletableFuture<Result> get(URI uri, HttpAuthHeaders requestHeaders, Duration timeout);

    /**
     * One round's HTTP response, reduced to what the SASL state machine needs: the status code and the
     * response headers (the server's SASL challenge headers on a {@code 401}, or the role-token headers on
     * the terminal {@code 200}).
     *
     * @param statusCode      the HTTP status code
     * @param responseHeaders the response headers (never {@code null}; empty when the server sent none)
     */
    record Result(int statusCode, HttpAuthHeaders responseHeaders) {
        public Result {
            if (responseHeaders == null) {
                responseHeaders = HttpAuthHeaders.empty();
            }
        }
    }
}
