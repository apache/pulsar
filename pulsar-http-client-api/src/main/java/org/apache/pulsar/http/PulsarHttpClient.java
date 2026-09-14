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

import java.util.concurrent.CompletableFuture;

/**
 * A framework-managed HTTP client handed to authentication plugins (PIP-478).
 *
 * <p>Instances are obtained from
 * {@code AuthenticationInitContext.httpClientFactory()}; plugins MUST NOT construct private HTTP
 * clients directly, since doing so defeats the framework's shared event-loop / DNS /
 * TLS-material-refresh integration. The framework may hand out multiple instances per
 * {@code PulsarClient} (for example a different TLS configuration for an OAuth2 token endpoint than for
 * HTTP topic lookup) that nonetheless share the underlying resources.
 *
 * <p>The HTTP <em>backend</em> is framework-owned (built on AsyncHttpClient) and deliberately not
 * pluggable. Implementations must be thread-safe.
 */
public interface PulsarHttpClient extends AutoCloseable {

    /**
     * Execute an HTTP request asynchronously.
     *
     * <p>Never throws synchronously: all failures — including argument validation — are reported by
     * completing the returned future exceptionally, never by throwing on the calling thread.
     *
     * @param request the request to send
     * @return a future completing with the {@link HttpResponse}, or completing exceptionally on
     *         transport failure
     */
    CompletableFuture<HttpResponse> execute(HttpRequest request);

    /**
     * Release this instance. Idempotent. Instances are framework-owned: a plugin MAY close an instance
     * it no longer needs, and the framework closes every remaining instance when the owning
     * {@code PulsarClient} closes — so calling this is optional for plugins.
     */
    @Override
    void close();
}
