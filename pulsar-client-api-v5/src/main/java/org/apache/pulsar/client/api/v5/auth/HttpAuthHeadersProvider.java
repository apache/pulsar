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

import java.util.concurrent.CompletableFuture;

/**
 * Capability: single-pass credential exchange for the Pulsar HTTP transport (PIP-478).
 */
public interface HttpAuthHeadersProvider {

    /**
     * Produce the authentication headers for an outgoing HTTP request (for example
     * {@code Authorization: Bearer <jwt>}, or a custom header such as {@code Athenz-Role-Auth}).
     *
     * <p>The returned {@link HttpAuthHeaders} are attached to the request; most implementations produce
     * the same credential for every call. Completes exceptionally on failure (see the error model in
     * {@link org.apache.pulsar.client.api.v5.PulsarClientException}). Never throws synchronously: all
     * failures are reported by completing the returned future exceptionally, never by throwing on the
     * calling thread.
     *
     * @param ctx the per-call context
     * @return a future of the HTTP authentication headers
     */
    CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx);
}
