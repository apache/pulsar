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
 * Capability: SASL-style multi-round challenge/response over HTTP (PIP-478).
 *
 * <p>Handles the existing Pulsar SASL-over-HTTP mechanism, where the server returns HTTP {@code 401}
 * carrying the challenge in Pulsar's custom SASL headers (for example {@code SASL-Token} /
 * {@code State} / {@code SASL-Server-ID}) rather than the standard {@code WWW-Authenticate} header.
 *
 * <p>Standard {@code WWW-Authenticate}-based schemes (for example HTTP digest) are expected to be
 * served by a separate sibling interface; the framework's HTTP auth driver dispatches to whichever a
 * plugin implements.
 */
public interface HttpAuthChallengeHandler {

    /**
     * Compute the headers for the next round.
     *
     * <p>The server's challenge headers from the prior {@code 401} are available via
     * {@link HttpAuthCallContext#serverChallengeHeaders()}; the returned {@link HttpAuthHeaders} are
     * attached to the resubmitted request. Cross-round conversation state is kept in the context's
     * state slot. Never throws synchronously: all failures are reported by completing the returned
     * future exceptionally, never by throwing on the calling thread.
     *
     * @param ctx the per-call context (holds the server challenge and cross-round state)
     * @return a future of the headers for the resubmitted request
     */
    CompletableFuture<HttpAuthHeaders> respondToHttpChallengeAsync(HttpAuthCallContext ctx);
}
